// Copyright © Rob Burke inchworks.com, 2021.

// Package etx uses logging to make extended transactions from a sequence of operations.
//
// For example it allows a web server request to be split between an immediate operation and asynchronous completion,
// with a guarantee that the second part will be completed even if the server is restarted between the two parts.
// It also allows responsibility for continuing the extended transaction to be passed between resource managers.
package etx

import (
	"encoding/json"
	"errors"
	"math"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"time"
	"unicode/utf8"
)

// App is the interface to functions provided by the parent application.
type App interface {
	// Log optionally records an error
	Log(error)
}

// Extended transaction identifier
type TxId int64

// Operation identifier
type opId int64

// RM is the interface for a resource manager, which implements operations.
type RM interface {
	Name() string                         // manager name, for the redo log
	ForOperation(opType int) Op           // return operation struct to receive unmarshalled data
	Operation(tx TxId, opType int, op Op) // operation for execution, to be executed in current goroutine
}

// Op is the interface to an RM operation.
// Operations must either be database transactions or idempotent.
type Op interface {
}

const (
	redoNext  = 1
	redoTimed = 2
)

// Redo struct holds the stored data for a V2 transaction operation.
type Redo struct {
	Id        int64  // operation ID
	Tx        int64  // transaction ID
	Manager   string // resource manager name
	RedoType  int    // transaction manager's type
	Delay     int    // timed delay, in seconds
	OpType    int    // operation type
	Operation []byte // operation arguments, in JSON
}

// RedoStore is the interface for storage of V2 extended transactions, implemented by the parent application.
// The store must implement database transactions so that tx.Redo records are stored either:
// (1) with the associated RM database transaction, or (2) before the associated RM idempotent operation.
type RedoStore interface {
	All() []*Redo                               // all redo log entries in ID order
	DeleteId(id int64) error                    // delete redo
	ForManager(rm string, before int64) []*Redo // aged log entries for RM
	GetIf(id int64) (*Redo, error)              // get entry if it still exists
	Insert(t *Redo) error                       // add entry
	Update(t *Redo) error                       // update entry
}

// TM holds transaction manager state, and dependencies of this package on the parent application.
// It has no non-volatile state of its own.
type TM struct {
	app     App
	store   RedoStore
	storeV1 RedoStoreV1

	// background worker
	tick   *time.Ticker
	chDone chan bool

	// state
	mu     sync.Mutex
	// etx1   map[TxId][]*nextOp // delayed
	etxs   map[TxId]*etxOps
	lastId int64
	maxV1  TxId // the last operation found in the V1 store
}

// etxOps holds the state and caches the operations for a transaction
type etxOps struct {
	isTimed   bool // true when current or next operation is timed
	active    bool  // operation not ended
	sorted    bool  // timed operations sorted in due order
	current   int  // operation, -1 if not started
	immediate []*nextOp
	timed     []*nextOp
}

// nextOp caches the next operation for a transaction
type nextOp struct {
	id     opId
	rm     RM // set nil when operation is ended or forgotten
	due    time.Time
	tmType int
	opType int
	op     Op
}

// New initialises the transaction manager.
func New(app App, store RedoStore) *TM {

	tm := &TM{
		app:    app,
		store:  store,
		mu:     sync.Mutex{},
		etxs:   make(map[TxId]*etxOps, 8),
	}

	// start background worker
	tm.tick = time.NewTicker(time.Minute)
	go tm.worker(tm.tick.C, tm.chDone)

	return tm
}

// Begin returns the ID for a new extended transaction.
func (tm *TM) Begin() TxId {

	// SERIALIZED
	tm.mu.Lock()

	id := tm.newId()

	tm.mu.Unlock()

	return TxId(id)
}

// AddNext adds an operation to the extended transaction, to be executed after the previous one.
// Database changes may have been requested, but must not be commmitted yet.
func (tm *TM) AddNext(tx TxId, rm RM, opType int, op Op) error {

	return tm.addOp(tx, rm, redoNext, 0, opType, op)
}

// AddTimed adds an operation to the extended transaction, to be executed after the specified delay.
// The delay time has a resolution of one minute, and a maximum of 90 days.
func (tm *TM) AddTimed(tx TxId, rm RM, opType int, op Op, after time.Duration) error {

	// delay in seconds
	delay := int(math.Round(after.Seconds()))
	if delay == 0 {
		delay = 1
	}
	return tm.addOp(tx, rm, redoTimed, delay, opType, op)
}

// Do executes the operations specified by AddNext().
// It is called to start the first operation and after the completion of each asyncronous operation.
// It must be called after database changes have been committed.
func (tm *TM) Do(tx TxId) error {

	for {
		if tm.doNext(tx, false) {
			return nil // no more synchronous ops
		}
	}
}

// End terminates and forgets the current operation.
func (tm *TM) End(txId TxId) error {

	// V1 end is simple - just forget the operation because V1 transactions are single operations
	if txId <= tm.maxV1 {
		return tm.storeV1.DeleteId(int64(txId))
	}

	// SERIALIZED
	tm.mu.Lock()

	// transaction
	etx, exists := tm.etxs[txId]
	if !exists {
		return errors.New("etx: Invalid transaction ID")
	}
	if etx.current < 0 {
		return errors.New("etx: No current operation")
	}
	etx.active = false // operation ended

	// current operation
	var ops []*nextOp
	if etx.isTimed {
		ops = etx.timed
	} else {
		ops = etx.immediate
	}
	if etx.current >= len(ops) {
		return errors.New("etx: Operation already ended")
	}
	op := ops[etx.current]
	opId := op.id

	// forget this operation
	op.rm = nil
	tm.mu.Unlock()
	err := tm.store.DeleteId(int64(opId))
	if err != nil {
		return err
	}

	return err
}

// Forget discards all operations of a specified type in a transaction as not needed.
func (tm *TM) Forget(tx TxId, rm RM, opType int) error {

	// transaction
	etx, exists := tm.etxs[tx]
	if !exists {
		return errors.New("etx: Invalid transaction ID")
	}
	
	// remove from immediate and timed operations
	etx.immediate = tm.forget(etx.immediate, rm, opType)
	etx.timed = tm.forget(etx.timed, rm, opType)

	return nil
}

// Id returns a transaction identifier from its string representation.
func Id(s string) (TxId, error) {
	id, err := strconv.ParseInt(reverse(s), 36, 64)
	return TxId(id), err
}

// Recover reads and processes the redo log, to complete interrupted transactions after a server restart.
func (tm *TM) Recover(mgrs ...RM) error {

	// index resource managers by name
	rms := make(map[string]RM, 2)
	for _, rm := range mgrs {
		rms[rm.Name()] = rm
	}

	// recover from transaction log
	rs := tm.store.All()
	for _, r := range rs {

		// SERIALISED ## unnecessary?
		tm.mu.Lock()

		// RM and operation
		rm := rms[r.Manager]
		if rm == nil {
			return errors.New("Missing resource manager")
		}
		op := rm.ForOperation(r.OpType)
		if err := json.Unmarshal(r.Operation, op); err != nil {
			tm.mu.Unlock()
			tm.app.Log(err)
			continue
		}
		tx := TxId(r.Tx)
		nxt := &nextOp{
			id:     opId(r.Id),
			rm:     rm,
			due:    Timestamp(tx).Add(time.Second * time.Duration(r.Delay)),
			tmType: r.RedoType,
			opType: r.OpType,
			op:     op,
		}

		tm.saveOp(tx, nxt)
		tm.mu.Unlock()
	}

	// redo first immediate operation of each extended transaction
	for tx := range tm.etxs {
		for {
			if tm.doNext(tx, false) {
				break // no more synchronous ops
			}
		}	
	}

	return nil
}

// String returns a formatted a transaction number.
// The string is reversed so that file names contructed from ID do not look similar.
func String(tx TxId) string {
	return reverse(strconv.FormatInt(int64(tx), 36))
}

// Timestamp returns the start time of an extended transaction.
func Timestamp(tx TxId) time.Time {
	nTx := int64(tx)
	return time.Unix(0, nTx) // transaction ID is also a timestamp
}

// addOp adds a next or timed operation to an extended transaction.
// delay is specified in seconds.
func (tm *TM) addOp(tx TxId, rm RM, tmType int, delay int, opType int, op Op) error {

	nxt := &nextOp{
		rm:     rm,
		due:    Timestamp(tx).Add(time.Second * time.Duration(delay)),
		tmType: tmType,
		opType: opType,
		op:     op,
	}

	// SERIALISED
	tm.mu.Lock()

	tm.saveOp(tx, nxt)

	id := tm.newId()
	nxt.id = opId(id)

	tm.mu.Unlock()

	// .. and save for recovery
	r := Redo{
		Id:       id,
		Tx:       int64(tx),
		Manager:  rm.Name(),
		RedoType: tmType,
		Delay:    delay,
		OpType:   opType,
	}
	var err error
	r.Operation, err = json.Marshal(op)
	if err != nil {
		return err
	}

	return tm.store.Insert(&r)
}

// doNext executes the next immediate operation of the specified transaction.
// It returns true if the operation was ended synchronously and there is another one.
func (tm *TM) doNext(tx TxId, timed bool) bool {

	// SERIALIZED
	tm.mu.Lock()
	defer tm.mu.Unlock()

	// transaction
	etx, exists := tm.etxs[tx]
	if !exists {
		return false
	}

	// select type of operations
	var ops []*nextOp
	if timed {
		if len(etx.immediate) > 0 {
			// new immediate operation(s) have been added, give them priority
			etx.current = -1
			timed = false
			ops = etx.immediate
		} else {
			if !etx.sorted {
				// stable sort is faster because items likely to be in order
				sort.SliceStable(etx.timed, func(p, q int) bool {
					return etx.timed[p].due.Before(etx.timed[q].due)
				})
				etx.sorted = true
				etx.current = -1
			}
			ops = etx.timed
		}
	} else {
		ops = etx.immediate
	}

	next :=  etx.current + 1
	if next < len(ops) {
		op := ops[next]
		
		// check operation time
		if timed && op.due.After(time.Now()) {
			return false // next op is not due yet
		}

		// next operation
		etx.current = next
		etx.isTimed = timed

		// skip forgotten ops
		if op.rm != nil {
			etx.active = true

			// do operation
			tm.mu.Unlock()
			op.rm.Operation(tx, op.opType, op.op)
			tm.mu.Lock()

			if etx.active {
				return false // asynchronous operation
			}
		}
		next = next + 1
	}

	// delete list if there are no more ops
	if next >= len(ops)  {
		ops = nil
	}
	if len(etx.immediate) + len(etx.timed) == 0 {
		delete(tm.etxs, tx)
		return false
	}

	return true
}

// forget discards operations of a specified type, either immediate or timed.
func (tm *TM) forget(ops []*nextOp, rm RM, opType int) []*nextOp {

	toDel := make([]opId, 0, 4)

	// SERIALIZED
	tm.mu.Lock()

	// we can't compare interfaces
	name := rm.Name()

	// scan slide from the end
	for i := len(ops)-1; i >= 0; i-- {
		opn := ops[i]
		if opn.rm.Name() == name && opn.opType == opType {
			if i == len(ops)-1 {
				// trim the final element (a common case)
				ops = ops[:len(ops)-1]
			} else {
				// disable in cache (quicker than shuffing the slice)
				opn.rm = nil
			}

			// delete after unlocking
			toDel = append(toDel, opn.id) 
		}
	}

	tm.mu.Unlock()
	for id := range toDel {
		// forget this operation
		if err := tm.store.DeleteId(int64(id)); err != nil {
			tm.app.Log(err)
		}
	}

	return ops
}

// newId returns a new operation or transaction ID
func (tm *TM) newId() int64 {

	id := time.Now().UnixNano()

	// IDs have a nanosecond resolution, but are they precise? Make sure they are unique.
	if id == tm.lastId {
		id = id + 1
	}
	tm.lastId = id
	return id
}

// reverse returns the characters of a string in reverse order.
// It is not suitable for strings that include combining characters.
func reverse(s string) string {

	// Copied from rmuller's solution in
	// https://stackoverflow.com/questions/1752414/how-to-reverse-a-string-in-go/34521190#34521190

	size := len(s)
	buf := make([]byte, size)
	for start := 0; start < size; {
		r, n := utf8.DecodeRuneInString(s[start:])
		start += n
		utf8.EncodeRune(buf[size-start:], r)
	}
	return string(buf)
}

// saveOp adds an operation to the cached list.
func (tm *TM) saveOp(tx TxId, nxt *nextOp) {

	etx := tm.etxs[tx]
	if etx == nil {
		// new transaction
		etx = &etxOps{
			current: -1,
			immediate: make([]*nextOp, 0, 4),
			timed: make([]*nextOp, 0, 4),
		}
	}

	// choose map for immediate or timed operations
	var ops *[]*nextOp
	switch nxt.tmType {
	case redoNext:
		ops = &etx.immediate
	case redoTimed:
		ops = &etx.immediate
		etx.sorted = false
	default:
		tm.app.Log(errors.New("etx: Unknown RedoType"))
		return
	}

	*ops = append(*ops, nxt)
}

// worker executes delayed operations.
func (tm *TM) worker(
	chTick <-chan time.Time,
	chDone <-chan bool) {

	for {
		// returns to client sooner?
		runtime.Gosched()

		select {
		case <-chTick:
			// find all operations that are due
			for tx := range tm.etxs {
				for {
					if tm.doNext(tx, true) {
						break // no more synchronous ops
					}
				}
			}

		case <-chDone:
			return
		}
	}
}
