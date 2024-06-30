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
	etxs   map[TxId]*etxOps
	lastId int64
	maxV1  TxId // the last operation found in the V1 store
}

// ## This implementation is not efficient for transactions with hundreds or more of operations.

// etxOps holds the state and caches the operations for a transaction
type etxOps struct {
	isTimed   bool // true when current or next operation is timed
	active    bool // operation not ended
	sorted    bool // timed operations sorted in due order
	immediate []*etxOp
	timed     []*etxOp
}

// etxOp caches an operation for a transaction
type etxOp struct {
	id     opId
	rm     RM // set nil when operation is ended or forgotten
	due    time.Time
	opType int
	op     Op // RM operation
}

// New initialises the transaction manager.
func New(app App, store RedoStore) *TM {

	tm := &TM{
		app:   app,
		store: store,
		mu:    sync.Mutex{},
		etxs:  make(map[TxId]*etxOps, 8),
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

	for tm.doNext(tx, false) {
	} // until no more synchronous ops

	return nil
}

// End terminates and forgets an operation.
// It must be called by each RM.Operation, synchronously or asynchronously, in the context of a RedoStore transaction.
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
	etx.active = false // operation ended

	// current operation
	var id opId
	var err error
	if etx.isTimed {
		id, err = end(&etx.timed)
	} else {
		id, err = end(&etx.immediate)
	}
	tm.mu.Unlock()

	// delete redo record
	if err == nil {
		err = tm.store.DeleteId(int64(id))
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

	// SERIALISED
	tm.mu.Lock()

	// index resource managers by name
	rms := make(map[string]RM, 2)
	for _, rm := range mgrs {
		rms[rm.Name()] = rm
	}

	// recover from transaction log
	rs := tm.store.All()
	for _, r := range rs {

		// RM and operation
		rm := rms[r.Manager]
		if rm == nil {
			tm.mu.Unlock()
			return errors.New("Missing resource manager")
		}
		rmOp := rm.ForOperation(r.OpType)
		if err := json.Unmarshal(r.Operation, rmOp); err != nil {
			tm.mu.Unlock()
			tm.app.Log(err)
			continue
		}
		tx := TxId(r.Tx)
		tmOp := &etxOp{
			id:     opId(r.Id),
			rm:     rm,
			due:    Timestamp(tx).Add(time.Second * time.Duration(r.Delay)),
			opType: r.OpType,
			op:     rmOp,
		}

		tm.saveOp(tx, r.RedoType, tmOp)
	}
	tm.mu.Unlock()

	// redo first immediate operation of each extended transaction
	for tx := range tm.etxs {
		for tm.doNext(tx, false) {
		} // until no more synchronous ops
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
func (tm *TM) addOp(tx TxId, rm RM, tmType int, delay int, opType int, rmOp Op) error {

	tmOp := &etxOp{
		rm:     rm,
		due:    Timestamp(tx).Add(time.Second * time.Duration(delay)),
		opType: opType,
		op:     rmOp,
	}

	// SERIALISED
	tm.mu.Lock()

	tm.saveOp(tx, tmType, tmOp)

	id := tm.newId()
	tmOp.id = opId(id)

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
	r.Operation, err = json.Marshal(rmOp)
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
	var ops []*etxOp
	if timed {
		if !etx.sorted {
			// stable sort is faster because items likely to be in order
			sort.SliceStable(etx.timed, func(p, q int) bool {
				return etx.timed[p].due.Before(etx.timed[q].due)
			})
			etx.sorted = true
		}
		ops = etx.timed
	} else {
		ops = etx.immediate
	}

	if len(ops) > 0 {
		tmOp := ops[0]

		// check operation time
		if timed && tmOp.due.After(time.Now()) {
			return false // next op is not due yet
		}

		// next operation
		etx.isTimed = timed
		etx.active = true

		// do operation
		tm.mu.Unlock()
		tmOp.rm.Operation(tx, tmOp.opType, tmOp.op)
		tm.mu.Lock()

		if etx.active {
			return false // asynchronous operation
		}
	}

	// delete transaction if there are no more ops
	if len(etx.immediate)+len(etx.timed) == 0 {
		delete(tm.etxs, tx)
		return false
	}

	// synchronous and more ops?
	if timed {
		return len(etx.timed) > 0
	} else {
		return len(etx.immediate) > 0
	}
}

// end forgets an operation (immediate or timed), and returns its redo record ID.
func end(ops *[]*etxOp) (opId, error) {

	if len(*ops) == 0 {
		return 0, errors.New("etx: Operation already ended")
	}
	opId := (*ops)[0].id

	// forget this operation
	*ops = (*ops)[1:]

	return opId, nil
}

// forget discards operations of a specified type, either immediate or timed.
func (tm *TM) forget(ops []*etxOp, rm RM, opType int) []*etxOp {

	toDel := make([]opId, 0, 4)

	// SERIALIZED
	tm.mu.Lock()

	// we can't compare interfaces
	name := rm.Name()

	// scan operations from the end
	for i := len(ops) - 1; i >= 0; i-- {
		tmOp := ops[i]
		if tmOp.rm.Name() == name && tmOp.opType == opType {
			if i == len(ops)-1 {
				// trim the final element (a common case)
				ops = ops[:i]
			} else {
				// shuffle to remove operation
				ops = append(ops[:i], ops[i+1:]...)
			}

			// delete after unlocking
			toDel = append(toDel, tmOp.id)
		}
	}

	tm.mu.Unlock()
	for _, id := range toDel {
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
func (tm *TM) saveOp(tx TxId, tmType int, tmOp *etxOp) {

	etx := tm.etxs[tx]
	if etx == nil {
		// new transaction
		etx = &etxOps{
			immediate: make([]*etxOp, 0, 4),
			timed:     make([]*etxOp, 0, 4),
		}
		tm.etxs[tx] = etx
	}

	// choose map for immediate or timed operations
	var ops *[]*etxOp
	switch tmType {
	case redoNext:
		ops = &etx.immediate
	case redoTimed:
		ops = &etx.timed
		etx.sorted = false
	default:
		tm.app.Log(errors.New("etx: Unknown RedoType"))
		return
	}

	*ops = append(*ops, tmOp)
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
				for tm.doNext(tx, true) {
				} // until no more synchronous ops
			}

		case <-chDone:
			return
		}
	}
}
