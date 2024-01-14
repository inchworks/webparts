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
type OpId int64

// RM is the interface for a resource manager, which implements operations.
// ## The id parameter is clumsy, because the RM will need to embed it in the op before calling a worker,
// ## so that the worker can choose to end the transaction :-(.
type RM interface {
	Name() string                         // manager name, for the redo log
	ForOperation(opType int) Op           // return operation struct to receive unmarshalled data
	Operation(id OpId, opType int, op Op) // operation for execution
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
	Manager   string // resource manager name
	RedoType  int    // transaction manager's type
	Delay     int    // timed delay, in seconds
	OpType    int    // operation type
	Operation []byte // operation arguments, in JSON
}

// RedoStore is the interface for storage of V2 extended transactions, implemented by the parent application.
// The store must implement database transactions so that tx.Redo2 records are stored either:
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
	etx0    map[TxId][]*nextOp // immediate
	etx1    map[TxId][]*nextOp // delayed
	lastId TxId
	maxV1  OpId // the last operation found in the V1 store
}

// next caches the next operation for a transaction
type nextOp struct {
	id     OpId
	rm     RM // set nil when operation is ended or forgotten
	due    time.Time
	tmType int
	opType int
	op     Op
}

// New initialises the transaction manager.
func New(app App, store RedoStore) *TM {

	tm := &TM{
		app:   app,
		store: store,
		mu:    sync.Mutex{},
		etx0:   make(map[TxId][]*nextOp, 8),
		etx1:   make(map[TxId][]*nextOp, 8),
	}

	// start background worker
	tm.tick = time.NewTicker(time.Minute)
	go tm.worker(tm.tick.C, tm.chDone)

	return tm
}

// Begin returns the ID for a new extended transaction.
func (tm *TM) Begin() TxId {

	id := idTx(time.Now().UnixNano())

	// SERIALIZED
	tm.mu.Lock()

	// IDs have a microsecond resolution. Make sure they are unique.
	if id == tm.lastId {
		id = id + 1<<10
	}
	tm.lastId = id
	tm.mu.Unlock()

	return id
}

// AddNext adds an operation to the extended transaction, to be executed after the previous one.
// Database changes may have been requested, but must not be commmitted yet.
// The returned operation ID is needed only if TM.Forget is to be called.
func (tm *TM) AddNext(tx TxId, rm RM, opType int, op Op) (OpId, error) {

	return tm.addOp(tx, rm, redoNext, 0, opType, op)
}

// AddTimed adds an operation to the extended transaction, to be executed after the specified delay.
// The delay time has a resolution of one minute, and a maximum of 90 days.
func (tm *TM) AddTimed(tx TxId, rm RM, opType int, op Op, after time.Duration) (OpId, error) {

	// delay in seconds
	delay := int(math.Round(after.Seconds()))
	if delay == 0 {
		delay = 1
	}
	return tm.addOp(tx, rm, redoTimed, delay, opType, op)
}

// Do executes the operations specified by AddNext().
// It must be called after database changes have been committed.
// ## This should be the etx ID? Enforce that?
func (tm *TM) Do(tx TxId) error {

	return tm.doNext(tx, 0) // first op
}

// End terminates and forgets the operation, and executes the next one.
func (tm *TM) End(opId OpId) error {

	// V1 end is simple - just forget the operation because V1 transactions are single operations
	if opId <= tm.maxV1 {
		return tm.storeV1.DeleteId(int64(opId))
	}

	// forget this operation
	err := tm.store.DeleteId(int64(opId))
	if err != nil {
		return err
	}

	txId := Transaction(opId)
	if tm.etx0[txId] == nil {
		// execute the next immediate operation
		err = tm.doNext(txId, opId)
	} else {
		// if there are no immediate operations, we might have another timed operation
		err = tm.doTimed(txId)
	}
	return err
}

// Forget discards all operations of a specified type in a transaction as not needed.
func (tm *TM) Forget(tx TxId, rm RM, opType int) error {

	if err := tm.forget(tx, rm, opType, tm.etx0); err != nil {
		return err
	}
	return tm.forget(tx, rm, opType, tm.etx1)
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
	ts := tm.store.All()
	for _, t := range ts {

		// SERIALISED ## unnecessary?
		tm.mu.Lock()

		// RM and operation
		rm := rms[t.Manager]
		if rm == nil {
			return errors.New("Missing resource manager")
		}
		op := rm.ForOperation(t.OpType)
		if err := json.Unmarshal(t.Operation, op); err != nil {
			tm.mu.Unlock()
			tm.app.Log(err)
			continue
		}
		nxt := &nextOp{
			id:     OpId(t.Id),
			rm:     rm,
			due:    Timestamp(TxId(t.Id)).Add(time.Minute * time.Duration(t.Delay)),
			tmType: t.RedoType,
			opType: t.OpType,
			op:     op,
		}
		tx, _ := txOp(OpId(t.Id))

		tm.saveOp(tx, nxt)
		tm.mu.Unlock()
	}

	// redo first immediate operation of each extended transaction
	for _, etx := range tm.etx0 {
		if err := tm.doNext(Transaction(etx[0].id), 0); err != nil {
			tm.app.Log(err)
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

// Transaction returns the extended transaction ID for an operation.
func Transaction(opId OpId) TxId {
	tx, _ := txOp(opId)
	return tx
}

// addOp adds a next or timed operation to an extended transaction
func (tm *TM) addOp(tx TxId, rm RM, tmType int, delay int, opType int, op Op) (OpId, error) {

	nxt := &nextOp{
		rm:     rm,
		due:    Timestamp(tx).Add(time.Minute * time.Duration(delay)),
		tmType: tmType,
		opType: opType,
		op:     op,
	}

	// SERIALISED
	tm.mu.Lock()

	nOps := tm.saveOp(tx, nxt)

	newOpId := idOp(tx, nOps-1)
	nxt.id = newOpId

	tm.mu.Unlock()

	// .. and save for recovery
	r := Redo{
		Id:      int64(newOpId),
		Manager: rm.Name(),
		OpType:  opType,
	}
	var err error
	r.Operation, err = json.Marshal(op)
	if err != nil {
		return 0, err
	}

	return newOpId, tm.store.Insert(&r)
}

// doNext executes the next immediate operation of the specified transaction.
func (tm *TM) doNext(tx TxId, afterOp OpId) error {

	// SERIALIZED
	tm.mu.Lock()

	// transaction
	etx := tm.etx0[tx]
	if etx == nil {
		tm.mu.Unlock()
		return errors.New("etx: Invalid operation ID")
	}

	// find the next operation, if there is one
	// Note that on recovery the first operation needn't be #0, so we search.
	var op *nextOp
	var rm RM
	for _, nxt := range etx {
		if nxt.id > afterOp && nxt.rm != nil {

			// disable in cache (quicker than trimming the slice)
			rm = nxt.rm //
			nxt.rm = nil

			op = nxt
			break
		}
	}

	// delete transaction if there are no more ops
	if op == nil {
		delete(tm.etx0, tx)
		tm.mu.Unlock()
		return nil
	}

	// operation
	tm.mu.Unlock()
	rm.Operation(op.id, op.opType, op.op)
	return nil
}

// doTimed executes the next ready timed operation of the specified transaction.
func (tm *TM) doTimed(tx TxId) error {

	now := time.Now()

	// SERIALIZED
	tm.mu.Lock()

	// transaction
	etx := tm.etx1[tx]
	if etx == nil {
		tm.mu.Unlock()
		return errors.New("etx: Invalid operation ID")
	}

	// find the next operation, if there is one
	// Note that on recovery the first operation needn't be #0, so we search.
	var op *nextOp
	var rm RM
	// ## inefficient because not ordered by due time, and may be lots
	for _, nxt := range etx {
		if nxt.rm != nil && nxt.due.Before(now)  {

			// disable in cache (quicker than trimming the slice)
			rm = nxt.rm //
			nxt.rm = nil

			op = nxt
			break
		}
	}

	// delete transaction if there are no more ops
	if op == nil {
		delete(tm.etx1, tx)
		tm.mu.Unlock()
		return nil
	}

	// operation
	tm.mu.Unlock()
	rm.Operation(op.id, op.opType, op.op)
	return nil
}

// forget discards operations of a specified type, either immediate or timed.
func (tm *TM) forget(tx TxId, rm RM, opType int, etxMap map[TxId][]*nextOp) error {

	for _, opn := range etxMap[tx] {
		if opn.rm == rm && opn.op == opType {
			// disable in cache (quicker than trimming the slice)
			opn.rm = nil

			// forget this operation
			if err := tm.store.DeleteId(int64(opn.id)); err != nil {
				return err
			}
		}
	}
	return nil
}

// idOp constructs an operation ID from extended transaction ID and operation number
func idOp(tx TxId, nOp int) OpId {
	// ## no more than 1023 ops
	return OpId(int64(tx) + int64(nOp))
}

// idTx constructs an extended transaction ID from a time in nS.
func idTx(t int64) TxId {
	return TxId(t - t&(1<<10-1)) // remove low 9 bits
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

// saveOp adds an operation to the cached list. It returns the number of operations.
func (tm *TM) saveOp(tx TxId, nxt *nextOp) int {

	// choose map for immediate or timed operations
	var etxMap map[TxId][]*nextOp
	switch nxt.tmType {
	case redoNext:
		etxMap = tm.etx0
	case redoTimed:
		etxMap = tm.etx1
	default:
		tm.app.Log(errors.New("etx: Unknown RedoType"))
		return 0
	}

	if etxMap[tx] == nil {
		// first operation for the transaction
		etxMap[tx] = make([]*nextOp, 1, 4)
		etxMap[tx][0] = nxt
	} else {
		// add an operation
		etxMap[tx] = append(etxMap[tx], nxt)
	}
	return len(etxMap)
}

// txop returns the extended transaction ID and operation number
func txOp(opId OpId) (tx TxId, nOp int) {
	t := int64(opId)
	n := t & (1<<10 - 1)
	tx = TxId(t - n)
	nOp = int(n)
	return
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
			for _, etx := range tm.etx1 {

				// only when there are no immediate operations for this transaction
				if tm.etx0[Transaction(etx[0].id)] == nil {
					if err := tm.doTimed(Transaction(etx[0].id)); err != nil {
						tm.app.Log(err)
					}
				}
			}

		case <-chDone:
			return
		}
	}
}
