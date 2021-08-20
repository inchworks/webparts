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
	"strconv"
	"sync"
	"time"
)

// App is the interface to functions provided by the parent application.
type App interface {
	// Log optionally records an error
	Log(error) // ## not used
}

// Extended transaction identifier
type TxId int64

// RM is the interface for a resource manager, which implements operations.
// ## The id parameter is clumsy, because the RM will need to embed it in the op before calling a worker,
// ## so that the worker can choose to end the transaction :-(.
type RM interface {
	Name() string                         // manager name, for the redo log
	ForOperation(opType int) Op           // return operation struct to receive unmarshalled data
	Operation(id TxId, opType int, op Op) // operation for execution
}

// Op is the interface to an RM operation.
// Operations must either be database transactions or idempotent.
type Op interface {
}

// Transaction struct holds the stored data for a transaction.
type Redo struct {
	Id        int64  // transaction ID
	Manager   string // resource manager name
	OpType    int    // operation type
	Operation []byte // operation arguments, in JSON
}

// RedoStore is the interface for storage of extended transactions, implemented by the parent application.
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
// It has no state of its own.
type TM struct {
	app   App
	store RedoStore

	// state
	mu     sync.Mutex
	next   map[TxId][]*nextOp
	lastId TxId
}

// next caches the next operation for a transaction
type nextOp struct {
	id     TxId
	rm     RM
	opType int
	op     Op
}

// New initialises the transaction manager and recovers all logged operations.
// ## Separate func to add RMs?
func New(app App, store RedoStore) *TM {

	return &TM{
		app:   app,
		store: store,
		mu:    sync.Mutex{},
		next:  make(map[TxId][]*nextOp, 8),
	}
}

// Begin returns the transaction ID for a new extended transaction.
func (tm *TM) Begin() TxId {

	// SERIALIZED
	tm.mu.Lock()

	id := TxId(time.Now().UnixNano())

	// no idea if two calls could return the same time, but just in case we'll increment it
	if id == tm.lastId {
		id = id + 1
	}
	tm.lastId = id
	tm.mu.Unlock()

	return id
}

// BeginNext starts another extended transaction, with an operation executed after the first one.
// It's just a convenience to avoid multiple DoNext calls when a set of extended transactions are started at the same time.
func (tm *TM) BeginNext(first TxId, rm RM, opType int, op Op) error {
	return tm.setNext(first, tm.Begin(), rm, opType, op)
}

// End terminates and forgets the transaction.
func (tm *TM) End(id TxId) error {

	return tm.store.DeleteId(int64(id))
}

// Id returns a transaction identifier from its string reresentation.
func Id(s string) (TxId, error) {
	id, err := strconv.ParseInt(s, 36, 64)
	return TxId(id), err
}

// Recover reads and processes the redo log, to complete interrupted transactions after a server restart.
func (tm *TM) Recover(mgrs ...RM) error {

	// index resource managers by name
	rms := make(map[string]RM, 2)
	for _, rm := range mgrs {
		rms[rm.Name()] = rm
	}

	// recover using transaction log
	ts := tm.store.All()
	for _, t := range ts {
		// RM and operation
		rm := rms[t.Manager]
		if rm == nil {
			return errors.New("Missing resource manager")
		}
		op := rm.ForOperation(t.OpType)
		if err := json.Unmarshal(t.Operation, op); err != nil {
			return err
		}

		// redo operation
		rm.Operation(TxId(t.Id), t.OpType, op)
	}

	return nil
}

// SetNext sets or updates the next operation for an extended transaction.
// Database changes may have been requested, but must not be commmitted yet.
func (tm *TM) SetNext(id TxId, rm RM, opType int, op Op) error {
	return tm.setNext(id, id, rm, opType, op)
}

// DoNext executes the operation specified in SetNext.
// It must be called after database changes have been committed.
func (tm *TM) DoNext(id TxId) {

	// SERIALIZED
	tm.mu.Lock()

	// operations from SetNext and AlsoNext
	ops := tm.next[id]
	delete(tm.next, id)
	tm.mu.Unlock()

	if ops != nil {
		for _, op := range ops {
			op.rm.Operation(op.id, op.opType, op.op)
		}
	}
}

// String formats a transaction ID.
func String(id TxId) string {
	return strconv.FormatInt(int64(id), 36)
}

// 	Timestamp returns the start time of an extended transaction.
func Timestamp(id TxId) time.Time {
	return time.Unix(0, int64(id)) // transaction ID is also a timestamp
}

// Timeout executes any old operations for a resource manager.
func (tm *TM) Timeout(rm RM, before time.Time) error {

	// recover using transaction log
	ts := tm.store.ForManager(rm.Name(), time.Now().UnixNano())
	for _, t := range ts {
		// operation
		op := rm.ForOperation(t.OpType)
		if err := json.Unmarshal(t.Operation, op); err != nil {
			return err
		}

		// do operation
		rm.Operation(TxId(t.Id), t.OpType, op)
	}
	return nil
}

// setNext saves the logged redo entry for an operation, and adds it to the list for DoNext.
func (tm *TM) setNext(head TxId, id TxId, rm RM, opType int, op Op) error {

	// get redo log entry, or add new one
	var add bool
	r, err := tm.store.GetIf(int64(id))
	if err != nil {
		return err
	}
	if r == nil {
		add = true
		r = &Redo{Id: int64(id)}
	}

	// set the next operation
	r.Manager = rm.Name()
	r.OpType = opType
	r.Operation, err = json.Marshal(op)
	if err != nil {
		return err
	}
	nxt := &nextOp{
		id:     id,
		rm:     rm,
		opType: opType,
		op:     op,
	}

	// SERIALISED
	tm.mu.Lock()

	if tm.next[head] == nil {
		// save the first operation, for execution ..
		tm.next[head] = make([]*nextOp, 1, 4)
		tm.next[head][0] = nxt
	} else if id == head {
		// update the operation
		tm.next[head][0] = nxt
	} else {
		// add an operation
		tm.next[head] = append(tm.next[head], nxt)
	}

	tm.mu.Unlock()

	// .. and for redo
	if add {
		err = tm.store.Insert(r)
	} else {
		err = tm.store.Update(r)
	}
	return err
}
