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

// Extended transaction operation identifier
// (Misnamed now, to avoid an interface change.)
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

// Redo struct holds the stored data for a transaction operation.
type Redo struct {
	Id        int64  // operation ID
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
// It has no non-volatile state of its own.
type TM struct {
	app   App
	store RedoStore

	// state
	mu     sync.Mutex
	etx   map[int64][]*nextOp
	lastId TxId
}

// next caches the next operation for a transaction
type nextOp struct {
	id     TxId
	rm     RM
	opType int
	op     Op
}

// New initialises the transaction manager.
func New(app App, store RedoStore) *TM {

	return &TM{
		app:   app,
		store: store,
		mu:    sync.Mutex{},
		etx:  make(map[int64][]*nextOp, 8),
	}
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
// The returned operation ID is needed only if TM.Change is to be called.
func (tm *TM) AddNext(opId TxId, rm RM, opType int, op Op) (TxId, error) {

	nTx, _ := txOp(opId) // ## verify nOp?
	nxt := &nextOp{
		id:     opId,
		rm:     rm,
		opType: opType,
		op:     op,
	}
	
	// SERIALISED
	tm.mu.Lock()

	etx := tm.etx[nTx]
	if etx == nil {
		// save the first operation, for execution ..
		tm.etx[nTx] = make([]*nextOp, 1, 4)
		tm.etx[nTx][0] = nxt
	} else {
		// add an operation
		tm.etx[nTx] = append(tm.etx[nTx], nxt)
	}
	newOpId := idOp(nTx, len(etx)-1)
	
	tm.mu.Unlock()

	// .. and save for recovery
	r := Redo{
		Id: int64(newOpId),
		Manager: rm.Name(),
		OpType: opType,
	}
	var err error
	r.Operation, err = json.Marshal(op)
	if err != nil {
		return 0, err
	}

	return newOpId, tm.store.Insert(&r)
}

// Change modifies an operation before it has been executed.
// Typically this is used where an RM has added an operation to rollback preparation,
// and now knows that the extended transaction is to go ahead.
// #### But how can the RM know the operation ID?
func (tm *TM) Change(opId TxId, opType int, op Op) error {

	nTx, nOp := txOp(opId)
	
	// SERIALISED
	tm.mu.Lock()

	etx := tm.etx[nTx]
	if etx == nil || nOp >= len(etx) {
		tm.mu.Unlock()
		return errors.New("TM.Change: invalid ID")
	}
	txOp := etx[nOp]

	// change operation
	txOp.opType = opType
	txOp.op = op

	tm.mu.Unlock()

	// .. and save for recovery
	r := Redo{
		Id: int64(opId),
		Manager: txOp.rm.Name(),
		OpType: opType,
	}
	var err error
	r.Operation, err = json.Marshal(op)
	if err != nil {
		return err
	}

	return tm.store.Update(&r)
}

// Do executes the operations specified by AddNext().
// It must be called after database changes have been committed.
// ## This should be the etx ID? Enforce that?
func (tm *TM) Do(opId TxId) error {

	return tm.do(opId, 0) // first op
}

// End terminates and forgets the operation, and executes the next one.
func (tm *TM) End(opId TxId) error {

	// forget this operation
	if err := tm.store.DeleteId(int64(opId)); err != nil {
		return err
	}

	// execute the next one
	return tm.do(opId, opId)
}

// Id returns an operation identifier from its string representation.
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

	// #### link operations into transactions

	// recover from transaction log
	ts := tm.store.All()
	var lastTx int64
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
			return err
		}
	
		nxt := &nextOp{
			id:     TxId(t.Id),
			rm:     rm,
			opType: t.OpType,
			op:     op,
		}
		nTx, _ := txOp(TxId(t.Id))
	
		if nTx != lastTx {
			// first operation of transaction
			tm.etx[nTx] = make([]*nextOp, 1, 4)
			tm.etx[nTx][0] = nxt
			lastTx = nTx
		} else {
			// add an operation
			tm.etx[nTx] = append(tm.etx[nTx], nxt)
		}
		tm.mu.Unlock()
	}

	// redo first operation of each extended transaction
	// #### needn't be starting at the first. Could be some missing? End() must handle these cases.
	// ## ok that returns on any error
	for _, etx := range tm.etx {
		if err := tm.Do(etx[0].id); err != nil {
			return err
		}
	}
	
	return nil
}

// String formats an operation identifier.
func String(opId TxId) string {
	return strconv.FormatInt(int64(opId), 36)
}

// 	Timestamp returns the start time of an extended transaction.
func Timestamp(opId TxId) time.Time {
	nTx, _ := txOp(opId)
	return time.Unix(0, nTx) // transaction ID is also a timestamp
}

// Timeout executes any old operations for a resource manager.
// A non-zero opType selects the specified type.
// #### Need a more controlled execute/purge choice.
func (tm *TM) Timeout(rm RM, opType int, before time.Time) error {

	// recover using transaction log
	ts := tm.store.ForManager(rm.Name(), before.UnixNano())
	for _, t := range ts {
		if opType == 0 || t.OpType == opType {
			// operation
			op := rm.ForOperation(t.OpType)
			if err := json.Unmarshal(t.Operation, op); err != nil {
				return err
			}

			// do operation
			rm.Operation(TxId(t.Id), t.OpType, op)
		}
	}
	return nil
}

// Deprecated as a misleading name. Use the equivalent TM.AddNext instead.
func (tm *TM) BeginNext(first TxId, rm RM, opType int, op Op) error {
	_, err := tm.AddNext(first, rm, opType, op)
	return err
}

// Deprecated as a misleading name. Use the equivalent TM.Do instead.
func (tm *TM) DoNext(id TxId) {
	tm.Do(id)
}

// Deprecated as too general. Use TM.AddNext in most cases, and TM.Change to modify an existing operation.
func (tm *TM) SetNext(id TxId, rm RM, opType int, op Op) error {

		// adding or changing operation?
		r, err := tm.store.GetIf(int64(id))
		if err != nil {
			return err
		}
		if r == nil {
			_, err := tm.AddNext(id, rm, opType, op)
			return err
		} else {
			return tm.Change(id, opType, op)
		}		
}

// do executes the specifed operation.
// The transaction is specified by any operation of the transaction.
func (tm *TM) do(txId TxId, afterOp TxId) error {

	nTx, _ := txOp(txId)

	// SERIALIZED
	tm.mu.Lock()

	// transaction
	etx := tm.etx[nTx]
	if etx == nil {
		tm.mu.Unlock()
		return errors.New("etx: Invalid operation ID")
	}

	// find the next operation, if there is one
	// Note that on recovery the first operation needn't be #0, so we search.
	var op *nextOp
	for _, nxt := range etx {
		if nxt.id > afterOp {
			op = nxt
			break
		}
	}

	// delete transaction if there are no more ops
	if op == nil {
		delete(tm.etx, nTx)
		tm.mu.Unlock()
		return nil
	}

	// operation
	tm.mu.Unlock()
	op.rm.Operation(op.id, op.opType, op.op)
	return nil	
}

// idOp constructs an operation ID from extended transaction ID and operation number
func idOp(nTx int64, nOp int) TxId {
	// ## no more than 1023 ops
	return TxId(nTx + int64(nOp))
}

// idTx constructs an extended transaction ID from a time in nS.
func idTx(t int64) TxId {
	return TxId(t - t & 1<<10-1)
}

// txop returns the extended transaction ID and operation number
func txOp(opId TxId) (tx int64, nOp int) {
	t := int64(opId)
	n := t & 1<<10-1
	tx = t - n
	nOp = int(n)
	return
}
