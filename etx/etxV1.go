// Copyright © Rob Burke inchworks.com, 2023.

package etx

import (
	"encoding/json"
	"errors"
	"time"
)

// The remnants of the V1 interface, as needed to process any redo log records from before a server upgrade.
//
// The problem with V1 was that it did not scale to more than a few concurrent operations. On recovery
// all pending operatioms would be executed at once, potentially overloading the memory of a small server.

// Redo struct holds the stored data for a V1 transaction operation.
type RedoV1 struct {
	Id        int64  // operation ID
	Manager   string // resource manager name
	OpType    int    // operation type
	Operation []byte // operation arguments, in JSON
}

// RedoStoreV1 is the interface for storage of V1 extended transactions, implemented by the parent application.
type RedoStoreV1 interface {
	All() []*RedoV1                               // all redo log entries in ID order
	DeleteId(id int64) error                      // delete redo
	ForManager(rm string, before int64) []*RedoV1 // aged log entries for RM
	GetIf(id int64) (*RedoV1, error)              // get entry if it still exists
}

// RecoverV1 reads and processes the V1 redo log,
// to complete any interrupted transactions from before server upgrade and restart.
func (tm *TM) RecoverV1(store RedoStoreV1, mgrs ...RM) error {

	tm.storeV1 = store

	// index resource managers by name
	rms := make(map[string]RM, 2)
	for _, rm := range mgrs {
		rms[rm.Name()] = rm
	}

	// recover using transaction log
	ts := tm.storeV1.All()
	for _, t := range ts {
		// note last V1 operation, so we can recognise their IDs
		tm.maxV1 = OpId(t.Id)

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
		rm.Operation(OpId(t.Id), t.OpType, op)
	}

	return nil
}

// Timeout executes any old operations for a resource manager.
// A non-zero opType selects the specified type.
func (tm *TM) TimeoutV1(rm RM, opType int, before time.Time) error {

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
			rm.Operation(OpId(t.Id), t.OpType, op)
		}
	}
	return nil
}

// Deprecated from V1 as a misleading name. Use the equivalent TM.AddNext instead.
func (tm *TM) BeginNext(first TxId, rm RM, opType int, op Op) error {
	_, err := tm.AddNext(first, rm, opType, op)
	return err
}

// Deprecated from V1 as a misleading name. Use the equivalent TM.Do instead.
func (tm *TM) DoNext(id TxId) {
	tm.Do(id)
}

// Deprecated from V1 as too general. Use TM.AddNext in most cases, with TM.Forget+TM.AddNext to modify an existing operation.
func (tm *TM) SetNext(id TxId, rm RM, opType int, op Op) error {

	_, err := tm.AddNext(id, rm, opType, op)
	return err
}
