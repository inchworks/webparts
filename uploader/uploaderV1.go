// Copyright © Rob Burke inchworks.com, 2020.

package uploader

import (
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/inchworks/webparts/etx"
)

// Processing for any V1 ETX operations, remaining before the server upgrade.
//
// There were three problems with the V1 design.
// (1) It didn't allow for cached web pages referencing recently deleted or replaced media files.
//     The V2 design defers the removal of media files for a specified duration.
// (2) A media file upload was not actually committed on disk until after it had been processed,
//     so that a server restart could cause it to be lost after the user believed it had been uploaded
//     successfully. This was not a significant problem for image files, but videos can take minutes to
//     process.
// (3) Uploading media files faster than they could be processed could overload the memory of a small server.
//
// The improvements in turn required an upgrade to ETX, so that it could manage the recovery of
// a larger number of operations without overloading the memory of a small server.

// V1 requests the uploader to handle V1 extended transactions.
func (up *Uploader) V1() {
	up.chDoneV1 = make(chan bool, 1)
	up.tick = time.NewTicker(up.MaxAge / 8)
	go up.workerV1(up.tick.C, up.chDoneV1)
}

// StartClaimV1 prepares for the client to identify the files it references.
func (up *Uploader) StartClaimV1(tx etx.TxId) *Claim {

	c := &Claim{
		up:        up,
		tx:        tx,
		unclaimed: make(map[string]bool, 4),
	}

	// list all files for this transaction
	txCode := etx.StringV1(tx)
	uploads, _ := filepath.Glob(filepath.Join(up.FilePath, "P-"+txCode+"-*"))

	for _, f := range uploads {
		c.unclaimed[f] = true
	}

	return c
}

// FileV1 specifies a file that is referenced by the client.
// It is legal to specify files from other transactions; these will be ignored.
func (c *Claim) FileV1(name string) {

	// ok for unknown name, may have been claimed earlier
	if c.unclaimed[name] {
		delete(c.unclaimed, name)
	}
}

// EndV1 deletes any unclaimed files.
func (c *Claim) EndV1() {

	up := c.up
	for nm := range c.unclaimed {
		if err := os.Remove(filepath.Join(up.FilePath, nm)); err != nil {
			up.errorLog.Print(err.Error())
		}
	}
}

// Deprecated as a misleading name. Use the equivalent Uploader.Commit instead.
func (up *Uploader) ValidCode(tx etx.TxId) bool {
	return up.Commit(tx) == nil
}

// request timeout for extended transactions started before the cutoff time 
func (up *Uploader) timeoutV1Uploads() {

	cutoff := time.Now().Add(-1 * up.MaxAge)
	if err := up.tm.TimeoutV1(up, 0, cutoff); err != nil {
		up.errorLog.Print(err.Error())
	}
}

// worker does background processing for media.
func (up *Uploader) workerV1(
	chTick <-chan time.Time,
	chDone <-chan bool) {

	for {
		// returns to client sooner?
		runtime.Gosched()
		select {
		case <-chTick:
			up.timeoutV1Uploads()

		case <-chDone:
			return
		}
	}
}
