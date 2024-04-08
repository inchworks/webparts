// Copyright © Rob Burke inchworks.com, 2020.

// Package uploader manages media files uploaded to a server.
// The files are expected to belong to a parent database object,
// such as a slideshow, with the files being uploaded asynchronously from a form
// that modifies the parent. The parent need not exist at the time of upload,
// and an extended transaction model is used to maintain consistency between the database and the media files.
//
// Images are resized to fit within limits specified by the server.
// Audio files are converted to M4A format and videos to MP4 format. Thumbnails are generated for both images and videos.
// Deleted media files are retained for a specified period, allowing that they may still be referenced from cached web pages.
//
// Uploader maintains consistency across server restarts between the parent object and the media files it references.
// Operation is intended to be robust for processing that may take a long time, such as converting a video file.
// To prevent overloading a modest server, on a server restart the workload for recovery is limited to that similar to normal operation.
//
// Uploading is handled in five steps:
//
// (1) A extended transaction ID is allocated when a web request is received to create or update a parent object.
//
// (2) As media files are uploaded, temporary copies are saved on disk for processing later.
// They will eventually be deleted if no further action is taken, or if they are not eventually referenced by the parent object.
//
// (3) When a web request is received to create or update a parent object, a database transaction saves the changes to the parent object,
// together with requests to delete media files being removed, and a request to perform step 4.
//
// (4) An extended transaction operation identifies the new media files that are being referenced and starts their conversion to displayable sizes and formats.
// The operation is idempotent and will be repeated should the server be restarted while the operation is in progress.
//
// (5) When processing of all media files for the extended transaction is completed, a database transaction updates the parent object
// to reference the processed media, and deletes the stage 4 operation request.
// Any media files uploaded but not referenced are deleted.
//
// Uploader also provides support for delayed deletion of media files belonging to parent object.
// In single database transaction, call tx.Begin to start an extended transaction,
// call Delete for each media file referenced, delete the object and commit the deletion to the database.
//
// Use Thumbnail to get the file name for a thumbnail image corresponding to a media file.
//
// Note that because files are labelled with a transaction ID,
// the different name forces browsers to fetch an updated file after a file with the same name has been uploaded.
//
// Uploads are not displayable until step 5.
// A caller might save just the temporary file names at step 2 and,
// if a request is made to display the parent object, show a dummy image or thumbnail for any temporary files.
// Or a caller might store both previous and new file names, and continue to show previous images until step 5.
// Uploader will not delete old files until all new file processing is completed.
//
// ## Issue: what if client sends an upload after submitting the form. Can it be recognised for error and deletion?

package uploader

import (
	"embed"
	"errors"
	"fmt"
	"image"
	"io"
	"io/fs"
	"log"
	"mime/multipart"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/disintegration/imaging"

	"github.com/inchworks/webparts/etx"
)

const (
	MediaImage = 1
	MediaVideo = 2
	MediaAudio = 3
)

// Uploader holds the parameters and state for uploading files. Typically only one is needed.
type Uploader struct {

	// parameters
	FilePath        string
	MaxW            int
	MaxH            int
	MaxSize         int // maximum size for AV to use original without processing
	ThumbW          int
	ThumbH          int
	MaxAge          time.Duration // maximum time for a parent update
	DeleteAfter     time.Duration // delay before deleting a file
	SnapshotAt      time.Duration // snapshot time in video (-ve for none)
	AudioTypes      []string
	VideoPackage    string // software for video processing: ffmpeg, or a docker-hosted implementation of ffmpeg, for debugging
	VideoResolution int
	VideoTypes      []string

	// components
	errorLog *log.Logger
	db       DB
	tm       *etx.TM

	// background worker
	chDone    chan bool
	chSave    chan reqSave
	chClaimed chan reqClaimed

	// worker for V1 operations
	chDoneV1 chan bool
	tick     *time.Ticker

	// separate worker for video processing
	chVideosDone chan bool
	chConvert    chan reqSave

	// uploads in progress for each transaction
	muUploads sync.Mutex // ## move to claim?
}

// Context for a sequence of claim calls and subsequent processing.
type Claim struct {
	up        *Uploader
	tx        etx.TxId // transaction ID, used to match media files with parent
	uploads   int      // number of uploads in progress
	done      Uploaded
	unclaimed map[string]bool
}

// Callback when all claimed files are ready for use.
type Uploaded func(error)

// Context for a sequence of bind calls.
type Bind struct {
	up       *Uploader
	tx       etx.TxId
	prefixTx string
	unbound  map[string]string //  // stem -> stem.ext
}

type reqSave struct {
	mediaType int
	name      string // file name
	toType    string // for conversion
	claim     *Claim
}

type reqClaimed struct {
	claim *Claim
}

type reqBound struct {
	bind *Bind
}

// DB is an interface to the database manager that handles parent transactions.
type DB interface {
	Begin() func() // start transaction and return commit function
}

// WebFiles are the package's web resources (templates and static files)
//
//go:embed web
var WebFiles embed.FS

// Name, ForOperation and Operation implement the RM interface for webparts.etx.

// Operation types
const (
	OpOrphansV1Type = 0
	OpCancelType    = 1
	OpDeleteType    = 2
)

type OpCancel struct {
}

type OpDelete struct {
	Name string
}

func (up *Uploader) Name() string {
	return "webparts.uploader"
}

func (up *Uploader) ForOperation(opType int) etx.Op {
	switch opType {
	case OpOrphansV1Type, OpCancelType:
		return &OpCancel{}
	case OpDeleteType:
		return &OpDelete{}
	default:
		var unknown struct{}
		return &unknown
	}
}

// Do operation requested via TM.
func (up *Uploader) Operation(id etx.TxId, opType int, op etx.Op) {

	// send the request to the worker
	switch req := op.(type) {
	case *OpCancel:
		// delete all media for an abandoned transaction
		if err := up.removeOrphans(id); err != nil {
			up.errorLog.Print(err.Error())
		}

	case *OpDelete:
		// delete a media file
		if err := up.removeOrphan(id, req.Name); err != nil {
			up.errorLog.Print(err.Error())
		}

	default:
		up.errorLog.Print("Unknown TX operation")
	}
}

// Initialise starts the file uploader.
func (up *Uploader) Initialise(log *log.Logger, db DB, tm *etx.TM) {

	// default parameters
	if up.DeleteAfter == 0 {
		up.DeleteAfter = time.Hour
	}
	if up.MaxAge == 0 {
		up.MaxAge = time.Hour
	}
	if up.MaxSize == 0 {
		up.MaxSize = 3 * 1024 * 1024
	}
	if up.VideoResolution == 0 {
		up.VideoResolution = 1080
	}

	up.errorLog = log
	up.db = db
	up.tm = tm

	// channels for background worker
	up.chDone = make(chan bool, 1)
	up.chSave = make(chan reqSave, 64)
	up.chClaimed = make(chan reqClaimed, 4)

	// start background worker
	go up.worker(up.chSave, up.chClaimed, up.chDone)

	// separate worker for video processing
	up.chVideosDone = make(chan bool, 1)
	if up.VideoPackage != "" {
		up.chConvert = make(chan reqSave, 16)
		go up.avWorker(up.chConvert, up.chDone)
	} else {
		up.SnapshotAt = -1 // no snapshots
	}
}

// Stop shuts down the uploader.
func (up *Uploader) Stop() {
	// legacy operations
	if up.tick != nil {
		up.tick.Stop()
		close(up.chDoneV1)
	}

	// main operations
	close(up.chDone)
	if up.VideoPackage != "" {
		close(up.chVideosDone)
	}
}

// STEP 1 : when web request received to create or update parent object.
// Call Begin for a transaction ID to give to the client, and use NameFromFile to extract the media names shown to users from the media file names.

// Begin returns a transaction code for an update that may include a set of uploads.
// It expects that a database transaction (needed to write redo records) has been started.
func (up *Uploader) Begin() (string, error) {

	txId := up.tm.Begin()

	// add operation to remove all files, if the update is abandoned
	if err := up.tm.AddTimed(txId, up, OpCancelType, &OpCancel{}, up.MaxAge); err != nil {
		return "", err
	}

	return etx.String(txId), nil
}

// NameFromFile returns the media file name from a file name.
func NameFromFile(fileName string) string {
	if len(fileName) > 0 {
		if fileName[0] == 'P' {
			// old format: sf[1] is owner$version
			sf := strings.SplitN(fileName, "-", 3)
			if len(sf) == 3 {
				return sf[2]
			}
		} else {
			// sf[0] is "M" or "T", sf[1] a transaction ID, sf[2] an upload version
			sf := strings.SplitN(fileName, "-", 4)
			if len(sf) == 4 {
				return sf[3]
			}
		}
	}
	return ""
}

// STEP 2 : when AJAX request received to upload file. Call Save with the transaction code.

// An upload version is specified to make filenames unique within the transaction,
// so that later deletion of one reference does not invalidate other references. It also
// distinguishes files of different types but with the same name.

// Save creates a temporary copy of an uploaded file, to be processed later.
func (up *Uploader) Save(fh *multipart.FileHeader, tx etx.TxId, version int) (err error, byClient bool) {

	// get image from request header
	file, err := fh.Open()
	if err != nil {
		return err, false
	}
	defer file.Close()
	name := CleanName(fh.Filename)

	// check file, as much as we can easily
	// ## could do more?
	if up.MediaType(name) == 0 {
		return errors.New("uploader: File format not supported"), true
	}

	// save temporary file
	fn := fileFromNameNew("T", tx, version, name)
	path := filepath.Join(up.FilePath, fn)
	temp, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return err, false // could be a bad name?
	}
	_, err = io.Copy(temp, file)
	temp.Close()
	if err != nil {
		return err, false
	}
	return nil, false
}

// STEP 3 : when web form to create or update parent object received.
// Note that this is possible even when the server has restarted since step 2, because the client may have cached the form across the restart.
//
// (i) Start a database transaction and call Commit to keep the temporary files and indicate that step 4 will be executed.
//
// (ii) Use CleanName to sanitise the user's names for media, and use MediaType to check that uploaded file types are acceptable.
//
// (iii) If the media name is new or changed, call FileFromName to get the temporary file name to be stored in the database.
// Call Delete for existing media files that are to be deleted or replaced.
//
// (iv) Call tx.AddNext ensure the next step will be executed, commit the change to the database, and call tx.Do.

// CleanName removes unwanted characters from a filename, to make it safe for display and storage.
// From https://stackoverflow.com/questions/54461423/efficient-way-to-remove-all-non-alphanumeric-characters-from-large-text.
// ## This is far more restrictive than we need.
func CleanName(name string) string {

	s := []byte(name)
	j := 0
	for _, b := range s {
		if ('a' <= b && b <= 'z') ||
			('A' <= b && b <= 'Z') ||
			('0' <= b && b <= '9') ||
			b == '.' ||
			b == '-' ||
			b == '_' ||
			b == ' ' ||
			b == '(' ||
			b == ')' {
			s[j] = b
			j++
		}
	}
	return string(s[:j])
}

// FileFromName returns the stored file name for a newly uploaded file.
func FileFromName(id etx.TxId, version int, name string) string {
	return fileFromNameNew("T", id, version, name)
}

// MediaType returns the media type. It is 0 if not accepted.
func (up *Uploader) MediaType(name string) int {

	mt, _, _ := getType(name, up.AudioTypes, up.VideoTypes)
	return mt
}

// Commit makes temporary uploaded files permanent.
// It returns false if the transaction ID for a set of uploads has expired.
func (up *Uploader) Commit(tx etx.TxId) error {

	// cutoff time for orphans, less 20%
	max := (up.MaxAge * 4) / 5
	cutoff := time.Now().Add(-1 * max)

	// transaction ID is also a timestamp
	if etx.Timestamp(tx).Before(cutoff) {
		return errors.New("uploader: media files expired")
	}

	// no need to cancel uploads now
	if err := up.tm.Forget(tx, up, OpCancelType); err != nil {
		return err
	}

	return nil
}

// Delete removes a media file after a delay to allow for cached web pages holding references to be aged.
func (up *Uploader) Delete(tx etx.TxId, filename string) error {

	// Ignore deletion requests for temporary files, so current processing not interfered.
	// (Could be from an overlapping transaction.)
	if filename == "" || filename[0] == 'T' {
		return nil
	}

	if err := up.tm.AddTimed(tx, up, OpDeleteType, &OpDelete{Name: filename}, up.DeleteAfter); err != nil {
		return err
	}

	return nil
}

// STEP 4 : handle the parent claim request resulting from tx.Do, to start processing of the uploads that are actually needed,
// now that the parent changes have been committed.
//
// (i) Call StartClaim to start identifying the media files that are wanted.
//
// (ii) For each media file referenced by the parent, call Bind.File. This starts uploader processing, to resize or convert the file.
//
// (iii) Call Bind.End and specify a function to be called when all uploader processing has been done.
// Any files uploaded and not referenced will be deleted.

// StartClaim prepares for the client to identify the files it references.
func (up *Uploader) StartClaim(tx etx.TxId) *Claim {

	c := &Claim{
		up:        up,
		tx:        tx,
		unclaimed: make(map[string]bool, 4),
	}

	// list all unclaimed files
	// ## error ignored
	txCode := etx.String(tx)
	uploads, _ := filepath.Glob(filepath.Join(up.FilePath, "T-"+txCode+"-*"))

	for _, f := range uploads {
		c.unclaimed[filepath.Base(f)] = true
	}

	return c
}

// File specifies a file that is referenced by the client.
// It is legal to specify files from other transactions; these will be ignored.
func (c *Claim) File(name string) {

	up := c.up

	// ok for unknown name, may have been claimed earlier
	if c.unclaimed[name] {
		delete(c.unclaimed, name)

		//SERIALISED
		up.muUploads.Lock()

		// count claims in progress
		c.uploads++
		up.muUploads.Unlock()

		// resizing or converting is slow, so do the remaining processing in background worker
		up.chSave <- reqSave{
			name:  name,
			claim: c,
		}
	}
}

// End requests notification when uploads are done. Any unclaimed temporary files are deleted.
func (c *Claim) End(fn Uploaded) {

	up := c.up

	// SERIALISED add one more operation in progress
	up.muUploads.Lock()
	c.uploads++
	up.muUploads.Unlock()

	// delete any unclaimed files, and notify when processing is done
	// (can never notify synchronously because caller probably holds database locks)
	c.done = fn
	up.chClaimed <- reqClaimed{
		claim: c,
	}
}

// STEP 5 : handle the callback resulting from step 4 when media file processing has been completed.
//
// (i) Start a database transaction, and call StartBind.
//
// (ii) For each media file referenced by the parent, call Bind.File and change the parent to use the new permanent file name for the media.
//
// (iii) Call Bind.End and commit the parent update to the database.

// StartBind initiates linking a parent object to a set of uploaded files, returning a context for calls to Bind and EndBind.
func (up *Uploader) StartBind(tx etx.TxId) *Bind {

	// target temporarary files
	txCode := etx.String(tx)
	prefixTx := "T-" + txCode

	// ## keep function for future implementation with error reporting.
	b := &Bind{
		up:       up,
		tx:       tx,
		prefixTx: prefixTx,
		unbound:  make(map[string]string, 4),
	}

	// list all processed files (extensions may have changed)
	processed, _ := filepath.Glob(filepath.Join(up.FilePath, "M-"+txCode+"-*"))

	for _, f := range processed {
		fn := filepath.Base(f)
		nm := stem(fn)
		b.unbound[nm] = fn
	}
	return b
}

// File is called to check if a file has changed.
// If so, it returns the new filename. An empty string indicates no change.
func (b *Bind) File(fileName string) (string, error) {

	// ## keep error return for future implementation with error reporting.

	// is there a temporary media file?
	// Excludes old format "P" names and permanent files (which must already have processed).
	if fileName == "" || fileName[0] != 'T' {
		return "", nil
	}

	// prefix+tx and media name
	prefixTx, _ := cutN(fileName, '-', 2)

	if prefixTx == b.prefixTx {
		nm := stem(fileName)

		pnm := "M" + nm[1:]
		newName := b.unbound[pnm] // new name and extension
		if newName != "" {
			delete(b.unbound, pnm)
		}

		return newName, nil // new permanent name
	}

	return "", nil
}

// End completes the linking a parent object, and deletes any files for the transaction that are not required.
func (b *Bind) End() error {

	// delete unbound media for transaction
	// We can't do file deletion sooner, because additional files may become unused after an overlapping transactions.
	// requested even if there are none, so caller always gets an asynchronous notification
	// (can't notify synchronously because caller probably holds database locks)
	toDel := b.unbound
	for _, nm := range toDel {
		if err := b.up.removeMedia(nm); err != nil {
			b.up.errorLog.Print(err.Error())
		}
	}

	// ## keep status for future implementation with error reporting.
	return nil
}

// DISPLAY MEDIA FILES

// Working returns the processed status for an image.
// 0 = no file, 1 = processing, 100 = ready. Percent processed and errors may be implemented in future.
func Status(filename string) int {
	if filename == "" {
		return 0
	} else if filename[0] == 'T' {
		return 1
	} else {
		return 100
	}
}

// Thumbnail returns the prefixed name for a thumbnail.
func Thumbnail(filename string) string {

	switch filepath.Ext(filename) {

	case ".jpg", ".png":
		return "S" + filename[1:]

	// ## extensions not normalised for current websites :-(
	case ".jpeg", ".JPG", ".PNG", ".JPEG":
		return "S" + filename[1:]

	default:
		// replace file extension
		tn := changeExt(filename, ".jpg")
		return "S" + tn[1:]
	}
}

// IMPLEMENTATION

// changeExt returns a file name with the specified extension.
func changeExt(name string, ext string) string {
	return stem(name) + ext
}

// changePrefix returns a stored file name with a new prefix.
func changePrefix(prefix string, name string) string {
	// assume single character prefix
	return prefix + name[1:]
}

// changeType normalises a media file extension, and returns the displayable file type to which it should be converted.
// A blank name is returned for an unsupported format, and a blank type if no conversion is needed.
func changeType(name string, audioTypes []string, videoTypes []string) (nm string, toType string, convert bool) {

	mt, ext, cvt := getType(name, audioTypes, videoTypes)
	if mt != 0 {
		nm = changeExt(name, ext)
		toType = ext
		convert = cvt
	}
	return
}

// copyStatic copies a static file to the specified directory.
func copyStatic(toDir, name string, fromFS fs.FS, path string) error {
	var src fs.File
	var dst *os.File
	var err error

	if src, err = fromFS.Open(path); err != nil {
		return err
	}
	defer src.Close()

	if name == "" {
		name = filepath.Base(path)
	}

	if dst, err = os.Create(filepath.Join(toDir, name)); err != nil {
		return err
	}
	defer dst.Close()

	if _, err := io.Copy(dst, src); err != nil {
		return err
	}
	return nil
}

// cutN returns two strings, before and after the Nth separator
func cutN(s string, sep rune, n int) (string, string) {
	for i, sep2 := range s {
		if sep2 == sep {
			n--
			if n == 0 {
				return s[:i], s[i+1:]
			}
		}
	}
	return s, ""
}

// fileFromNameNew returns a stored file name from a user's name for a newly uploaded file.
// The prefix is "T" for a temporary file, and "M" for a permanent media one.
// It has no revision number, so it doesn't overwrite a previous copy yet.
func fileFromNameNew(prefix string, id etx.TxId, version int, name string) string {
	if name != "" {
		return fmt.Sprintf("%s-%s-%x-%s", prefix, etx.String(id), version, name)
	} else {
		return ""
	}
}

// getType returns the mediaType and normalised file extension, and indicates if it needs conversion.
// A blank name is returned for an unsupported format.
func getType(name string, audioTypes []string, videoTypes []string) (mediaType int, ext string, convert bool) {

	if fmt, err := imaging.FormatFromFilename(name); err == nil {
		// image formats
		mediaType = MediaImage

		switch fmt {
		case imaging.JPEG:
			ext = ".jpg"
			convert = false

		case imaging.PNG:
			ext = ".png"
			convert = false

		default:
			// convert to JPG
			ext = ".jpg"
			convert = true
		}
	} else {
		t := strings.ToLower(filepath.Ext(name))

		// acceptable audio formats, all converted to M4A
		for _, vt := range audioTypes {
			if t == vt {
				mediaType = MediaAudio
				ext = ".m4a"
				convert = (t != ext)
				break
			}
		}

		// acceptable video formats, all converted to MP4
		for _, vt := range videoTypes {
			if t == vt {
				mediaType = MediaVideo
				ext = ".mp4"
				convert = (t != ext)
				break
			}
		}
	}

	return
}

// opDone decrements the count of in-progress uploads, and requests the next operation when ready.
func (up *Uploader) opDone(c *Claim) {

	var done Uploaded

	// SERIALISED
	up.muUploads.Lock()

	// decrement uploads in progress
	if c.uploads > 1 {
		c.uploads--
	} else {
		// uploads complete
		done = c.done
	}
	up.muUploads.Unlock()

	// notify caller
	if done != nil {
		done(nil)
	}
}

// removeMedia unlinks an image file and the corresponding thumbnail.
func (up *Uploader) removeOrphan(tx etx.TxId, name string) error {

	if err := up.removeMedia(name); err != nil {
		up.errorLog.Print(err.Error())
	}

	// make a database transaction (needed by TM to delete redo record)
	defer up.db.Begin()()

	// end operation
	return up.tm.End(tx)
}

// removeOrphans deletes all files for an abandoned transaction.
func (up *Uploader) removeOrphans(id etx.TxId) error {

	// all files for transaction
	tn := etx.String(id)
	files, _ := filepath.Glob(filepath.Join(up.FilePath, "?-"+tn+"-*"))

	for _, f := range files {
		if err := up.removeMedia(f); err != nil {
			up.errorLog.Print(err.Error())
		}
	}

	// make a database transaction (needed by TM to delete redo record)
	defer up.db.Begin()()

	// end operation
	return up.tm.End(id)
}

// removeMedia unlinks an image file and the corresponding thumbnail.
// (If this is the sole link, the file is deleted.)
func (up *Uploader) removeMedia(fileName string) error {
	nm := fileName

	// remove file
	err := os.Remove(filepath.Join(up.FilePath, nm))
	if err != nil && errors.Is(err, fs.ErrNotExist) {

		// Is it a legacy file saved by an earlier implementation?
		if filepath.Ext(nm) == ".jpg" {
			nm = changeExt(nm, ".jpeg")
			err = os.Remove(filepath.Join(up.FilePath, nm))
		}
	}

	// To make the operation idempotent, we accept that a file may already be deleted.
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}

	// remove corresponding thumbnail
	if err := os.Remove(filepath.Join(up.FilePath, Thumbnail(nm))); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}

// saveImage completes image saving, converting and resizing as needed.
func (up *Uploader) saveImage(req reqSave) error {

	// read temporary image
	// ## could cache small images when received
	fromPath := filepath.Join(up.FilePath, req.name)

	tf, err := os.Open(fromPath)
	if err != nil {
		return err
	}

	// decode image
	img, err := imaging.Decode(tf, imaging.AutoOrientation(true))
	tf.Close()

	resize := false
	size := img.Bounds().Size()
	if size.X > up.MaxW || size.Y > up.MaxH {
		resize = true
	}

	// convert non-displayable file types to JPG
	toName, _, convert := changeType(req.name, []string{}, []string{})
	if toName == "" {
		return errors.New("uploader: Unsupported file " + req.name) // ## shouldn't get this far?
	}
	toName = changePrefix("M", toName)

	// path for saved files
	toPath := filepath.Join(up.FilePath, toName)
	thumbPath := filepath.Join(up.FilePath, Thumbnail(toName))

	// rename uploaded image if it was small enough to use unchanged
	if !resize && !convert {

		if err := os.Rename(fromPath, toPath); err != nil {
			return err
		}

	} else {

		// make smaller image and delete original
		// ## Could set compression option, or sharpen, but how much?
		resized := imaging.Fit(img, up.MaxW, up.MaxH, imaging.Lanczos)
		runtime.Gosched()

		if err := imaging.Save(resized, toPath); err != nil {
			return err // ## could be a bad name?
		}
		if err := os.Remove(fromPath); err != nil {
			return err
		}
	}

	// save thumbnail
	if err := up.saveThumbnail(img, thumbPath); err != nil {
		return err
	}

	return nil
}

// saveMedia performs image or video processing, called from background worker.
func (up *Uploader) saveMedia(req reqSave) error {
	var convert bool
	var err error

	mt := up.MediaType(req.name)
	req.mediaType = mt
	switch mt {
	case MediaAudio, MediaVideo:
		convert, err = up.saveAV(req)
		if !convert {
			up.opDone(req.claim)
		}
		// otherwise, processing continued in AV worker

	case MediaImage:
		err = up.saveImage(req)
		up.opDone(req.claim)
	}

	return err
}

// saveThumbnail generates a thumbnail for an image
func (up *Uploader) saveThumbnail(img image.Image, to string) error {
	// save thumbnail
	thumbnail := imaging.Fit(img, up.ThumbW, up.ThumbH, imaging.Lanczos)
	return imaging.Save(thumbnail, to)
}

// stem returns the filename without the extension.
func stem(fn string) string {
	return strings.TrimSuffix(fn, filepath.Ext(fn))
}

// worker does background processing for media.
func (up *Uploader) worker(
	chSave <-chan reqSave,
	chClaimed <-chan reqClaimed,
	chDone <-chan bool) {

	for {
		// returns to client sooner?
		runtime.Gosched()

		select {

		case req := <-chSave:
			// resize and save media, with thumbnail
			if err := up.saveMedia(req); err != nil {
				up.errorLog.Print(err.Error())
			}

		case req := <-chClaimed:
			// delete unclaimed media for transaction
			for nm := range req.claim.unclaimed {
				if err := os.Remove(filepath.Join(up.FilePath, nm)); err != nil {
					up.errorLog.Print(err.Error())
				}
			}

			// notify that all uploaded files have been processed
			up.opDone(req.claim)

		case <-chDone:
			// ## do something to finish other pending requests
			return
		}
	}
}
