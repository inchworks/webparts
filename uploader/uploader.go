// Copyright © Rob Burke inchworks.com, 2020.

// Package uploader manages media files uploaded to a server.
// The files are assumed to be destined to belong to a parent database object,
// such as a slideshow, with the files being uploaded asynchronously from a form
// that modifies the parent. The parent need not exist at the time of upload,
// and a log is used to maintain consistency between the database and the media files.
//
// Images are resized to fit within limits specified by the server.
// Videos are converted to MP4 format. Thumbnails are generated for both images and videos.
//
// Note that files are given revision numbers for these reasons:
// (1) A different name forces browsers to fetch the updated file after its content has been changed.
// (2) It allows us to upload an file without overwriting the current one, and then forget it if the update form is not submitted.
// (3) The committed state of an object may be displayed with its files, while a new state is being prepared.
//
// uploader maintains consistency between the parent object and the media files it references, with these limitations:
//
// - Object saved before EndSave is completed : there is a brief period when new media files cannot be displayed.
// ## Can I check?
//
// - After object saved, and before the bind operation, there is a brief period where it references new files and has deleted files removed,
//  but still references the previous versions of updated files.
//
// Use the uploader as follows:
//
// (1) A web request is received to create or update a parent object: call Begin and add the transaction code as a hidden field in the form.
// Use NameFromFile to extract the media names shown to users from the media file names.
//
// (2) A media file is uploaded via an AJAX request: call Save with the transaction code.
// Images are resized and thumbnails generated asynchronously to the request.
//
// (3) A parent object is created, updated or deleted: call SetParent to associate the transaction code with the object.
// Use CleanName to sanitise user names for media, and use MediaType to check that uploaded file types are acceptable.
// If the media name is new or changed, call FileFromName to get the file name to be stored in the database.
// (Changed versions for existing names are handled in step 5.)
// Call tx.SetNext ensure the next step will be executed, commit the change to the database.
//
// (4) Call DoNext. The uploader waits for any uploads to be processed, and then calls the operation specified by etx.SetNext.
//
// (5) Handle the parent bind request from DoNext.
//
// (i) Call StartBind to begin updating references between the parent and the media files.
//
// (ii) For each media file referenced by the parent, call Bind.File, and record any new or updated references.
// Save the parent in the database.
//
// (6) After the parent update has been committed, call Bind.End.
// Any existing files not listed by Bind will be deleted.
//
// When deleting an object, call StartBind (with no request code), delete the object and then call EndBind.
//
// Use Thumbnail to get the file name for a thumbnail image corresponding to a media file.
package uploader

import (
	"bytes"
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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/disintegration/imaging"

	"github.com/inchworks/webparts/etx"
)

const (
	MediaImage = 1
	MediaVideo = 2
)

// op holds the state of uploading media for a single transaction
type op struct {
	next    bool // true if the parent's next operation has been specified
	uploads int  // number of uploads in progress
}

// Uploader holds the parameters and state for uploading files. Typically only one is needed.
type Uploader struct {

	// parameters
	FilePath     string
	MaxW         int
	MaxH         int
	ThumbW       int
	ThumbH       int
	MaxAge       time.Duration // maximum time for a parent update
	SnapshotAt   time.Duration // snapshot time in video (-ve for none)
	VideoPackage string        // software for video processing: ffmpeg, or a docker-hosted implementation of ffmpeg, for debugging
	VideoTypes   []string

	// components
	errorLog *log.Logger
	db       DB
	tick     *time.Ticker
	tm       *etx.TM

	// background worker
	chDone    chan bool
	chSave    chan reqSave
	chOrphans chan OpOrphans

	// separate worker for video processing
	chVideosDone chan bool
	chConvert    chan reqConvert

	// uploads in progress for each transaction
	muUploads sync.Mutex
	ops     map[etx.TxId]op
}

// Context for a sequence of bind calls.
type Bind struct {
	up          *Uploader
	tx          etx.TxId
	parentId    int64
	versions    map[string]fileVersion
	delVersions []fileVersion
}

type OpOrphans struct {
	tx etx.TxId
}

type reqSave struct {
	name      string       // file name
	tx        etx.TxId     // transaction ID, used to match media files with parent form
	mediaType int          // image or video
	fullsize  bytes.Buffer // original image or video
	img       image.Image  // nil for video
}

// DB is an interface to the database manager that handles parent transactions.
type DB interface {
	Begin() func() // start transaction and return commit function
}

type fileVersion struct {
	fileName string
	revision int
	upload   bool
	keep     bool
}

// WebFiles are the package's web resources (templates and static files)
//go:embed web
var WebFiles embed.FS

// Name, ForOperation and Operaion implement the RM interface for webparts.etx.

func (up *Uploader) Name() string {
	return "webparts.uploader"
}

func (up *Uploader) ForOperation(opType int) etx.Op {
	return &OpOrphans{}
}

func (up *Uploader) Operation(id etx.TxId, opType int, op etx.Op) {

	// this is the only operation we log
	opO := op.(*OpOrphans)
	opO.tx = id

	// remove files for abandoned transaction
	up.chOrphans <- *opO
}

// Initialise starts the file uploader.
func (up *Uploader) Initialise(log *log.Logger, db DB, tm *etx.TM) {

	up.errorLog = log
	up.db = db
	up.tm = tm
	up.chDone = make(chan bool, 1)
	up.chSave = make(chan reqSave, 20)
	up.chOrphans = make(chan OpOrphans, 4)
	up.ops = make(map[etx.TxId]op, 8)

	up.chVideosDone = make(chan bool, 1)

	// start background worker
	up.tick = time.NewTicker(up.MaxAge / 8)
	go up.worker(up.chSave, up.chOrphans, up.tick.C, up.chDone)

	// separate worker for video processing
	if up.VideoPackage != "" {
		up.chConvert = make(chan reqConvert, 20)
		go up.videoWorker(up.chConvert, up.chDone)
	} else {
		up.SnapshotAt = -1 // no snapshots
	}
}

// Stop shuts down the uploader.
func (up *Uploader) Stop() {
	up.tick.Stop()
	up.chDone <- true
	if up.VideoPackage != "" {
		up.chVideosDone <- true
	}
}

// STEP 1 : when web request received to create or update parent object.

// Begin returns an identifier for an update that may include a set of uploads.
// It expects that a database transaction (needed to write redo records) has been started.
func (up *Uploader) Begin() (string, error) {

	id := up.tm.Begin()

	// add operation to remove orphan files, if the update is abandoned
	if err := up.tm.SetNext(id, up, 0, &OpOrphans{}); err != nil {
		return "", err
	}

	return etx.String(id), nil
}

// NameFrommFile returns the owner ID, media file name and revision from a file name.
// If the revision is 0, the owner is the request, otherwise the owner is a parent object.
func NameFromFile(fileName string) (string, string, int) {
	if len(fileName) > 0 {
		// sf[0] is "P"
		sf := strings.SplitN(fileName, "-", 3)
		ss := strings.Split(sf[1], "$")

		var rev int64
		if len(ss) > 1 {
			rev, _ = strconv.ParseInt(ss[1], 36, 0)
		}
		return ss[0], sf[2], int(rev)

	} else {
		return "", "", 0
	}
}

// STEP 2 : when AJAX request received to upload file.

// Save decodes an uploaded file, and schedules it to be saved in the filesystem.
func (up *Uploader) Save(fh *multipart.FileHeader, tx etx.TxId) (err error, byClient bool) {

	// get image from request header
	file, err := fh.Open()
	if err != nil {
		return err, false
	}
	defer file.Close()

	// unmodified copy of file
	var buffered bytes.Buffer

	// image or video?
	var img image.Image
	name := CleanName(fh.Filename)
	ft := up.MediaType(name)

	switch ft {
	case MediaImage:
		// duplicate file in buffer, since we can only read it from the header once
		tee := io.TeeReader(file, &buffered)

		// decode image
		img, err = imaging.Decode(tee, imaging.AutoOrientation(true))
		if err != nil {
			return err, true // this is a bad image from client
		}

	case MediaVideo:
		if _, err := io.Copy(&buffered, file); err != nil {
			return err, false // don't know why this might fail
		}

	default:
		return errors.New("File format not supported"), true
	}

	//SERIALISED
	up.muUploads.Lock()

	// count uploads in progress
	op := up.ops[tx]
	op.uploads++
	up.ops[tx] = op
	up.muUploads.Unlock()

	// resizing or converting is slow, so do the remaining processing in background worker
	up.chSave <- reqSave{
		name:      name,
		tx:        tx,
		mediaType: ft,
		fullsize:  buffered,
		img:       img,
	}

	return nil, true
}

// STEP 3 : when web form to create or update parent object received.

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

// fileFromNameRev returns a stored file name from a user's name for a saved media file.
// Once the parent update has been saved, the owner is the parent ID and the name has a revision number.
func fileFromNameRev(ownerId int64, name string, rev int) string {
	if name != "" {
		return fmt.Sprintf("P-%s$%s-%s",
			strconv.FormatInt(ownerId, 36),
			strconv.FormatInt(int64(rev), 36),
			name)
	} else {
		return ""
	}
}

// FileFromName returns a stored file name from a user's name for a newly uploaded file.
// The owner is a transaction code, because the parent object may not exist yet.
// It has no revision number, so it doesn't overwrite a previous copy yet.
func FileFromName(id etx.TxId, name string) string {
	if name != "" {
		return fmt.Sprintf("P-%s-%s", etx.String(id), name)
	} else {
		return ""
	}
}

// MediaType returns the media type. It is 0 if not accepted.
func (up *Uploader) MediaType(name string) int {

	mt, _, _ := getType(name, up.VideoTypes)
	return mt
}

// ValidCode returns false if the transaction code for a set of uploads has expired.
func (up *Uploader) ValidCode(tx etx.TxId) bool {

	// cutoff time for orphans, less 20%
	max := (up.MaxAge * 4) / 5
	cutoff := time.Now().Add(-1 * max)

	// transaction ID is also a timestamp
	return etx.Timestamp(tx).After(cutoff)
}

// DoNext executes the parent's operation specified by etx.SetNext, when all uploaded images have been saved.
func (up *Uploader) DoNext(tx etx.TxId) {

	// SERIALISED
	up.muUploads.Lock()

	// #### There is no way we should lose the operation map, but somehow it has happened, twice.
	// Deserialise before we panic, so that we don't hang all new uploads until we run out of memory :-(.
	if up.ops == nil {
		up.muUploads.Unlock()
		panic("Uploader: missing ops map!")
	}

	// uploads in progress?
	op := up.ops[tx]
	wait := op.uploads > 0
	if wait {
		op.next = true
		up.ops[tx] = op
	}
	up.muUploads.Unlock()

	// execute without waiting
	if !wait {
		up.tm.DoNext(tx)
	}
}

// STEP 4 : asyncronous processing of a parent update.

// StartBind initiates linking a parent object to a set of uploaded files, returning a context for calls to Bind and EndBind.
// It loads updated file versions. tx is 0 when we are deleting a parent object.
func (up *Uploader) StartBind(parentId int64, tx etx.TxId) *Bind {

	b := &Bind{
		up:       up,
		tx:       tx,
		parentId: parentId,
	}

	parentName := strconv.FormatInt(parentId, 36)

	// find existing versions
	b.versions = up.globVersions(filepath.Join(up.FilePath, "P-"+parentName+"$*"))

	// generate new revision nunbers
	if tx != 0 {

		txCode := etx.String(tx)

		// find new files and set version number for each
		newVersions := up.globVersions(filepath.Join(up.FilePath, "P-"+txCode+"-*"))

		for lc, nv := range newVersions {
			nv.upload = true

			cv := b.versions[lc]
			if cv.revision != 0 {

				// If the operation is being redone on recovery this will increment
				// the version unnecessarily, and Bind.File will add a third link.
				// ## That's OK, but we could use io.Stat and io.SameFile to detect this case.

				// current version is to be replaced and deleted
				nv.revision = cv.revision + 1
				b.delVersions = append(b.delVersions, cv)

			} else {
				// this is a new name
				nv.revision = 1
			}
			b.versions[lc] = nv

			// the name with txCode is to be deleted
			b.delVersions = append(b.delVersions, nv)
		}
	}

	return b
}

// File is called to check if a file has changed.
// If so, it links the file to a new version, and returns the new filename.
// An empty string indicates no change.
func (b *Bind) File(fileName string) (string, error) {

	up := b.up

	// is there an media file?
	if fileName == "" {
		return "", nil
	}

	// name and revision
	_, name, rev := NameFromFile(fileName)

	// change user's file type, to match converted media
	name, _ = changeType(name, up.VideoTypes)
	lc := strings.ToLower(name)

	// current version
	cv := b.versions[lc]
	if cv.revision == 0 {
		// we have a name but no image file - the client shouldn't allow this
		return "", fmt.Errorf("missing file upload for %v", fileName)
	}

	var err error
	var newName string
	if rev != cv.revision {

		// first reference to uploaded file?
		if cv.upload {

			// the newly uploaded file is being used
			cv.fileName, err = up.saveVersion(b.parentId, b.tx, name, cv.revision)
			if err != nil {
				return "", fmt.Errorf("cannot bind upload for %v: %w", fileName, err)
			}
			cv.upload = false
		}
		newName = cv.fileName
	}

	// keep this file
	cv.keep = true
	b.versions[lc] = cv

	return newName, nil
}

// End completes the linking a parent object. It deletes unused files.
// This includes:
//  - old versions that have been superseded;
//  - the upload names (resulting in deletion if the file wasn't referenced in the saved parent);
//  - files that are no referenced no more.
func (b *Bind) End() error {

	up := b.up

	// add files that are now unreferenced to the deletion list (exclude uploads because these versions were never linked)
	for _, cv := range b.versions {

		if !cv.keep && !cv.upload {
			b.delVersions = append(b.delVersions, cv)
		}
	}

	// delete unreferenced and old versions (ok if they don't exist, because we are redoing the operation)
	for _, cv := range b.delVersions {
		if err := up.removeMedia(cv.fileName); err != nil {
			return err
		}
	}
	return nil
}

// DISPLAY MEDIA FILES

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
		tn := strings.TrimSuffix(filename, filepath.Ext(filename)) + ".jpg"
		return "S" + tn[1:]
	}
}

// IMPLEMENTATION

// getType returns the mediaType and normalised file extension, and indicates if it is converted.
// A blank name is returned for an unsupported format.
func getType(name string, videoTypes []string) (mediaType int, ext string, changed bool) {

	if fmt, err := imaging.FormatFromFilename(name); err == nil {
		// image formats
		mediaType = MediaImage

		switch fmt {
		case imaging.JPEG:
			ext = ".jpg"
			changed = false

		case imaging.PNG:
			ext = ".png"
			changed = false

		default:
			// convert to JPG
			ext = ".jpg"
			changed = true
		}
	} else {
		// acceptable video formats, all converted to MP4
		t := strings.ToLower(filepath.Ext(name))
		for _, vt := range videoTypes {
			if t == vt {
				mediaType = MediaVideo
				ext = ".mp4"
				changed = (t != ext)
				break
			}
		}
	}

	return
}

// changeType normalises a media file extension, and indicates if it should be converted to a displayable type.
// A blank name is returned for an unsupported format.
func changeType(name string, videoTypes []string) (nm string, changed bool) {
	var mt int
	var ext string

	if mt, ext, changed = getType(name, videoTypes); mt != 0 {
		nm = strings.TrimSuffix(name, filepath.Ext(name)) + ext
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

// globVersions finds versions of new or existing files.
func (up *Uploader) globVersions(pattern string) map[string]fileVersion {

	versions := make(map[string]fileVersion)

	newFiles, _ := filepath.Glob(pattern)
	for _, newFile := range newFiles {

		fileName := filepath.Base(newFile)
		_, name, rev := NameFromFile(fileName)

		// index case-blind
		versions[strings.ToLower(name)] = fileVersion{
			fileName: fileName,
			revision: rev,
		}
	}

	return versions
}

// opDone decrements the count of in-progress uploads, and requests the next operation when ready.
func (up *Uploader) opDone(tx etx.TxId) {

	var next bool

	// SERIALISED
	up.muUploads.Lock()

	// decrement uploads in progress
	op := up.ops[tx]
	if op.uploads > 1 {
		op.uploads--
		up.ops[tx] = op
	} else {
		// uploads complete
		next = op.next
		delete(up.ops, tx)
	}
	up.muUploads.Unlock()

	// next operation
	if next {
		up.tm.DoNext(tx)
	}
}

// removeMedia unlinks an image file and the corresponding thumbnail.
// (If this is the sole link, the file is deleted)
func (up *Uploader) removeMedia(fileName string) error {

	// To make the operation idempotent, we accept that a file may already be deleted.
	if err := os.Remove(filepath.Join(up.FilePath, fileName)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	if err := os.Remove(filepath.Join(up.FilePath, Thumbnail(fileName))); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}

// removeOrphans deletes all files for an abandoned transaction.
func (up *Uploader) removeOrphans(id etx.TxId) error {

	// make a database transaction (needed by TM to delete redo record)
	defer up.db.Begin()()

	// all files for transaction
	tn := etx.String(id)
	files := up.globVersions(filepath.Join(up.FilePath, "P-"+tn+"-*"))

	for _, f := range files {
		if err := up.removeMedia(f.fileName); err != nil {
			return err
		}
	}

	// end transaction
	return up.tm.End(id)
}

// saveImage completes image saving, converting and resizing as needed.
func (up *Uploader) saveImage(req reqSave) error {

	// convert non-displayable file types to JPG
	name, convert := changeType(req.name, []string{})

	// path for saved files
	filename := FileFromName(req.tx, name)
	savePath := filepath.Join(up.FilePath, filename)
	thumbPath := filepath.Join(up.FilePath, Thumbnail(filename))

	// check if uploaded image small enough to save
	size := req.img.Bounds().Size()
	if size.X <= up.MaxW && size.Y <= up.MaxH && !convert {

		// save uploaded file unchanged
		saved, err := os.OpenFile(savePath, os.O_WRONLY|os.O_CREATE, 0666)
		if err != nil {
			return err // could be a bad name?
		}
		defer saved.Close()
		if _, err = io.Copy(saved, &req.fullsize); err != nil {
			return err
		}

	} else {

		// ## Could set compression option, or sharpen, but how much?
		resized := imaging.Fit(req.img, up.MaxW, up.MaxH, imaging.Lanczos)
		runtime.Gosched()

		if err := imaging.Save(resized, savePath); err != nil {
			return err // ## could be a bad name?
		}
	}

	// save thumbnail
	if err := up.saveThumbnail(req.img, thumbPath); err != nil {
		return err
	}

	return nil
}

// saveMedia performs image or video processing, called from background worker.
func (up *Uploader) saveMedia(req reqSave) error {
	var err error

	switch req.mediaType {
	case MediaImage:
		err = up.saveImage(req)
		up.opDone(req.tx)

	case MediaVideo:
		var done bool
		done, err = up.saveVideo(req)
		if done {
			up.opDone(req.tx)
		}
		// otherwise, processing continued in video worker
	}

	return err
}

// saveThumbnail generates a thumbnail for an image
func (up *Uploader) saveThumbnail(img image.Image, to string) error {
	// save thumbnail
	thumbnail := imaging.Fit(img, up.ThumbW, up.ThumbH, imaging.Lanczos)
	return imaging.Save(thumbnail, to)
}

// saveVersion saves a new file with a revision number.
func (up *Uploader) saveVersion(parentId int64, tx etx.TxId, name string, rev int) (string, error) {

	// Link the file, rather than rename it, so the current version of the parent continues to work.
	// We'll remove the old name once the parent update has been committed.

	// the file should already be saved without a revision nuumber
	uploaded := FileFromName(tx, name)
	revised := fileFromNameRev(parentId, name, rev)

	// main image ..
	uploadedPath := filepath.Join(up.FilePath, uploaded)
	revisedPath := filepath.Join(up.FilePath, revised)
	if err := os.Link(uploadedPath, revisedPath); err != nil {
		return revised, err
	}

	// .. and thumbnail
	uploadedPath = filepath.Join(up.FilePath, Thumbnail(uploaded))
	revisedPath = filepath.Join(up.FilePath, Thumbnail(revised))
	err := os.Link(uploadedPath, revisedPath)

	// rename with a revision number
	return revised, err
}

// worker does background processing for media.
func (up *Uploader) worker(
	chSave <-chan reqSave,
	chOrphans <-chan OpOrphans,
	chTick <-chan time.Time,
	chDone <-chan bool) {

	for {
		// returns to client sooner?
		runtime.Gosched()

		select {

		case req := <-chSave:

			// resize and save image, with thumbnail
			if err := up.saveMedia(req); err != nil {
				up.errorLog.Print(err.Error())
			}

		case req := <-chOrphans:
			if err := up.removeOrphans(req.tx); err != nil {
				up.errorLog.Print(err.Error())
			}

		case <-chTick:
			// cutoff time for orphans
			cutoff := time.Now().Add(-1 * up.MaxAge)

			// request timeout for extended transactions started before the cutoff time
			if err := up.tm.Timeout(up, 0, cutoff); err != nil {
				up.errorLog.Print(err.Error())
			}

		case <-chDone:
			// ## do something to finish other pending requests
			return
		}
	}
}
