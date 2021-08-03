// Copyright © Rob Burke inchworks.com, 2020.

// Package images manages image files uploaded to a server.
// The files are assumed to be destined to belong to a parent database object,
// such as a slideshow, with the files being uploaded asynchronously from a form
// that modifies the parent. The parent may not exist at the time of upload,
// so the files are initially "owned" by a user object, and attached to the parent later.
// It is assumed that a user can create or modify only one owner at a time.
//
// Note that files are given revision numbers for two reasons.
// (1) A different name forces browsers to fetch the updated file after its content has been changed.
// (2) It allows us to upload an file without overwriting the current one, and then forget it if the update form is not submitted.

package uploader

import (
	"bytes"
	"embed"
	"errors"
	"fmt"
	"image"
	"io"
	"io/fs"
	"mime/multipart"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"github.com/disintegration/imaging"
)

const (
	MediaImage = 1
	MediaVideo = 2
)

type Uploader struct {

	// parameters
	FilePath   string
	MaxW       int
	MaxH       int
	ThumbW     int
	ThumbH     int
	VideoTypes []string

	// state
	showId      int64
	timestamp   string
	versions    map[string]fileVersion
	delVersions []fileVersion
}

type fileVersion struct {
	fileName string
	revision int
	replace  bool
	keep     bool
}

type ReqSave struct {
	Name      string
	Timestamp string // request timestamp, to match image with form
	FileType  int
	Fullsize  bytes.Buffer
	Img       image.Image
}

// webFiles are the package's web resources (templates and static files)
//go:embed web
var WebFiles embed.FS

// cleanName removes unwanted characters from a filename, to make it safe for display and storage.
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

// FileFromName returns a stored file name from a user's name for an image.
// For a newly uploaded file, the owner is a request timestamp, because the slideshow may not exist yet.
// It has no revision number, so it doesn't overwrite a previous copy yet.
// Once the slideshow updates have been saved, the owner is the slideshow ID and the name has a revision number.
func FileFromName(ownerId string, name string, rev int) string {
	if name != "" {
		if rev != 0 {
			return fmt.Sprintf("P-%s$%s-%s",
				ownerId,
				strconv.FormatInt(int64(rev), 36),
				name)
		} else {
			return fmt.Sprintf("P-%s-%s", ownerId, name)
		}
	} else {
		return ""
	}
}

// FileType returns the image type (0 if not accepted)
func (im *Uploader) FileType(name string) int {

	_, err := imaging.FormatFromFilename(name)
	if err == nil {
		return MediaImage
	} else {
		// check for acceptable video types
		t := filepath.Ext(name)
		for _, vt := range im.VideoTypes {
			if t == vt {
				return MediaVideo
			}
		}
	}
	return 0
}

// NameFrommFile returns the owner ID, image name and revison from a file name.
// If the revision is 0, the owner is the request, otherwise the owner is the slideshow.
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

// ReadVersions loads updated image versions. timestamp is empty when we are deleting a slideshow.
func (im *Uploader) ReadVersions(showId int64, timestamp string) error {

	// reset state
	im.showId = showId
	im.timestamp = timestamp
	im.delVersions = nil

	showName := strconv.FormatInt(showId, 36)

	// find existing versions
	im.versions = im.globVersions(filepath.Join(im.FilePath, "P-"+showName+"$*"))

	// generate new revision nunbers
	if timestamp != "" {

		// find new files
		newVersions := im.globVersions(filepath.Join(im.FilePath, "P-"+timestamp+"-*"))

		for lc, nv := range newVersions {
			nv.replace = true

			cv := im.versions[lc]
			if cv.revision != 0 {

				// current version is to be replaced and deleted
				nv.revision = cv.revision + 1
				im.delVersions = append(im.delVersions, cv)

			} else {

				// this is a new name
				nv.revision = 1
			}
			im.versions[lc] = nv
		}
	}

	return nil
}

// RemoveVersions deletes unused files.
// This includes both old versions that have been superseded, and files that were uploaded but not referenced in a saved form.
func (im *Uploader) RemoveVersions() error {

	// add unreferenced files to the deletion list
	for _, cv := range im.versions {

		if !cv.keep {
			im.delVersions = append(im.delVersions, cv)
		}
	}

	// delete unreferenced and old versions
	for _, cv := range im.delVersions {
		if err := os.Remove(filepath.Join(im.FilePath, cv.fileName)); err != nil {
			return err
		}
		if err := os.Remove(filepath.Join(im.FilePath, Thumbnail(cv.fileName))); err != nil {
			return err
		}
	}

	return nil
}

// Save decodes an uploaded file, and schedules it to be saved in the filesystem.
func (im *Uploader) Save(fh *multipart.FileHeader, timestamp string, chImage chan<- ReqSave) (err error, byClient bool) {

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
	ft := im.FileType(name)

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
		// ## examine video
		if _, err := io.Copy(&buffered, file); err != nil {
			return err, false // don't know why this might fail
		}

	default:
		return errors.New("File format not supported"), true
	}

	// resizing or converting is slow, so do the remaining processing in background worker
	chImage <- ReqSave{
		Timestamp: timestamp,
		Name:      name,
		FileType:  ft,
		Fullsize:  buffered,
		Img:       img,
	}

	return nil, true
}

// SaveRequested performs image or video processing, called from background worker.
func (im *Uploader) SaveRequested(req ReqSave) error {

	switch req.FileType {
	case MediaImage:
		return im.saveImage(req)

	case MediaVideo:
		return im.saveVideo(req)

	default:
		return nil
	}
}

// Thumbnail returns the prefixed name for a thumbnail
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

// Updated is called from a background worker to check if a file has changed.
// If so, it renames the file to a new version, removes the old version and returns the new filename.
// An empty string indicates no change.
func (im *Uploader) Updated(fileName string) (string, error) {

	// is there an image?
	if fileName == "" {
		return "", nil
	}

	// name and revision
	_, name, rev := NameFromFile(fileName)

	// convert non-displayable file types, to match converted image
	// ## could we safely just check slide.Format
	if im.FileType(name) == MediaImage {
		name, _ = changeType(name)
	}
	lc := strings.ToLower(name)

	// current version
	cv := im.versions[lc]
	if cv.revision == 0 {
		// we have a name but no image file - upload delayed or failed
		// never mind, we'll fix it on the next call
		return "", errors.New("Missing file upload")
	}

	var err error
	var newName string
	if rev != cv.revision {

		// first slide to use the new image?
		if cv.replace {

			// the newly uploaded image is being used on a slide
			cv.fileName, err = im.saveVersion(im.showId, im.timestamp, name, cv.revision)
			if err != nil {
				return "", err
			}
			cv.replace = false
		}
		newName = cv.fileName
	}

	// keep this file
	cv.keep = true
	im.versions[lc] = cv

	return newName, nil
}

// copyFile copies a static file to the specified directory.
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

// changeType normalises an image file extension, and indicates if it should be converted to a displayable type.
func changeType(name string) (nm string, changed bool) {

	// convert other file types to JPG
	fmt, err := imaging.FormatFromFilename(name)
	if err != nil {
		return name, false
	} // unikely error, never mind

	var ext string
	switch fmt {
	case imaging.JPEG:
		ext = ".jpg"
		changed = false

	case imaging.PNG:
		ext = ".png"
		changed = false

	default:
		// convert to JPG
		ext = "jpg"
		changed = true
	}

	nm = strings.TrimSuffix(name, filepath.Ext(name)) + ext
	return
}

// globVersions finds versions of new or existing files.
func (im *Uploader) globVersions(pattern string) map[string]fileVersion {

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

// saveImage completes image saving, converting and resizing as needed.
func (im *Uploader) saveImage(req ReqSave) error {

	// convert non-displayable file types to JPG
	name, convert := changeType(req.Name)

	// path for saved files
	filename := FileFromName(req.Timestamp, name, 0)
	savePath := filepath.Join(im.FilePath, filename)
	thumbPath := filepath.Join(im.FilePath, Thumbnail(filename))

	// check if uploaded image small enough to save
	size := req.Img.Bounds().Size()
	if size.X <= im.MaxW && size.Y <= im.MaxH && !convert {

		// save uploaded file unchanged
		saved, err := os.OpenFile(savePath, os.O_WRONLY|os.O_CREATE, 0666)
		if err != nil {
			return err // could be a bad name?
		}
		defer saved.Close()
		if _, err = io.Copy(saved, &req.Fullsize); err != nil {
			return err
		}

	} else {

		// ## set compression option
		// ## could sharpen, but how much?
		// ## give someone else a chance - not sure if it helps
		resized := imaging.Fit(req.Img, im.MaxW, im.MaxH, imaging.Lanczos)
		runtime.Gosched()

		if err := imaging.Save(resized, savePath); err != nil {
			return err // ## could be a bad name?
		}
	}

	// save thumbnail
	thumbnail := imaging.Fit(req.Img, im.ThumbW, im.ThumbH, imaging.Lanczos)
	if err := imaging.Save(thumbnail, thumbPath); err != nil {
		return err
	}
	return nil
}

// saveVersion saves new file with a revision number.
func (im *Uploader) saveVersion(showId int64, timestamp string, name string, rev int) (string, error) {

	// the file should already be saved without a revision nuumber
	uploaded := FileFromName(timestamp, name, 0)
	revised := FileFromName(strconv.FormatInt(showId, 36), name, rev)

	// main image ..
	uploadedPath := filepath.Join(im.FilePath, uploaded)
	revisedPath := filepath.Join(im.FilePath, revised)
	if err := os.Rename(uploadedPath, revisedPath); err != nil {
		return revised, err
	}

	// .. and thumbnail
	uploadedPath = filepath.Join(im.FilePath, Thumbnail(uploaded))
	revisedPath = filepath.Join(im.FilePath, Thumbnail(revised))
	err := os.Rename(uploadedPath, revisedPath)

	// rename with a revision number
	return revised, err
}

// saveVideo completes video saving, converting as needed.
func (im *Uploader) saveVideo(req ReqSave) error {

	// path for saved file
	fn := FileFromName(req.Timestamp, req.Name, 0)
	savePath := filepath.Join(im.FilePath, fn)

	// save uploaded file unchanged
	saved, err := os.OpenFile(savePath, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return err // could be a bad name?
	}
	defer saved.Close()
	if _, err = io.Copy(saved, &req.Fullsize); err != nil {
		return err
	}

	// set thumbnail, replacing video type by JPG
	// ## This is temporary, as I hope to generate a thumbnail for each video in future.
	// ## So no provision for app to customise the thumbnail.
	if err = copyStatic(im.FilePath, Thumbnail(fn), WebFiles, "web/static/video.jpg"); err != nil {
		return nil
	}

	return nil
}