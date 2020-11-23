// Copyright © Rob Burke inchworks.com, 2020.

// Package images manages image files uploaded to a server.
// The files are assumed to belong to a parent database object, such as a slideshow, with the
// files being uploaded asynchronously when the slideshow is modified.
//
// Note that images are given revision numbers for two reasons.
// (1) A different name forces browsers to fetch the updated image after an image has been changed.
// (2) It allows us to upload an image without overwriting the current one, and then forget it the update form is not submitted.
package images

import (
	"bytes"
	"fmt"
	"image"
	"io"
	"mime/multipart"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"github.com/disintegration/imaging"
)

// Parameters and state for image management.
type Images struct {

	// parameters
	ImagePath string
	MaxW      int
	MaxH      int
	ThumbW    int
	ThumbH    int

	// state
	parentId    int64
	versions    map[string]fileVersion
	delVersions []fileVersion
}

type fileVersion struct {
	fileName string
	revision int
	replace  bool
	keep     bool
}

// Request to save an image.
type ReqSave struct {
	parentId int64
	Name     string
	Fullsize bytes.Buffer
	Img      image.Image
}

// FileFromName makes a stored file name from user's name for image.
//
// A newly uploaded file has rev=0, so that it doesn't overwrite any previous copy yet.
func FileFromName(parentId int64, name string, rev int) string {
	if name != "" {
		if rev != 0 {
			return fmt.Sprintf("P-%s$%s-%s",
				strconv.FormatInt(parentId, 36),
				strconv.FormatInt(int64(rev), 36),
				name)
		} else {
			return fmt.Sprintf("P-%s-%s", strconv.FormatInt(parentId, 36), name)
		}
	} else {
		return ""
	}
}

// NameFromFile extracts the parent's ID, image name and revison from the file name.
func NameFromFile(fileName string) (int64, string, int) {
	if len(fileName) > 0 {
		// ss[0] is "P"
		sf := strings.SplitN(fileName, "-", 3)
		ss := strings.Split(sf[1], "$")
		parentId, _ := strconv.ParseInt(ss[0], 36, 64)

		var rev int64
		if len(ss) > 1 {
			rev, _ = strconv.ParseInt(ss[1], 36, 0)
		}
		return parentId, sf[2], int(rev)

	} else {
		return 0, "", 0
	}
}

// ReadVersions loads updated versions for processing.
func (im *Images) ReadVersions(parentId int64) error {

	// reset state
	im.parentId = parentId
	im.delVersions = nil

	s := strconv.FormatInt(parentId, 36)

	// find new files, and existing ones
	// (newVersions could be just a slice)
	newVersions := im.globVersions(filepath.Join(im.ImagePath, "P-"+s+"-*"))
	im.versions = im.globVersions(filepath.Join(im.ImagePath, "P-"+s+"$*"))

	// generate new revision nunbers
	// Note that fileNames for new files don't have revision numbers yet, we may need to delete some files.
	for name, nv := range newVersions {
		nv.replace = true

		cv := im.versions[name]
		if cv.revision != 0 {

			// current version is to be replaced and deleted
			nv.revision = cv.revision + 1
			im.delVersions = append(im.delVersions, cv)

		} else {

			// this is a new name
			nv.revision = 1
		}
		im.versions[name] = nv
	}

	return nil
}

// RemoveVersions deletes files for unused versions.
func (im *Images) RemoveVersions() error {

	// add unreferenced files to the deletion list
	for _, cv := range im.versions {

		if !cv.keep {
			im.delVersions = append(im.delVersions, cv)
		}
	}

	// delete unreferenced and old versions
	for _, cv := range im.delVersions {
		if err := os.Remove(filepath.Join(im.ImagePath, cv.fileName)); err != nil {
			return err
		}
		if err := os.Remove(filepath.Join(im.ImagePath, Thumbnail(cv.fileName))); err != nil {
			return err
		}
	}

	return nil
}

// Save decodes an image from an HTML request, and queues it for processing by a background worker.
func Save(fh *multipart.FileHeader, parentId int64, chImage chan<- ReqSave) (err error, byClient bool) {

	// get image from request header
	file, err := fh.Open()
	if err != nil {
		return err, false
	}
	defer file.Close()

	// duplicate file in buffer, since we can only read it from the header once
	var buffered bytes.Buffer
	tee := io.TeeReader(file, &buffered)

	// decode image
	img, err := imaging.Decode(tee, imaging.AutoOrientation(true))
	if err != nil {
		return err, true // this is a bad image from client
	}

	// resizing is slow, so do the remaining processing in background worker
	chImage <- ReqSave{
		parentId: parentId,
		Name:     fh.Filename,
		Fullsize: buffered,
		Img:      img,
	}

	return nil, true
}

// SaveResized is called by a background worker to process an image, resizing it if needed.
func (im *Images) SaveResized(req ReqSave) error {

	// convert non-displayable file types to JPG
	name, convert := changeType(req.Name)

	// path for saved files
	filename := FileFromName(req.parentId, name, 0)
	savePath := filepath.Join(im.ImagePath, filename)
	thumbPath := filepath.Join(im.ImagePath, Thumbnail(filename))

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

// Thumbnail returns a prefixed name from a filename.
func Thumbnail(filename string) string { return "S" + filename[1:] }

// Updated checks if an image file has changed, called from background worker.
// It returns an updated indication and the filename for the current version.
func (im *Images) Updated(fileName string) (bool, string, error) {

	// is there an image?
	if fileName == "" {
		return false, "", nil
	}

	// name and revision
	_, name, rev := NameFromFile(fileName)

	// convert non-displayable file types, to match converted image
	name, _ = changeType(name)

	cv := im.versions[name]
	if cv.revision == 0 {
		// we might have no versioned file if the user has just changed the parent object a second time
		// never mind, we'll fix it on the next call
		return false, "", nil
	}

	var err error
	var updated bool
	if rev != cv.revision {

		// first use of the new image?
		if cv.replace {

			// the newly uploaded image is already being used
			cv.fileName, err = im.saveVersion(im.parentId, name, cv.revision)
			if err != nil {
				return false, "", err
			}
			cv.replace = false
		}
		updated = true
	}

	// keep this file
	cv.keep = true
	im.versions[name] = cv

	return updated, cv.fileName, nil
}

// ValidType returns true if the file is an image.
func ValidType(name string) bool {

	_, err := imaging.FormatFromFilename(name)
	return err == nil
}

// changeType changes the file extension to a displayable type.
func changeType(name string) (nm string, changed bool) {

	// convert other file types to JPG
	fmt, err := imaging.FormatFromFilename(name)
	if err != nil {
		return name, false
	} // unikely error, never mind

	switch fmt {
	case imaging.JPEG:
		fallthrough

	case imaging.PNG:
		nm = name
		changed = false

	default:
		// change filename to JPG
		nm = strings.TrimSuffix(name, filepath.Ext(name)) + ".jpg"
		changed = true
	}
	return
}

// globVersions finds versions of new or existing files.
func (im *Images) globVersions(pattern string) map[string]fileVersion {

	versions := make(map[string]fileVersion)

	newFiles, _ := filepath.Glob(pattern)
	for _, newFile := range newFiles {

		fileName := filepath.Base(newFile)
		_, name, rev := NameFromFile(fileName)
		versions[name] = fileVersion{
			fileName: fileName,
			revision: rev,
		}
	}

	return versions
}

// saveVersion savea a new file with a revision number.
func (im *Images) saveVersion(parentId int64, name string, rev int) (string, error) {

	// the file should already be saved without a revision nuumber
	uploaded := FileFromName(parentId, name, 0)
	revised := FileFromName(parentId, name, rev)

	// main image ..
	uploadedPath := filepath.Join(im.ImagePath, uploaded)
	revisedPath := filepath.Join(im.ImagePath, revised)
	if err := os.Rename(uploadedPath, revisedPath); err != nil {
		return revised, err
	}

	// .. and thumbnail
	uploadedPath = filepath.Join(im.ImagePath, Thumbnail(uploaded))
	revisedPath = filepath.Join(im.ImagePath, Thumbnail(revised))
	err := os.Rename(uploadedPath, revisedPath)

	// rename with a revision number
	return revised, err
}
