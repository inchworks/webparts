// Copyright © Rob Burke inchworks.com, 2025.

package uploader

// Document file processing.

import (
	"io/fs"
	"os"
	"path/filepath"
)

// saveDocument saves the document file and a thumbnail.
func (up *Uploader) saveDocument(req reqSave) error {

	var err error
	fromName := req.name
	fromPath := filepath.Join(up.FilePath, fromName)
	toName := changeExt(req.name, req.format.toType)  // normalised file extension

	// rename to a permanent file
	toName = changePrefix("M", toName)
	if err = os.Rename(fromPath, filepath.Join(up.FilePath, toName)); err != nil {
		return err
	}

	// add a dummy thumbnail for the document type
	err = copyStatic(up.FilePath, Thumbnail(fromName), WebFiles, thumbnailForExt(req.format.toType))
	if err == fs.ErrNotExist {
		// default thumbnail
		err = copyStatic(up.FilePath, Thumbnail(fromName), WebFiles, "web/static/document.png")
	}
	return err
}

// staticForExt returns a static thumbnail image for a file extension
func thumbnailForExt(ext string) string {
	return "web/static/" + ext[1:] + ".png"
}