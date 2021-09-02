// Copyright © Rob Burke inchworks.com, 2021.

package uploader

// Video file processing.

import (
	"fmt"
	"image"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/disintegration/imaging"

	"github.com/inchworks/webparts/etx"
)

type reqConvert struct {
	file string
	tx etx.TxId
}

// convert saves a video file in the specified type.
func (up *Uploader) convert(file string, toType string) error {

	// output file name
	to := strings.TrimSuffix(file, filepath.Ext(file)) + "." + toType

	// the file may have already been converted, if we are redoing the operations
	if _, err := os.Stat(file); err != nil {
		if os.IsNotExist(err) {
			return nil
		} else {
			return err
		}
	}

	// convert to specified type
	err := ffmpeg("-i", file, to)

	// remove original
	if err != nil {
		err = os.Remove(file)
	}
	return err
}

// saveSnapshot saves a video thumbnail.
func (up *Uploader) saveSnapshot(videoPath string) error {

	var err error
	if up.SnapshotAt >= 0 {

		// get snapshot for thumbnail (if possible; may fail for e.g. tiny video)
		var snPath string
		snPath, err = snapshot(videoPath, "S-", up.SnapshotAt)

		// read full-size snapshot
		var sn *os.File
		var img image.Image
		if err == nil {
			sn, err = os.Open(videoPath)
		}
		if err == nil {
			img, err = imaging.Decode(sn, imaging.AutoOrientation(true))
			sn.Close()
		}

		if err == nil {
			// save thumbnail, assuming we can overwrite the full-sized image
			err = up.saveThumbnail(img, snPath)
		}

		if err != nil {
			up.errorLog.Print(err.Error())
		}
	}

	if up.SnapshotAt < 0 || err != nil {
		// dummy thumbnail, instead
		err = copyStatic(up.FilePath, Thumbnail(videoPath), WebFiles, "web/static/video.jpg")
	}
	return err
}

// saveVideo saves the video file and a thumbnail. It returns true if no format conversion is needed.
func (up *Uploader) saveVideo(req reqSave) (bool, error) {

	// path for saved file
	fn := FileFromName(req.tx, req.name)
	videoPath := filepath.Join(up.FilePath, fn)

	// save uploaded video file
	video, err := os.OpenFile(videoPath, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return true, err // could be a bad name?
	}
	_, err = io.Copy(video, &req.fullsize)
	video.Close()
	if err != nil {
		return true, err
	}

	// add a snapshot thumbnail
	err = up.saveSnapshot(videoPath)
	if err != nil {
		return true, err
	}

	// convert video format
	t := strings.ToLower(filepath.Ext(fn))
	if t != ".mp4" {
		up.chConvert <- reqConvert{file: videoPath, tx: req.tx}
		return false, nil
	} else {
		// #### could use "ffmpeg -f null" to validate as a video
		return true, nil // done
	}
}

// frame generates a freeze frame image, and returns its path.
func snapshot(file string, prefix string, after time.Duration) (string, error){

	// output file name
	to := prefix + strings.TrimSuffix(file[1:], filepath.Ext(file)) + ".jpg"

	if err := ffmpeg("-ss", strDuration(after), "-i", file, "-vframes", "1", to); err != nil {
		return "", err
	} else {
		return to, nil
	}
}

// ffmpeg executes an FFMPEG command.
func ffmpeg(arg ...string) error {

	c := exec.Command("ffmpeg", arg...)
	return c.Run()
}

// strDuration returns a duration in hh:mm:ss format.
func strDuration(d time.Duration) string {
	d = d.Round(time.Second)
	h := d / time.Hour
	d -= h * time.Hour
	m := d / time.Minute
	d -= m * time.Minute
	s := d / time.Second
	return fmt.Sprintf("%02d:%02d:%02d", h, m, s)
}

// videoWorker does background video processing.
func (up *Uploader) videoWorker(
	chConvert <-chan reqConvert,
	done <-chan bool) {

	for {
		select {
		case req := <-chConvert:

			// convert video
			if err := up.convert(req.file, ".mp4"); err != nil {
				up.errorLog.Print(err.Error())
			}
			up.opDone(req.tx)

		case <-done:
			// ## do something to finish other pending requests
			return
		}
	}
}
