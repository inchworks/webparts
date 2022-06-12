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
func (up *Uploader) convert(fromName string, toType string) error {

	fromPath := filepath.Join(up.FilePath, fromName)

	// the file may have already been converted, if we are redoing the operations
	if exists, err := exists(fromPath); err != nil {
		return err
	} else if !exists {
		return nil
	}

	// output file
	to := strings.TrimSuffix(fromName, filepath.Ext(fromName)) + toType

	// convert to specified type
	err := up.ffmpeg("-v", "error", "-i", fromName, to)

	// remove original
	if err == nil {
		err = os.Remove(fromPath)
	}
	return err
}

// exists returns true if a file already exists
func exists(path string) (bool, error) {
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		} else {
			return false, err
		}
	} else {
		return true, err
	}
}

// saveSnapshot saves a video thumbnail.
func (up *Uploader) saveSnapshot(videoName string) error {

	var err error
	if up.SnapshotAt >= 0 {

		// get snapshot for thumbnail (if possible; may fail for e.g. tiny video)
		var snPath string
		snPath, err = up.snapshot(videoName, "S", up.SnapshotAt)

		// read full-size snapshot
		var sn *os.File
		var img image.Image
		if err == nil {
			sn, err = os.Open(snPath)
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
		err = copyStatic(up.FilePath, Thumbnail(videoName), WebFiles, "web/static/video.jpg")
	}
	return err
}

// saveVideo saves the video file and a thumbnail. It returns true if no format conversion is needed.
func (up *Uploader) saveVideo(req reqSave) (bool, error) {

	// convert non-displable file types to MP3
	name, convert := changeType(req.name, []string{}, up.VideoTypes)
	if convert {
		name = req.name // keep orginal name for files to be converted
	}

	// path for saved file
	fn := FileFromName(req.tx, name)
	path := filepath.Join(up.FilePath, fn)

	// save uploaded video file
	video, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return true, err // could be a bad name?
	}
	_, err = io.Copy(video, &req.fullsize)
	video.Close()
	if err != nil {
		return true, err
	}

	// add a snapshot thumbnail
	err = up.saveSnapshot(fn)
	if err != nil {
		return true, err
	}

	// convert video format, if we can
	if convert && up.VideoPackage != "" {
		up.chConvert <- reqConvert{file: fn, tx: req.tx}
		return false, nil
	} else {
		// #### could use "ffmpeg -f null" to validate as a video
		return true, nil // done
	}
}

// frame generates a freeze frame image, and returns its path.
func (up *Uploader) snapshot(fromName string, prefix string, after time.Duration) (string, error){

	// output file name
	to := prefix + strings.TrimSuffix(fromName[1:], filepath.Ext(fromName)) + ".jpg"
	toPath := filepath.Join(up.FilePath, to)

	// the snapshot may have already been created, if we are redoing the operations, and FFmpeg will not overwrite it
	if exists, err := exists(toPath); err != nil {
		return "", err
	} else if exists {
		return toPath, nil
	}

	// take a snapshot
	if err := up.ffmpeg("-v", "error", "-ss", strDuration(after), "-i", fromName, "-vframes", "1", to); err != nil {
		return "", err
	} else {
		return toPath, nil
	}
}

// ffmpeg executes an FFmpeg command, either direct or using Docker (as a convenience for testing on MacOS).
func (up *Uploader) ffmpeg(arg ...string) error {

	// absolute path to files
	abs, err := filepath.Abs(up.FilePath)
	if err != nil {
		return err
	}

	var c *exec.Cmd
	if up.VideoPackage == "ffmpeg" {
		// a direct command to the local implementation of FFmpeg
		c = exec.Command("ffmpeg", arg...)
		c.Dir = abs

	} else {
		// map directory to container working directory
		volume := abs + ":/uploader"

		// run FFmpeg in a Docker container
		dockerArgs := []string{"run", "-v", volume, "-w", "/uploader", up.VideoPackage}
		dockerArgs = append(dockerArgs, arg...)

		c = exec.Command("docker", dockerArgs...)
	}
	c.Stderr = up.errorLog.Writer()
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
