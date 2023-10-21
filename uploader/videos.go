// Copyright © Rob Burke inchworks.com, 2021.

package uploader

// Video file processing.

import (
	"fmt"
	"image"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/disintegration/imaging"

	"github.com/inchworks/webparts/etx"
)

type reqConvert struct {
	name string
	tx etx.TxId
}

// convert saves a media file in the specified type.
func (up *Uploader) convert(req reqConvert, toType string, arg ...string) error {

	fromName := fileFromNameNew("T", req.tx, req.name)
	fromPath := filepath.Join(up.FilePath, fromName)

	// the file may have already been converted, if we are redoing the operations
	// #### can this really be redone?
	if exists, err := exists(fromPath); err != nil {
		return err
	} else if !exists {
		return nil
	}

	// output file
	toName := fileFromNameNew("P", req.tx, req.name)
	toName = strings.TrimSuffix(toName, filepath.Ext(toName)) + toType

	// convert to specified type
	args := []string{
		"-v", "error",
		"-i", fromName}
	args = append(args, arg...)
	args = append(args,"-i", fromName, toName)
	err := up.ffmpeg(arg...)

	// remove original
	if err == nil {
		err = os.Remove(fromPath)
	}
	return err
}

// convertAudio saves an audio file.
func (up *Uploader) convertAudio(req reqConvert, toType string) error {

	return up.convert(req, ".m4a",
		"-c:a", "aac",
		"-b:a, 128k",
	)
}

// convertVideo saves a video file.
func (up *Uploader) convertVideo(req reqConvert) error {

	return up.convert(req, ".mp4",
		"-vf", fmt.Sprint("scale=-2:'min(", up.VideoResolution, ",ih)'"),
		"-c:v", "libx264",
		"-preset", "fast",
		"-c:a", "aac")
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

	// temporary file
	from := fileFromNameNew("T", req.tx, req.name)

	// add a snapshot thumbnail
	err := up.saveSnapshot(from)
	if err != nil {
		return true, err
	}

	// convert non-displayable video formats to MP4, if we can
	_, convert := changeType(req.name, []string{}, up.VideoTypes)
	if convert && up.VideoPackage != "" {
		up.chConvert <- reqConvert{name: req.name, tx: req.tx}
		return false, nil

	} else {
		// rename to a permanent file
		to := fileFromNameNew("P", req.tx, req.name)
		err := os.Rename(filepath.Join(up.FilePath, from), filepath.Join(up.FilePath, to))
			return true, err
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
			if err := up.convertVideo(req); err != nil {
				up.errorLog.Print(err.Error())
			}
			up.opDone(req.tx)

		case <-done:
			// ## do something to finish other pending requests
			return
		}
	}
}
