// Copyright © Rob Burke inchworks.com, 2021.

package uploader

// Audio and video file processing.

import (
	"errors"
	"fmt"
	"image"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/disintegration/imaging"
)

// convert saves a media file in the specified type and returns the converted file name.
func (up *Uploader) convert(req reqSave, toType string, outOpt ...string) (string, error) {

	fromName := req.name
	fromPath := filepath.Join(up.FilePath, fromName)

	// output file
	toName := changePrefix("M", fromName)
	toName = changeExt(toName, toType)

	// the file may have already been converted, if we are redoing the operations
	ok, err := exists(fromPath)
	if err != nil {
		return "", err // file system error
	}
	if !ok {
		if ok, err = exists(filepath.Join(up.FilePath, toName)); err != nil {
			return "", err  // unlikely file system error
		}
		if !ok {
			return "", errors.New("Uploaded media file " + fromName + " missing")
		} else {
			return toName, nil // conversion already complete
		}
	}

	// convert to specified type
	args := []string{
		"-v", "error",
		"-i", fromName}
	args = append(args, outOpt...)
	args = append(args, toName)
	err = up.ffmpeg(args...)

	// remove original
	if err == nil {
		err = os.Remove(fromPath)
	}
	return toName, err
}

// convertAV saves an audio or video file.
func (up *Uploader) convertAV(req reqSave) (string, error) {

	switch req.mediaType {
	case MediaAudio:
		return up.convert(req, req.toType,
			"-c:a", "aac",
			"-b:a, 128k",
		)
	case MediaVideo:
		return up.convert(req, req.toType,
			"-vf", fmt.Sprint("scale=-2:'min(", up.VideoResolution, ",ih)'"),
			"-c:v", "libx264",
			"-preset", "fast",
			"-c:a", "aac")
	}
	return "", errors.New("Unsupported AV type")
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

// saveVideo saves the video file and a thumbnail. It returns true if format conversion is needed.
func (up *Uploader) saveAV(req reqSave) (bool, error) {

	var err error
	fromName := req.name

	// convert non-displayable AV formats, if we can
	fromPath := filepath.Join(up.FilePath, fromName)
	toName, toType, convert := changeType(req.name, up.AudioTypes, up.VideoTypes)
	if toName == "" {
		return false, errors.New("uploader: Unsupported file " + req.name) // ## shouldn't get this far?
	}

	if up.VideoPackage != "" {
		if !convert {
			// is file small enough to keep the original unprocessed?
			fi, err := os.Stat(fromPath)
			if err == nil && fi.Size() > int64(up.MaxSize) {
				req.toType = toType
				convert = true
			}
		}
	}
	if convert {
		req.toType = toType
		up.chConvert <- req

	} else {
		// rename to a permanent file
		toName = changePrefix("M", toName)
		if err = os.Rename(fromPath, filepath.Join(up.FilePath, toName)); err != nil {
			return false, err
		}
	}

	switch req.mediaType {
	case MediaAudio:
		// add a dummy thumbnail
		err = copyStatic(up.FilePath, Thumbnail(fromName), WebFiles, "web/static/audio.png")

	case MediaVideo:
		// extract thumbnail from video (quicker to do after conversion)
		if !convert {
			err = up.saveSnapshot(toName)
		}
	}

	return convert, err
}

// frame generates a freeze frame image, and returns its path.
func (up *Uploader) snapshot(fromName string, prefix string, after time.Duration) (string, error) {

	// output file name
	to := prefix + stem(fromName[1:]) + ".jpg"
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

// avWorker does background audio/video processing.
func (up *Uploader) avWorker(
	chConvert <-chan reqSave,
	done <-chan bool) {

	for {
		select {
		case req := <-chConvert:
			// convert audio or video
			toName, err := up.convertAV(req)
			if err == nil {
				// extract snapshot from converted video
				err = up.saveSnapshot(changeExt(toName, req.toType))
			}
			if err != nil {
				up.errorLog.Print(err.Error())
			}

			up.opDone(req.claim)

		case <-done:
			// ## do something to finish other pending requests
			return
		}
	}
}
