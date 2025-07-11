// Copyright © Rob Burke inchworks.com, 2024.

package uploader

// Image file processing.

import (
	"errors"
	"image"
	"image/color"
	"image/jpeg"
	_ "image/gif"
	"image/png"
	"io"
	"os"
	"path/filepath"
	"runtime"

	_ "golang.org/x/image/bmp"
	"golang.org/x/image/draw"
	_ "golang.org/x/image/tiff"
	_ "golang.org/x/image/webp"
)

// decodeImage reads and decodes an image, with any EXIF orientation applied.
func decodeImage(r io.Reader) (image.Image, orientation, error) {

	// EXIF orientation is extracted in parallel with image decoding
	var orient orientation
	pr, pw := io.Pipe()
	r = io.TeeReader(r, pw)
	done := make(chan struct{})
	go func() {
		defer close(done)
		orient = ReadOrientation(pr)
		io.Copy(io.Discard, pr)
	}()

	// decode image
	img, _, err := image.Decode(r)

	// end orientation extraction
	pw.Close()
	<-done

	if err != nil {
		return nil, orientationUnspecified, err
	}

	return img, orient, nil
}

// decodeImageConfig returns the size of an uncompressed image.
// This is the only safe way to protect against arbitrarily large images which might be malformed.
func decodeImageConfig(r io.Reader) (height int, width int, pxSize int, err error) {

	config, _, err := image.DecodeConfig(r)
	if err != nil {
		return
	}
	height = config.Height
	width = config.Width

	switch config.ColorModel {
	case color.AlphaModel, color.GrayModel:
		pxSize = 1
	case color.Alpha16Model, color.Gray16Model:
		pxSize = 2
	case color.RGBAModel, color.NRGBAModel:
		pxSize = 4
	default: // color.RGBA64Model, color.NRGBA64Model and a pessimistic assumption about anything else
		pxSize = 8
	}
	return
}

// fit scales down the image to fit the specified maximum dimensions and returns the transformed image.
func fit(img image.Image, max image.Point) image.Image {

	// calculate new size
	if max.X <= 0 || max.Y <= 0 {
		return &image.NRGBA{} // invalid limits
	}

	srcBounds := img.Bounds()
	srcW := srcBounds.Dx()
	srcH := srcBounds.Dy()

	if srcW <= 0 || srcH <= 0 {
		return &image.NRGBA{}
	}

	if srcW <= max.X && srcH <= max.Y {
		return img // don't scale up
	}

	srcAspectRatio := float64(srcW) / float64(srcH)
	maxAspectRatio := float64(max.X) / float64(max.Y)

	var newW, newH int
	if srcAspectRatio > maxAspectRatio {
		newW = max.X
		newH = int(float64(newW) / srcAspectRatio)
	} else {
		newH = max.Y
		newW = int(float64(newH) * srcAspectRatio)
	}
	// smaller size
	dst := image.NewNRGBA(image.Rect(0, 0, newW, newH))

	// resize
	draw.CatmullRom.Scale(dst, dst.Rect, img, img.Bounds(), draw.Over, nil)
	return dst
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

	// we have already checked the file size, but we must also check image dimensions,
	// as the image could be malformed or super-compressed, perhaps maliciously.
	h, w, px, err := decodeImageConfig(tf)
	if err != nil {
		tf.Close()
		return err
	}
	if h * w * px > up.MaxDecoded {
		return errors.New("uploader: Decoded image too large")
	}

	// rewind to read for real
	tf.Seek(0, io.SeekStart)

	// decode image
	img, orient, err := decodeImage(tf)
	tf.Close()

	// does image need resizing
	// Note that we cannot check after decodeImageConfig because orientation isn't available at that stage.
	resize := false
	size := OrientationSize(img.Bounds().Size(), orient)
	if size.X > up.MaxW || size.Y > up.MaxH {
		resize = true
	}

	// convert non-displayable file types to JPG
	toName := changePrefix("M", changeExt(req.name, req.format.toType))

	// path for saved files
	toPath := filepath.Join(up.FilePath, toName)
	thumbPath := filepath.Join(up.FilePath, Thumbnail(toName))

	// rename uploaded image if it was small enough to use unchanged
	if !resize && !req.format.convert {

		if err := os.Rename(fromPath, toPath); err != nil {
			return err
		}

	} else {

		// make smaller image, adjusting for orientation
		// ## Could sharpen, but how much?
		img = fit(img, OrientationSize(image.Point{X: up.MaxW, Y: up.MaxH}, orient))
		runtime.Gosched()

		// apply any re-orientation (after resizing, for efficiency)
		img = FixOrientation(img, orient)
		orient = orientationNormal // fixed for thumbnail too
		runtime.Gosched()

		// save and delete original
		if err := saveImageAs(img, toPath, up.ImageQuality); err != nil {
			return err // ## could be a bad name?
		}
		if err := os.Remove(fromPath); err != nil {
			return err
		}
	}

	// save thumbnail
	if err := up.saveThumbnail(img, orient, thumbPath); err != nil {
		return err
	}

	return nil
}

// saveImage saves the image to a file with the specified format.
func saveImageAs(img image.Image, name string, quality int) error {
	file, err := os.Create(name)
	if err != nil {
		return err
	}

	switch filepath.Ext(name) {
	case ".jpg": 
		err = jpeg.Encode(file, img, &jpeg.Options{Quality: quality})
	
	case ".png":
		err = png.Encode(file, img)
	}

	if errc := file.Close(); errc != nil {
		return errc
	}
	return err
}

// saveThumbnail generates a thumbnail for an image.
func (up *Uploader) saveThumbnail(img image.Image, o orientation, name string) error {
	thumbnail := fit(img, image.Point{X: up.ThumbW, Y: up.ThumbH})
	thumbnail = FixOrientation(thumbnail, o)

	return saveImageAs(thumbnail, name, up.ImageQuality)
}
