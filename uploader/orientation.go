// Copyright © Rob Burke inchworks.com, 2024.

package uploader

// Image orientation EXIF processing.
// This is an extract of disintegration/imaging support for orientation.
// (The package was removed because of an unresolved Dependabot issue).

import (
	"encoding/binary"
	"image"
	"image/color"
	"io"
	"runtime"
	"sync"
)

// orientation is an EXIF flag that specifies the transformation
// that should be applied to image to display it correctly.
type orientation int

const (
	orientationUnspecified = 0
	orientationNormal      = 1
	orientationFlipH       = 2
	orientationRotate180   = 3
	orientationFlipV       = 4
	orientationTranspose   = 5
	orientationRotate270   = 6
	orientationTransverse  = 7
	orientationRotate90    = 8
)

// OrientationSize returns the dimensions of the image after orientation is changed.
func OrientationSize(size image.Point, o orientation) image.Point {
	switch o {
	case orientationRotate90, orientationRotate270, orientationTranspose, orientationTransverse:
		return image.Point{X: size.Y, Y:size.X}
	default:
		return size // orientationUnspecified, orientationNormal, orientationFlipH, orientationFlipV, orientationRotate180
	}
}

// ReadOrientation tries to read the orientation EXIF flag from image data in r.
// If the EXIF data block is not found or the orientation flag is not found
// or any other error occures while reading the data, it returns the
// orientationUnspecified (0) value.
func ReadOrientation(r io.Reader) orientation {
	const (
		markerSOI      = 0xffd8
		markerAPP1     = 0xffe1
		exifHeader     = 0x45786966
		byteOrderBE    = 0x4d4d
		byteOrderLE    = 0x4949
		orientationTag = 0x0112
	)

	// Check if JPEG SOI marker is present.
	var soi uint16
	if err := binary.Read(r, binary.BigEndian, &soi); err != nil {
		return orientationUnspecified
	}
	if soi != markerSOI {
		return orientationUnspecified // Missing JPEG SOI marker.
	}

	// Find JPEG APP1 marker.
	for {
		var marker, size uint16
		if err := binary.Read(r, binary.BigEndian, &marker); err != nil {
			return orientationUnspecified
		}
		if err := binary.Read(r, binary.BigEndian, &size); err != nil {
			return orientationUnspecified
		}
		if marker>>8 != 0xff {
			return orientationUnspecified // Invalid JPEG marker.
		}
		if marker == markerAPP1 {
			break
		}
		if size < 2 {
			return orientationUnspecified // Invalid block size.
		}
		if _, err := io.CopyN(io.Discard, r, int64(size-2)); err != nil {
			return orientationUnspecified
		}
	}

	// Check if EXIF header is present.
	var header uint32
	if err := binary.Read(r, binary.BigEndian, &header); err != nil {
		return orientationUnspecified
	}
	if header != exifHeader {
		return orientationUnspecified
	}
	if _, err := io.CopyN(io.Discard, r, 2); err != nil {
		return orientationUnspecified
	}

	// Read byte order information.
	var (
		byteOrderTag uint16
		byteOrder    binary.ByteOrder
	)
	if err := binary.Read(r, binary.BigEndian, &byteOrderTag); err != nil {
		return orientationUnspecified
	}
	switch byteOrderTag {
	case byteOrderBE:
		byteOrder = binary.BigEndian
	case byteOrderLE:
		byteOrder = binary.LittleEndian
	default:
		return orientationUnspecified // Invalid byte order flag.
	}
	if _, err := io.CopyN(io.Discard, r, 2); err != nil {
		return orientationUnspecified
	}

	// Skip the EXIF offset.
	var offset uint32
	if err := binary.Read(r, byteOrder, &offset); err != nil {
		return orientationUnspecified
	}
	if offset < 8 {
		return orientationUnspecified // Invalid offset value.
	}
	if _, err := io.CopyN(io.Discard, r, int64(offset-8)); err != nil {
		return orientationUnspecified
	}

	// Read the number of tags.
	var numTags uint16
	if err := binary.Read(r, byteOrder, &numTags); err != nil {
		return orientationUnspecified
	}

	// Find the orientation tag.
	for i := 0; i < int(numTags); i++ {
		var tag uint16
		if err := binary.Read(r, byteOrder, &tag); err != nil {
			return orientationUnspecified
		}
		if tag != orientationTag {
			if _, err := io.CopyN(io.Discard, r, 10); err != nil {
				return orientationUnspecified
			}
			continue
		}
		if _, err := io.CopyN(io.Discard, r, 6); err != nil {
			return orientationUnspecified
		}
		var val uint16
		if err := binary.Read(r, byteOrder, &val); err != nil {
			return orientationUnspecified
		}
		if val < 1 || val > 8 {
			return orientationUnspecified // Invalid tag value.
		}
		return orientation(val)
	}
	return orientationUnspecified // Missing orientation tag.
}

// FixOrientation applies a transform to img corresponding to the given orientation flag.
func FixOrientation(img image.Image, o orientation) image.Image {
	switch o {
	case orientationNormal:
	case orientationFlipH:
		img = flipH(img)
	case orientationFlipV:
		img = flipV(img)
	case orientationRotate90:
		img = rotate90(img)
	case orientationRotate180:
		img = rotate180(img)
	case orientationRotate270:
		img = rotate270(img)
	case orientationTranspose:
		img = transpose(img)
	case orientationTransverse:
		img = transverse(img)
	}
	return img
}

// TRANSFORMATIONS

// flipH flips the image horizontally (from left to right) and returns the transformed image.
func flipH(img image.Image) *image.NRGBA {
	src := newScanner(img)
	dstW := src.w
	dstH := src.h
	rowSize := dstW * 4
	dst := image.NewNRGBA(image.Rect(0, 0, dstW, dstH))
	parallel(0, dstH, func(ys <-chan int) {
		for dstY := range ys {
			i := dstY * dst.Stride
			srcY := dstY
			src.scan(0, srcY, src.w, srcY+1, dst.Pix[i:i+rowSize])
			reverse(dst.Pix[i : i+rowSize])
		}
	})
	return dst
}

// flipV flips the image vertically (from top to bottom) and returns the transformed image.
func flipV(img image.Image) *image.NRGBA {
	src := newScanner(img)
	dstW := src.w
	dstH := src.h
	rowSize := dstW * 4
	dst := image.NewNRGBA(image.Rect(0, 0, dstW, dstH))
	parallel(0, dstH, func(ys <-chan int) {
		for dstY := range ys {
			i := dstY * dst.Stride
			srcY := dstH - dstY - 1
			src.scan(0, srcY, src.w, srcY+1, dst.Pix[i:i+rowSize])
		}
	})
	return dst
}

// rotate90 rotates the image 90 degrees counter-clockwise and returns the transformed image.
func rotate90(img image.Image) *image.NRGBA {
	src := newScanner(img)
	dstW := src.h
	dstH := src.w
	rowSize := dstW * 4
	dst := image.NewNRGBA(image.Rect(0, 0, dstW, dstH))
	parallel(0, dstH, func(ys <-chan int) {
		for dstY := range ys {
			i := dstY * dst.Stride
			srcX := dstH - dstY - 1
			src.scan(srcX, 0, srcX+1, src.h, dst.Pix[i:i+rowSize])
		}
	})
	return dst
}

// rotate180 rotates the image 180 degrees counter-clockwise and returns the transformed image.
func rotate180(img image.Image) *image.NRGBA {
	src := newScanner(img)
	dstW := src.w
	dstH := src.h
	rowSize := dstW * 4
	dst := image.NewNRGBA(image.Rect(0, 0, dstW, dstH))
	parallel(0, dstH, func(ys <-chan int) {
		for dstY := range ys {
			i := dstY * dst.Stride
			srcY := dstH - dstY - 1
			src.scan(0, srcY, src.w, srcY+1, dst.Pix[i:i+rowSize])
			reverse(dst.Pix[i : i+rowSize])
		}
	})
	return dst
}

// rotate270 rotates the image 270 degrees counter-clockwise and returns the transformed image.
func rotate270(img image.Image) *image.NRGBA {
	src := newScanner(img)
	dstW := src.h
	dstH := src.w
	rowSize := dstW * 4
	dst := image.NewNRGBA(image.Rect(0, 0, dstW, dstH))
	parallel(0, dstH, func(ys <-chan int) {
		for dstY := range ys {
			i := dstY * dst.Stride
			srcX := dstY
			src.scan(srcX, 0, srcX+1, src.h, dst.Pix[i:i+rowSize])
			reverse(dst.Pix[i : i+rowSize])
		}
	})
	return dst
}

// transpose flips the image horizontally and rotates 90 degrees counter-clockwise.
func transpose(img image.Image) *image.NRGBA {
	src := newScanner(img)
	dstW := src.h
	dstH := src.w
	rowSize := dstW * 4
	dst := image.NewNRGBA(image.Rect(0, 0, dstW, dstH))
	parallel(0, dstH, func(ys <-chan int) {
		for dstY := range ys {
			i := dstY * dst.Stride
			srcX := dstY
			src.scan(srcX, 0, srcX+1, src.h, dst.Pix[i:i+rowSize])
		}
	})
	return dst
}

// transverse flips the image vertically and rotates 90 degrees counter-clockwise.
func transverse(img image.Image) *image.NRGBA {
	src := newScanner(img)
	dstW := src.h
	dstH := src.w
	rowSize := dstW * 4
	dst := image.NewNRGBA(image.Rect(0, 0, dstW, dstH))
	parallel(0, dstH, func(ys <-chan int) {
		for dstY := range ys {
			i := dstY * dst.Stride
			srcX := dstH - dstY - 1
			src.scan(srcX, 0, srcX+1, src.h, dst.Pix[i:i+rowSize])
			reverse(dst.Pix[i : i+rowSize])
		}
	})
	return dst
}

// UTILITIES

// parallel processes the data in separate goroutines.
func parallel(start, stop int, fn func(<-chan int)) {
	count := stop - start
	if count < 1 {
		return
	}

	procs := runtime.GOMAXPROCS(0)
	if procs > count {
		procs = count
	}

	c := make(chan int, count)
	for i := start; i < stop; i++ {
		c <- i
	}
	close(c)

	var wg sync.WaitGroup
	for i := 0; i < procs; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			fn(c)
		}()
	}
	wg.Wait()
}

func reverse(pix []uint8) {
	if len(pix) <= 4 {
		return
	}
	i := 0
	j := len(pix) - 4
	for i < j {
		pi := pix[i : i+4 : i+4]
		pj := pix[j : j+4 : j+4]
		pi[0], pj[0] = pj[0], pi[0]
		pi[1], pj[1] = pj[1], pi[1]
		pi[2], pj[2] = pj[2], pi[2]
		pi[3], pj[3] = pj[3], pi[3]
		i += 4
		j -= 4
	}
}

// IMAGE SCANNER

type scanner struct {
	image   image.Image
	w, h    int
	palette []color.NRGBA
}

func newScanner(img image.Image) *scanner {
	s := &scanner{
		image: img,
		w:     img.Bounds().Dx(),
		h:     img.Bounds().Dy(),
	}
	if img, ok := img.(*image.Paletted); ok {
		s.palette = make([]color.NRGBA, len(img.Palette))
		for i := 0; i < len(img.Palette); i++ {
			s.palette[i] = color.NRGBAModel.Convert(img.Palette[i]).(color.NRGBA)
		}
	}
	return s
}

// scan scans the given rectangular region of the image into dst.
func (s *scanner) scan(x1, y1, x2, y2 int, dst []uint8) {
	switch img := s.image.(type) {
	case *image.NRGBA:
		size := (x2 - x1) * 4
		j := 0
		i := y1*img.Stride + x1*4
		if size == 4 {
			for y := y1; y < y2; y++ {
				d := dst[j : j+4 : j+4]
				s := img.Pix[i : i+4 : i+4]
				d[0] = s[0]
				d[1] = s[1]
				d[2] = s[2]
				d[3] = s[3]
				j += size
				i += img.Stride
			}
		} else {
			for y := y1; y < y2; y++ {
				copy(dst[j:j+size], img.Pix[i:i+size])
				j += size
				i += img.Stride
			}
		}

	case *image.NRGBA64:
		j := 0
		for y := y1; y < y2; y++ {
			i := y*img.Stride + x1*8
			for x := x1; x < x2; x++ {
				s := img.Pix[i : i+8 : i+8]
				d := dst[j : j+4 : j+4]
				d[0] = s[0]
				d[1] = s[2]
				d[2] = s[4]
				d[3] = s[6]
				j += 4
				i += 8
			}
		}

	case *image.RGBA:
		j := 0
		for y := y1; y < y2; y++ {
			i := y*img.Stride + x1*4
			for x := x1; x < x2; x++ {
				d := dst[j : j+4 : j+4]
				a := img.Pix[i+3]
				switch a {
				case 0:
					d[0] = 0
					d[1] = 0
					d[2] = 0
					d[3] = a
				case 0xff:
					s := img.Pix[i : i+4 : i+4]
					d[0] = s[0]
					d[1] = s[1]
					d[2] = s[2]
					d[3] = a
				default:
					s := img.Pix[i : i+4 : i+4]
					r16 := uint16(s[0])
					g16 := uint16(s[1])
					b16 := uint16(s[2])
					a16 := uint16(a)
					d[0] = uint8(r16 * 0xff / a16)
					d[1] = uint8(g16 * 0xff / a16)
					d[2] = uint8(b16 * 0xff / a16)
					d[3] = a
				}
				j += 4
				i += 4
			}
		}

	case *image.RGBA64:
		j := 0
		for y := y1; y < y2; y++ {
			i := y*img.Stride + x1*8
			for x := x1; x < x2; x++ {
				s := img.Pix[i : i+8 : i+8]
				d := dst[j : j+4 : j+4]
				a := s[6]
				switch a {
				case 0:
					d[0] = 0
					d[1] = 0
					d[2] = 0
				case 0xff:
					d[0] = s[0]
					d[1] = s[2]
					d[2] = s[4]
				default:
					r32 := uint32(s[0])<<8 | uint32(s[1])
					g32 := uint32(s[2])<<8 | uint32(s[3])
					b32 := uint32(s[4])<<8 | uint32(s[5])
					a32 := uint32(s[6])<<8 | uint32(s[7])
					d[0] = uint8((r32 * 0xffff / a32) >> 8)
					d[1] = uint8((g32 * 0xffff / a32) >> 8)
					d[2] = uint8((b32 * 0xffff / a32) >> 8)
				}
				d[3] = a
				j += 4
				i += 8
			}
		}

	case *image.Gray:
		j := 0
		for y := y1; y < y2; y++ {
			i := y*img.Stride + x1
			for x := x1; x < x2; x++ {
				c := img.Pix[i]
				d := dst[j : j+4 : j+4]
				d[0] = c
				d[1] = c
				d[2] = c
				d[3] = 0xff
				j += 4
				i++
			}
		}

	case *image.Gray16:
		j := 0
		for y := y1; y < y2; y++ {
			i := y*img.Stride + x1*2
			for x := x1; x < x2; x++ {
				c := img.Pix[i]
				d := dst[j : j+4 : j+4]
				d[0] = c
				d[1] = c
				d[2] = c
				d[3] = 0xff
				j += 4
				i += 2
			}
		}

	case *image.YCbCr:
		j := 0
		x1 += img.Rect.Min.X
		x2 += img.Rect.Min.X
		y1 += img.Rect.Min.Y
		y2 += img.Rect.Min.Y

		hy := img.Rect.Min.Y / 2
		hx := img.Rect.Min.X / 2
		for y := y1; y < y2; y++ {
			iy := (y-img.Rect.Min.Y)*img.YStride + (x1 - img.Rect.Min.X)

			var yBase int
			switch img.SubsampleRatio {
			case image.YCbCrSubsampleRatio444, image.YCbCrSubsampleRatio422:
				yBase = (y - img.Rect.Min.Y) * img.CStride
			case image.YCbCrSubsampleRatio420, image.YCbCrSubsampleRatio440:
				yBase = (y/2 - hy) * img.CStride
			}

			for x := x1; x < x2; x++ {
				var ic int
				switch img.SubsampleRatio {
				case image.YCbCrSubsampleRatio444, image.YCbCrSubsampleRatio440:
					ic = yBase + (x - img.Rect.Min.X)
				case image.YCbCrSubsampleRatio422, image.YCbCrSubsampleRatio420:
					ic = yBase + (x/2 - hx)
				default:
					ic = img.COffset(x, y)
				}

				yy1 := int32(img.Y[iy]) * 0x10101
				cb1 := int32(img.Cb[ic]) - 128
				cr1 := int32(img.Cr[ic]) - 128

				r := yy1 + 91881*cr1
				if uint32(r)&0xff000000 == 0 {
					r >>= 16
				} else {
					r = ^(r >> 31)
				}

				g := yy1 - 22554*cb1 - 46802*cr1
				if uint32(g)&0xff000000 == 0 {
					g >>= 16
				} else {
					g = ^(g >> 31)
				}

				b := yy1 + 116130*cb1
				if uint32(b)&0xff000000 == 0 {
					b >>= 16
				} else {
					b = ^(b >> 31)
				}

				d := dst[j : j+4 : j+4]
				d[0] = uint8(r)
				d[1] = uint8(g)
				d[2] = uint8(b)
				d[3] = 0xff

				iy++
				j += 4
			}
		}

	case *image.Paletted:
		j := 0
		for y := y1; y < y2; y++ {
			i := y*img.Stride + x1
			for x := x1; x < x2; x++ {
				c := s.palette[img.Pix[i]]
				d := dst[j : j+4 : j+4]
				d[0] = c.R
				d[1] = c.G
				d[2] = c.B
				d[3] = c.A
				j += 4
				i++
			}
		}

	default:
		j := 0
		b := s.image.Bounds()
		x1 += b.Min.X
		x2 += b.Min.X
		y1 += b.Min.Y
		y2 += b.Min.Y
		for y := y1; y < y2; y++ {
			for x := x1; x < x2; x++ {
				r16, g16, b16, a16 := s.image.At(x, y).RGBA()
				d := dst[j : j+4 : j+4]
				switch a16 {
				case 0xffff:
					d[0] = uint8(r16 >> 8)
					d[1] = uint8(g16 >> 8)
					d[2] = uint8(b16 >> 8)
					d[3] = 0xff
				case 0:
					d[0] = 0
					d[1] = 0
					d[2] = 0
					d[3] = 0
				default:
					d[0] = uint8(((r16 * 0xffff) / a16) >> 8)
					d[1] = uint8(((g16 * 0xffff) / a16) >> 8)
					d[2] = uint8(((b16 * 0xffff) / a16) >> 8)
					d[3] = uint8(a16 >> 8)
				}
				j += 4
			}
		}
	}
}
