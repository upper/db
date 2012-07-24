/*
  Copyright (c) 2012 José Carlos Nieto, http://xiam.menteslibres.org/

  Permission is hereby granted, free of charge, to any person obtaining
  a copy of this software and associated documentation files (the
  "Software"), to deal in the Software without restriction, including
  without limitation the rights to use, copy, modify, merge, publish,
  distribute, sublicense, and/or sell copies of the Software, and to
  permit persons to whom the Software is furnished to do so, subject to
  the following conditions:

  The above copyright notice and this permission notice shall be
  included in all copies or substantial portions of the Software.

  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
  NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
  LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
  OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
  WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

package canvas

/*
#cgo LDFLAGS: -lMagickWand -lMagickCore
#cgo CFLAGS: -fopenmp -I/usr/include/ImageMagick  

#include <wand/magick_wand.h>

char *MagickGetPropertyName(char **properties, size_t index) {
  return properties[index];
}
*/
import "C"

import "math"

import "fmt"

import "unsafe"

import "strings"

import "strconv"

var (
	STROKE_BUTT_CAP   = uint(C.ButtCap)
	STROKE_ROUND_CAP  = uint(C.RoundCap)
	STROKE_SQUARE_CAP = uint(C.SquareCap)

	STROKE_MITER_JOIN = uint(C.MiterJoin)
	STROKE_ROUND_JOIN = uint(C.RoundJoin)
	STROKE_BEVEL_JOIN = uint(C.BevelJoin)

	FILL_EVEN_ODD_RULE = uint(C.EvenOddRule)
	FILL_NON_ZERO_RULE = uint(C.NonZeroRule)

	RAD_TO_DEG = 180 / math.Pi
	DEG_TO_RAD = math.Pi / 180

	UNDEFINED_ORIENTATION    = uint(C.UndefinedOrientation)
	TOP_LEFT_ORIENTATION     = uint(C.TopLeftOrientation)
	TOP_RIGHT_ORIENTATION    = uint(C.TopRightOrientation)
	BOTTOM_RIGHT_ORIENTATION = uint(C.BottomRightOrientation)
	BOTTOM_LEFT_ORIENTATION  = uint(C.BottomLeftOrientation)
	LEFT_TOP_ORIENTATION     = uint(C.LeftTopOrientation)
	RIGHT_TOP_ORIENTATION    = uint(C.RightTopOrientation)
	RIGHT_BOTTOM_ORIENTATION = uint(C.RightBottomOrientation)
	LEFT_BOTTOM_ORIENTATION  = uint(C.LeftBottomOrientation)
)

type Canvas struct {
	wand *C.MagickWand

	fg *C.PixelWand
	bg *C.PixelWand

	drawing *C.DrawingWand

	fill   *C.PixelWand
	stroke *C.PixelWand

	filename string
	width    string
	height   string
}

// Private: returns wand's hexadecimal color.
func getPixelHexColor(p *C.PixelWand) string {
	var rgb [3]float64

	rgb[0] = float64(C.PixelGetRed(p))
	rgb[1] = float64(C.PixelGetGreen(p))
	rgb[2] = float64(C.PixelGetBlue(p))

	return fmt.Sprintf("#%02x%02x%02x", int(rgb[0]*255.0), int(rgb[1]*255.0), int(rgb[2]*255.0))
}

// Private: returns MagickTrue or MagickFalse 
func magickBoolean(value bool) C.MagickBooleanType {
	if value == true {
		return C.MagickTrue
	}
	return C.MagickFalse
}

// Initializes the canvas environment.
func (cv Canvas) Init() {
	C.MagickWandGenesis()
}

// Opens an image file, returns true on success.
func (cv Canvas) Open(filename string) bool {
	cv.filename = filename
	status := C.MagickReadImage(cv.wand, C.CString(cv.filename))
	if status == C.MagickFalse {
		return false
	}
	return true
}

// Auto-orientates canvas based on its original image's EXIF metadata
func (cv Canvas) AutoOrientate() bool {

	data := cv.Metadata()

	orientation, err := strconv.Atoi(data["exif:Orientation"])

	if err != nil {
		return false
	}

	switch uint(orientation) {
	case TOP_LEFT_ORIENTATION:
		// normal

	case TOP_RIGHT_ORIENTATION:
		cv.Flop()

	case BOTTOM_RIGHT_ORIENTATION:
		cv.RotateCanvas(math.Pi)

	case BOTTOM_LEFT_ORIENTATION:
		cv.Flip()

	case LEFT_TOP_ORIENTATION:
		cv.Flip()
		cv.RotateCanvas(-math.Pi / 2)

	case RIGHT_TOP_ORIENTATION:
		cv.RotateCanvas(-math.Pi / 2)

	case RIGHT_BOTTOM_ORIENTATION:
		cv.Flop()
		cv.RotateCanvas(-math.Pi / 2)

	case LEFT_BOTTOM_ORIENTATION:
		cv.RotateCanvas(math.Pi / 2)

	default:
		return false
	}

	C.MagickSetImageOrientation(cv.wand, (C.OrientationType)(TOP_LEFT_ORIENTATION))
	cv.SetMetadata("exif:Orientation", (string)(TOP_LEFT_ORIENTATION))

	return true
}

// Returns all metadata keys from the currently loaded image.
func (cv Canvas) Metadata() map[string]string {
	var n C.size_t
	var i C.size_t

	var value *C.char
	var key *C.char

	data := make(map[string]string)

	properties := C.MagickGetImageProperties(cv.wand, C.CString("*"), &n)

	for i = 0; i < n; i++ {
		key = C.MagickGetPropertyName(properties, i)
		value = C.MagickGetImageProperty(cv.wand, key)

		data[strings.Trim(C.GoString(key), " ")] = strings.Trim(C.GoString(value), " ")

		C.MagickRelinquishMemory(unsafe.Pointer(value))
		C.MagickRelinquishMemory(unsafe.Pointer(key))
	}

	return data
}

// Associates a metadata key with its value.
func (cv Canvas) SetMetadata(key string, value string) {
	C.MagickSetImageProperty(cv.wand, C.CString(key), C.CString(value))
}

// Creates a horizontal mirror image by reflecting the pixels around the central y-axis.
func (cv Canvas) Flop() bool {
	status := C.MagickFlopImage(cv.wand)
	if status == C.MagickFalse {
		return false
	}
	return true
}

// Creates a vertical mirror image by reflecting the pixels around the central x-axis.
func (cv Canvas) Flip() bool {
	status := C.MagickFlipImage(cv.wand)
	if status == C.MagickFalse {
		return false
	}
	return true
}

// Creates a centered thumbnail of the canvas.
func (cv Canvas) Thumbnail(width uint, height uint) bool {

	var ratio float64

	// Normalizing image.

	ratio = math.Min(float64(cv.Width())/float64(width), float64(cv.Height())/float64(height))

	if ratio < 1.0 {
		// Origin image is smaller than the thumbnail image.
		max := uint(math.Max(float64(width), float64(height)))

		// Empty replacement buffer with transparent background.
		replacement := New()

		replacement.SetBackgroundColor("none")

		replacement.Blank(max, max)

		// Putting original image in the center of the replacement canvas.
		replacement.AppendCanvas(cv, int(int(width-cv.Width())/2), int(int(height-cv.Height())/2))

		// Replacing wand
		C.DestroyMagickWand(cv.wand)

		cv.wand = C.CloneMagickWand(replacement.wand)

	} else {
		// Is bigger, just resizing.
		cv.Resize(uint(float64(cv.Width())/ratio), uint(float64(cv.Height())/ratio))
	}

	// Now we have an image that we can use to crop the thumbnail from.
	cv.Crop(int(int(cv.Width()-width)/2), int(int(cv.Height()-height)/2), width, height)

	return true

}

// Puts a canvas on top of the current one.
func (cv Canvas) AppendCanvas(source Canvas, x int, y int) bool {
	status := C.MagickCompositeImage(cv.wand, source.wand, C.OverCompositeOp, C.ssize_t(x), C.ssize_t(y))
	if status == C.MagickFalse {
		return false
	}
	return true
}

// Rotates the whole canvas.
func (cv Canvas) RotateCanvas(rad float64) {
	C.MagickRotateImage(cv.wand, cv.bg, C.double(RAD_TO_DEG*rad))
}

// Returns canvas' width.
func (cv Canvas) Width() uint {
	return uint(C.MagickGetImageWidth(cv.wand))
}

// Returns canvas' height.
func (cv Canvas) Height() uint {
	return uint(C.MagickGetImageHeight(cv.wand))
}

// Writes canvas to a file, returns true on success.
func (cv Canvas) Write(filename string) bool {
	cv.Update()
	status := C.MagickWriteImage(cv.wand, C.CString(filename))
	if status == C.MagickFalse {
		return false
	}
	return true
}

// Changes the size of the canvas, returns true on success.
func (cv Canvas) Resize(width uint, height uint) bool {
	status := C.MagickResizeImage(cv.wand, C.size_t(width), C.size_t(height), C.GaussianFilter, C.double(1.0))
	if status == C.MagickFalse {
		return false
	}
	return true
}

// Adaptively changes the size of the canvas, returns true on success.
func (cv Canvas) AdaptiveResize(width uint, height uint) bool {
	status := C.MagickAdaptiveResizeImage(cv.wand, C.size_t(width), C.size_t(height))
	if status == C.MagickFalse {
		return false
	}
	return true
}

// Changes the compression quality of the canvas. Ranges from 1 (lowest) to 100 (highest).
func (cv Canvas) SetQuality(quality uint) bool {
	status := C.MagickSetImageCompressionQuality(cv.wand, C.size_t(quality))
	if status == C.MagickFalse {
		return false
	}
	return true
}

// Returns the compression quality of the canvas. Ranges from 1 (lowest) to 100 (highest).
func (cv Canvas) Quality() uint {
	return uint(C.MagickGetImageCompressionQuality(cv.wand))
}

/*
// Sets canvas's foreground color.
func (cv Canvas) SetColor(color string) (bool) {
  status := C.PixelSetColor(cv.fg, C.CString(color))
  if status == C.MagickFalse {
    return false
  }
  return true
}
*/

// Sets canvas' background color.
func (cv Canvas) SetBackgroundColor(color string) bool {
	C.PixelSetColor(cv.bg, C.CString(color))
	status := C.MagickSetImageBackgroundColor(cv.wand, cv.bg)
	if status == C.MagickFalse {
		return false
	}
	return true
}

// Returns canvas' background color.
func (cv Canvas) BackgroundColor() string {
	return getPixelHexColor(cv.bg)
}

// Sets antialiasing setting for the current drawing stroke.
func (cv Canvas) SetStrokeAntialias(value bool) {
	C.DrawSetStrokeAntialias(cv.drawing, magickBoolean(value))
}

// Returns antialiasing setting for the current drawing stroke.
func (cv Canvas) StrokeAntialias() bool {
	value := C.DrawGetStrokeAntialias(cv.drawing)
	if value == C.MagickTrue {
		return true
	}
	return false
}

// Sets the width of the stroke on the current drawing surface.
func (cv Canvas) SetStrokeWidth(value float64) {
	C.DrawSetStrokeWidth(cv.drawing, C.double(value))
}

// Returns the width of the stroke on the current drawing surface.
func (cv Canvas) StrokeWidth() float64 {
	return float64(C.DrawGetStrokeWidth(cv.drawing))
}

// Sets the opacity of the stroke on the current drawing surface.
func (cv Canvas) SetStrokeOpacity(value float64) {
	C.DrawSetStrokeOpacity(cv.drawing, C.double(value))
}

// Returns the opacity of the stroke on the current drawing surface.
func (cv Canvas) StrokeOpacity() float64 {
	return float64(C.DrawGetStrokeOpacity(cv.drawing))
}

// Sets the type of the line cap on the current drawing surface.
func (cv Canvas) SetStrokeLineCap(value uint) {
	C.DrawSetStrokeLineCap(cv.drawing, C.LineCap(value))
}

// Returns the type of the line cap on the current drawing surface.
func (cv Canvas) StrokeLineCap() uint {
	return uint(C.DrawGetStrokeLineCap(cv.drawing))
}

// Sets the type of the line join on the current drawing surface.
func (cv Canvas) SetStrokeLineJoin(value uint) {
	C.DrawSetStrokeLineJoin(cv.drawing, C.LineJoin(value))
}

// Returns the type of the line join on the current drawing surface.
func (cv Canvas) StrokeLineJoin() uint {
	return uint(C.DrawGetStrokeLineJoin(cv.drawing))
}

/*
func (cv Canvas) SetFillRule(value int) {
  C.DrawSetFillRule(cv.drawing, C.FillRule(value))
}
*/

// Sets the fill color for enclosed areas on the current drawing surface.
func (cv Canvas) SetFillColor(color string) {
	C.PixelSetColor(cv.fill, C.CString(color))
	C.DrawSetFillColor(cv.drawing, cv.fill)
}

// Returns the fill color for enclosed areas on the current drawing surface.
func (cv Canvas) FillColor() string {
	return getPixelHexColor(cv.fill)
}

// Sets the stroke color on the current drawing surface.
func (cv Canvas) SetStrokeColor(color string) {
	C.PixelSetColor(cv.stroke, C.CString(color))
	C.DrawSetStrokeColor(cv.drawing, cv.stroke)
}

// Returns the stroke color on the current drawing surface.
func (cv Canvas) StrokeColor() string {
	return getPixelHexColor(cv.stroke)
}

// Draws a circle over the current drawing surface.
func (cv Canvas) Circle(radius float64) {
	C.DrawCircle(cv.drawing, C.double(0), C.double(0), C.double(radius), C.double(0))
}

// Draws a rectangle over the current drawing surface.
func (cv Canvas) Rectangle(x float64, y float64) {
	C.DrawRectangle(cv.drawing, C.double(0), C.double(0), C.double(x), C.double(y))
}

// Moves the current coordinate system origin to the specified coordinate.
func (cv Canvas) Translate(x float64, y float64) {
	C.DrawTranslate(cv.drawing, C.double(x), C.double(y))
}

// Applies a scaling factor to the units of the current coordinate system.
func (cv Canvas) Scale(x float64, y float64) {
	C.DrawScale(cv.drawing, C.double(x), C.double(y))
}

// Draws a line starting on the current coordinate system origin and ending on the specified coordinates.
func (cv Canvas) Line(x float64, y float64) {
	C.DrawLine(cv.drawing, C.double(0), C.double(0), C.double(x), C.double(y))
}

/*
func (cv Canvas) Skew(x float64, y float64) {
  C.DrawSkewX(cv.drawing, C.double(x))
  C.DrawSkewY(cv.drawing, C.double(y))
}
*/

// Applies a rotation of a given angle (in radians) on the current coordinate system.
func (cv Canvas) Rotate(rad float64) {
	deg := RAD_TO_DEG * rad
	C.DrawRotate(cv.drawing, C.double(deg))
}

// Draws an ellipse centered at the current coordinate system's origin.
func (cv Canvas) Ellipse(a float64, b float64) {
	C.DrawEllipse(cv.drawing, C.double(0), C.double(0), C.double(a), C.double(b), 0, 360)
}

// Clones the current drawing surface and stores it in a stack.
func (cv Canvas) PushDrawing() bool {
	status := C.PushDrawingWand(cv.drawing)
	if status == C.MagickFalse {
		return false
	}
	return true
}

// Destroys the current drawing surface and returns the latest surface that was pushed to the stack.
func (cv Canvas) PopDrawing() bool {
	status := C.PopDrawingWand(cv.drawing)
	if status == C.MagickFalse {
		return false
	}
	return true
}

// Copies a drawing surface to the canvas.
func (cv Canvas) Update() {
	C.MagickDrawImage(cv.wand, cv.drawing)
}

// Destroys canvas.
func (cv Canvas) Destroy() {
	if cv.wand != nil {
		C.DestroyMagickWand(cv.wand)
	}
	C.MagickWandTerminus()
}

// Creates an empty canvas of the given dimensions.
func (cv Canvas) Blank(width uint, height uint) bool {
	status := C.MagickNewImage(cv.wand, C.size_t(width), C.size_t(height), cv.bg)
	if status == C.MagickFalse {
		return false
	}
	return true
}

// Convolves the canvas with a Gaussian function given its standard deviation.
func (cv Canvas) Blur(sigma float64) bool {
	status := C.MagickBlurImage(cv.wand, C.double(0), C.double(sigma))
	if status == C.MagickFalse {
		return false
	}
	return true
}

// Adaptively blurs the image by blurring less intensely near the edges and more intensely far from edges.
func (cv Canvas) AdaptiveBlur(sigma float64) bool {
	status := C.MagickAdaptiveBlurImage(cv.wand, C.double(0), C.double(sigma))
	if status == C.MagickFalse {
		return false
	}
	return true
}

// Adds random noise to the canvas.
func (cv Canvas) AddNoise() bool {
	status := C.MagickAddNoiseImage(cv.wand, C.GaussianNoise)
	if status == C.MagickFalse {
		return false
	}
	return true
}

// Removes a region of a canvas and collapses the canvas to occupy the removed portion.
func (cv Canvas) Chop(x int, y int, width uint, height uint) bool {
	status := C.MagickChopImage(cv.wand, C.size_t(width), C.size_t(height), C.ssize_t(x), C.ssize_t(y))
	if status == C.MagickFalse {
		return false
	}
	return true
}

// Extracts a region from the canvas.
func (cv Canvas) Crop(x int, y int, width uint, height uint) bool {
	status := C.MagickCropImage(cv.wand, C.size_t(width), C.size_t(height), C.ssize_t(x), C.ssize_t(y))
	if status == C.MagickFalse {
		return false
	}
	return true
}

// Adjusts the canvas's brightness given a factor (-1.0 thru 1.0)
func (cv Canvas) SetBrightness(factor float64) bool {

	factor = math.Max(-1, factor)
	factor = math.Min(1, factor)

	status := C.MagickModulateImage(cv.wand, C.double(100+factor*100.0), C.double(100), C.double(100))

	if status == C.MagickFalse {
		return false
	}

	return true
}

// Adjusts the canvas's saturation given a factor (-1.0 thru 1.0)
func (cv Canvas) SetSaturation(factor float64) bool {

	factor = math.Max(-1, factor)
	factor = math.Min(1, factor)

	status := C.MagickModulateImage(cv.wand, C.double(100), C.double(100+factor*100.0), C.double(100))

	if status == C.MagickFalse {
		return false
	}

	return true
}

// Adjusts the canvas's hue given a factor (-1.0 thru 1.0)
func (cv Canvas) SetHue(factor float64) bool {

	factor = math.Max(-1, factor)
	factor = math.Min(1, factor)

	status := C.MagickModulateImage(cv.wand, C.double(100), C.double(100), C.double(100+factor*100.0))

	if status == C.MagickFalse {
		return false
	}

	return true
}

// Returns a new canvas object.
func New() *Canvas {
	cv := &Canvas{}

	cv.Init()

	cv.wand = C.NewMagickWand()

	cv.fg = C.NewPixelWand()
	cv.bg = C.NewPixelWand()

	cv.fill = C.NewPixelWand()
	cv.stroke = C.NewPixelWand()

	cv.drawing = C.NewDrawingWand()

	//cv.SetColor("#ffffff")
	cv.SetBackgroundColor("none")

	cv.SetStrokeColor("#ffffff")
	cv.SetStrokeAntialias(true)
	cv.SetStrokeWidth(1.0)
	cv.SetStrokeOpacity(1.0)
	cv.SetStrokeLineCap(STROKE_ROUND_CAP)
	cv.SetStrokeLineJoin(STROKE_ROUND_JOIN)

	//cv.SetFillRule(FILL_EVEN_ODD_RULE)
	cv.SetFillColor("#888888")

	return cv
}
