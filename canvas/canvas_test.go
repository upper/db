package canvas

import "testing"

import "math"

/*
  Example image is form Yuko Honda
  http://www.flickr.com/photos/yukop/6779040884/
*/

func TestOpenWrite(t *testing.T) {
	canvas := New()

	opened := canvas.Open("examples/input/example.png")

	if opened {
		canvas.AutoOrientate()

		canvas.SetQuality(90)

		canvas.Write("examples/output/example.jpg")
	}

	canvas.Destroy()
}

func TestThumbnail(t *testing.T) {
	canvas := New()

	opened := canvas.Open("examples/input/example.png")

	if opened {
		canvas.AutoOrientate()

		canvas.Thumbnail(100, 100)

		canvas.Write("examples/output/example-thumbnail.png")
	}

	canvas.Destroy()
}

func TestResize(t *testing.T) {
	canvas := New()

	opened := canvas.Open("examples/input/example.png")

	if opened {
		canvas.Resize(100, 100)
		canvas.Write("examples/output/example-100x100.png")
	}

	canvas.Destroy()
}

func TestBlank(t *testing.T) {
	canvas := New()

	canvas.SetBackgroundColor("#00ff00")

	success := canvas.Blank(400, 400)

	if success {
		canvas.Write("examples/output/example-blank.png")
	}

	canvas.Destroy()
}

func TestSettersAndGetters(t *testing.T) {

	canvas := New()

	success := canvas.Blank(400, 400)
	if success != true {
		t.Errorf("Could not create blank image.")
	}

	const backgroundColor = "#112233"

	canvas.SetBackgroundColor(backgroundColor)

	if gotBackgroundColor := canvas.BackgroundColor(); gotBackgroundColor != backgroundColor {
		t.Errorf("Got %s, expecting %s", gotBackgroundColor, backgroundColor)
	}

	const strokeAntialias = true

	canvas.SetStrokeAntialias(strokeAntialias)

	if gotStrokeAntialias := canvas.StrokeAntialias(); gotStrokeAntialias != strokeAntialias {
		t.Errorf("Got %t, expecting %t.", gotStrokeAntialias, strokeAntialias)
	}

	const strokeWidth = 2.0

	canvas.SetStrokeWidth(strokeWidth)

	if gotStrokeWidth := canvas.StrokeWidth(); gotStrokeWidth != strokeWidth {
		t.Errorf("Got %f, expecting %f.", gotStrokeWidth, strokeWidth)
	}

	const strokeOpacity = 1.0

	canvas.SetStrokeOpacity(strokeOpacity)

	if gotStrokeOpacity := canvas.StrokeOpacity(); gotStrokeOpacity != strokeOpacity {
		t.Errorf("Got %f, expecting %f.", gotStrokeOpacity, strokeOpacity)
	}

	strokeLineCap := STROKE_SQUARE_CAP

	canvas.SetStrokeLineCap(strokeLineCap)

	if gotStrokeLineCap := canvas.StrokeLineCap(); gotStrokeLineCap != strokeLineCap {
		t.Errorf("Got %d, expecting %d.", gotStrokeLineCap, strokeLineCap)
	}

	strokeLineJoin := STROKE_ROUND_JOIN

	canvas.SetStrokeLineJoin(strokeLineJoin)

	if gotStrokeLineJoin := canvas.StrokeLineJoin(); gotStrokeLineJoin != strokeLineJoin {
		t.Errorf("Got %d, expecting %d.", gotStrokeLineJoin, strokeLineJoin)
	}

	const fillColor = "#112233"

	canvas.SetFillColor(fillColor)

	if gotFillColor := canvas.FillColor(); gotFillColor != fillColor {
		t.Errorf("Got %s, expecting %s", gotFillColor, fillColor)
	}

	const strokeColor = "#112233"

	canvas.SetStrokeColor(strokeColor)

	if gotStrokeColor := canvas.StrokeColor(); gotStrokeColor != strokeColor {
		t.Errorf("Got %s, expecting %s", gotStrokeColor, strokeColor)
	}

	const quality = 76

	canvas.SetQuality(quality)

	if gotQuality := canvas.Quality(); gotQuality != quality {
		t.Errorf("Got %d, expecting %d", gotQuality, quality)
	}

	canvas.Destroy()
}

func TestDrawLine(t *testing.T) {

	canvas := New()

	canvas.SetBackgroundColor("#000000")

	success := canvas.Blank(400, 400)

	if success {

		canvas.Translate(200, 200)
		canvas.SetStrokeWidth(10)
		canvas.SetStrokeColor("#ffffff")
		canvas.Line(100, 100)

		canvas.Write("examples/output/example-line.png")
	}

	canvas.Destroy()
}

func TestDrawCircle(t *testing.T) {
	canvas := New()

	canvas.SetBackgroundColor("#000000")

	success := canvas.Blank(400, 400)

	if success {

		canvas.SetFillColor("#ff0000")

		canvas.PushDrawing()
		canvas.Translate(200, 200)
		canvas.SetStrokeWidth(5)
		canvas.SetStrokeColor("#ffffff")
		canvas.Circle(100)
		canvas.PopDrawing()

		canvas.PushDrawing()
		canvas.Translate(100, 100)
		canvas.SetStrokeWidth(3)
		canvas.SetStrokeColor("#ffffff")
		canvas.Circle(20)
		canvas.PopDrawing()

		canvas.Write("examples/output/example-circle.png")
	}

	canvas.Destroy()
}

func TestDrawRectangle(t *testing.T) {
	canvas := New()

	canvas.SetBackgroundColor("#000000")

	success := canvas.Blank(400, 400)

	if success {

		canvas.SetFillColor("#ff0000")

		canvas.Translate(200-50, 200+75)
		canvas.SetStrokeWidth(5)
		canvas.SetStrokeColor("#ffffff")
		canvas.Rectangle(100, -150)

		canvas.Write("examples/output/example-rectangle.png")
	}

	canvas.Destroy()
}

func TestDrawEllipse(t *testing.T) {
	canvas := New()

	success := canvas.Blank(400, 400)

	if success {

		canvas.SetFillColor("#ff0000")

		canvas.PushDrawing()
		canvas.Translate(200, 200)
		canvas.Rotate(math.Pi / 3)
		canvas.Ellipse(50, 180)
		canvas.PopDrawing()

		canvas.SetFillColor("#ff00ff")

		canvas.PushDrawing()
		canvas.Translate(200, 200)
		canvas.Rotate(-math.Pi / 3)
		canvas.Ellipse(25, 90)
		canvas.PopDrawing()

		canvas.Write("examples/output/example-ellipse.png")
	}

	canvas.Destroy()
}

func TestBlur(t *testing.T) {
	canvas := New()
	defer canvas.Destroy()

	opened := canvas.Open("examples/input/example.png")

	if opened {
		canvas.Blur(3)
		canvas.Write("examples/output/example-blur.png")
	}
}

func TestModulate(t *testing.T) {
	canvas := New()
	defer canvas.Destroy()

	opened := canvas.Open("examples/input/example.png")

	if opened {
		canvas.SetBrightness(-0.5)
		canvas.SetHue(0.2)
		canvas.SetSaturation(0.9)
		canvas.Write("examples/output/example-modulate.png")
	}
}

func TestAdaptive(t *testing.T) {

	canvas := New()
	defer canvas.Destroy()

	opened := canvas.Open("examples/input/example.png")

	if opened {
		canvas.AdaptiveBlur(1.2)
		canvas.AdaptiveResize(100, 100)
		canvas.Write("examples/output/example-adaptive.png")
	}
}

func TestNoise(t *testing.T) {
	canvas := New()
	defer canvas.Destroy()

	opened := canvas.Open("examples/input/example.png")

	if opened {
		canvas.AddNoise()
		canvas.Write("examples/output/example-noise.png")
	}
}

func TestChop(t *testing.T) {
	canvas := New()
	defer canvas.Destroy()

	opened := canvas.Open("examples/input/example.png")

	if opened {
		canvas.Chop(0, 0, 100, 50)
		canvas.Write("examples/output/example-chop.png")
	}
}

func TestCrop(t *testing.T) {
	canvas := New()
	defer canvas.Destroy()

	opened := canvas.Open("examples/input/example.png")

	if opened {
		canvas.Crop(100, 200, 200, 100)
		canvas.Write("examples/output/example-crop.png")
	}
}
