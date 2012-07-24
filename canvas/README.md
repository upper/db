## gosexy/canvas

``gosexy/canvas`` is an image processing library based on ImageMagick's MagickWand, for the Go programming language.

## Requeriments

### Mac OSX

The ImageMagick's header files are required. If you're using ``brew`` the installation is straightforward.

    $ brew install imagemagick

### Debian

Debian has an old version of MagickWand, in order to install gocanvas we need to install the old version and then upgrade it.

Getting the old version of MagickWand along all its dependencies.

    $ sudo aptitude install libmagickwand-dev

Installing a newer version of ImageMagick over the old files.

    $ sudo su
    # cd /usr/local/src
    # wget http://www.imagemagick.org/download/ImageMagick.tar.gz
    # tar xvzf ImageMagick.tar.gz
    # cd ImageMagick-6.x.y
    # ./configure --prefix=/usr
    # make
    # make install

### Arch Linux

Arch Linux already has a recent version of MagickWand.

    $ sudo pacman -S extra/imagemagick

### Windows

Choose your [favorite binary](http://imagemagick.com/script/binary-releases.php#windows) and try it.

### Other OS

Please, follow the [install from source](http://imagemagick.com/script/install-source.php?ImageMagick=9uv1bcgofrv21mhftmlk4v1465) tutorial.

## Installation

After installing ImageMagick's header files, pull gocanvas from github:

    $ go get github.com/xiam/gosexy/canvas

## Updating

After installing, you can use `go get -u github.com/xiam/gosexy/canvas` to keep up to date.

## Usage

    package main

    import "github.com/xiam/gosexy/canvas"

    func main() {
      cv := canvas.New()
      defer cv.Destroy()

      // Opening some image from disk.
      opened := cv.Open("examples/input/example.png")

      if opened {

        // Photo auto orientation based on EXIF tags.
        canvas.AutoOrientate()

        // Creating a squared thumbnail
        canvas.Thumbnail(100, 100)

        // Saving the thumbnail to disk.
        canvas.Write("examples/output/example-thumbnail.png")

      }
    }

## Documentation

You can read ``gosexy/canvas`` documentation from a terminal

    $ go doc github.com/xiam/gosexy/canvas

Or you can [browse it](http://go.pkgdoc.org/github.com/xiam/gosexy/canvas) online.
