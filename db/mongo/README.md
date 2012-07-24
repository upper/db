# gosexy/db/mongo

This driver is a wrapper of [mgo](http://labix.org/mgo)

## Requirements

The [bazaar](http://bazaar.canonical.com/en/) version control system is required by ``mgo``.

If you're using ``brew`` and OSX, you can install it like this

		$ brew install bzr

On ArchLinux you could use

		$ sudo pacman -S bzr

And on Debian based distros

		$ sudo aptitude install bzr

## Installation

		$ go get github.com/xiam/gosexy/db/mongo

## Usage

		import (
			"github.com/xiam/gosexy/db"
			"github.com/xiam/gosexy/db/mongo"
		)

## Connecting to a MongoDB database

		sess := mongo.Session(db.DataSource{Host: "127.0.0.1"})

		err := sess.Open()
		defer sess.Close()

Read full documentation and examples on the [gosexy/db](/xiam/gosexy/tree/master/db) manual.
