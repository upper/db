# gosexy/db/mysql

	This driver is a wrapper of [go-mysql-driver](http://code.google.com/p/go-mysql-driver/)

## Requirements

The [mercurial](http://mercurial.selenic.com/) version control system is required by ``go-mysql-driver``.

If you're using ``brew`` and OSX, you can install it like this

		$ brew install hg

On ArchLinux you could use

		$ sudo pacman -S mercurial

And on Debian based distros

		$ sudo aptitude install mercurial

## Installation

		$ go get github.com/xiam/gosexy/db/mysql

## Usage

		import (
			"github.com/xiam/gosexy/db"
			"github.com/xiam/gosexy/db/mysql"
		)

## Connecting to a database

		sess := mysql.Session(db.DataSource{Host: "127.0.0.1"})

		err := sess.Open()
		defer sess.Close()

Read full documentation and examples on the [gosexy/db](/xiam/gosexy/tree/master/db) manual.
