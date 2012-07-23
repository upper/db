# gosexy/db/mysql

	This driver is a wrapper of [go-mysql-driver](http://code.google.com/p/go-mysql-driver/)

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
