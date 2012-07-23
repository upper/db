# gosexy/db/postgresql

	This driver is a wrapper of [sqlite3](https://github.com/mattn/go-sqlite3). In order to work with ``gosexy/db`` the original driver had to be [forked](https://github.com/xiam/gosqlite3) as the changes made to it are incompatible with some of sqlite3's own features.

## Installation

		$ go get github.com/xiam/gosexy/db/postgresql

## Usage

		import (
			"github.com/xiam/gosexy/db"
			"github.com/xiam/gosexy/db/postgresql"
		)

## Connecting to a database

		sess := postgresql.Session(db.DataSource{Host: "127.0.0.1"})

		err := sess.Open()
		defer sess.Close()

Read full documentation and examples on the [gosexy/db](/xiam/gosexy/tree/master/db) manual.
