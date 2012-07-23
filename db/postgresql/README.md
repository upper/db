# gosexy/db/postgresql

	This driver is a wrapper of [pq](https://github.com/bmizerany/pq). In order to work with ``gosexy/db`` the original driver had to be [forked](https://github.com/xiam/gopostgresql) as the changes made to it are incompatible with some of pq's own features.

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
