# gosexy/db

This package is a wrapper of many third party database drivers. The goal of this abstraction is to provide a common,
simplified, consistent layer for working with different databases without the need of SQL statements.

You can read our online documentation at [gosexy.org](http://gosexy.org).

## Installation

Use ``go get`` to download and install ``gosexy/db``.

    $ go get github.com/gosexy/db

This package provides shared interfaces and datatypes only, in order to connect to an actual database a wrapper is required.

## Available wrappers

* [mongo](http://gosexy.org/db/wrappers/mongo)
* [mysql](http://gosexy.org/db/wrappers/mysql)
* [postgresql](http://gosexy.org/db/wrappers/postgresql)
* [sqlite](http://gosexy.org/db/wrappers/sqlite)

## Connecting to a database

You may want to read a more descriptive reference on [how to connect](http://gosexy.org/db) to databases using ``gosexy/db``.

### Importing the database

Once you've installed a driver, you need to import it into your Go code:

    import "github.com/gosexy/db/mysql"

### Setting up a database source

We are going to use the [mysql](http://gosexy.org/db/wrappers/mysql) driver in our examples. If you want to use another driver
(such as ``mongo``) just replace ``mysql`` with the name of your driver and everything should work the same.

    sess := mysql.Session(db.DataSource{Host: "localhost", Database: "test", User: "myuser", Password: "mypass"})

The ``db.DataSource`` is a generic structure than can store connection values for any database in a consistent way.

    // Connection and authentication data.
    type DataSource struct {
      Host     string
      Port     int
      Database string
      User     string
      Password string
    }

### Connecting to a database

Use your recently configured ``db.Database`` to request the driver to actually connect to the selected database using ``db.Database.Open()``.

    // Setting up database.
    sess := mysql.Session(db.DataSource{Host: "localhost", Database: "test", User: "myuser", Password: "mypass"})
    err := sess.Open()

    // Don't forget to close the connection when it's not required anymore.
    if err == nil {
      defer sess.Close()
    }

## Documentation

To know how to query the database you've just connected, please read the [online reference](http://gosexy.org/db).

You can also read ``gosexy/db`` documentation from a terminal

    $ go doc github.com/gosexy/db

## Things to do

This is an evolving project, so there is a lot of work to do:

* Add db.Upsert and db.Modify for SQL databases.
* Add Go ``pkg/time`` datatype for database dates.
* Improve database -> Go type guessing.
* Improve error handling.
* Add CouchDB support.

## Changelog

    2012/08/29 - Created the main site docs and moved the repo to "http://github.com/gosexy".
    2012/07/23 - Splitted database wrappers into packages. Changed ``db.Where`` to ``db.Cond``.
    2012/07/09 - First public beta with MySQL, MongoDB, PostgreSQL and SQLite3.
