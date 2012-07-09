# gosexy/db

This package is a wrapper of [mgo](http://launchpad.net/mgo), [database/sql](http://golang.org/pkg/database/sql) and some of its database drivers friends, the goal of this abstraction is to provide a common, simplified, consistent layer for working with different databases using Go.

## Installation

    $ go get github.com/xiam/gosexy/db

## Available interfaces

* MongoDB with [mgo](http://launchpad.net/mgo)
* MySQL with [go-mysql-driver](http://code.google.com/p/go-mysql-driver/)
* PostgreSQL with (a fork of) [pq](https://github.com/bmizerany/pq)
* SQLite3 with (a fork of) [sqlite3](https://github.com/mattn/go-sqlite3)

## Usage

Import ``github.com/xiam/gosexy/db`` into your project.

A handful of methods will be available, each database has its specific ways of doing the same task but this will be handled by the drivers, the interface is the same for all of them.

### Setting up a database

The first step is to choose a driver and set up the connection, this is how it would be done using ``MysqlSession``

    sess := db.MysqlSession(db.DataSource{Host: "localhost", Database: "test", User: "myuser", Password: "mypass"})

The ``db.DataSource`` is a generic structure than can store connection values in a consistent way.

    // Connection and authentication data.
    type DataSource struct {
      Host     string
      Port     int
      Database string
      User     string
      Password string
    }

You may use other drivers to setup a connection, available drivers are ``db.MysqlSession``, ``db.MongodbSession``, ``db.PostgresqlSession`` and ``db.SqliteSession`` each one of them receives a ``db.DataSource`` and returns a ``db.Database``.

### Connecting to the database

Use your recently configured ``db.Database`` to request the driver to actually connect to the selected database.

    // Setting up database.
    sess := db.MysqlSession(db.DataSource{Host: "localhost", Database: "test", User: "myuser", Password: "mypass"})
    sess.Open()

    // Don't forget to close the connection when it's not required anymore.
	  defer sess.Close()

### Database methods.

The ``db.Database`` interface exposes the very same methods for all databases.

    // Database methods.
    type Database interface {
      Driver() interface{}

      Open() error
      Close() error

      Collection(string) Collection
      Collections() []string

      Use(string) error
      Drop() error
    }

#### db.Database.Driver() interface{}

Returns the raw driver as an ``interface{}``, for example, if you're using ``MongoSession`` it will return an interface to ``*mgo.Session``, and if you're using ``MysqlSession`` it will return an interface to ``*sql.DB``, this is the only method that may return different data structures on different databases.

#### db.Database.Open() error

Requests a connection to the database session. Returns an error if it fails.

#### db.Database.Close() error

Disconnects from the database session. Returns an error if it fails.

#### db.Database.Collection(name string) Collection

Returns a ``db.Collection`` object from the current database given the name, collections are sets of rows or documents, this could be a MongoDB collection or a MySQL/PostgreSQL/SQLite table. You can create, read, update or delete rows from a collection. Please read all the methods avaiable for ``db.Collection`` further into this manual.

#### db.Database.Collections() []string

Returns the names of all the collections in the current database.

#### db.Database.Use(name string) error

Makes the session switch between databases given the name. Returns an error if it fails.

#### db.Database.Drop() error

Erases the entire database and all the collections. Returns an error if it fails.

### Collection methods

Collections are sets of rows or documents, this could be a MongoDB collection or a MySQL/PostgreSQL/SQLite table. You can create, read, update or delete rows from a collection.

When you request data from a Collection with ``Find()`` or ``FindAll()``, a special structure named ``Item`` will be returned.

    // Collection methods.
    type Collection interface {
      Append(...interface{}) bool

      Count(...interface{}) int

      Find(...interface{}) Item
      FindAll(...interface{}) []Item

      Update(...interface{}) bool

      Remove(...interface{}) bool

      Truncate() bool
    }

    // Rows from a result.
    type Item map[string]interface{}

#### db.Collection.Append(...interface{}) bool

#### db.Collection.Count(...interface{}) int

#### db.Collection.Find(...interface{}) Item

#### db.Collection.FindAll(...interface{}) []Item

#### db.Collection.Update(...interface{}) bool

#### db.Collection.Remove(...interface{}) bool

#### db.Collection.Truncate() bool

## Documentation

You can read ``gosexy/db`` documentation from a terminal

    $ go doc github.com/xiam/gosexy/db

Or you can [browse it](http://go.pkgdoc.org/github.com/xiam/gosexy/db) online.
