# gosexy/db

This package is a wrapper of [mgo](http://launchpad.net/mgo), [database/sql](http://golang.org/pkg/database/sql) and some of its database drivers friends, the goal of this abstraction is to provide a common, simplified, consistent layer for working with different databases using Go.

## Installation

Please read docs on the [gosexy](https://github.com/xiam/gosexy) package before rushing to install ``gosexy/db``

    $ go get github.com/xiam/gosexy/db

## Available interfaces

* MongoDB via [mgo](http://launchpad.net/mgo)
* MySQL via [go-mysql-driver](http://code.google.com/p/go-mysql-driver/)
* PostgreSQL via (a fork of) [pq](https://github.com/bmizerany/pq)
* SQLite3 via (a fork of) [sqlite3](https://github.com/mattn/go-sqlite3)

## Recommended usage

For the sake of ease, it is recommended that you import ``github.com/xiam/gosexy/db`` into the current namespace, this will allow your Go program to use unprefixed structures, for example, it would be a lot easier to write ``Item`` or ``Where`` rather than ``db.Item`` or ``db.Where``.

    import . "github.com/xiam/gosexy/db"

All the examples in this page are shown without prefixes.

### Setting up a database source

The first step is to choose a driver and set up the connection using a ``DataSource``, this is how it would be done using ``MysqlSession``

    sess := MysqlSession(DataSource{Host: "localhost", Database: "test", User: "myuser", Password: "mypass"})

Please note that each database has its very specific way of doing the same task, but the interface and methods of ``gosexy/db`` are the same for any of them.

The ``DataSource`` is a generic structure than can store connection values in a consistent way.

    // Connection and authentication data.
    type DataSource struct {
      Host     string
      Port     int
      Database string
      User     string
      Password string
    }

You may use other drivers to setup a connection, available drivers are ``MysqlSession``, ``MongodbSession``, ``PostgresqlSession`` and ``SqliteSession`` each one of them receives a ``DataSource`` and returns a ``Database``.

### Connecting to the database

Use your recently configured ``Database`` to request the driver to actually connect to the selected database using ``Database.Open()``.

    // Setting up database.
    sess := MysqlSession(DataSource{Host: "localhost", Database: "test", User: "myuser", Password: "mypass"})
    sess.Open()

    // Don't forget to close the connection when it's not required anymore.
    defer sess.Close()

### Database methods.

The ``Database`` interface exposes the very same methods for all databases.

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

#### Database.Driver() interface{}

Returns the raw driver as an ``interface{}``, for example, if you're using ``MongoSession`` it will return an interface to ``*mgo.Session``, and if you're using ``MysqlSession`` it will return an interface to ``*sql.DB``, this is the only method that may return different data structures on different databases.

#### Database.Open() error

Requests a connection to the database session. Returns an error if it fails.

#### Database.Close() error

Disconnects from the database session. Returns an error if it fails.

#### Database.Collection(name string) Collection

Returns a ``Collection`` object from the current database given the name, collections are sets of rows or documents, this could be a MongoDB collection or a MySQL/PostgreSQL/SQLite table. You can create, read, update or delete rows from a collection. Please read all the methods avaiable for ``Collection`` further into this manual.

#### Database.Collections() []string

Returns the names of all the collections in the current database.

#### Database.Use(name string) error

Makes the session switch between databases given the name. Returns an error if it fails.

#### Database.Drop() error

Erases the entire database and all the collections. Returns an error if it fails.

### Collection methods

Collections are sets of rows or documents, this could be a MongoDB collection or a MySQL/PostgreSQL/SQLite table. You can create, read, update or delete rows from a collection.

When you request data from a Collection with ``Collection.Find()`` or ``Collection.FindAll()``, a special object with structure ``Item`` will be returned.

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

#### Collection.Append(...interface{}) bool

Appends one or more items to the collection.

    collection.Append(Item { "name": "Peter" })

#### Collection.Count(...interface{}) int

Returns the number of total items matching the provided conditions.

    total := collection.Count(Where { "name": "Peter" })

#### Collection.Find(...interface{}) Item

Return the first Item of the collection that matches all the provided conditions. Ordering of the conditions does not matter, but you must take in account that they are evaluated from left to right and from top to bottom.

    // The following statement is equivalent to WHERE name = "John" AND last_name = "Doe" AND (age = 15 OR age = 20)
    collection.Find(
     Where { "name": "John" },
     Where { "last_name": "Doe" },
     Or {
       Where { "age": 15 },
       Where { "age": 20 },
     },
    )

You can also use relations in your definition

    collection.FindAll(
      // One-to-one relation with the table "places".
      Relate{
        "lives_in": On{
          session.Collection("places"),
          // Relates rows of the table "places" where place.code_id = collection.place_code_id.
          Where{"code_id": "{place_code_id}"},
        },
      },
      RelateAll{
        // One-to-many relation with the table "children".
        "has_children": On{
          session.Collection("children"),
          // Relates rows of the table "children" where children.parent_id = collection.id
          Where{"parent_id": "{id}"},
        },
        // One-to-many relation with the table "visits".
        "has_visited": On{
          session.Collection("visits"),
          // Relates rows of the table "visits" where visits.person_id = collection.id
          Where{"person_id": "{id}"},
          // A nested relation
          Relate{
            // Relates rows of the table "places" with the "visits" table.
            "place": On{
              session.Collection("places"),
              // Where places.id = visits.place_id
              Where{"id": "{place_id}"},
            },
          },
        },
      },
    )

#### Collection.FindAll(...interface{}) []Item

Returns all the Items (``[]Item``) of the collection that match all the provided conditions. See ``Collection.Find()``.

Be aware that there are some extra parameters that you can pass to ``Collection.FindAll()`` but not to ``Collection.Find()``, like ``Limit(n)`` or ``Offset(n)``.

    // Just give me the the first 10 rows with last_name = "Smith"
    collection.Find(
      Where { "last_name": "Smith" },
      Limit(10),
    )

#### Collection.Update(...interface{}) bool

Updates all the items of the collection that match all the provided conditions. You can specify the modification type by using ``Set``, ``Modify`` or ``Upsert``. At the time of this writing ``Modify`` and ``Upsert`` are only available for MongoSession.

    // Example of assigning field values with Set:
    collection.Update(
      Where { "name": "José" },
      Set { "name": "Joseph"},
    )

    // Example of custom modification with Modify (for MongoSession):
    collection.Update(
      Where { "times <": "10" },
      Modify { "$inc": { "times": 1 } },
    )

    // Example of inserting if none matches with Upsert (for MongoSession):
    collection.Update(
      Where { "name": "Roberto" },
      Upsert { "name": "Robert"},
    )

#### Collection.Remove(...interface{}) bool

Deletes all the items of the collection that match the provided conditions.

    collection.Remove(
      Where { "name": "Peter" },
      Where { "last_name": "Parker" },
    )

#### Collection.Truncate() bool

Deletes the whole collection.

    collection.Truncate()

## Documentation

You can read ``gosexy/db`` documentation from a terminal

    $ go doc github.com/xiam/gosexy/db

Or you can [browse it](http://go.pkgdoc.org/github.com/xiam/gosexy/db) online.

## TO-DO

* Add Upsert and Modify for SQL databases.
* Add Go time datatype.
* Improve datatype guessing.
* Improve error handling.
* Add CouchDB support.

## Changelog

2012/07/09 - First public beta with MySQL, MongoDB, PostgreSQL and SQLite3.
