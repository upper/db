# gosexy/db

This package is a wrapper of many third party database drivers. The goal of this abstraction is to provide a common, simplified, consistent layer for working with different databases without the need of SQL statements.

## Installation

Use ``go get`` to download and install ``gosexy/db``.

    $ go get github.com/xiam/gosexy/db

This package provides shared interfaces and datatypes only, in order to connect to an actual database a driver is required.

Please refer to the database driver documentation to learn how to install a specific driver.

## Available drivers

* [mongo](/xiam/gosexy/tree/master/db/mongo)
* [mysql](/xiam/gosexy/tree/master/db/mysql)
* [postgresql](/xiam/gosexy/tree/master/db/postgresql)
* [sqlite](/xiam/gosexy/tree/master/db/sqlite)

### Importing the database

Once you've installed a driver, you need to import it into your Go code:

    import "github.com/xiam/gosexy/db/mysql"

### Setting up a database source

We are going to use the [mysql](/xiam/gosexy/tree/master/db/mysql) driver in our examples. If you want to use another driver (such as ``mongo``) just replace ``mysql`` with the name of your driver and everything should work the same.

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
    sess.Open()

    // Don't forget to close the connection when it's not required anymore.
    defer sess.Close()

### db.Database methods

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

Returns the raw driver as an ``interface{}``, for example, if you're using ``mongo.Session`` it will return an interface to ``*mgo.Session``, and if you're using ``mysql.Session`` it will return an interface to ``*sql.DB``, this is the only method that may return different data structures on different databases.

#### db.Database.Open() error

Requests a connection to the database session. Returns an error if something goes wrong.

#### db.Database.Close() error

Disconnects from the database session. Returns an error if something goes wrong.

#### db.Database.Collection(name string) Collection

Returns a ``db.Collection`` object from the current database given the name, collections are sets of rows or documents, this could be a MongoDB collection or a MySQL/PostgreSQL/SQLite table. You can create, read, update or delete rows from a collection. Please read all the methods avaiable for ``db.Collection`` further into this manual.

#### db.Database.Collections() []string

Returns the names of all the collections in the current database.

#### db.Database.Use(name string) error

Makes the session switch between databases given the name. Returns an error if it fails.

#### db.Database.Drop() error

Erases the entire database and all the collections. Returns an error if it fails.

### db.Collection methods

Collections are sets of rows or documents, this could be a MongoDB collection or a MySQL/PostgreSQL/SQLite table. You can create, read, update or delete rows from a collection.

When you request data from a Collection with ``db.Collection.Find()`` or ``db.Collection.FindAll()``, a special object with structure ``db.Item`` is returned.

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

Appends one or more items to the collection.

    collection.Append(Item { "name": "Peter" })

#### db.Collection.Count(...interface{}) int

Returns the number of total items matching the provided conditions.

    total := collection.Count(Cond { "name": "Peter" })

#### db.Collection.Find(...interface{}) db.Item

Return the first Item of the collection that matches all the provided conditions. Ordering of the conditions does not matter, but you must take in account that they are evaluated from left to right and from top to bottom.

    // The following statement is equivalent to WHERE name = "John" AND last_name = "Doe" AND (age = 15 OR age = 20)
    collection.Find(
     db.Cond { "name": "John" },
     db.Cond { "last_name": "Doe" },
     db.Or {
       db.Cond { "age": 15 },
       db.Cond { "age": 20 },
     },
    )

You can also use relations in your definition

    collection.FindAll(
      // One-to-one relation with the table "places".
      db.Relate{
        "lives_in": db.On{
          session.Collection("places"),
          // Relates rows of the table "places" where place.code_id = collection.place_code_id.
          db.Cond{"code_id": "{place_code_id}"},
        },
      },
      db.RelateAll{
        // One-to-many relation with the table "children".
        "has_children": On{
          session.Collection("children"),
          // Relates rows of the table "children" where children.parent_id = collection.id
          db.Cond{"parent_id": "{id}"},
        },
        // One-to-many relation with the table "visits".
        "has_visited": db.On{
          session.Collection("visits"),
          // Relates rows of the table "visits" where visits.person_id = collection.id
          db.Cond{"person_id": "{id}"},
          // A nested relation
          db.Relate{
            // Relates rows of the table "places" with the "visits" table.
            "place": db.On{
              session.Collection("places"),
              // Cond places.id = visits.place_id
              db.Cond{"id": "{place_id}"},
            },
          },
        },
      },
    )

#### db.Collection.FindAll(...interface{}) []db.Item

Returns all the Items (``[]db.Item``) of the collection that match all the provided conditions. See ``db.Collection.Find()``.

Be aware that there are some extra parameters that you can pass to ``db.Collection.FindAll()`` but not to ``db.Collection.Find()``, like ``db.Limit(n)`` or ``db.Offset(n)``.

    // Just give me the the first 10 rows with last_name = "Smith"
    collection.Find(
      db.Cond { "last_name": "Smith" },
      db.Limit(10),
    )

#### db.Collection.Update(...interface{}) bool

Updates all the items of the collection that match all the provided conditions. You can specify the modification type by using ``db.Set``, ``db.Modify`` or ``db.Upsert``. At the time of this writing ``db.Modify`` and ``db.Upsert`` are only available for ``mongo.Session``.

    // Example of assigning field values with Set:
    collection.Update(
      db.Cond { "name": "José" },
      db.Set { "name": "Joseph"},
    )

    // Example of custom modification with db.Modify (for mongo.Session):
    collection.Update(
      db.Cond { "times <": "10" },
      db.Modify { "$inc": { "times": 1 } },
    )

    // Example of inserting if none matches with db.Upsert (for mongo.Session):
    collection.Update(
      db.Cond { "name": "Roberto" },
      db.Upsert { "name": "Robert"},
    )

#### db.Collection.Remove(...interface{}) bool

Deletes all the items of the collection that match the provided conditions.

    collection.Remove(
      db.Cond { "name": "Peter" },
      db.Cond { "last_name": "Parker" },
    )

#### db.Collection.Truncate() bool

Deletes the whole collection.

    collection.Truncate()

## Documentation

You can read ``gosexy/db`` documentation from a terminal

    $ go doc github.com/xiam/gosexy/db

Or you can [browse it](http://go.pkgdoc.org/github.com/xiam/gosexy/db) online.

## TO-DO

* Add db.Upsert and db.Modify for SQL databases.
* Add Go time datatype.
* Improve datatype guessing.
* Improve error handling.
* Add CouchDB support.

## Changelog

2012/07/23 - Splitted databases wrapper into packages. Changed ``Cond`` to ``Cond``.

2012/07/09 - First public beta with MySQL, MongoDB, PostgreSQL and SQLite3.
