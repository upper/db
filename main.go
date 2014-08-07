// Copyright (c) 2012-2014 José Carlos Nieto, https://menteslibres.net/xiam
//
// Permission is hereby granted, free of charge, to any person obtaining
// a copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to
// permit persons to whom the Software is furnished to do so, subject to
// the following conditions:
//
// The above copyright notice and this permission notice shall be
// included in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
// LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
// OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
// WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

// The `upper.io/db` package for Go provides a single interface for interacting
// with different data sources through the use of adapters that wrap well-known
// database drivers.
//
// As of today, `upper.io/db` fully supports MySQL, PostgreSQL and SQLite (CRUD
// + Transactions) and provides partial support for MongoDB and QL (CRUD only).
//
// Usage:
//
// 	import(
//		// Main package.
// 		"upper.io/db"
//		// PostgreSQL adapter.
// 		"upper.io/db/postgresql"
// 	)
//
// `upper.io/db` is not an ORM and thus does not impose any hard restrictions
// on data structures:
//
//	// This code works the same for all supported databases.
//	var people []Person
//	res = col.Find(db.Cond{"name": "Max"}).Limit(2).Sort("-input")
//	err = res.All(&people)
package db

// The `db.Cond{}` expression is used to define conditions used as arguments to
// `db.Collection.Find()` and `db.Result.Where()`.
//
// Examples:
//
//	// Where age equals 18.
//	db.Cond { "age": 18 }
//
//	Where age is greater than or equal to 18.
//	db.Cond { "age >=": 18 }
//
//	Where age is lower than 18 (On MongoDB context).
//	db.Cond { "age $lt": 18 }
type Cond map[string]interface{}

// The `db.Func{}` expression is used to represent database functions.
//
// Examples:
//
//	// MOD(29, 9)
//	db.Func{"MOD", []int{29, 9}}
//
//	// CONCAT("foo", "bar")
//	db.Func{"CONCAT", []string{"foo", "bar"}}
//
//	// NOW()
//	db.Func{"NOW"}
//
//	// RTRIM("Hello   ")
//	db.Func{"RTRIM", "Hello  "}
type Func struct {
	Name string
	Args interface{}
}

// The `db.And{}` expression is used to join two or more expressions under
// logical conjunction, it accepts `db.Cond{}`, `db.Or{}`, `db.Raw{}` and other
// `db.And{}` expressions.
//
// Examples:
//
//	// SQL: name = "Peter" AND last_name = "Parker"
//	db.And (
// 		db.Cond { "name": "Peter" },
// 		db.Cond { "last_name": "Parker "},
// 	)
//
//	// SQL: (name = "Peter" OR name = "Mickey") AND last_name = "Mouse"
// 	db.And {
// 		db.Or {
// 			db.Cond{ "name": "Peter" },
// 			db.Cond{ "name": "Mickey" },
// 		},
// 		db.Cond{ "last_name": "Mouse" },
// 	}
type And []interface{}

// The `db.Or{}` expression is used to glue two or more expressions under
// logical disjunction, it accepts `db.Cond{}`, `db.And{}`, `db.Raw{}` and
// other `db.Or{}` expressions.
//
// Example:
//
// 	// SQL: year = 2012 OR year = 1987
// 	db.Or {
// 		db.Cond{"year": 2012},
// 		db.Cond{"year": 1987},
// 	}
type Or []interface{}

// The `db.Raw{}` expression is mean to hold chunks of data to be passed to the
// database without any filtering.
//
// When using `db.Raw{}`, the developer is responsible of providing a sanitized
// instruction to the database.
//
// The `db.Raw{}` expression is allowed as element on `db.Cond{}`, `db.And{}`,
// `db.Or{}` expressions and as argument on `db.Result.Select()` and
// `db.Collection.Find()` methods.
//
// Example:
//
//	// SQL: SOUNDEX('Hello')
//	Raw{"SOUNDEX('Hello')"}
type Raw struct {
	Value interface{}
}

// The `db.Settings{}` struct holds database connection and authentication
// data. Not all fields must be supplied, is any field is skipped, the database
// adapter will either try to use database defaults or return an error. Refer
// to the specific adapter to see which fields are required.
//
// Example:
//
// 	db.Settings{
// 		Host: "127.0.0.1",
// 		Database: "tests",
// 		User: "john",
// 		Password: "doe",
// 	}
type Settings struct {
	// Database server hostname or IP. This field is ignored if using unix
	// sockets or if the database does not require a connection to any host
	// (SQLite, QL).
	Host string
	// Database server port. This field is ignored if using unix sockets or if
	// the database does not require a connection to any host (SQLite, QL). If
	// not provided, the default database port is tried.
	Port int
	// Name of the database. You can also use a filename if the database supports
	// opening a raw file (SQLite, QL).
	Database string
	// Username for authentication, if required.
	User string
	// Password for authentication, if required.
	Password string
	// A path to a UNIX socket file. Leave blank if you rather use host and port.
	Socket string
	// Database charset. You can leave this field blank to use the default
	// database charset.
	Charset string
}

// The `db.Database` interface defines methods that all adapters must provide.
type Database interface {
	// Returns the underlying driver the wrapper uses. As an `interface{}`.
	//
	// In order to actually use the `interface{}` you must cast it to the known
	// database driver type.
	//
	// Example:
	//	internalSQLDriver := sess.Driver().(*sql.DB)
	Driver() interface{}

	// Attempts to stablish a connection with the database server, a previous
	// call to Setup() is required.
	Open() error

	// Clones the current database session. Returns an error if the clone could
	// not be carried out.
	Clone() (Database, error)

	// Returns error if the database server cannot be reached.
	Ping() error

	// Closes the currently active connection to the database.
	Close() error

	// Returns a `db.Collection{}` struct by name. Some databases support
	// collections of more than one source or table, refer to the documentation
	// of the specific database adapter to see if using multiple sources is
	// supported.
	Collection(...string) (Collection, error)

	// Returns the names of all non-system sources or tables within the active
	// database.
	Collections() ([]string, error)

	// Attempts to connect to another database using the same connection
	// settings. Similar to MySQL's `USE` instruction.
	Use(string) error

	// Drops the active database.
	Drop() error

	// Sets the database connection settings. In order to connect, a call to
	// `db.Database.Open()` is required.
	Setup(Settings) error

	// Returns the name of the active database.
	Name() string

	// Starts a transaction block. Some databases do not support transactions,
	// refer to the documentation of the specific database adapter to see the
	// current status on transactions.
	Transaction() (Tx, error)
}

// The `db.Tx` interface provides the same methods that the `db.Database` does,
// plus some other that help the user deal with database transactions. In order
// to get a proper `db.Tx` interface the `db.Database.Transaction()` method
// must be called on an already opened database session.
//
// Example:
//	...
// 	if sess, err = db.Open(postgresql.Adapter, settings); err != nil {
// 		log.Fatal(err)
// 	}
//
// 	var tx db.Tx
// 	if tx, err = sess.Transaction(); err != nil {
// 		log.Fatal(err)
// 	}
//
// 	var artist db.Collection
// 	if artist, err = tx.Collection("artist"); err != nil {
// 		log.Fatal(err)
// 	}
//	...
type Tx interface {
	Database

	// Discards all the instructions issued during the transaction.
	Rollback() error

	// Verifies that all the instructions isssued during the transaction were
	// executed.
	Commit() error
}

// The `db.Collection` interface defines methods for handling data sources or
// tables.
type Collection interface {

	// Inserts a new item into the collection. Accepts a map or a struct as
	// argument.
	Append(interface{}) (interface{}, error)

	// Returns true if the collection exists.
	Exists() bool

	// Sets a filter on the collection with the given conditions and returns a
	// result set.
	Find(...interface{}) Result

	// Removes all elements on the collection and resets the IDs.
	Truncate() error

	// Returns the name of the collection.
	Name() string
}

// The `db.Result` interface defines methods for working with result sets.
type Result interface {

	// Defines the maximum number of results in this set.
	Limit(uint) Result

	// Skips over the *n* initial results.
	Skip(uint) Result

	// Receives fields that define the order in which elements will be returned
	// in a query, field names may be prefixed with a minus sign (-) indicating
	// descending order; ascending order would be used by default.
	Sort(...interface{}) Result

	// Defines specific fields to be fulfilled on results in this result set.
	Select(...interface{}) Result

	// Discards the initial filtering conditions and sets new ones.
	Where(...interface{}) Result

	// Groups results using a key.
	Group(...interface{}) Result

	// Removes all items within the result set.
	Remove() error

	// Updates all items within the result set. Receives an struct or an interface{}.
	Update(interface{}) error

	// Returns the number of items that match the set conditions (Limit and
	// Offset settings are excluded from this query).
	Count() (uint64, error)

	// Fetches the next result within the result set and dumps it into the given
	// pointer to struct or pointer to map. You must manually call Close() after
	// finishing using Next().
	Next(interface{}) error

	// Fetches the first result within the result set and dumps it into the given
	// pointer to struct or pointer to map. Then it calls Close() to free the
	// result set.
	One(interface{}) error

	// Fetches all results within the result set and dumps them into the given
	// pointer to slice of maps or structs. Then it calls Close() to free the
	// result set.
	All(interface{}) error

	// Closes the result set.
	Close() error
}

// If the user sets the `UPPERIO_DB_DEBUG` environment variable to a
// non-empty value, all generated statements will be printed at runtime to
// the standard logger.
//
// Example:
//
//	UPPERIO_DB_DEBUG=1 go test
//
//	UPPERIO_DB_DEBUG=1 ./go-program
var EnvEnableDebug = `UPPERIO_DB_DEBUG`
