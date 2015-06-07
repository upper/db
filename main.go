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

// Package db provides a single interface for interacting with different data
// sources through the use of adapters that wrap well-known database drivers.
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
package db // import "upper.io/db"

// Cond is a map used to define conditions passed to `db.Collection.Find()` and
// `db.Result.Where()`.
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

// Func is a struct that represents database functions.
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

// And is an array of interfaces that is used to join two or more expressions
// under logical conjunction, it accepts `db.Cond{}`, `db.Or{}`, `db.Raw{}` and
// other `db.And{}` values.
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

// Or is an array of interfaced that is used to join two or more expressions
// under logical disjunction, it accepts `db.Cond{}`, `db.And{}`, `db.Raw{}`
// and other `db.Or{}` values.
//
// Example:
//
// 	// SQL: year = 2012 OR year = 1987
// 	db.Or {
// 		db.Cond{"year": 2012},
// 		db.Cond{"year": 1987},
// 	}
type Or []interface{}

// Raw holds chunks of data to be passed to the database without any filtering.
// Use with care.
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

// Database is an interface that defines methods that must be provided by
// database adapters.
type Database interface {
	// Driver() Returns the underlying driver the wrapper uses. As an
	// `interface{}`.
	//
	// In order to actually use the `interface{}` you must cast it to the known
	// database driver type.
	//
	// Example:
	//	internalSQLDriver := sess.Driver().(*sql.DB)
	Driver() interface{}

	// Open() attempts to stablish a connection with the database server, a
	// previous call to Setup() is required.
	Open() error

	// Clone() duplicates the current database session. Returns an error if the
	// clone could not be carried out.
	Clone() (Database, error)

	// Ping() returns error if the database server cannot be reached.
	Ping() error

	// Close() closes the currently active connection to the database.
	Close() error

	// Collection() returns a `db.Collection{}` struct by name. Some databases
	// support collections of more than one source or table, refer to the
	// documentation of the specific database adapter to see if using multiple
	// sources is supported.
	Collection(...string) (Collection, error)

	// Collections() returns the names of all non-system sources or tables within
	// the active database.
	Collections() ([]string, error)

	// Use() attempts to connect to another database using the same connection
	// settings. Similar to MySQL's `USE` instruction.
	Use(string) error

	// Drop() drops the active database.
	Drop() error

	// Setup() sets the database connection settings. In order to connect, a call
	// to `db.Database.Open()` is required.
	Setup(ConnectionURL) error

	// Name() returns the name of the active database.
	Name() string

	// Transaction() starts a transaction block. Some databases do not support
	// transactions, refer to the documentation of the specific database adapter
	// to see the current status on transactions.
	Transaction() (Tx, error)
}

// Tx is an interface that provides the same methods that the `db.Database`
// does, plus some other that help the user deal with database transactions. In
// order to get a proper `db.Tx` interface the `db.Database.Transaction()`
// method must be called on an already opened database session.
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

// Collection is an interface that defines methods for handling data sources or
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

// Result is an interface that defines methods for working with result sets.
type Result interface {

	// Limit() defines the maximum number of results in this set.
	Limit(uint) Result

	// Skip() ignores the first *n* results.
	Skip(uint) Result

	// Sort() receives field names that define the order in which elements will
	// be returned in a query, field names may be prefixed with a minus sign (-)
	// indicating descending order; ascending order would be used by default.
	Sort(...interface{}) Result

	// Select() defines specific fields to be fulfilled on results in this result
	// set.
	Select(...interface{}) Result

	// Where() discards the initial filtering conditions and sets new ones.
	Where(...interface{}) Result

	// Group() is used to group results that have the same value in the same
	// column or columns.
	Group(...interface{}) Result

	// Remove() deletes all items within the result set.
	Remove() error

	// Update() modified all items within the result set. Receives an struct or
	// an interface{}.
	Update(interface{}) error

	// Count() returns the number of items that match the set conditions (Limit
	// and Offset settings are excluded from this query).
	Count() (uint64, error)

	// Next() fetches the next result within the result set and dumps it into the
	// given pointer to struct or pointer to map. You must manually call Close()
	// after finishing using Next().
	Next(interface{}) error

	// One() fetches the first result within the result set and dumps it into the
	// given pointer to struct or pointer to map. Then it calls Close() to free
	// the result set.
	One(interface{}) error

	// All() fetches all results within the result set and dumps them into the
	// given pointer to slice of maps or structs. Then it calls Close() to free
	// the result set.
	All(interface{}) error

	// Close() closes the result set.
	Close() error
}

// ConnectionURL is the interface that defines methods for connection strings.
type ConnectionURL interface {
	// String returns the connection string that is going to be passed to the
	// adapter.
	String() string
}

// EnvEnableDebug may be used by adapters to determine if the user has enabled
// debugging.
//
// If the user sets the `UPPERIO_DB_DEBUG` environment variable to a
// non-empty value, all generated statements will be printed at runtime to
// the standard logger.
//
// Example:
//
//	UPPERIO_DB_DEBUG=1 go test
//
//	UPPERIO_DB_DEBUG=1 ./go-program
const EnvEnableDebug = `UPPERIO_DB_DEBUG`

// Marshaler is the interface implemented by structs that can marshal
// themselves into data suitable for storage.
type Marshaler interface {
	MarshalDB() (interface{}, error)
}

// Unmarshaler is the interface implemented by structs that can transform
// themselves from storage data into a valid value.
type Unmarshaler interface {
	UnmarshalDB(interface{}) error
}

// IDSetter is the interface implemented by structs that can set their own ID
// after calling Append().
type IDSetter interface {
	SetID(map[string]interface{}) error
}

// Constrainer is the interface implemented by structs that can delimit
// themselves.
type Constrainer interface {
	Constraint() Cond
}

// Int64IDSetter implements a common pattern for setting int64 IDs.
type Int64IDSetter interface {
	SetID(int64) error
}

// Uint64IDSetter implements a common pattern for setting uint64 IDs.
type Uint64IDSetter interface {
	SetID(uint64) error
}
