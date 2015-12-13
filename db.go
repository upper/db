// Copyright (c) 2012-2015 The upper.io/db authors. All rights reserved.
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
// As of today, `upper.io/db.v2` fully supports MySQL, PostgreSQL and SQLite (CRUD
// + Transactions) and provides partial support for MongoDB and QL (CRUD only).
//
// Usage:
//
// 	import(
//		// Main package.
// 		"upper.io/db.v2"
//		// PostgreSQL adapter.
// 		"upper.io/db.v2/postgresql"
// 	)
//
// `upper.io/db.v2` is not an ORM and thus does not impose any hard restrictions
// on data structures:
//
//	// This code works the same for all supported databases.
//	var people []Person
//	res = col.Find(db.Cond{"name": "Max"}).Limit(2).Sort("-input")
//	err = res.All(&people)
package db // import "upper.io/db.v2"

import (
	"reflect"

	"upper.io/builder"
)

// Cond is a map that defines conditions that can be passed to
// `db.Collection.Find()` and `db.Result.Where()`.
//
// Each entry of the map represents a condition (a column-value relation bound
// by a comparison operator). The comparison operator is optional and can be
// specified after the column name, if no comparison operator is provided the
// equality is used.
//
// Examples:
//
//	// Where age equals 18.
//	db.Cond{"age": 18}
//
//	// Where age is greater than or equal to 18.
//	db.Cond{"age >=": 18}
//
//	// Where id is in a list of ids.
//	db.Cond{"id IN": []{1, 2, 3}}
//
//	// Where age is lower than 18 (mongodb-like operator).
//	db.Cond{"age $lt": 18}
//
//  // Where age > 32 and age < 35
//  db.Cond{"age >": 32, "age <": 35}
type Cond builder.M

// Constraints returns all the conditions on the map.
func (m Cond) Constraints() []builder.Constraint {
	return builder.M(m).Constraints()
}

// Operator returns the logical operator that joins the conditions (defaults to
// "AND").
func (m Cond) Operator() builder.CompoundOperator {
	return builder.M(m).Operator()
}

// Sentences returns the map as a compound, so it can be used with Or() and
// And().
func (m Cond) Sentences() []builder.Compound {
	return builder.M(m).Sentences()
}

// Func represents a database function.
//
// Examples:
//
//	// MOD(29, 9)
//	db.Func("MOD", 29, 9)
//
//	// CONCAT("foo", "bar")
//	db.Func("CONCAT", "foo", "bar")
//
//	// NOW()
//	db.Func("NOW")
//
//	// RTRIM("Hello  ")
//	db.Func("RTRIM", "Hello  ")
func Func(name string, args ...interface{}) builder.Function {
	if len(args) == 1 {
		if reflect.TypeOf(args[0]).Kind() == reflect.Slice {
			iargs := make([]interface{}, len(args))
			for i := range args {
				iargs[i] = args[i]
			}
			args = iargs
		}
	}
	return &dbFunc{name: name, args: args}
}

// And joins conditions under logical conjunction. Conditions can be
// represented by db.Cond{}, db.Or() or db.And().
//
// Examples:
//
//	// name = "Peter" AND last_name = "Parker"
//	db.And(
// 		db.Cond{"name": "Peter"},
// 		db.Cond{"last_name": "Parker "},
// 	)
//
//	// (name = "Peter" OR name = "Mickey") AND last_name = "Mouse"
// 	db.And(
// 		db.Or(
// 			db.Cond{"name": "Peter"},
// 			db.Cond{"name": "Mickey"},
// 		),
// 		db.Cond{"last_name": "Mouse"},
// 	)
var And = builder.And

// Or joins conditions under logical disjunction. Conditions can be represented
// by db.Cond{}, db.Or() or db.And().
//
// Example:
//
// 	// year = 2012 OR year = 1987
// 	db.Or(
// 		db.Cond{"year": 2012},
// 		db.Cond{"year": 1987},
// 	)
var Or = builder.Or

// Raw marks chunks of data as protected, so they pass directly to the query
// without any filtering. Use with care.
//
// Example:
//
//	// SOUNDEX('Hello')
//	Raw("SOUNDEX('Hello')")
var Raw = builder.Raw

// Database is an interface that defines methods that must be satisfied by
// database adapters.
type Database interface {
	// Driver returns the underlying driver the wrapper uses.
	//
	// In order to actually use the driver the `interface{}` value has to be
	// casted to the appropriate type.
	//
	// Example:
	//  internalSQLDriver := sess.Driver().(*sql.DB)
	Driver() interface{}

	// Builder returns a query builder that can be used to execute advanced
	// queries. Builder may not be defined for all database adapters, in that
	// case the return value would be nil.
	Builder() builder.Builder

	// Open attempts to stablish a connection with the database manager, a
	// previous call to `Setup()` is required.
	Open() error

	// Clone duplicates the current database session. Returns an error if the
	// clone did not succeed.
	Clone() (Database, error)

	// Ping returns an error if the database manager cannot be reached.
	Ping() error

	// Close closes the currently active connection to the database.
	Close() error

	// C is a short-hand for `Collection()`. If the given collection does not
	// exists subsequent calls to any `Collection{}` or `Result{}` method that
	// expect the collection to exists will fail returning the original error a
	// call to `Collection()` would have returned. The output of `C()` may be a
	// cached collection value.
	C(string) Collection

	// Collection returns a `Collection{}` given a table name.
	Collection(string) (Collection, error)

	// Collections returns the names of all non-system tables on the database.
	Collections() ([]string, error)

	// Use attempts to connect to another database using the same connection
	// settings.
	Use(string) error

	// Drop deletes all tables on the active database and drops the database.
	Drop() error

	// Setup stores database connection settings.
	Setup(ConnectionURL) error

	// Name returns the name of the active database.
	Name() string

	// Transaction starts a transaction block. Some databases do not support
	// transactions, refer to the documentation of the specific database adapter
	// to see the current status on transactions.
	Transaction() (Tx, error)
}

// Tx is an interface that enhaces the `Database` interface with additional
// methods for transactions.
//
// Example:
//	// [...]
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
//	// [...]
type Tx interface {
	Database

	// Rollback discards all the instructions on the current transaction.
	Rollback() error

	// Commit commits the current transactions.
	Commit() error
}

// Collection is an interface that defines methods useful for handling data
// sources or tables.
type Collection interface {

	// Append inserts a new item into the collection. Accepts a map or a struct
	// as argument.
	Append(interface{}) (interface{}, error)

	// Exists returns true if the collection exists.
	Exists() bool

	// Find returns a result set with the given filters.
	Find(...interface{}) Result

	// Truncate removes all elements on the collection and resets its IDs.
	Truncate() error

	// Name returns the name of the collection.
	Name() string
}

// Result is an interface that defines methods useful for working with result
// sets.
type Result interface {

	// Limit defines the maximum number of results in this set. It only has
	// effect on `One()`, `All()` and `Next()`.
	Limit(uint) Result

	// Skip ignores the first *n* results. It only has effect on `One()`, `All()`
	// and `Next()`.
	Skip(uint) Result

	// Sort receives field names that define the order in which elements will be
	// returned in a query, field names may be prefixed with a minus sign (-)
	// indicating descending order, ascending order will be used otherwise.
	Sort(...interface{}) Result

	// Select defines specific columns to be returned from the elements of the
	// set.
	Select(...interface{}) Result

	// Where discards the initial filtering conditions and sets new ones.
	Where(...interface{}) Result

	// Group is used to group results that have the same value in the same column
	// or columns.
	Group(...interface{}) Result

	// Remove deletes all items within the result set. `Skip()` and `Limit()` are
	// not honoured by `Remove()`.
	Remove() error

	// Update modifies all items within the result set. `Skip()` and `Limit()`
	// are not honoured by `Update()`.
	Update(interface{}) error

	// Count returns the number of items that match the set conditions. `Skip()`
	// and `Limit()` are not honoured by `Count()`
	Count() (uint64, error)

	// Next fetches the next result within the result set and dumps it into the
	// given pointer to struct or pointer to map. You must manually call
	// `Close()` after finishing using `Next()`.
	Next(interface{}) error

	// One fetches the first result within the result set and dumps it into the
	// given pointer to struct or pointer to map. The result set is automatically
	// closed after picking the element, so there is no need to call `Close()`
	// manually.
	One(interface{}) error

	// All fetches all results within the result set and dumps them into the
	// given pointer to slice of maps or structs.  The result set is
	// automatically closed, so there is no need to call `Close()` manually.
	All(interface{}) error

	// Close closes the result set.
	Close() error
}

// ConnectionURL represents a connection string
type ConnectionURL interface {
	// String returns the connection string that is going to be passed to the
	// adapter.
	String() string
}

// Marshaler is the interface implemented by structs that can marshal
// themselves into data suitable for storage.
type Marshaler builder.Marshaler

// Unmarshaler is the interface implemented by structs that can transform
// themselves from storage data into a valid value.
type Unmarshaler builder.Unmarshaler

// IDSetter defines methods to be implemented by structs tha can update their
// own IDs.
type IDSetter interface {
	SetID(map[string]interface{}) error
}

// Constrainer defined methods to be implemented by structs that can set its
// own constraints.
type Constrainer interface {
	Constraints() Cond
}

// Int64IDSetter defined methods to be implemented by structs that can update
// their own int64 ID.
type Int64IDSetter interface {
	SetID(int64) error
}

// Uint64IDSetter defined methods to be implemented by structs that can update
// their own uint64 ID.
type Uint64IDSetter interface {
	SetID(uint64) error
}

// EnvEnableDebug can be used by adapters to determine if the user has enabled
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

type dbFunc struct {
	name string
	args []interface{}
}

func (f *dbFunc) Arguments() []interface{} {
	return f.args
}

func (f *dbFunc) Name() string {
	return f.name
}

var (
	_ = builder.Constraints(Cond{})
	_ = builder.Compound(Cond{})

	_ = builder.Function(&dbFunc{})
)
