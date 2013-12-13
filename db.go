/*
  Copyright (c) 2012-2013 José Carlos Nieto, https://menteslibres.net/xiam

  Permission is hereby granted, free of charge, to any person obtaining
  a copy of this software and associated documentation files (the
  "Software"), to deal in the Software without restriction, including
  without limitation the rights to use, copy, modify, merge, publish,
  distribute, sublicense, and/or sell copies of the Software, and to
  permit persons to whom the Software is furnished to do so, subject to
  the following conditions:

  The above copyright notice and this permission notice shall be
  included in all copies or substantial portions of the Software.

  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
  NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
  LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
  OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
  WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

/*
	The main goal of the upper.io/db package is to provide a simple way to save
	and retrive Go structs to and from permantent storage.
*/
package db

import (
	"errors"
	"fmt"
	"reflect"
)

/*
	The db.Cond{} expression is used to filter results in a query, it can be
	viewed as a replacement for the SQL "WHERE" clause.

	Examples:

	db.Cond { "age": 18 }				// Where age equals 18.

	db.Cond { "age >=": 18 }		// Where age is greater than or equal to 18.

	db.Cond { "age $lt": 18 }		// Where age is lower than 18 (MongoDB specific).
*/
type Cond map[string]interface{}

/*
	The db.And() expression is used to glue two or more expressions under logical
	conjunction, it accepts db.Cond{}, db.Or() and other db.And() expressions.

	Examples:

	db.And (
		db.Cond { "name": "Peter" },
		db.Cond { "last_name": "Parker "},
	)

	db.And (
		db.Or (
			db.Cond{ "name": "Peter" },
			db.Cond{ "name": "Mickey" },
		),
		db.Cond{ "last_name": "Mouse" },
	)
*/
type And []interface{}

/*
	The db.Or() expression is used to glue two or more expressions under logical
	disjunction, it accepts db.Cond{}, db.And() and other db.Or() expressions.

	Example:

	db.Or (
		db.Cond { "year": 2012 },
		db.Cond { "year": 1987 },
	)
*/
type Or []interface{}

// Connection and authentication data.
type Settings struct {
	// Host to connect to. Cannot be used if Socket is specified.
	Host string
	// Port to connect to. Cannot be used if Socket is specified.
	Port int
	// Name of the database to use.
	Database string
	// Authentication user name.
	User string
	// Authentication password.
	Password string
	// A path of a UNIX socket. Cannot be user if Host is specified.
	Socket string
	// Charset of the database.
	Charset string
}

// Database methods.
type Database interface {
	// Returns the underlying driver the wrapper uses as an interface{}, so you
	// can still use database-specific features when you need it.
	Driver() interface{}

	// Attempts to open a connection using the current settings.
	Open() error

	// Closes the currently active connection to the database.
	Close() error

	// Returns a db.Collection struct by name.
	Collection(string) (Collection, error)

	// Returns the names of all the collections within the active database.
	Collections() ([]string, error)

	// Changes the active database.
	Use(string) error

	// Drops the active database.
	Drop() error

	// Sets database connection settings.
	Setup(Settings) error

	// Returns the string name of the active database.
	Name() string

	// Starts a transaction block (if the database supports transactions).
	Begin() error

	// Ends a transaction block (if the database supports transactions).
	End() error
}

// Collection methods.
type Collection interface {

	// Inserts a new item into the collection. Can work with maps or structs.
	Append(interface{}) (interface{}, error)

	// Returns true if the collection exists.
	Exists() bool

	// Creates a filter with the given conditions and returns a result set.
	Find(...interface{}) Result

	// Truncates the collection.
	Truncate() error

	// Returns the string name of the collection.
	Name() string
}

// Result methods.
type Result interface {
	// Defines the maximum number of results on this set.
	Limit(uint) Result

	// Skips over the n initial results from a query.
	Skip(uint) Result

	// Receives fields that define the order in which elements will be returned in
	// a query, field names may be prefixed with a minus sign (-) indicating
	// descending order, ascending order would be used otherwise.
	Sort(...string) Result

	// Defines specific fields to be returned on results on this result set.
	Select(...string) Result

	// Removes all items within the result set.
	Remove() error

	// Updates all items within the result set. Receives an struct or an interface{}.
	Update(interface{}) error

	// Counts all items within the result set.
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

// Error messages
var (
	ErrExpectingPointer        = errors.New(`Expecting a pointer destination (dst interface{}).`)
	ErrExpectingSlicePointer   = errors.New(`Expecting a pointer to an slice (dst interface{}).`)
	ErrExpectingSliceMapStruct = errors.New(`Expecting a pointer to an slice of maps or structs (dst interface{}).`)
	ErrExpectingMapOrStruct    = errors.New(`Expecting either a pointer to a map or a pointer to a struct.`)
	ErrNoMoreRows              = errors.New(`There are no more rows in this result set.`)
	ErrNotConnected            = errors.New(`You're currently not connected.`)
	ErrMissingDatabaseName     = errors.New(`Missing a database name.`)
	ErrCollectionDoesNotExists = errors.New(`Collection does not exists.`)
	ErrSockerOrHost            = errors.New(`You can connect either to a socket or a host but not both.`)
	ErrQueryLimitParam         = errors.New(`A query can accept only one db.Limit() parameter.`)
	ErrQuerySortParam          = errors.New(`A query can accept only one db.Sort{} parameter.`)
	ErrQueryOffsetParam        = errors.New(`A query can accept only one db.Offset() parameter.`)
	ErrMissingConditions       = errors.New(`Missing selector conditions.`)
	ErrQueryIsPending          = errors.New(`Can't execute this instruction while the result set is still open.`)
)

// Registered wrappers.
var wrappers = make(map[string]Database)

// Registers a database wrapper with a unique name.
func Register(name string, driver Database) {

	if name == "" {
		panic("Missing wrapper name.")
	}

	if _, ok := wrappers[name]; ok != false {
		panic("Register called twice for driver " + name)
	}

	wrappers[name] = driver
}

// Configures a connection to a database using the named wrapper and the given
// settings.
func Open(name string, settings Settings) (Database, error) {

	driver, ok := wrappers[name]

	if ok == false {
		panic(fmt.Sprintf("Unknown wrapper: %s.", name))
	}

	// Creating a new connection everytime Open() is called.
	t := reflect.ValueOf(driver).Elem().Type()

	conn := reflect.New(t).Interface().(Database)

	// Setting up the connection with the given source.
	err := conn.Setup(settings)

	if err != nil {
		return nil, err
	}

	return conn, nil
}
