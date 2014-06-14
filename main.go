/*
  Copyright (c) 2012-2014 José Carlos Nieto, https://menteslibres.net/xiam

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

// upper.io/db is a package that deals with saving and retrieving data from
// different databases using a reduced instruction set.
package db

/*
	The db.Cond{} expression is used to define conditions in a query, it can be
	viewed as a replacement for the SQL "WHERE" clause.

	Examples:

	db.Cond { "age": 18 }				// Where age equals 18.

	db.Cond { "age >=": 18 }		// Where age is greater than or equal to 18.

	db.Cond { "age $lt": 18 }		// Where age is lower than 18 (MongoDB specific).
*/
type Cond map[string]interface{}

/*
	The db.Func expression is used to represent database functions.
*/
type Func struct {
	Name string
	Args interface{}
}

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

type Raw struct {
	Value interface{}
}

// Connection and authentication data.
type Settings struct {
	// Database server hostname or IP. Leave blank if using unix sockets.
	Host string
	// Database server port. Leave blank if using unix sockets.
	Port int
	// Name of the database.
	Database string
	// Username for authentication.
	User string
	// Password for authentication.
	Password string
	// A path of a UNIX socket file. Leave blank if using host and port.
	Socket string
	// Database charset.
	Charset string
}

// Database methods.
type Database interface {
	// Returns the underlying driver the wrapper uses as an interface{}.
	Driver() interface{}

	// Attempts to stablish a connection with the database server.
	Open() error

	// Clones the current database session.
	Clone() (Database, error)

	// Returns error if the database server cannot be reached.
	Ping() error

	// Closes the currently active connection to the database.
	Close() error

	// Returns a db.Collection struct by name.
	Collection(...string) (Collection, error)

	// Returns the names of all non-system collections within the active
	// database.
	Collections() ([]string, error)

	// Switches the active database.
	Use(string) error

	// Drops the active database.
	Drop() error

	// Sets database connection settings.
	Setup(Settings) error

	// Returns the name of the active database.
	Name() string

	// Starts a transaction block (if the database supports transactions).
	Transaction() (Tx, error)
}

// A transaction is basically a copy of the Database interface{} with an
// additional method.
type Tx interface {
	Database

	Rollback() error

	Commit() error
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

	// Returns the name of the collection.
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
	// descending order; ascending order would be used otherwise.
	Sort(...string) Result

	// Defines specific fields to be returned on results on this result set.
	Select(...string) Result

	// Sets conditions.
	Where(...interface{}) Result

	// Removes all items within the result set.
	Remove() error

	// Updates all items within the result set. Receives an struct or an interface{}.
	Update(interface{}) error

	// Counts all the items on the result set.
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

var (
	EnvEnableDebug = `UPPERIO_DB_DEBUG`
)
