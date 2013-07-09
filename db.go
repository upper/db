/*
  Copyright (c) 2012-2013 José Carlos Nieto, http://xiam.menteslibres.org/

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
	This package is a wrapper of many third party database drivers. The goal of
	this abstraction is to provide a simple, common and consistent layer for
	executing operationg among different kinds of databases without the need of
	explicit SQL statements.
*/
package db

import (
	"errors"
	"fmt"
	"reflect"
)

/*
	Handles conditions and operators in an expression.

	Examples:


	db.Cond { "age": 18 } // Age equals 18

	db.Cond { "age >=": 18 } // Age greater or equal than 18 (SQL/NoSQL)

	db.Cond { "age $lt": 18 } // Age less than 18 (MongoDB specific)
*/
type Cond map[string]interface{}

/*
	Logical conjuction, accepts db.Cond{}, db.Or{} and other db.And{} expressions.

	Example:

	db.And (
		db.Cond { "name": "Peter" },
		db.Cond { "last_name": "Parker "},
	)

	db.And (
		db.Or {
			db.Cond{ "name": "Peter" },
			db.Cond{ "name": "Mickey" },
		},
		db.Cond{ "last_name": "Mouse" },
	)
*/
type And []interface{}

/*
	Logical disjuction, accepts db.Cond{}, db.And{} and other db.Or{} expressions.

	Example:

	db.Or (
		db.Cond { "year": 2012 },
		db.Cond { "year": 1987 },
	)
*/
type Or []interface{}

/*
	Determines how results will be sorted.

	Example:
	db.Sort { "age": -1 } // Order by age, descendent.
	db.Sort { "age": 1 } // Order by age, ascendent.

	db.Sort { "age": "DESC" } // Order by age, descendent.
	db.Sort { "age": "ASC" } // Order by age, ascendent.
*/
type Sort map[string]interface{}

/*
	How rows are going to be modified when using *db.Collection.Update() and
	*db.Collection.UpdateAll().

	Currently unused.

	Example:

	db.Modify {
	 "$inc": {
		 "counter": 1
	 }
	}
*/
type Modify map[string]interface{}

/*
	Defines a relation between each one of the results of a query and any other
	collection. You can relate an item with another item in any other collection
	using a condition.

	A constant condition looks like this:

	db.Cond { "external_field": "value" }

	A dynamic condition looks like this (note the brackets):

	db.Cond { "id": "{foreign_key}" }

	The above condition will match the result where the "id" column is equal to
	the "foreign_key" value of the local collection.

	Example:

	// The db.On constraint.

	db.On {
		// The external collection.
		sess.ExistentCollection("parents"),

		// Reference constraint.
		Cond { "id": "{parent_id}" },
	}

	You can use db.On only as a value for db.Relate and db.RelateAll maps.
*/
type On []interface{}

/*
	A map that defines a one-to-one relation with another table.

	The name of the key will define the name of the relation. A db.On{} constraint
	is required.

	Example that relates a result with a row from the "parents" collection.

	// A relation exists where the parents.id column matches the

	// collection.parent_id value.

	db.Relate {
		"myparent": On {
			db.ExistentCollection("parents"),
			Cond { "id": "{parent_id}" },
		}
	}

*/
type Relate map[string]On

/*
	Like db.Relate but defines a one-to-many relation.

	Example that relates a result with many rows from the "sons" collection:


	// A relation exists where the sons.parent_id column matches the collection.id

	// value

	db.RelateAll {
		"children": db.On {
			db.ExistentCollection("sons"),
			Cond { "age <=": 18 },
			Cond { "parent_id": "{id}" },
		}
	}
*/
type RelateAll map[string]On

type Relation struct {
	All        bool
	Name       string
	Collection Collection
	On         On
}

/*
	Sets the maximum number of rows to be fetched in a query.

	If no db.Limit is specified, all matches will be returned.

	Example:

		db.Limit(10)
*/
type Limit uint

/*
	Sets the number of rows to be skipped before counting the limit in a query.

	If no db.Offset is specified no rows will be skipped.

	Example:

		db.Offset(7)
*/
type Offset uint

/*
	Determines new values for fields in *db.Collection.Update() and
	*db.Collection.UpdateAll() expressions.

	Example:

	db.Set {
		"name": "New Name",
	}
*/
type Set map[string]interface{}

/*
	Like db.Set{} but it will insert the specified values if no match is found.

	Currently unused.

	db.Upsert {
		"name": "New Name",
	}
*/
type Upsert map[string]interface{}

// A query result.
type Item map[string]interface{}

// A result id.
type Id string

// Connection and authentication data.
type DataSource struct {
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
	/*
		Returns an interface{} to the underlying driver the wrapper uses. Useful
		for custom SQL queries.
	*/
	Driver() interface{}

	/*
		Attempts to open a connection using the db.DataSource data.
	*/
	Open() error

	/*
		Closes the currently active connection to the database, if any.
	*/
	Close() error

	/*
		Returns a db.Collection struct by name. Returns an error if the collection
		does not exists.
	*/
	Collection(string) (Collection, error)

	/*
		Returns a db.Collection struct, panics if the collection does not exists.
	*/
	ExistentCollection(string) Collection
	/*
		Returns the names of all the collections in the active database.
	*/
	Collections() []string

	/*
		Changes the active database.
	*/
	Use(string) error
	/*
		Drops the active database.
	*/
	Drop() error

	/*
		Sets the connection data.
	*/
	Setup(DataSource) error

	/*
		Returns the name of the active database.
	*/
	Name() string

	/*
		Starts a transaction block.
	*/
	Begin() error

	/*
		Ends a transaction block.
	*/
	End() error
}

// Collection methods.
type Collection interface {
	/*
		Inserts an item into the collection. Accepts maps or structs only.
	*/
	Append(...interface{}) ([]Id, error)

	/*
		Returns the number of rows that given the given conditions.
	*/
	Count(...interface{}) (int, error)

	/*
		Returns a db.Item map of the first item that matches the given conditions.
	*/
	Find(...interface{}) (Item, error)

	/*
		Returns a []db.Item slice of all the items that match the given conditions.

		Useful for small datasets.
	*/
	FindAll(...interface{}) ([]Item, error)

	/*
		Finds a matching row and sets new values for the given fields.
	*/
	Update(interface{}, interface{}) error

	/*
		Returns true if the collection exists.
	*/
	Exists() bool

	/*
		Returns a db.Result that can be used for iterating over the rows.

		Useful for large datasets.
	*/
	Query(...interface{}) (Result, error)

	/*
		Deletes all the rows that match the given conditions.
	*/
	Remove(...interface{}) error

	/*
		Deletes all the rows in the collection.
	*/
	Truncate() error

	/*
		Returns the name of the collection.
	*/
	Name() string
}

// Result methods.
type Result interface {
	/*
		Fetches all the results of the query into the given pointer.

		Accepts a pointer to slice of maps or structs.
	*/
	All(interface{}) error

	/*
		Fetches the first result of the query into the given pointer and discards
		the rest.

		Accepts a pointer to map or struct.
	*/
	One(interface{}) error

	/*
		Fetches the next result of the query into the given pointer. Returns error if
		there are no more results.

		Warning: If you're only using part of these results you must manually Close()
		the result.

		Accepts a pointer to map or struct.
	*/
	Next(interface{}) error

	/*
		Closes the result.
	*/
	Close() error
}

// Specifies which fields will be returned in a query.
type Fields []string

// These are internal variables.
type MultiFlag bool
type SqlValues []string
type SqlArgs []string

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
)

// Registered wrappers.
var wrappers = make(map[string]Database)

/*
	Registers a database wrapper with an unique name.
*/
func Register(name string, driver Database) {

	if name == "" {
		panic("Missing wrapper name.")
	}

	if _, ok := wrappers[name]; ok != false {
		panic("Register called twice for driver " + name)
	}

	wrappers[name] = driver
}

/*
	Opens a database using the named driver and the db.DataSource settings.
*/
func Open(name string, settings DataSource) (Database, error) {

	driver, ok := wrappers[name]

	if ok == false {
		panic(fmt.Sprintf("Unknown wrapper: %s.", name))
	}

	// Creating a new connection everytime Open() is called.
	conn := reflect.New(reflect.ValueOf(driver).Elem().Type()).Interface().(Database)

	err := conn.Setup(settings)

	if err != nil {
		return nil, err
	}

	return conn, nil
}
