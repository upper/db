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
	"fmt"
	"reflect"
)

// Constraint interface represents a condition.
type Constraint interface {
	Key() interface{}
	Value() interface{}
}

// Constraints interface provides the Constraints() method.
type Constraints interface {
	Constraints() []Constraint
}

// Compound represents a compound statement created by joining constraints.
type Compound interface {
	Sentences() []Compound
	Operator() CompoundOperator
}

// CompoundOperator represents the operator of a compound.
type CompoundOperator uint

// Compound operators.
const (
	OperatorNone = CompoundOperator(iota)
	OperatorAnd
	OperatorOr
)

// RawValue interface represents values that can bypass SQL filters. Use with
// care.
type RawValue interface {
	fmt.Stringer
}

// Function interface defines methods for representing database functions.
type Function interface {
	Arguments() []interface{}
	Name() string
}

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
type Cond map[interface{}]interface{}

// Constraints returns each one of the map records as a constraint.
func (c Cond) Constraints() []Constraint {
	z := make([]Constraint, 0, len(c))
	for k, v := range c {
		z = append(z, NewConstraint(k, v))
	}
	return z
}

// Sentences returns each one of the map records as a compound.
func (c Cond) Sentences() []Compound {
	z := make([]Compound, 0, len(c))
	for k, v := range c {
		z = append(z, Cond{k: v})
	}
	return z
}

// Operator returns the default compound operator.
func (c Cond) Operator() CompoundOperator {
	return OperatorNone
}

// rawValue implements RawValue
type rawValue struct {
	v string
}

func (r rawValue) String() string {
	return r.v
}

type compound struct {
	conds []Compound
}

func newCompound(c ...Compound) *compound {
	return &compound{c}
}

func (c *compound) Sentences() []Compound {
	return c.conds
}

func (c *compound) Operator() CompoundOperator {
	return OperatorNone
}

func (c *compound) push(a ...Compound) *compound {
	c.conds = append(c.conds, a...)
	return c
}

// Union represents a compound joined by OR.
type Union struct {
	*compound
}

// Or adds more terms to the compound.
func (o *Union) Or(conds ...Compound) *Union {
	o.compound.push(conds...)
	return o
}

// Operator returns the OR operator.
func (o *Union) Operator() CompoundOperator {
	return OperatorOr
}

// And adds more terms to the compound.
func (a *Intersection) And(conds ...Compound) *Intersection {
	a.compound.push(conds...)
	return a
}

// Intersection represents a compound joined by AND.
type Intersection struct {
	*compound
}

// Operator returns the AND operator.
func (a *Intersection) Operator() CompoundOperator {
	return OperatorAnd
}

type constraint struct {
	k interface{}
	v interface{}
}

func (c constraint) Key() interface{} {
	return c.k
}

func (c constraint) Value() interface{} {
	return c.v
}

// NewConstraint creates a condition
func NewConstraint(key interface{}, value interface{}) Constraint {
	return constraint{k: key, v: value}
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
func Func(name string, args ...interface{}) Function {
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
func And(conds ...Compound) *Intersection {
	return &Intersection{compound: newCompound(conds...)}
}

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
func Or(conds ...Compound) *Union {
	return &Union{compound: newCompound(conds...)}
}

// Raw marks chunks of data as protected, so they pass directly to the query
// without any filtering. Use with care.
//
// Example:
//
//	// SOUNDEX('Hello')
//	Raw("SOUNDEX('Hello')")
func Raw(s string) RawValue {
	return rawValue{v: s}
}

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

	// Open attempts to stablish a connection with a database manager.
	Open(ConnectionURL) error

	// Clone duplicates the current database session. Returns an error if the
	// clone did not succeed.
	// Clone() (Database, error)

	// Ping returns an error if the database manager cannot be reached.
	Ping() error

	// Close closes the currently active connection to the database.
	Close() error

	// Collection returns a Collection given a table name. The collection may or
	// may not exists and that could be an error when querying depending on the
	// database you're working with, MongoDB does not care but SQL databases do
	// care. If you want to know if a Collection exists use the Exists() method
	// on a Collection.
	Collection(string) Collection

	// Collections returns the names of all non-system tables on the database.
	Collections() ([]string, error)

	// Name returns the name of the active database.
	Name() string

	ConnectionURL() ConnectionURL
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
// 	if tx, err = sess.NewTransaction(); err != nil {
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

	// Insert inserts a new item into the collection, it accepts a map or a
	// struct as argument and returns the ID of the newly added element. The type
	// of this ID depends on the database adapter. The ID returned by Insert()
	// can be passed directly to Find() to find the recently added element.
	//
	// Insert does not alter the passed element.
	Insert(interface{}) (interface{}, error)

	// InsertReturning is like Insert() but it updates the passed pointer to map
	// or struct with the newly inserted element. This is all done atomically
	// within a transaction. If the database does not support transactions this
	// method returns db.ErrUnsupported.
	InsertReturning(interface{}) error

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
	// String satisfies fmt.Stringer
	String() string

	// Limit defines the maximum number of results in this set. It only has
	// effect on `One()`, `All()` and `Next()`.
	Limit(int) Result

	// Offset ignores the first *n* results. It only has effect on `One()`, `All()`
	// and `Next()`.
	Offset(int) Result

	// OrderBy receives field names that define the order in which elements will be
	// returned in a query, field names may be prefixed with a minus sign (-)
	// indicating descending order, ascending order will be used otherwise.
	OrderBy(...interface{}) Result

	// Select defines specific columns to be returned from the elements of the
	// set.
	Select(...interface{}) Result

	// Where discards the initial filtering conditions and sets new ones.
	Where(...interface{}) Result

	// Group is used to group results that have the same value in the same column
	// or columns.
	Group(...interface{}) Result

	// Delete deletes all items within the result set. `Offset()` and `Limit()` are
	// not honoured by `Delete()`.
	Delete() error

	// Update modifies all items within the result set. `Offset()` and `Limit()`
	// are not honoured by `Update()`.
	Update(interface{}) error

	// Count returns the number of items that match the set conditions. `Offset()`
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

var (
	_ = Function(&dbFunc{})
	_ = Constraints(Cond{})
	_ = Compound(Cond{})
	_ = Constraint(&constraint{})
	_ = RawValue(&rawValue{})
)
