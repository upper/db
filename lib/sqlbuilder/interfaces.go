// Copyright (c) 2015 The upper.io/db.v2/lib/sqlbuilder authors. All rights reserved.
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

package sqlbuilder

import (
	"database/sql"
	"fmt"
)

// Builder defines methods that can serve as starting points for SQL queries.
type Builder interface {

	// Select initializes and returns a Selector pointed at the given columns.
	//
	// This Selector does not initially point to any table, a call to From() is
	// expected after Select().
	//
	// Example:
	//
	//  q := sqlbuilder.Select("first_name", "last_name").From("people").Where(...)
	Select(columns ...interface{}) Selector

	// SelectFrom creates a Selector that selects all columns (like SELECT *)
	// from the given table.
	//
	// Example:
	//
	//  q := sqlbuilder.SelectFrom("people").Where(...)
	SelectFrom(table string) Selector

	// InsertInto prepares an returns a Inserter that points at the given table.
	//
	// Example:
	//
	//   q := sqlbuilder.InsertInto("books").Columns(...).Values(...)
	InsertInto(table string) Inserter

	// DeleteFrom prepares a Deleter that points at the given table.
	//
	// Example:
	//
	//  q := sqlbuilder.DeleteFrom("tasks").Where(...)
	DeleteFrom(table string) Deleter

	// Update prepares and returns an Updater that points at the given table.
	//
	// Example:
	//
	//  q := sqlbuilder.Update("profile").Set(...).Where(...)
	Update(table string) Updater

	// Exec executes the given SQL query and returns the sql.Result.
	//
	// Example:
	//
	//  sqlbuilder.Exec(`INSERT INTO books (title) VALUES("La Ciudad y los Perros")`)
	Exec(query interface{}, args ...interface{}) (sql.Result, error)

	// Query executes the given SQL query and returns *sql.Rows.
	//
	// Example:
	//
	//  sqlbuilder.Query(`SELECT * FROM people WHERE name = "Mateo"`)
	Query(query interface{}, args ...interface{}) (*sql.Rows, error)

	// QueryRow executes the given SQL query and returns *sql.Row.
	//
	// Example:
	//
	//  sqlbuilder.QueryRow(`SELECT * FROM people WHERE name = "Haruki" AND last_name = "Murakami" LIMIT 1`)
	QueryRow(query interface{}, args ...interface{}) (*sql.Row, error)

	// Iterator executes the given SQL query and returns an Iterator.
	//
	// Example:
	//
	//  sqlbuilder.Iterator(`SELECT * FROM people WHERE name LIKE "M%"`)
	Iterator(query interface{}, args ...interface{}) Iterator
}

// Selector represents a SELECT statement.
type Selector interface {
	// Columns defines which columns to retrive.
	//
	// You should call From() after Columns() if you want to query data from an
	// specific table.
	//
	//   s.Columns("name", "last_name").From(...)
	//
	// It is also possible to use an alias for the column, this could be handy if
	// you plan to use the alias later, use the "AS" keyword to denote an alias.
	//
	//   s.Columns("name AS n")
	//
	// or the shortcut:
	//
	//   s.Columns("name n")
	//
	// If you don't want the column to be escaped use the sqlbuilder.RawString
	// function.
	//
	//   s.Columns(sqlbuilder.RawString("DATABASE_NAME()"))
	//
	// The above statement is equivalent to:
	//
	//   s.Columns(sqlbuilder.Func("DATABASE_NAME"))
	Columns(columns ...interface{}) Selector

	// From represents a FROM clause and is tipically used after Columns().
	//
	// FROM defines from which table data is going to be retrieved
	//
	//   s.Columns(...).From("people")
	//
	// It is also possible to use an alias for the table, this could be handy if
	// you plan to use the alias later:
	//
	//   s.Columns(...).From("people AS p").Where("p.name = ?", ...)
	//
	// Or with the shortcut:
	//
	//   s.Columns(...).From("people p").Where("p.name = ?", ...)
	From(tables ...string) Selector

	// Distict represents a DISCTING clause.
	//
	// DISCTINC is used to ask the database to return only values that are
	// different.
	Distinct() Selector

	// Where specifies the conditions that columns must match in order to be
	// retrieved.
	//
	// Where accepts raw strings and fmt.Stringer to define conditions and
	// interface{} to specify parameters. Be careful not to embed any parameters
	// within the SQL part as that could lead to security problems. You can use
	// que question mark (?) as placeholder for parameters.
	//
	//   s.Where("name = ?", "max")
	//
	//   s.Where("name = ? AND last_name = ?", "Mary", "Doe")
	//
	//   s.Where("last_name IS NULL")
	//
	// You can also use other types of parameters besides only strings, like:
	//
	//   s.Where("online = ? AND last_logged <= ?", true, time.Now())
	//
	// and Where() will transform them into strings before feeding them to the
	// database.
	//
	// When an unknown type is provided, Where() will first try to match it with
	// the Marshaler interface, then with fmt.Stringer and finally, if the
	// argument does not satisfy any of those interfaces Where() will use
	// fmt.Sprintf("%v", arg) to transform the type into a string.
	Where(conds ...interface{}) Selector

	// GroupBy represents a GROUP BY statement.
	//
	// GROUP BY defines which columns should be used to aggregate and group
	// results.
	//
	//   s.GroupBy("country_id")
	//
	// GroupBy accepts more than one column:
	//
	//   s.GroupBy("country_id", "city_id")
	GroupBy(columns ...interface{}) Selector

	// Having(...interface{}) Selector

	// OrderBy represents a ORDER BY statement.
	//
	// ORDER BY is used to define which columns are going to be used to sort
	// results.
	//
	// Use the column name to sort results in ascendent order.
	//
	//   // "last_name" ASC
	//   s.OrderBy("last_name")
	//
	// Prefix the column name with the minus sign (-) to sort results in
	// descendent order.
	//
	//   // "last_name" DESC
	//   s.OrderBy("-last_name")
	//
	// If you would rather be very explicit, you can also use ASC and DESC.
	//
	//   s.OrderBy("last_name ASC")
	//
	//   s.OrderBy("last_name DESC", "name ASC")
	OrderBy(columns ...interface{}) Selector

	// Join represents a JOIN statement.
	//
	// JOIN statements are used to define external tables that the user wants to
	// include as part of the result.
	//
	// You can use the On() method after Join() to define the conditions of the
	// join.
	//
	//   s.Join("author").On("author.id = book.author_id")
	//
	// If you don't specify conditions for the join, a NATURAL JOIN will be used.
	//
	// On() accepts the same arguments as Where()
	//
	// You can also use Using() after Join().
	//
	//   s.Join("employee").Using("department_id")
	Join(table ...interface{}) Selector

	// FullJoin is like Join() but with FULL JOIN.
	FullJoin(...interface{}) Selector

	// CrossJoin is like Join() but with CROSS JOIN.
	CrossJoin(...interface{}) Selector

	// RightJoin is like Join() but with RIGHT JOIN.
	RightJoin(...interface{}) Selector

	// LeftJoin is like Join() but with LEFT JOIN.
	LeftJoin(...interface{}) Selector

	// Using represents the USING clause.
	//
	// USING is used to specifiy columns to join results.
	//
	//   s.LeftJoin(...).Using("country_id")
	Using(...interface{}) Selector

	// On represents the ON clause.
	//
	// ON is used to define conditions on a join.
	//
	//   s.Join(...).On("b.author_id = a.id")
	On(...interface{}) Selector

	// Limit represents the LIMIT parameter.
	//
	// LIMIT defines the maximum number of rows to return from the table.
	//
	//  s.Limit(42)
	Limit(int) Selector

	// Offset represents the OFFSET parameter.
	//
	// OFFSET defines how many results are going to be skipped before starting to
	// return results.
	Offset(int) Selector

	// Iterator provides methods to iterate over the results returned by the
	// Selector.
	Iterator() Iterator

	// Getter provides methods to compile and execute a query that returns
	// results.
	Getter

	// ResultMapper provides methods to retrieve and map results.
	ResultMapper

	// fmt.Stringer provides `String() string`, you can use `String()` to compile
	// the `Selector` into a string.
	fmt.Stringer

	Prefix(string, ...interface{}) Selector

	Suffix(string, ...interface{}) Selector

	Wrap(string, string, ...interface{}) Selector
}

// Inserter represents an INSERT statement.
type Inserter interface {
	// Columns represents the COLUMNS clause.
	//
	// COLUMNS defines the columns that we are going to provide values for.
	//
	//   i.Columns("name", "last_name").Values(...)
	Columns(...string) Inserter

	// Values represents the VALUES clause.
	//
	// VALUES defines the values of the columns.
	//
	//   i.Columns(...).Values("María", "Méndez")
	//
	//   i.Values(map[string][string]{"name": "María"})
	Values(...interface{}) Inserter

	// Returning represents a RETURNING clause.
	//
	// RETURNING specifies which columns should be returned after INSERT.
	//
	// RETURNING may not be supported by all SQL databases.
	Returning(columns ...string) Inserter

	// Iterator provides methods to iterate over the results returned by the
	// Inserter. This is only possible when using Returning().
	Iterator() Iterator

	// Execer provides the Exec method.
	Execer

	// Getter provides methods to return query results from INSERT statements
	// that support such feature (e.g.: queries with Returning).
	Getter

	// fmt.Stringer provides `String() string`, you can use `String()` to compile
	// the `Inserter` into a string.
	fmt.Stringer
}

// Deleter represents a DELETE statement.
type Deleter interface {
	// Where represents the WHERE clause.
	//
	// See Selector.Where for documentation and usage examples.
	Where(...interface{}) Deleter

	// Limit represents the LIMIT clause.
	//
	// See Selector.Limit for documentation and usage examples.
	Limit(int) Deleter

	// Execer provides the Exec method.
	Execer

	// fmt.Stringer provides `String() string`, you can use `String()` to compile
	// the `Inserter` into a string.
	fmt.Stringer
}

// Updater represents an UPDATE statement.
type Updater interface {
	// Set represents the SET clause.
	Set(...interface{}) Updater

	// Where represents the WHERE clause.
	//
	// See Selector.Where for documentation and usage examples.
	Where(...interface{}) Updater

	// Limit represents the LIMIT parameter.
	//
	// See Selector.Limit for documentation and usage examples.
	Limit(int) Updater

	// Execer provides the Exec method.
	Execer

	// fmt.Stringer provides `String() string`, you can use `String()` to compile
	// the `Inserter` into a string.
	fmt.Stringer
}

// Execer provides methods for executing statements that do not return results.
type Execer interface {
	// Exec executes a statement and returns sql.Result.
	Exec() (sql.Result, error)
}

// Getter provides methods for executing statements that return results.
type Getter interface {
	// Query returns *sql.Rows.
	Query() (*sql.Rows, error)

	// QueryRow returns only one row.
	QueryRow() (*sql.Row, error)
}

// ResultMapper defined methods for a result mapper.
type ResultMapper interface {
	// All dumps all the results into the given slice, All() expects a pointer to
	// slice of maps or structs.
	//
	// The behaviour of One() extends to each one of the results.
	All(destSlice interface{}) error

	// One maps the row that is in the current query cursor into the
	// given interface, which can be a pointer to either a map or a
	// struct.
	//
	// If dest is a pointer to map, each one of the columns will create a new map
	// key and the values of the result will be set as values for the keys.
	//
	// Depending on the type of map key and value, the results columns and values
	// may need to be transformed.
	//
	// If dest if a pointer to struct, each one of the fields will be tested for
	// a `db` tag which defines the column mapping. The value of the result will
	// be set as the value of the field.
	One(dest interface{}) error
}

// Iterator provides methods for iterating over query results.
type Iterator interface {
	// ResultMapper provides methods to retrieve and map results.
	ResultMapper

	// Scan dumps the current result into the given pointer variable pointers.
	Scan(dest ...interface{}) error

	// Next dumps the current element into the given destination, which could be
	// a pointer to either a map or a struct.
	Next(dest ...interface{}) bool

	// Err returns the last error produced by the cursor.
	Err() error

	// Close closes the iterator and frees up the cursor.
	Close() error
}
