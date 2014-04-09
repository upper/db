/*
  Copyright (c) 2014 José Carlos Nieto, https://menteslibres.net/xiam

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

package ql

import (
	"database/sql"
	"fmt"
	_ "github.com/cznic/ql/driver"
	"reflect"
	"strings"
	"upper.io/db"
)

var Debug = true

// Format for saving dates.
var DateFormat = "2006-01-02 15:04:05"

// Format for saving times.
var TimeFormat = "%d:%02d:%02d.%d"

const driverName = `ql`

func init() {
	db.Register(driverName, &Source{})
}

type sqlValues_t []string

type Source struct {
	config      db.Settings
	session     *sql.DB
	name        string
	collections map[string]db.Collection
}

type sqlQuery struct {
	Query []string
	Args  []interface{}
}

func sqlCompile(terms []interface{}) *sqlQuery {
	q := &sqlQuery{}

	q.Query = []string{}

	for _, term := range terms {
		switch t := term.(type) {
		case string:
			q.Query = append(q.Query, t)
		case []string:
			for _, arg := range t {
				q.Args = append(q.Args, arg)
			}
		case sqlValues_t:
			args := make([]string, len(t))
			for i, arg := range t {
				args[i] = `?`
				q.Args = append(q.Args, arg)
			}
			q.Query = append(q.Query, `(`+strings.Join(args, `, `)+`)`)
		}
	}

	return q
}

func sqlFields(names []string) string {
	return `(` + strings.Join(names, `, `) + `)`
}

func sqlValues(values []string) sqlValues_t {
	ret := make(sqlValues_t, len(values))
	for i, _ := range values {
		ret[i] = values[i]
	}
	return ret
}

func (self *Source) doExec(terms ...interface{}) (res sql.Result, err error) {
	var tx *sql.Tx

	if self.session == nil {
		return nil, db.ErrNotConnected
	}

	chunks := sqlCompile(terms)

	query := strings.Join(chunks.Query, ` `)

	for i := 0; i < len(chunks.Args); i++ {
		query = strings.Replace(query, `?`, fmt.Sprintf(`$%d`, i+1), 1)
	}

	if Debug == true {
		fmt.Printf("Q: %s\n", query)
		fmt.Printf("A: %v\n", chunks.Args)
	}

	if tx, err = self.session.Begin(); err != nil {
		return nil, err
	}

	if res, err = tx.Exec(query, chunks.Args...); err != nil {
		return nil, err
	}

	if err = tx.Commit(); err != nil {
		return nil, err
	}

	return res, nil
}

func (self *Source) doQuery(terms ...interface{}) (*sql.Rows, error) {
	if self.session == nil {
		return nil, db.ErrNotConnected
	}

	chunks := sqlCompile(terms)

	query := strings.Join(chunks.Query, ` `)

	for i := 0; i < len(chunks.Args); i++ {
		query = strings.Replace(query, `?`, fmt.Sprintf(`$%d`, i+1), 1)
	}

	if Debug == true {
		fmt.Printf("Q: %s\n", query)
		fmt.Printf("A: %v\n", chunks.Args)
	}

	return self.session.Query(query, chunks.Args...)
}

func (self *Source) doQueryRow(terms ...interface{}) (*sql.Row, error) {
	if self.session == nil {
		return nil, db.ErrNotConnected
	}

	chunks := sqlCompile(terms)

	query := strings.Join(chunks.Query, ` `)

	for i := 0; i < len(chunks.Args); i++ {
		query = strings.Replace(query, `?`, fmt.Sprintf(`$%d`, i+1), 1)
	}

	if Debug == true {
		fmt.Printf("Q: %s\n", query)
		fmt.Printf("A: %v\n", chunks.Args)
	}

	return self.session.QueryRow(query, chunks.Args...), nil
}

// Returns the string name of the database.
func (self *Source) Name() string {
	return self.config.Database
}

// Stores database settings.
func (self *Source) Setup(config db.Settings) error {
	self.config = config
	self.collections = make(map[string]db.Collection)
	return self.Open()
}

// Returns the underlying *sql.DB instance.
func (self *Source) Driver() interface{} {
	return self.session
}

// Attempts to connect to a database using the stored settings.
func (self *Source) Open() error {
	var err error

	if self.config.Database == "" {
		return db.ErrMissingDatabaseName
	}

	self.session, err = sql.Open(`ql`, self.config.Database)

	if err != nil {
		return err
	}

	return nil
}

// Closes the current database session.
func (self *Source) Close() error {
	if self.session != nil {
		return self.session.Close()
	}
	return nil
}

// Changes the active database.
func (self *Source) Use(database string) error {
	self.config.Database = database
	return self.Open()
}

// Starts a transaction block.
func (self *Source) Begin() error {
	_, err := self.session.Exec(`BEGIN`)
	return err
}

// Ends a transaction block.
func (self *Source) End() error {
	_, err := self.session.Exec(`END`)
	return err
}

// Drops the currently active database.
func (self *Source) Drop() error {
	self.session.Query(fmt.Sprintf(`DROP DATABASE "%s"`, self.config.Database))
	return nil
}

// Returns a list of all tables within the currently active database.
func (self *Source) Collections() ([]string, error) {
	var collections []string
	var collection string

	rows, err := self.session.Query(`SELECT Name FROM __Table`)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	for rows.Next() {
		if err = rows.Scan(&collection); err != nil {
			return nil, err
		}
		collections = append(collections, collection)
	}

	err = rows.Err()

	if err != nil {
		return nil, err
	}

	return collections, nil
}

// Returns a collection instance by name.
func (self *Source) Collection(name string) (db.Collection, error) {

	if collection, ok := self.collections[name]; ok == true {
		return collection, nil
	}

	table := &Table{}

	table.source = self
	table.DB = self
	table.PrimaryKey = `id`

	table.SetName = name

	// Table exists?
	if table.Exists() == false {
		return table, db.ErrCollectionDoesNotExists
	}

	// Fetching table datatypes and mapping to internal gotypes.
	rows, err := table.source.doQuery(
		`SELECT
			Name, Type
		FROM __Column
		WHERE
			TableName == ?`,
		[]string{table.Name()},
	)

	if err != nil {
		return table, err
	}

	columns := []struct {
		Name string
		Type string
	}{}

	err = table.FetchRows(&columns, rows)

	if err != nil {
		return nil, err
	}

	table.ColumnTypes = make(map[string]reflect.Kind, len(columns))

	for _, column := range columns {

		column.Name = strings.ToLower(column.Name)
		column.Type = strings.ToLower(column.Type)

		// Default properties.
		dtype := column.Type

		ctype := reflect.String

		// Guessing datatypes.
		switch dtype {
		case `string`:
			ctype = reflect.String
		}

		table.ColumnTypes[column.Name] = ctype
	}

	self.collections[name] = table

	return table, nil
}
