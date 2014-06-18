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

package sqlite

import (
	"database/sql"
	"fmt"
	// This hack is not required anymore.
	// See: https://github.com/mattn/go-sqlite3/issues/40
	//_ "github.com/xiam/gosqlite3"
	"log"
	"os"
	"reflect"
	"regexp"
	"strings"

	_ "github.com/mattn/go-sqlite3"
	"upper.io/db"
)

// Format for saving dates.
var DateFormat = `2006-01-02 15:04:05`

// Format for saving times.
var TimeFormat = `%d:%02d:%02d.%d`

var columnPattern = regexp.MustCompile(`^([a-zA-Z]+)\(?([0-9,]+)?\)?\s?([a-zA-Z]*)?`)

const driverName = `sqlite`

type sqlValues_t []interface{}

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

func debugEnabled() bool {
	if os.Getenv(db.EnvEnableDebug) != "" {
		return true
	}
	return false
}

func init() {
	db.Register(driverName, &Source{})
}

func debugLogQuery(s string, q *sqlQuery) {
	log.Printf("SQL: %s\nARGS: %v\n", strings.TrimSpace(s), q.Args)
}

func sqlCompile(terms []interface{}) *sqlQuery {
	q := &sqlQuery{}

	q.Query = []string{}

	for _, term := range terms {
		switch t := term.(type) {
		case sqlValues_t:
			args := make([]string, len(t))
			for i, arg := range t {
				args[i] = `?`
				q.Args = append(q.Args, arg)
			}
			q.Query = append(q.Query, `(`+strings.Join(args, `, `)+`)`)
		case string:
			q.Query = append(q.Query, t)
		default:
			if reflect.TypeOf(t).Kind() == reflect.Slice {
				var v = reflect.ValueOf(t)
				for i := 0; i < v.Len(); i++ {
					q.Args = append(q.Args, v.Index(i).Interface())
				}
			} else {
				q.Args = append(q.Args, t)
			}
		}
	}

	return q
}

func sqlFields(names []string) string {
	for i, _ := range names {
		names[i] = strings.Replace(names[i], `"`, `\"`, -1)
	}
	return `("` + strings.Join(names, `", "`) + `")`
}

func sqlValues(values []interface{}) sqlValues_t {
	ret := make(sqlValues_t, len(values))
	for i, _ := range values {
		ret[i] = values[i]
	}
	return ret
}

func (self *Source) doExec(terms ...interface{}) (sql.Result, error) {
	if self.session == nil {
		return nil, db.ErrNotConnected
	}

	chunks := sqlCompile(terms)

	query := strings.Join(chunks.Query, ` `)

	if debugEnabled() == true {
		debugLogQuery(query, chunks)
	}

	return self.session.Exec(query, chunks.Args...)
}

func (self *Source) doQuery(terms ...interface{}) (*sql.Rows, error) {

	if self.session == nil {
		return nil, db.ErrNotConnected
	}

	chunks := sqlCompile(terms)

	query := strings.Join(chunks.Query, ` `)

	if debugEnabled() == true {
		debugLogQuery(query, chunks)
	}

	return self.session.Query(query, chunks.Args...)
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

	self.session, err = sql.Open(`sqlite3`, fmt.Sprintf(`file:%s?cache=shared`, self.config.Database))

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
	_, err := self.session.Exec(fmt.Sprintf(`DROP DATABASE '%s'`, self.config.Database))
	return err
}

// Returns a list of all tables within the currently active database.
func (self *Source) Collections() ([]string, error) {
	var collections []string
	var collection string

	rows, err := self.session.Query(`SELECT tbl_name FROM sqlite_master WHERE type = ?`, `table`)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	for rows.Next() {
		rows.Scan(&collection)
		collections = append(collections, collection)
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

	table.SetName = name

	// Table exists?
	if table.Exists() == false {
		return table, db.ErrCollectionDoesNotExists
	}

	// Fetching table datatypes and mapping to internal gotypes.
	rows, err := table.source.session.Query(fmt.Sprintf(`PRAGMA TABLE_INFO('%s')`, table.Name()))

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

		results := columnPattern.FindStringSubmatch(column.Type)

		// Default properties.
		dextra := ""
		dtype := `text`

		dtype = results[1]

		if len(results) > 3 {
			dextra = results[3]
		}

		ctype := reflect.String

		// Guessing datatypes.
		switch dtype {
		case `integer`:
			if dextra == `unsigned` {
				ctype = reflect.Uint64
			} else {
				ctype = reflect.Int64
			}
		case `real`, `numeric`:
			ctype = reflect.Float64
		default:
			ctype = reflect.String
		}

		table.ColumnTypes[column.Name] = ctype
	}

	self.collections[name] = table

	return table, nil
}
