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

package sqlite

import (
	"database/sql"
	"fmt"
	"github.com/gosexy/db"
	_ "github.com/xiam/gosqlite3"
	"reflect"
	"regexp"
	"strings"
)

var Debug = false

const DateFormat = "2006-01-02 15:04:05"
const TimeFormat = "%d:%02d:%02d.%09d"

var columnPattern = regexp.MustCompile("^([a-z]+)\\(?([0-9,]+)?\\)?\\s?([a-z]*)?")

func init() {
	db.Register("sqlite", &Source{})
}

type sqlQuery struct {
	Query   []string
	SqlArgs []interface{}
}

func sqlCompile(terms []interface{}) *sqlQuery {
	q := &sqlQuery{}

	q.Query = []string{}

	for _, term := range terms {
		switch term.(type) {
		case string:
			q.Query = append(q.Query, term.(string))
		case db.SqlArgs:
			for _, arg := range term.(db.SqlArgs) {
				q.SqlArgs = append(q.SqlArgs, arg)
			}
		case db.SqlValues:
			args := make([]string, len(term.(db.SqlValues)))
			for i, arg := range term.(db.SqlValues) {
				args[i] = "?"
				q.SqlArgs = append(q.SqlArgs, arg)
			}
			q.Query = append(q.Query, "("+strings.Join(args, ", ")+")")
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

func sqlValues(values []string) db.SqlValues {
	ret := make(db.SqlValues, len(values))
	for i, _ := range values {
		ret[i] = values[i]
	}
	return ret
}

// Stores driver's session data.
type Source struct {
	config      db.DataSource
	session     *sql.DB
	name        string
	collections map[string]db.Collection
}

func (self *Source) Name() string {
	return self.config.Database
}

func (self *Source) doQueryRow(terms ...interface{}) (*sql.Row, error) {
	if self.session == nil {
		return nil, fmt.Errorf("You're currently not connected.")
	}

	chunks := sqlCompile(terms)

	query := strings.Join(chunks.Query, " ")

	if Debug == true {
		fmt.Printf("Q: %s\n", query)
		fmt.Printf("A: %v\n", chunks.SqlArgs)
	}

	return self.session.QueryRow(query, chunks.SqlArgs...), nil
}

// Wraps sql.DB.Query
func (self *Source) doQuery(terms ...interface{}) (*sql.Rows, error) {
	if self.session == nil {
		return nil, fmt.Errorf("You're currently not connected.")
	}

	chunks := sqlCompile(terms)

	query := strings.Join(chunks.Query, " ")

	if Debug == true {
		fmt.Printf("Q: %s\n", query)
		fmt.Printf("A: %v\n", chunks.SqlArgs)
	}

	return self.session.Query(query, chunks.SqlArgs...)
}

// Wraps sql.DB.Exec
func (self *Source) doExec(terms ...interface{}) (sql.Result, error) {
	if self.session == nil {
		return nil, fmt.Errorf("You're currently not connected.")
	}

	chunks := sqlCompile(terms)

	query := strings.Join(chunks.Query, " ")

	if Debug == true {
		fmt.Printf("Q: %s\n", query)
		fmt.Printf("A: %v\n", chunks.SqlArgs)
	}

	return self.session.Exec(query, chunks.SqlArgs...)
}

// Represents a SQLite table.
type Table struct {
	parent *Source
	name   string
	types  map[string]reflect.Kind
}

// Configures and returns a SQLite database session.
func (self *Source) Setup(config db.DataSource) error {
	self.config = config
	self.collections = make(map[string]db.Collection)
	return self.Open()
}

// Deprecated: Configures and returns a SQLite database session.
func SqliteSession(config db.DataSource) db.Database {
	m := &Source{}
	m.config = config
	m.collections = make(map[string]db.Collection)
	return m
}

// Returns a *sql.DB object that represents an internal session.
func (self *Source) Driver() interface{} {
	return self.session
}

// Tries to open a database file.
func (self *Source) Open() error {
	var err error

	if self.config.Database == "" {
		return fmt.Errorf("Missing database path.")
	}

	self.session, err = sql.Open("sqlite3", self.config.Database)

	if err != nil {
		return fmt.Errorf("Could not open %s: %s", self.config.Database, err.Error())
	}

	return nil
}

// Closes a previously opened SQLite database session.
func (self *Source) Close() error {
	if self.session != nil {
		return self.session.Close()
	}
	return nil
}

// Changes the active database.
func (self *Source) Use(database string) error {
	self.config.Database = database
	_, err := self.session.Exec(fmt.Sprintf("USE %s", database))
	return err
}

// Deletes the currently active database.
func (self *Source) Drop() error {
	_, err := self.session.Exec(fmt.Sprintf("DROP DATABASE %s", self.config.Database))
	return err
}

// Returns the list of SQLite tables in the current database.
func (self *Source) Collections() []string {
	var collections []string
	var collection string

	rows, _ := self.session.Query("SELECT tbl_name FROM sqlite_master WHERE type = ?", "table")

	for rows.Next() {
		rows.Scan(&collection)
		collections = append(collections, collection)
	}

	return collections
}

func (self *Source) ExistentCollection(name string) db.Collection {
	col, err := self.Collection(name)
	if err != nil {
		panic(err)
	}
	return col
}

// Returns a SQLite table structure by name.
func (self *Source) Collection(name string) (db.Collection, error) {

	if collection, ok := self.collections[name]; ok == true {
		return collection, nil
	}

	t := &Table{}

	t.parent = self
	t.name = name

	// Table exists?
	if t.Exists() == false {
		return t, fmt.Errorf("Table %s does not exists.", name)
	}

	// Fetching table datatypes and mapping to internal gotypes.

	rows, err := t.parent.session.Query(fmt.Sprintf("PRAGMA TABLE_INFO('%s')", t.name))

	if err != nil {
		return t, err
	}

	columns := t.FetchAll(rows)

	t.types = make(map[string]reflect.Kind, len(columns))

	for _, column := range columns {

		cname := strings.ToLower(column["name"].(string))
		ctype := strings.ToLower(column["type"].(string))

		results := columnPattern.FindStringSubmatch(string(ctype))

		// Default properties.
		dextra := ""
		dtype := "text"

		dtype = results[1]

		if len(results) > 3 {
			dextra = results[3]
		}

		vtype := reflect.String

		// Guessing datatypes.
		switch dtype {
		case "integer":
			if dextra == "unsigned" {
				vtype = reflect.Uint64
			} else {
				vtype = reflect.Int64
			}
		case "real", "numeric":
			vtype = reflect.Float64
		default:
			vtype = reflect.String
		}

		t.types[cname] = vtype
	}

	self.collections[name] = t

	return t, nil
}
