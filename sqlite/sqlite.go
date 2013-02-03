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
	//_ "github.com/mattn/go-sqlite3"
	//_ "bitbucket.org/minux/go.sqlite3"
	"reflect"
	"regexp"
	"strings"
)

var Debug = false

const dateFormat = "2006-01-02 15:04:05"
const timeFormat = "%d:%02d:%02d"

func init() {
	db.Register("sqlite", &Source{})
}

type sqlQuery struct {
	Query   []string
	SqlArgs []string
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

func (sl *Source) sqlExec(method string, terms ...interface{}) (sql.Rows, error) {

	var rows sql.Rows

	sn := reflect.ValueOf(sl.session)
	fn := sn.MethodByName(method)

	q := sqlCompile(terms)

	if Debug == true {
		fmt.Printf("Q: %v\n", q.Query)
		fmt.Printf("A: %v\n", q.SqlArgs)
	}

	args := make([]reflect.Value, len(q.SqlArgs)+1)

	args[0] = reflect.ValueOf(strings.Join(q.Query, " "))

	for i := 0; i < len(q.SqlArgs); i++ {
		args[1+i] = reflect.ValueOf(q.SqlArgs[i])
	}

	res := fn.Call(args)

	if res[1].IsNil() == false {
		return rows, res[1].Elem().Interface().(error)
	}

	switch res[0].Elem().Interface().(type) {
	case sql.Rows:
		rows = res[0].Elem().Interface().(sql.Rows)
	}

	return rows, nil
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
func (sl *Source) Driver() interface{} {
	return sl.session
}

// Tries to open a connection to the current SQLite session.
func (sl *Source) Open() error {
	var err error

	if sl.config.Database == "" {
		panic("Database name is required.")
	}

	sl.session, err = sql.Open("sqlite3", sl.config.Database)

	if err != nil {
		return fmt.Errorf("Could not connect to %s", sl.config.Host)
	}

	return nil
}

// Closes a previously opened SQLite database session.
func (sl *Source) Close() error {
	if sl.session != nil {
		return sl.session.Close()
	}
	return nil
}

// Changes the active database.
func (sl *Source) Use(database string) error {
	sl.config.Database = database
	sl.session.Query(fmt.Sprintf("USE %s", database))
	return nil
}

// Deletes the currently active database.
func (sl *Source) Drop() error {
	sl.session.Query(fmt.Sprintf("DROP DATABASE %s", sl.config.Database))
	return nil
}

// Returns the list of SQLite tables in the current database.
func (sl *Source) Collections() []string {
	var collections []string
	var collection string

	rows, _ := sl.session.Query("SELECT tbl_name FROM sqlite_master WHERE type = ?", "table")

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
func (sl *Source) Collection(name string) (db.Collection, error) {

	if collection, ok := sl.collections[name]; ok == true {
		return collection, nil
	}

	t := &Table{}

	t.parent = sl
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

	columns := t.slFetchAll(*rows)

	pattern, _ := regexp.Compile("^([a-z]+)\\(?([0-9,]+)?\\)?\\s?([a-z]*)?")

	t.types = make(map[string]reflect.Kind, len(columns))

	for _, column := range columns {

		cname := strings.ToLower(column["name"].(string))
		ctype := strings.ToLower(column["type"].(string))

		results := pattern.FindStringSubmatch(ctype)

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

		/*
		   fmt.Printf("Imported %v (from %v)\n", vtype, dtype)
		*/

		t.types[cname] = vtype
	}

	sl.collections[name] = t

	return t, nil
}
