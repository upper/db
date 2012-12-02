/*
  Copyright (c) 2012 José Carlos Nieto, http://xiam.menteslibres.org/

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

package postgresql

import (
	"database/sql"
	"fmt"
	"github.com/gosexy/db"
	_ "github.com/xiam/gopostgresql"
	"reflect"
	"regexp"
	"strings"
	"time"
)

func init() {
	db.Register("postgresql", &Source{})
}

var Debug = false

const dateFormat = "2006-01-02 15:04:05"
const timeFormat = "%d:%02d:%02d"

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
	return "(" + strings.Join(names, ", ") + ")"
}

func sqlValues(values []string) db.SqlValues {
	ret := make(db.SqlValues, len(values))
	for i, _ := range values {
		ret[i] = values[i]
	}
	return ret
}

// Stores PostgreSQL session data.
type Source struct {
	config      db.DataSource
	session     *sql.DB
	collections map[string]db.Collection
}

func (self *Source) Name() string {
	return self.config.Database
}

func (pg *Source) sqlExec(method string, terms ...interface{}) (sql.Rows, error) {

	sn := reflect.ValueOf(pg.session)
	fn := sn.MethodByName(method)

	q := sqlCompile(terms)

	if Debug {
		fmt.Printf("Q: %v\n", q.Query)
		fmt.Printf("A: %v\n", q.SqlArgs)
	}

	qs := strings.Join(q.Query, " ")

	args := make([]reflect.Value, len(q.SqlArgs)+1)

	for i := 0; i < len(q.SqlArgs); i++ {
		qs = strings.Replace(qs, "?", fmt.Sprintf("$%d", i+1), 1)
		args[1+i] = reflect.ValueOf(q.SqlArgs[i])
	}

	args[0] = reflect.ValueOf(qs)

	res := fn.Call(args)

	if res[1].IsNil() == false {
		return sql.Rows{}, res[1].Elem().Interface().(error)
	}

	switch res[0].Elem().Interface().(type) {
	case sql.Rows:
		return res[0].Elem().Interface().(sql.Rows), nil
	}

	return sql.Rows{}, nil
}

// Configures and returns a PostgreSQL dabase session.
func Session(config db.DataSource) db.Database {
	m := &Source{}
	m.config = config
	m.collections = make(map[string]db.Collection)
	return m
}

// Closes a previously opened PostgreSQL database session.
func (pg *Source) Close() error {
	if pg.session != nil {
		return pg.session.Close()
	}
	return nil
}

// Configures a datasource and tries to open a connection.
func (self *Source) Setup(config db.DataSource) error {
	self.config = config
	self.collections = make(map[string]db.Collection)
	return self.Open()
}

// Tries to open a connection to the current PostgreSQL session.
func (pg *Source) Open() error {
	var err error

	if pg.config.Host == "" {
		pg.config.Host = "127.0.0.1"
	}

	if pg.config.Port == 0 {
		pg.config.Port = 5432
	}

	if pg.config.Database == "" {
		panic("Database name is required.")
	}

	conn := fmt.Sprintf("user=%s password=%s host=%s port=%d dbname=%s sslmode=disable", pg.config.User, pg.config.Password, pg.config.Host, pg.config.Port, pg.config.Database)

	pg.session, err = sql.Open("postgres", conn)

	if err != nil {
		return fmt.Errorf("Could not connect to %s", pg.config.Host)
	}

	return nil
}

// Changes the active database.
func (pg *Source) Use(database string) error {
	pg.config.Database = database
	return pg.Open()
}

// Deletes the currently active database.
func (pg *Source) Drop() error {
	pg.session.Query(fmt.Sprintf("DROP DATABASE %s", pg.config.Database))
	return nil
}

// Returns a *sql.DB object that represents an internal session.
func (pg *Source) Driver() interface{} {
	return pg.session
}

// Returns the list of PostgreSQL tables in the current database.
func (pg *Source) Collections() []string {
	var collections []string
	var collection string

	rows, err := pg.session.Query("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")

	if err == nil {
		for rows.Next() {
			rows.Scan(&collection)
			collections = append(collections, collection)
		}
	} else {
		panic(err)
	}

	return collections
}

func toInternal(val interface{}) string {

	switch val.(type) {
	case []byte:
		return fmt.Sprintf("%s", string(val.([]byte)))
	case time.Time:
		return val.(time.Time).Format(dateFormat)
	case time.Duration:
		t := val.(time.Duration)
		return fmt.Sprintf(timeFormat, int(t.Hours()), int(t.Minutes())%60, int(t.Seconds())%60)
	case bool:
		if val.(bool) == true {
			return "1"
		} else {
			return "0"
		}
	}

	return fmt.Sprintf("%v", val)
}

// Returns a collection. Panics if the collection does not exists.
func (self *Source) ExistentCollection(name string) db.Collection {
	col, err := self.Collection(name)
	if err != nil {
		panic(err.Error())
	}
	return col
}

// Returns a collection by name.
func (pg *Source) Collection(name string) (db.Collection, error) {

	if collection, ok := pg.collections[name]; ok == true {
		return collection, nil
	}

	t := &Table{}

	t.parent = pg
	t.name = name

	// Table exists?
	if t.Exists() == false {
		return t, fmt.Errorf("Table %s does not exists.", name)
	}

	// Fetching table datatypes and mapping to internal gotypes.

	rows, err := t.parent.sqlExec(
		"Query",
		"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = ?", db.SqlArgs{t.name},
	)

	if err != nil {
		return t, err
	}

	columns := t.sqlFetchAll(rows)

	pattern, _ := regexp.Compile("^([a-z]+)\\(?([0-9,]+)?\\)?\\s?([a-z]*)?")

	t.types = make(map[string]reflect.Kind, len(columns))

	for _, column := range columns {
		cname := strings.ToLower(column["column_name"].(string))
		ctype := strings.ToLower(column["data_type"].(string))

		results := pattern.FindStringSubmatch(ctype)

		// Default properties.
		dextra := ""
		dtype := "varchar"

		dtype = results[1]

		if len(results) > 3 {
			dextra = results[3]
		}

		vtype := reflect.String

		// Guessing datatypes.
		switch dtype {
		case "smallint", "integer", "bigint", "serial", "bigserial":
			if dextra == "unsigned" {
				vtype = reflect.Uint64
			} else {
				vtype = reflect.Int64
			}
		case "real", "double":
			vtype = reflect.Float64
		}

		//fmt.Printf("Imported %v (from %v)\n", vtype, dtype)

		t.types[cname] = vtype
	}

	pg.collections[name] = t

	return t, nil
}
