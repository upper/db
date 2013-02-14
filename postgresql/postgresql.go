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
	//_ "github.com/bmizerany/pq"
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

// Stores PostgreSQL session data.
type Source struct {
	config      db.DataSource
	session     *sql.DB
	collections map[string]db.Collection
}

func (self *Source) Name() string {
	return self.config.Database
}

// Wraps sql.DB.QueryRow
func (self *Source) doQueryRow(terms ...interface{}) (*sql.Row, error) {
	if self.session == nil {
		return nil, fmt.Errorf("You're currently not connected.")
	}

	chunks := sqlCompile(terms)

	query := strings.Join(chunks.Query, " ")

	for i := 0; i < len(chunks.SqlArgs); i++ {
		query = strings.Replace(query, "?", fmt.Sprintf("$%d", i+1), 1)
	}

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

	for i := 0; i < len(chunks.SqlArgs); i++ {
		query = strings.Replace(query, "?", fmt.Sprintf("$%d", i+1), 1)
	}

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

	for i := 0; i < len(chunks.SqlArgs); i++ {
		query = strings.Replace(query, "?", fmt.Sprintf("$%d", i+1), 1)
	}

	if Debug == true {
		fmt.Printf("Q: %s\n", query)
		fmt.Printf("A: %v\n", chunks.SqlArgs)
	}

	return self.session.Exec(query, chunks.SqlArgs...)
}

// Configures and returns a PostgreSQL dabase session.
func Session(config db.DataSource) db.Database {
	m := &Source{}
	m.config = config
	m.collections = make(map[string]db.Collection)
	return m
}

// Closes a previously opened PostgreSQL database session.
func (self *Source) Close() error {
	if self.session != nil {
		return self.session.Close()
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
func (self *Source) Open() error {
	var err error

	if self.config.Host == "" {
		self.config.Host = "127.0.0.1"
	}

	if self.config.Port == 0 {
		self.config.Port = 5432
	}

	if self.config.Database == "" {
		return fmt.Errorf("Database name is required.")
	}

	conn := fmt.Sprintf("user=%s password=%s host=%s port=%d dbname=%s sslmode=disable", self.config.User, self.config.Password, self.config.Host, self.config.Port, self.config.Database)

	self.session, err = sql.Open("postgres", conn)

	if err != nil {
		return fmt.Errorf("Could not connect to %s: %s", self.config.Host, err.Error())
	}

	return nil
}

// Changes the active database.
func (self *Source) Use(database string) error {
	self.config.Database = database
	return self.Open()
}

// Deletes the currently active database.
func (self *Source) Drop() error {
	self.session.Query(fmt.Sprintf("DROP DATABASE %s", self.config.Database))
	return nil
}

// Returns a *sql.DB object that represents an internal session.
func (self *Source) Driver() interface{} {
	return self.session
}

// Returns the list of PostgreSQL tables in the current database.
func (self *Source) Collections() []string {
	var collections []string
	var collection string

	rows, err := self.session.Query("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")

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

	rows, err := t.parent.doQuery(
		"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = ?",
		db.SqlArgs{t.name},
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

	self.collections[name] = t

	return t, nil
}
