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

package mysql

import (
	_ "github.com/Go-SQL-Driver/MySQL"
	//_ "github.com/ziutek/mymysql/godrv"
	"database/sql"
	"fmt"
	"github.com/gosexy/db"
	"reflect"
	"regexp"
	"strings"
	"time"
)

var Debug = false

const dateFormat = "2006-01-02 15:04:05.000000000"
const timeFormat = "%d:%02d:%02d.%09d"

func init() {
	db.Register("mysql", &Source{})
}

// MySQl datasource.
type Source struct {
	session     *sql.DB
	config      db.DataSource
	collections map[string]db.Collection
}

// Mysql table/collection.
type Table struct {
	parent *Source
	name   string
	types  map[string]reflect.Kind
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

func sqlTable(name string) string {
	return name
}

func sqlFields(names []string) string {
	for i, _ := range names {
		names[i] = strings.Replace(names[i], "`", "``", -1)
	}
	return "(`" + strings.Join(names, "`, `") + "`)"
}

func sqlValues(values []string) db.SqlValues {
	ret := make(db.SqlValues, len(values))
	for i, _ := range values {
		ret[i] = values[i]
	}
	return ret
}

// Returns database name.
func (self *Source) Name() string {
	return self.config.Database
}

func toInternal(val interface{}) string {

	switch val.(type) {
	case []byte:
		return fmt.Sprintf("%s", string(val.([]byte)))
	case time.Time:
		return val.(time.Time).Format(dateFormat)
	case time.Duration:
		t := val.(time.Duration)
		return fmt.Sprintf(timeFormat, int(t.Hours()), int(t.Minutes())%60, int(t.Seconds())%60, t.Nanoseconds())
	case bool:
		if val.(bool) == true {
			return "1"
		} else {
			return "0"
		}
	}

	return fmt.Sprintf("%v", val)
}

func toNative(val interface{}) interface{} {

	switch val.(type) {
	}

	return val

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

// Configures and returns a database session.
func Session(config db.DataSource) db.Database {
	self := &Source{}
	self.config = config
	self.collections = make(map[string]db.Collection)
	return self
}

// Closes a previously opened database session.
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

// Tries to open a connection to the current datasource.
func (self *Source) Open() error {
	var err error

	if self.config.Host == "" {
		self.config.Host = "127.0.0.1"
	}

	if self.config.Port == 0 {
		self.config.Port = 3306
	}

	if self.config.Database == "" {
		return fmt.Errorf("Database name is required.")
	}

	conn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", self.config.User, self.config.Password, self.config.Host, self.config.Port, self.config.Database)
	//conn := fmt.Sprintf("tcp:%s*%s/%s/%s", self.config.Host, self.config.Database, self.config.User, self.config.Password)

	self.session, err = sql.Open("mysql", conn)

	if err != nil {
		return err
	}

	return nil
}

// Changes the active database
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

// Returns the *sql.DB underlying driver.
func (self *Source) Driver() interface{} {
	return self.session
}

// Returns the names of all the collection on the current database.
func (self *Source) Collections() []string {
	var collections []string
	var collection string

	rows, err := self.session.Query("SHOW TABLES")

	if err == nil {
		for rows.Next() {
			rows.Scan(&collection)
			collections = append(collections, collection)
		}
	}

	return collections
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

	if col, ok := self.collections[name]; ok == true {
		return col, nil
	}

	table := &Table{}

	table.parent = self
	table.name = name

	// Table exists?
	if table.Exists() == false {
		return table, fmt.Errorf("Table %s does not exists.", name)
	}

	// Fetching table datatypes and mapping to internal gotypes.
	rows, err := table.parent.doQuery(
		"SHOW COLUMNS FROM", table.Name(),
	)

	if err != nil {
		return table, err
	}

	columns := table.sqlFetchAll(rows)

	pattern, _ := regexp.Compile("^([a-z]+)\\(?([0-9,]+)?\\)?\\s?([a-z]*)?")

	table.types = make(map[string]reflect.Kind, len(columns))

	for _, column := range columns {
		cname := strings.ToLower(column["field"].(string))
		ctype := strings.ToLower(column["type"].(string))
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
		case "tinyint", "smallint", "mediumint", "int", "bigint":
			if dextra == "unsigned" {
				vtype = reflect.Uint64
			} else {
				vtype = reflect.Int64
			}
		case "decimal", "float", "double":
			vtype = reflect.Float64
		}

		/*
		 fmt.Printf("Imported %v (from %v)\n", vtype, dtype)
		*/

		table.types[cname] = vtype
	}

	return table, nil
}
