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

package postgresql

import (
	"database/sql"
	"fmt"
	_ "github.com/xiam/gopostgresql"
	"menteslibres.net/gosexy/db"
	"reflect"
	"regexp"
	"strings"
)

var Debug = false

// Format for saving dates.
var DateFormat = "2006-01-02 15:04:05"

// Format for saving times.
var TimeFormat = "%d:%02d:%02d.%d"

var SSLMode = "disable"

var columnPattern = regexp.MustCompile(`^([a-z]+)\(?([0-9,]+)?\)?\s?([a-z]*)?`)

func init() {
	db.Register("postgresql", &Source{})
}

type sqlQuery struct {
	Query   []string
	SqlArgs []interface{}
}

func sqlCompile(terms []interface{}) *sqlQuery {
	q := &sqlQuery{}

	q.Query = []string{}

	for _, term := range terms {
		switch t := term.(type) {
		case string:
			q.Query = append(q.Query, t)
		case db.SqlArgs:
			for _, arg := range t {
				q.SqlArgs = append(q.SqlArgs, arg)
			}
		case db.SqlValues:
			args := make([]string, len(t))
			for i, arg := range t {
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
		return nil, db.ErrNotConnected
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
		return nil, db.ErrNotConnected
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
		return nil, db.ErrNotConnected
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

/*
	Closes a database session.
*/
func (self *Source) Close() error {
	if self.session != nil {
		return self.session.Close()
	}
	return nil
}

/*
	Configures and returns a database session.
*/
func (self *Source) Setup(config db.DataSource) error {
	self.config = config
	self.collections = make(map[string]db.Collection)
	return self.Open()
}

/*
	Tries to open a database.
*/
func (self *Source) Open() error {
	var err error

	if self.config.Host == "" {
		if self.config.Socket == "" {
			self.config.Host = "127.0.0.1"
		}
	}

	if self.config.Port == 0 {
		self.config.Port = 5432
	}

	if self.config.Database == "" {
		return db.ErrMissingDatabaseName
	}

	if self.config.Socket != "" && self.config.Host != "" {
		return db.ErrSockerOrHost
	}

	var conn string

	if self.config.Host != "" {
		conn = fmt.Sprintf("user=%s password=%s host=%s port=%d dbname=%s sslmode=%s", self.config.User, self.config.Password, self.config.Host, self.config.Port, self.config.Database, SSLMode)
	} else if self.config.Socket != "" {
		conn = fmt.Sprintf("user=%s password=%s host=%s dbname=%s sslmode=%s", self.config.User, self.config.Password, self.config.Socket, self.config.Database, SSLMode)
	}

	self.session, err = sql.Open("postgres", conn)

	if err != nil {
		return err
	}

	return nil
}

/*
	Changes the active database.
*/
func (self *Source) Use(database string) error {
	self.config.Database = database
	return self.Open()
}

/*
	Starts a transaction block.
*/
func (self *Source) Begin() error {
	_, err := self.session.Exec(`BEGIN`)
	return err
}

/*
	Ends a transaction block.
*/
func (self *Source) End() error {
	_, err := self.session.Exec(`END`)
	return err
}

/*
	Drops the currently active database.
*/
func (self *Source) Drop() error {
	self.session.Query(fmt.Sprintf(`DROP DATABASE "%s"`, self.config.Database))
	return nil
}

/*
	Returns a *sql.DB object that represents an internal session.
*/
func (self *Source) Driver() interface{} {
	return self.session
}

/*
	Returns a list of all tables in the current database.
*/
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

/*
	Returns a collection that must exists or panics.
*/
func (self *Source) ExistentCollection(name string) db.Collection {
	col, err := self.Collection(name)
	if err != nil {
		panic(err.Error())
	}
	return col
}

/*
	Returns a table struct by name.
*/
func (self *Source) Collection(name string) (db.Collection, error) {

	if collection, ok := self.collections[name]; ok == true {
		return collection, nil
	}

	table := &Table{}

	table.source = self
	table.DB = self
	table.PrimaryKey = "id"

	table.SetName = name

	// Table exists?
	if table.Exists() == false {
		return table, db.ErrCollectionDoesNotExists
	}

	// Fetching table datatypes and mapping to internal gotypes.
	rows, err := table.source.doQuery(
		"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = ?",
		db.SqlArgs{table.Name()},
	)

	if err != nil {
		return table, err
	}

	columns := []struct {
		ColumnName string
		DataType   string
	}{}

	err = table.FetchRows(&columns, rows)

	if err != nil {
		return nil, err
	}

	table.ColumnTypes = make(map[string]reflect.Kind, len(columns))

	for _, column := range columns {

		column.ColumnName = strings.ToLower(column.ColumnName)
		column.DataType = strings.ToLower(column.DataType)

		results := columnPattern.FindStringSubmatch(column.DataType)

		// Default properties.
		dextra := ""
		dtype := "varchar"

		dtype = results[1]

		if len(results) > 3 {
			dextra = results[3]
		}

		ctype := reflect.String

		// Guessing datatypes.
		switch dtype {
		case "smallint", "integer", "bigint", "serial", "bigserial":
			if dextra == "unsigned" {
				ctype = reflect.Uint64
			} else {
				ctype = reflect.Int64
			}
		case "real", "double":
			ctype = reflect.Float64
		}

		table.ColumnTypes[column.ColumnName] = ctype
	}

	self.collections[name] = table

	return table, nil
}
