/*
  Copyright (c) 2012-2013 José Carlos Nieto, https://menteslibres.net/xiam

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
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"reflect"
	"regexp"
	"strings"
	"upper.io/db"
)

var Debug = false

// Format for saving dates.
const DateFormat = "2006-01-02 15:04:05.000"

// Format for saving times.
const TimeFormat = "%d:%02d:%02d.%03d"

var columnPattern = regexp.MustCompile(`^([a-z]+)\(?([0-9,]+)?\)?\s?([a-z]*)?`)

const driverName = `mysql`

func init() {
	db.Register(driverName, &Source{})
}

type sqlValues_t []string

type Source struct {
	session     *sql.DB
	config      db.Settings
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
	for i, _ := range names {
		names[i] = strings.Replace(names[i], "`", "``", -1)
	}
	return "(`" + strings.Join(names, "`, `") + "`)"
}

func sqlValues(values []string) sqlValues_t {
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

	if Debug == true {
		fmt.Printf("Q: %s\n", query)
		fmt.Printf("A: %v\n", chunks.Args)
	}

	return self.session.Exec(query, chunks.Args...)
}

func (self *Source) doQuery(terms ...interface{}) (*sql.Rows, error) {
	if self.session == nil {
		return nil, db.ErrNotConnected
	}

	chunks := sqlCompile(terms)

	query := strings.Join(chunks.Query, " ")

	if Debug == true {
		fmt.Printf("Q: %s\n", query)
		fmt.Printf("A: %v\n", chunks.Args)
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
	self.session = nil
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

	if self.config.Host == "" {
		if self.config.Socket == "" {
			self.config.Host = `127.0.0.1`
		}
	}

	if self.config.Port == 0 {
		self.config.Port = 3306
	}

	if self.config.Database == "" {
		return db.ErrMissingDatabaseName
	}

	if self.config.Socket != "" && self.config.Host != "" {
		return db.ErrSockerOrHost
	}

	if self.config.Charset == "" {
		self.config.Charset = `utf8`
	}

	var conn string

	if self.config.Host != "" {
		conn = fmt.Sprintf(`%s:%s@tcp(%s:%d)/%s?charset=%s`, self.config.User, self.config.Password, self.config.Host, self.config.Port, self.config.Database, self.config.Charset)
	} else if self.config.Socket != `` {
		conn = fmt.Sprintf(`%s:%s@unix(%s)/%s?charset=%s`, self.config.User, self.config.Password, self.config.Socket, self.config.Database, self.config.Charset)
	}

	self.session, err = sql.Open(`mysql`, conn)

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
	_, err := self.session.Exec(fmt.Sprintf("USE `%s`", database))
	return err
}

// Starts a transaction block.
func (self *Source) Begin() error {
	_, err := self.session.Exec(`START TRANSACTION`)
	return err
}

// Ends a transaction block.
func (self *Source) End() error {
	_, err := self.session.Exec(`COMMIT`)
	return err
}

// Drops the currently active database.
func (self *Source) Drop() error {
	_, err := self.session.Exec(fmt.Sprintf("DROP DATABASE `%s`", self.config.Database))
	return err
}

// Returns a list of all tables within the currently active database.
func (self *Source) Collections() ([]string, error) {
	var collections []string
	var collection string

	rows, err := self.session.Query(`SHOW TABLES`)

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

	if col, ok := self.collections[name]; ok == true {
		return col, nil
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
	rows, err := table.source.doQuery(
		fmt.Sprintf(
			"SHOW COLUMNS FROM `%s`",
			table.Name(),
		),
	)

	if err != nil {
		return table, err
	}

	columns := []struct {
		Field string
		Type  string
	}{}

	err = table.FetchRows(&columns, rows)

	if err != nil {
		return nil, err
	}

	table.ColumnTypes = make(map[string]reflect.Kind, len(columns))

	for _, column := range columns {

		column.Field = strings.ToLower(column.Field)
		column.Type = strings.ToLower(column.Type)

		results := columnPattern.FindStringSubmatch(column.Type)

		// Default properties.
		dextra := ""
		dtype := `varchar`

		dtype = results[1]

		if len(results) > 3 {
			dextra = results[3]
		}

		ctype := reflect.String

		// Guessing datatypes.
		switch dtype {
		case `tinyint`, `smallint`, `mediumint`, `int`, `bigint`:
			if dextra == `unsigned` {
				ctype = reflect.Uint64
			} else {
				ctype = reflect.Int64
			}
		case `decimal`, `float`, `double`:
			ctype = reflect.Float64
		}

		table.ColumnTypes[column.Field] = ctype
	}

	self.collections[name] = table

	return table, nil
}
