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

package postgresql

import (
	"database/sql"
	"fmt"
	_ "github.com/xiam/gopostgresql"
	"log"
	"os"
	"reflect"
	"regexp"
	"strings"
	"upper.io/db"
	"upper.io/db/util/sqlutil"
)

// Format for saving dates.
var DateFormat = "2006-01-02 15:04:05"

// Format for saving times.
var TimeFormat = "%d:%02d:%02d.%d"

var SSLMode = "disable"

var columnPattern = regexp.MustCompile(`^([a-z]+)\(?([0-9,]+)?\)?\s?([a-z]*)?`)

const driverName = `postgresql`

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

	for i := 0; i < len(chunks.Args); i++ {
		query = strings.Replace(query, `?`, fmt.Sprintf(`$%d`, i+1), 1)
	}

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

	for i := 0; i < len(chunks.Args); i++ {
		query = strings.Replace(query, `?`, fmt.Sprintf(`$%d`, i+1), 1)
	}

	if debugEnabled() == true {
		debugLogQuery(query, chunks)
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

	if debugEnabled() == true {
		debugLogQuery(query, chunks)
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

	if self.config.Host == "" {
		if self.config.Socket == "" {
			self.config.Host = `127.0.0.1`
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
		conn = fmt.Sprintf(`user=%s password=%s host=%s port=%d dbname=%s sslmode=%s`, self.config.User, self.config.Password, self.config.Host, self.config.Port, self.config.Database, SSLMode)
	} else if self.config.Socket != `` {
		conn = fmt.Sprintf(`user=%s password=%s host=%s dbname=%s sslmode=%s`, self.config.User, self.config.Password, self.config.Socket, self.config.Database, SSLMode)
	}

	self.session, err = sql.Open(`postgres`, conn)

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

	rows, err := self.session.Query(`SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'`)

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

func (self *Source) tableExists(names ...string) error {
	for _, name := range names {
		rows, err := self.doQuery(
			fmt.Sprintf(`
				SELECT table_name
					FROM information_schema.tables
				WHERE table_catalog = '%s' AND table_name = '%s'
			`,
				self.Name(),
				name,
			),
		)

		if err != nil {
			return db.ErrCollectionDoesNotExists
		}

		defer rows.Close()

		if rows.Next() == false {
			return db.ErrCollectionDoesNotExists
		}
	}

	return nil
}

// Returns a collection instance by name.
func (self *Source) Collection(names ...string) (db.Collection, error) {

	if len(names) == 0 {
		return nil, db.ErrMissingCollectionName
	}

	col := &Table{
		source: self,
		T: sqlutil.T{
			PrimaryKey:  `id`,
			ColumnTypes: make(map[string]reflect.Kind),
		},
		names: names,
	}

	columns_t := []struct {
		ColumnName string `db:"column_name"`
		DataType   string `db:"data_type"`
	}{}

	for _, name := range names {
		chunks := strings.SplitN(name, " ", 2)

		if len(chunks) > 0 {

			name = chunks[0]

			if err := self.tableExists(name); err != nil {
				return nil, err
			}

			rows, err := self.doQuery(
				`SELECT
					column_name, data_type
				FROM information_schema.columns
				WHERE
					table_name = ?`,
				[]string{name},
			)

			if err != nil {
				return nil, err
			}

			err = col.FetchRows(&columns_t, rows)

			if err != nil {
				return nil, err
			}

			for _, column := range columns_t {

				column.ColumnName = strings.ToLower(column.ColumnName)
				column.DataType = strings.ToLower(column.DataType)

				results := columnPattern.FindStringSubmatch(column.DataType)

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
				case `smallint`, `integer`, `bigint`, `serial`, `bigserial`:
					if dextra == `unsigned` {
						ctype = reflect.Uint64
					} else {
						ctype = reflect.Int64
					}
				case `real`, `double`:
					ctype = reflect.Float64
				}

				col.ColumnTypes[column.ColumnName] = ctype
			}

		}
	}

	return col, nil
}
