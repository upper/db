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

// Package postgresql provides a PostgreSQL driver for upper.io/db.
package postgresql

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"reflect"
	"regexp"
	"strings"

	_ "github.com/xiam/gopostgresql" // for PostgreSQL driver
	"upper.io/db"
)

// Format for saving dates.
var DateFormat = "2006-01-02 15:04:05"

// Format for saving times.
var TimeFormat = "%d:%02d:%02d.%d"

var sslMode = "disable"

var columnPattern = regexp.MustCompile(`^([a-z]+)\(?([0-9,]+)?\)?\s?([a-z]*)?`)

const driverName = `postgresql`

type sqlValues []interface{}

type source struct {
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
	db.Register(driverName, &source{})
}

func debugLogQuery(s string, q *sqlQuery) {
	log.Printf("SQL: %s\nARGS: %v\n", strings.TrimSpace(s), q.Args)
}

func sqlCompile(terms []interface{}) *sqlQuery {
	q := &sqlQuery{}

	q.Query = []string{}

	for _, term := range terms {
		switch t := term.(type) {
		case sqlValues:
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
	for i := range names {
		names[i] = strings.Replace(names[i], `"`, `\"`, -1)
	}
	return `("` + strings.Join(names, `", "`) + `")`
}

func (s *source) doExec(terms ...interface{}) (sql.Result, error) {
	if s.session == nil {
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

	return s.session.Exec(query, chunks.Args...)
}

func (s *source) doQuery(terms ...interface{}) (*sql.Rows, error) {
	if s.session == nil {
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

	return s.session.Query(query, chunks.Args...)
}

func (s *source) doQueryRow(terms ...interface{}) (*sql.Row, error) {
	if s.session == nil {
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

	return s.session.QueryRow(query, chunks.Args...), nil
}

func (s *source) Name() string {
	return s.config.Database
}

func (s *source) Setup(config db.Settings) error {
	s.config = config
	s.collections = make(map[string]db.Collection)
	return s.Open()
}

func (s *source) Driver() interface{} {
	return s.session
}

func (s *source) Open() error {
	var err error

	if s.config.Host == "" {
		if s.config.Socket == "" {
			s.config.Host = `127.0.0.1`
		}
	}

	if s.config.Port == 0 {
		s.config.Port = 5432
	}

	if s.config.Database == "" {
		return db.ErrMissingDatabaseName
	}

	if s.config.Socket != "" && s.config.Host != "" {
		return db.ErrSockerOrHost
	}

	var conn string

	if s.config.Host != "" {
		conn = fmt.Sprintf(`user=%s password=%s host=%s port=%d dbname=%s sslmode=%s`, s.config.User, s.config.Password, s.config.Host, s.config.Port, s.config.Database, sslMode)
	} else if s.config.Socket != `` {
		conn = fmt.Sprintf(`user=%s password=%s host=%s dbname=%s sslmode=%s`, s.config.User, s.config.Password, s.config.Socket, s.config.Database, sslMode)
	}

	s.session, err = sql.Open(`postgres`, conn)

	if err != nil {
		return err
	}

	return nil
}

func (s *source) Close() error {
	if s.session != nil {
		return s.session.Close()
	}
	return nil
}

func (s *source) Use(database string) error {
	s.config.Database = database
	return s.Open()
}

func (s *source) Begin() error {
	_, err := s.session.Exec(`BEGIN`)
	return err
}

func (s *source) End() error {
	_, err := s.session.Exec(`END`)
	return err
}

func (s *source) Drop() error {
	s.session.Query(fmt.Sprintf(`DROP DATABASE "%s"`, s.config.Database))
	return nil
}

func (s *source) Collections() ([]string, error) {
	var collections []string
	var collection string

	rows, err := s.session.Query(`SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'`)

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

func (s *source) Collection(name string) (db.Collection, error) {

	if collection, ok := s.collections[name]; ok == true {
		return collection, nil
	}

	tbl := &table{}

	tbl.source = s
	tbl.DB = s
	tbl.PrimaryKey = `id`

	tbl.SetName = name

	// Table exists?
	if tbl.Exists() == false {
		return tbl, db.ErrCollectionDoesNotExists
	}

	// Fetching table datatypes and mapping to internal gotypes.
	rows, err := tbl.source.doQuery(
		`SELECT
			column_name, data_type
		FROM information_schema.columns
		WHERE
			table_name = ?`,
		[]string{tbl.Name()},
	)

	if err != nil {
		return tbl, err
	}

	columns := []struct {
		ColumnName string
		DataType   string
	}{}

	err = tbl.FetchRows(&columns, rows)

	if err != nil {
		return nil, err
	}

	tbl.ColumnTypes = make(map[string]reflect.Kind, len(columns))

	for _, column := range columns {

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

		tbl.ColumnTypes[column.ColumnName] = ctype
	}

	s.collections[name] = tbl

	return tbl, nil
}
