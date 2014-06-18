// Copyright (c) 2012-2014 José Carlos Nieto, https://menteslibres.net/xiam
//
// Permission is hereby granted, free of charge, to any person obtaining
// a copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to
// permit persons to whom the Software is furnished to do so, subject to
// the following conditions:
//
// The above copyright notice and this permission notice shall be
// included in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
// LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
// OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
// WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

package ql

import (
	"database/sql"
	"fmt"
	"os"
	"reflect"
	"strings"
	"time"

	_ "github.com/cznic/ql/driver"
	"upper.io/db"
	"upper.io/db/util/sqlgen"
	"upper.io/db/util/sqlutil"
)

const Adapter = `ql`

var (
	// Format for saving dates.
	DateFormat = "2006-01-02 15:04:05.000"
	// Format for saving times.
	TimeFormat = "%d:%02d:%02d.%03d"
	timeType   = reflect.TypeOf(time.Time{}).Kind()
)

var template *sqlgen.Template

var (
	sqlPlaceholder = sqlgen.Value{sqlgen.Raw{`?`}}
)

type Source struct {
	config      db.Settings
	session     *sql.DB
	collections map[string]db.Collection
	tx          *sql.Tx
}

type columnSchema_t struct {
	ColumnName string `db:"name"`
	DataType   string `db:"type"`
}

func debugEnabled() bool {
	if os.Getenv(db.EnvEnableDebug) != "" {
		return true
	}
	return false
}

func debugLog(query string, args []interface{}, err error) {
	if debugEnabled() == true {
		d := sqlutil.Debug{query, args, err}
		d.Print()
	}
}

func init() {

	template = &sqlgen.Template{
		qlColumnSeparator,
		qlIdentifierSeparator,
		qlIdentifierQuote,
		qlValueSeparator,
		qlValueQuote,
		qlAndKeyword,
		qlOrKeyword,
		qlNotKeyword,
		qlDescKeyword,
		qlAscKeyword,
		qlDefaultOperator,
		qlClauseGroup,
		qlClauseOperator,
		qlColumnValue,
		qlTableAliasLayout,
		qlColumnAliasLayout,
		qlSortByColumnLayout,
		qlWhereLayout,
		qlOrderByLayout,
		qlInsertLayout,
		qlSelectLayout,
		qlUpdateLayout,
		qlDeleteLayout,
		qlTruncateLayout,
		qlDropDatabaseLayout,
		qlDropTableLayout,
		qlSelectCountLayout,
	}

	db.Register(Adapter, &Source{})
}

func (self *Source) doExec(stmt sqlgen.Statement, args ...interface{}) (sql.Result, error) {

	var query string
	var res sql.Result
	var err error

	defer func() {
		debugLog(query, args, err)
	}()

	if self.session == nil {
		return nil, db.ErrNotConnected
	}

	query = stmt.Compile(template)

	l := len(args)
	for i := 0; i < l; i++ {
		query = strings.Replace(query, `?`, fmt.Sprintf(`$%d`, i+1), 1)
	}

	if self.tx != nil {
		res, err = self.tx.Exec(query, args...)
	} else {
		var tx *sql.Tx

		if tx, err = self.session.Begin(); err != nil {
			return nil, err
		}

		if res, err = tx.Exec(query, args...); err != nil {
			return nil, err
		}

		if err = tx.Commit(); err != nil {
			return nil, err
		}
	}

	return res, err
}

func (self *Source) doQuery(stmt sqlgen.Statement, args ...interface{}) (*sql.Rows, error) {
	var rows *sql.Rows
	var query string
	var err error

	defer func() {
		debugLog(query, args, err)
	}()

	if self.session == nil {
		return nil, db.ErrNotConnected
	}

	query = stmt.Compile(template)

	l := len(args)
	for i := 0; i < l; i++ {
		query = strings.Replace(query, `?`, fmt.Sprintf(`$%d`, i+1), 1)
	}

	if self.tx != nil {
		rows, err = self.tx.Query(query, args...)
	} else {
		var tx *sql.Tx

		if tx, err = self.session.Begin(); err != nil {
			return nil, err
		}

		if rows, err = tx.Query(query, args...); err != nil {
			return nil, err
		}

		if err = tx.Commit(); err != nil {
			return nil, err
		}
	}

	return rows, err
}

func (self *Source) doQueryRow(stmt sqlgen.Statement, args ...interface{}) (*sql.Row, error) {
	var query string
	var row *sql.Row
	var err error

	defer func() {
		debugLog(query, args, err)
	}()

	if self.session == nil {
		return nil, db.ErrNotConnected
	}

	query = stmt.Compile(template)

	l := len(args)
	for i := 0; i < l; i++ {
		query = strings.Replace(query, `?`, fmt.Sprintf(`$%d`, i+1), 1)
	}

	if self.tx != nil {
		row = self.tx.QueryRow(query, args...)
	} else {
		var tx *sql.Tx

		if tx, err = self.session.Begin(); err != nil {
			return nil, err
		}

		if row = tx.QueryRow(query, args...); err != nil {
			return nil, err
		}

		if err = tx.Commit(); err != nil {
			return nil, err
		}
	}

	return row, err
}

// Returns the string name of the database.
func (self *Source) Name() string {
	return self.config.Database
}

//  Ping verifies a connection to the database is still alive,
//  establishing a connection if necessary.
func (self *Source) Ping() error {
	return self.session.Ping()
}

func (self *Source) clone() (*Source, error) {
	src := &Source{}
	src.Setup(self.config)

	if err := src.Open(); err != nil {
		return nil, err
	}

	return src, nil
}

func (self *Source) Clone() (db.Database, error) {
	return self.clone()
}

func (self *Source) Transaction() (db.Tx, error) {
	var err error
	var clone *Source
	var sqlTx *sql.Tx

	if clone, err = self.clone(); err != nil {
		return nil, err
	}

	if sqlTx, err = clone.session.Begin(); err != nil {
		return nil, err
	}

	tx := &Tx{clone}

	clone.tx = sqlTx

	return tx, nil
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

	self.session, err = sql.Open(`ql`, self.config.Database)

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

// Drops the currently active database.
func (self *Source) Drop() error {

	_, err := self.doQuery(sqlgen.Statement{
		Type:     sqlgen.SqlDropDatabase,
		Database: sqlgen.Database{self.config.Database},
	})

	return err
}

// Returns a list of all tables within the currently active database.
func (self *Source) Collections() ([]string, error) {
	var collections []string
	var collection string

	rows, err := self.doQuery(sqlgen.Statement{
		Type:  sqlgen.SqlSelect,
		Table: sqlgen.Table{`__Table`},
		Columns: sqlgen.Columns{
			{`Name`},
		},
	})

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

		rows, err := self.doQuery(sqlgen.Statement{
			Type:  sqlgen.SqlSelect,
			Table: sqlgen.Table{`__Table`},
			Columns: sqlgen.Columns{
				{`Name`},
			},
			Where: sqlgen.Where{
				sqlgen.ColumnValue{sqlgen.Column{`Name`}, `==`, sqlPlaceholder},
			},
		}, name)

		if err != nil {
			return db.ErrCollectionDoesNotExist
		}

		defer rows.Close()

		if rows.Next() == false {
			return db.ErrCollectionDoesNotExist
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
		names:  names,
	}

	col.PrimaryKey = `id`

	columns_t := []columnSchema_t{}

	for _, name := range names {
		chunks := strings.SplitN(name, " ", 2)

		if len(chunks) > 0 {

			name = chunks[0]

			if err := self.tableExists(name); err != nil {
				return nil, err
			}

			rows, err := self.doQuery(sqlgen.Statement{
				Type:  sqlgen.SqlSelect,
				Table: sqlgen.Table{`__Column`},
				Columns: sqlgen.Columns{
					{`Name`},
					{`Type`},
				},
				Where: sqlgen.Where{
					sqlgen.ColumnValue{sqlgen.Column{`TableName`}, `==`, sqlPlaceholder},
				},
			}, name)

			if err != nil {
				return nil, err
			}

			if err = col.FetchRows(&columns_t, rows); err != nil {
				return nil, err
			}

			col.ColumnTypes = make(map[string]reflect.Kind, len(columns_t))

			for _, column := range columns_t {

				column.ColumnName = strings.ToLower(column.ColumnName)
				column.DataType = strings.ToLower(column.DataType)

				// Default properties.
				dtype := column.DataType
				ctype := reflect.String

				// Guessing datatypes.
				switch dtype {
				case `int`:
					ctype = reflect.Int
				case `int8`:
					ctype = reflect.Int8
				case `int16`:
					ctype = reflect.Int16
				case `int32`, `rune`:
					ctype = reflect.Int32
				case `int64`:
					ctype = reflect.Int64
				case `uint`:
					ctype = reflect.Uint
				case `uint8`:
					ctype = reflect.Uint8
				case `uint16`:
					ctype = reflect.Uint16
				case `uint32`:
					ctype = reflect.Uint32
				case `uint64`:
					ctype = reflect.Uint64
				case `float64`:
					ctype = reflect.Float64
				case `float32`:
					ctype = reflect.Float32
				case `time`:
					ctype = timeType
				default:
					ctype = reflect.String
				}

				col.ColumnTypes[column.ColumnName] = ctype
			}

		}
	}

	return col, nil
}
