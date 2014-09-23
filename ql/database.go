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
	// Importing QL driver
	_ "github.com/cznic/ql/driver"
	"upper.io/cache"
	"upper.io/db"
	"upper.io/db/util/sqlgen"
	"upper.io/db/util/sqlutil"
)

// Public adapters name under which this adapter registers its.
const Adapter = `ql`

var (
	// DateFormat defines the format used for storing dates.
	DateFormat = "2006-01-02 15:04:05.000"
	// TimeFormat defines the format used for storing time values.
	TimeFormat = "%d:%02d:%02d.%03d"
	timeType   = reflect.TypeOf(time.Time{}).Kind()
)

var template *sqlgen.Template

var (
	sqlPlaceholder = sqlgen.Value{sqlgen.Raw{`?`}}
)

type source struct {
	config      db.Settings
	session     *sql.DB
	collections map[string]db.Collection
	tx          *sql.Tx
}

type columnSchemaT struct {
	ColumnName string `db:"Name"`
	DataType   string `db:"Type"`
}

func debugEnabled() bool {
	if os.Getenv(db.EnvEnableDebug) != "" {
		return true
	}
	return false
}

func debugLog(query string, args []interface{}, err error, start int64, end int64) {
	if debugEnabled() == true {
		d := sqlutil.Debug{query, args, err, start, end}
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
		qlGroupByLayout,
		cache.NewCache(),
	}

	db.Register(Adapter, &source{})
}

func (s *source) doExec(stmt sqlgen.Statement, args ...interface{}) (sql.Result, error) {
	var query string
	var res sql.Result
	var err error
	var start, end int64

	start = time.Now().UnixNano()

	defer func() {
		end = time.Now().UnixNano()
		debugLog(query, args, err, start, end)
	}()

	if s.session == nil {
		return nil, db.ErrNotConnected
	}

	query = stmt.Compile(template)

	l := len(args)
	for i := 0; i < l; i++ {
		query = strings.Replace(query, `?`, fmt.Sprintf(`$%d`, i+1), 1)
	}

	if s.tx != nil {
		res, err = s.tx.Exec(query, args...)
	} else {
		var tx *sql.Tx

		if tx, err = s.session.Begin(); err != nil {
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

func (s *source) doQuery(stmt sqlgen.Statement, args ...interface{}) (*sql.Rows, error) {
	var rows *sql.Rows
	var query string
	var err error
	var start, end int64

	start = time.Now().UnixNano()

	defer func() {
		end = time.Now().UnixNano()
		debugLog(query, args, err, start, end)
	}()

	if s.session == nil {
		return nil, db.ErrNotConnected
	}

	query = stmt.Compile(template)

	l := len(args)
	for i := 0; i < l; i++ {
		query = strings.Replace(query, `?`, fmt.Sprintf(`$%d`, i+1), 1)
	}

	if s.tx != nil {
		rows, err = s.tx.Query(query, args...)
	} else {
		var tx *sql.Tx

		if tx, err = s.session.Begin(); err != nil {
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

func (s *source) doQueryRow(stmt sqlgen.Statement, args ...interface{}) (*sql.Row, error) {
	var query string
	var row *sql.Row
	var err error
	var start, end int64

	start = time.Now().UnixNano()

	defer func() {
		end = time.Now().UnixNano()
		debugLog(query, args, err, start, end)
	}()

	if s.session == nil {
		return nil, db.ErrNotConnected
	}

	query = stmt.Compile(template)

	l := len(args)
	for i := 0; i < l; i++ {
		query = strings.Replace(query, `?`, fmt.Sprintf(`$%d`, i+1), 1)
	}

	if s.tx != nil {
		row = s.tx.QueryRow(query, args...)
	} else {
		var tx *sql.Tx

		if tx, err = s.session.Begin(); err != nil {
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
func (s *source) Name() string {
	return s.config.Database
}

//  Ping verifies a connection to the database is still alive,
//  establishing a connection if necessary.
func (s *source) Ping() error {
	return s.session.Ping()
}

func (s *source) clone() (adapter *source, err error) {
	adapter = new(source)

	if err = adapter.Setup(s.config); err != nil {
		return nil, err
	}

	return adapter, nil
}

func (s *source) Clone() (db.Database, error) {
	return s.clone()
}

func (s *source) Transaction() (db.Tx, error) {
	var err error
	var clone *source
	var sqlTx *sql.Tx

	if sqlTx, err = s.session.Begin(); err != nil {
		return nil, err
	}

	if clone, err = s.clone(); err != nil {
		return nil, err
	}

	tx := &tx{clone}

	clone.tx = sqlTx

	return tx, nil
}

// Stores database settings.
func (s *source) Setup(config db.Settings) error {
	s.config = config
	s.collections = make(map[string]db.Collection)
	return s.Open()
}

// Returns the underlying *sql.DB instance.
func (s *source) Driver() interface{} {
	return s.session
}

// Attempts to connect to a database using the stored settings.
func (s *source) Open() error {
	var err error

	if s.config.Database == "" {
		return db.ErrMissingDatabaseName
	}

	if s.session, err = sql.Open(`ql`, s.config.Database); err != nil {
		return err
	}

	return nil
}

// Closes the current database session.
func (s *source) Close() error {
	if s.session != nil {
		return s.session.Close()
	}
	return nil
}

// Changes the active database.
func (s *source) Use(database string) error {
	s.config.Database = database
	return s.Open()
}

// Drops the currently active database.
func (s *source) Drop() error {

	_, err := s.doQuery(sqlgen.Statement{
		Type:     sqlgen.SqlDropDatabase,
		Database: sqlgen.Database{s.config.Database},
	})

	return err
}

// Returns a list of all tables within the currently active database.
func (s *source) Collections() ([]string, error) {
	var collections []string
	var collection string

	rows, err := s.doQuery(sqlgen.Statement{
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

func (s *source) tableExists(names ...string) error {
	for _, name := range names {

		rows, err := s.doQuery(sqlgen.Statement{
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
func (s *source) Collection(names ...string) (db.Collection, error) {

	if len(names) == 0 {
		return nil, db.ErrMissingCollectionName
	}

	col := &table{
		source: s,
		names:  names,
	}

	columnsT := []columnSchemaT{}

	for _, name := range names {
		chunks := strings.SplitN(name, " ", 2)

		if len(chunks) > 0 {

			name = chunks[0]

			if err := s.tableExists(name); err != nil {
				return nil, err
			}

			rows, err := s.doQuery(sqlgen.Statement{
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

			if err = sqlutil.FetchRows(rows, &columnsT); err != nil {
				return nil, err
			}

			col.Columns = make([]string, len(columnsT))
			col.columnTypes = make(map[string]reflect.Kind)

			for i, column := range columnsT {

				column.DataType = strings.ToLower(column.DataType)

				col.Columns[i] = column.ColumnName

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

				col.columnTypes[column.ColumnName] = ctype
			}

		}
	}

	return col, nil
}
