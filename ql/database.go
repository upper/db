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
	"upper.io/db/util/schema"
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
	connURL db.ConnectionURL
	session *sql.DB
	tx      *tx
	schema  *schema.DatabaseSchema
}

type columnSchemaT struct {
	Name string `db:"Name"`
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

func (s *source) populateSchema() (err error) {
	var collections []string

	s.schema = schema.NewDatabaseSchema()

	var conn ConnectionURL

	if conn, err = ParseURL(s.connURL.String()); err != nil {
		return err
	}

	s.schema.Name = conn.Database

	// The Collections() call will populate schema if its nil.
	if collections, err = s.Collections(); err != nil {
		return err
	}

	for i := range collections {
		// Populate each collection.
		if _, err = s.Collection(collections[i]); err != nil {
			return err
		}
	}

	return err
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
		res, err = s.tx.sqlTx.Exec(query, args...)
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
		rows, err = s.tx.sqlTx.Query(query, args...)
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
		row = s.tx.sqlTx.QueryRow(query, args...)
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
	return s.schema.Name
}

//  Ping verifies a connection to the database is still alive,
//  establishing a connection if necessary.
func (s *source) Ping() error {
	return s.session.Ping()
}

func (s *source) clone() (adapter *source, err error) {
	adapter = new(source)

	if err = adapter.Setup(s.connURL); err != nil {
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

	if clone, err = s.clone(); err != nil {
		return nil, err
	}

	if sqlTx, err = s.session.Begin(); err != nil {
		return nil, err
	}

	tx := &tx{source: clone, sqlTx: sqlTx}

	clone.tx = tx

	return tx, nil
}

// Stores database settings.
func (s *source) Setup(conn db.ConnectionURL) error {
	s.connURL = conn
	return s.Open()
}

// Returns the underlying *sql.DB instance.
func (s *source) Driver() interface{} {
	return s.session
}

// Attempts to connect to a database using the stored settings.
func (s *source) Open() error {
	var err error

	// Before db.ConnectionURL we used a unified db.Settings struct. This
	// condition checks for that type and provides backwards compatibility.
	if settings, ok := s.connURL.(db.Settings); ok {

		// User is providing a db.Settings struct, let's translate it into a
		// ConnectionURL{}.
		conn := ConnectionURL{
			Database: settings.Database,
		}

		s.connURL = conn
	}

	if s.session, err = sql.Open(`ql`, s.connURL.String()); err != nil {
		return err
	}

	if err = s.populateSchema(); err != nil {
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
func (s *source) Use(database string) (err error) {
	var conn ConnectionURL

	if conn, err = ParseURL(s.connURL.String()); err != nil {
		return err
	}

	conn.Database = database

	s.connURL = conn

	return s.Open()
}

// Drops the currently active database.
func (s *source) Drop() error {
	return db.ErrUnsupported
}

// Returns a list of all tables within the currently active database.
func (s *source) Collections() (collections []string, err error) {

	tablesInSchema := len(s.schema.Tables)

	// Is schema already populated?
	if tablesInSchema > 0 {
		// Pulling table names from schema.
		return s.schema.Tables, nil
	}

	// Schema is empty.

	// Querying table names.
	stmt := sqlgen.Statement{
		Type:  sqlgen.SqlSelect,
		Table: sqlgen.Table{`__Table`},
		Columns: sqlgen.Columns{
			{`Name`},
		},
	}

	// Executing statement.
	var rows *sql.Rows
	if rows, err = s.doQuery(stmt); err != nil {
		return nil, err
	}

	defer rows.Close()

	collections = []string{}

	var name string

	for rows.Next() {
		// Getting table name.
		if err = rows.Scan(&name); err != nil {
			return nil, err
		}

		// Adding table entry to schema.
		s.schema.AddTable(name)

		// Adding table to collections array.
		collections = append(collections, name)
	}

	return collections, nil
}

func (s *source) tableExists(names ...string) error {
	var stmt sqlgen.Statement
	var err error
	var rows *sql.Rows

	for i := range names {

		if s.schema.HasTable(names[i]) {
			// We already know this table exists.
			continue
		}

		stmt = sqlgen.Statement{
			Type:  sqlgen.SqlSelect,
			Table: sqlgen.Table{`__Table`},
			Columns: sqlgen.Columns{
				{`Name`},
			},
			Where: sqlgen.Where{
				sqlgen.ColumnValue{sqlgen.Column{`Name`}, `==`, sqlPlaceholder},
			},
		}

		if rows, err = s.doQuery(stmt, names[i]); err != nil {
			return db.ErrCollectionDoesNotExist
		}

		defer rows.Close()

		if rows.Next() == false {
			return db.ErrCollectionDoesNotExist
		}
	}

	return nil
}

func (s *source) tableColumns(tableName string) ([]string, error) {

	// Making sure this table is allocated.
	tableSchema := s.schema.Table(tableName)

	if len(tableSchema.Columns) > 0 {
		return tableSchema.Columns, nil
	}

	stmt := sqlgen.Statement{
		Type:  sqlgen.SqlSelect,
		Table: sqlgen.Table{`__Column`},
		Columns: sqlgen.Columns{
			{`Name`},
			{`Type`},
		},
		Where: sqlgen.Where{
			sqlgen.ColumnValue{sqlgen.Column{`TableName`}, `==`, sqlPlaceholder},
		},
	}

	var rows *sql.Rows
	var err error

	if rows, err = s.doQuery(stmt, tableName); err != nil {
		return nil, err
	}

	tableFields := []columnSchemaT{}

	if err = sqlutil.FetchRows(rows, &tableFields); err != nil {
		return nil, err
	}

	s.schema.TableInfo[tableName].Columns = make([]string, 0, len(tableFields))

	for i := range tableFields {
		s.schema.TableInfo[tableName].Columns = append(s.schema.TableInfo[tableName].Columns, tableFields[i].Name)
	}

	return s.schema.TableInfo[tableName].Columns, nil
}

// Returns a collection instance by name.
func (s *source) Collection(names ...string) (db.Collection, error) {
	var err error

	if len(names) == 0 {
		return nil, db.ErrMissingCollectionName
	}

	if s.tx != nil {
		if s.tx.done {
			return nil, sql.ErrTxDone
		}
	}

	col := &table{
		source: s,
		names:  names,
	}

	for _, name := range names {
		chunks := strings.SplitN(name, ` `, 2)

		if len(chunks) == 0 {
			return nil, db.ErrMissingCollectionName
		}

		tableName := chunks[0]

		if err := s.tableExists(tableName); err != nil {
			return nil, err
		}

		if col.Columns, err = s.tableColumns(tableName); err != nil {
			return nil, err
		}
	}

	return col, nil
}
