// Copyright (c) 2012-2015 José Carlos Nieto, https://menteslibres.net/xiam
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

package postgresql

import (
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq" // Go PostgreSQL driver.
	"upper.io/db"
	"upper.io/db/util/schema"
	"upper.io/db/util/sqlgen"
	"upper.io/db/util/sqlutil"
	"upper.io/db/util/sqlutil/tx"
)

var (
	sqlPlaceholder = sqlgen.RawValue(`?`)
)

type source struct {
	connURL db.ConnectionURL
	session *sqlx.DB
	tx      *sqltx.Tx
	schema  *schema.DatabaseSchema
}

type tx struct {
	*sqltx.Tx
	*source
}

type columnSchemaT struct {
	Name     string `db:"column_name"`
	DataType string `db:"data_type"`
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

// Driver returns the underlying *sqlx.DB instance.
func (s *source) Driver() interface{} {
	return s.session
}

// Open attempts to connect to the PostgreSQL server using the stored settings.
func (s *source) Open() error {
	var err error

	// Before db.ConnectionURL we used a unified db.Settings struct. This
	// condition checks for that type and provides backwards compatibility.
	if settings, ok := s.connURL.(db.Settings); ok {

		conn := ConnectionURL{
			User:     settings.User,
			Password: settings.Password,
			Address:  db.HostPort(settings.Host, uint(settings.Port)),
			Database: settings.Database,
			Options: map[string]string{
				"sslmode": "disable",
			},
		}

		s.connURL = conn
	}

	if s.session, err = sqlx.Open(`postgres`, s.connURL.String()); err != nil {
		return err
	}

	s.session.Mapper = sqlutil.NewMapper()

	if err = s.populateSchema(); err != nil {
		return err
	}

	return nil
}

// Clone returns a cloned db.Database session.
func (s *source) Clone() (db.Database, error) {
	return s.clone()
}

func (s *source) clone() (*source, error) {
	src := new(source)
	src.Setup(s.connURL)

	if err := src.Open(); err != nil {
		return nil, err
	}

	return src, nil
}

// Ping checks whether a connection to the database is still alive by pinging
// it, establishing a connection if necessary.
func (s *source) Ping() error {
	return s.session.Ping()
}

// Close terminates the current database session.
func (s *source) Close() error {
	if s.session != nil {
		return s.session.Close()
	}
	return nil
}

// Collection returns a table by name.
func (s *source) Collection(names ...string) (db.Collection, error) {
	var err error

	if len(names) == 0 {
		return nil, db.ErrMissingCollectionName
	}

	if s.tx != nil {
		if s.tx.Done() {
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

// Collections returns a list of non-system tables within the database.
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
		Type: sqlgen.Select,
		Columns: sqlgen.JoinColumns(
			sqlgen.ColumnWithName(`table_name`),
		),
		Table: sqlgen.TableWithName(`information_schema.tables`),
		Where: sqlgen.WhereConditions(
			&sqlgen.ColumnValue{
				Column:   sqlgen.ColumnWithName(`table_schema`),
				Operator: `=`,
				Value:    sqlgen.NewValue(`public`),
			},
		),
	}

	// Executing statement.
	var rows *sqlx.Rows
	if rows, err = s.Query(stmt); err != nil {
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

// Use changes the active database.
func (s *source) Use(database string) (err error) {
	var conn ConnectionURL

	if conn, err = ParseURL(s.connURL.String()); err != nil {
		return err
	}

	conn.Database = database

	s.connURL = conn

	return s.Open()
}

// Drop removes all tables within the current database.
func (s *source) Drop() error {
	_, err := s.Query(sqlgen.Statement{
		Type:     sqlgen.DropDatabase,
		Database: sqlgen.DatabaseWithName(s.schema.Name),
	})
	return err
}

// Setup stores database settings.
func (s *source) Setup(connURL db.ConnectionURL) error {
	s.connURL = connURL
	return s.Open()
}

// Name returns the name of the database.
func (s *source) Name() string {
	return s.schema.Name
}

// Transaction starts a transaction block and returns a db.Tx struct that can
// be used to issue transactional queries.
func (s *source) Transaction() (db.Tx, error) {
	var err error
	var clone *source
	var sqlTx *sqlx.Tx

	if sqlTx, err = s.session.Beginx(); err != nil {
		return nil, err
	}

	if clone, err = s.clone(); err != nil {
		return nil, err
	}

	clone.tx = sqltx.New(sqlTx)

	return tx{Tx: clone.tx, source: clone}, nil
}

func (s *source) Exec(stmt sqlgen.Statement, args ...interface{}) (sql.Result, error) {
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
		res, err = s.session.Exec(query, args...)
	}

	return res, err
}

func (s *source) Query(stmt sqlgen.Statement, args ...interface{}) (*sqlx.Rows, error) {
	var rows *sqlx.Rows
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
		rows, err = s.tx.Queryx(query, args...)
	} else {
		rows, err = s.session.Queryx(query, args...)
	}

	return rows, err
}

func (s *source) QueryRow(stmt sqlgen.Statement, args ...interface{}) (*sqlx.Row, error) {
	var query string
	var row *sqlx.Row
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
		query = strings.Replace(query, `?`, `$`+strconv.Itoa(i+1), 1)
	}

	if s.tx != nil {
		row = s.tx.QueryRowx(query, args...)
	} else {
		row = s.session.QueryRowx(query, args...)
	}

	return row, err
}

func (s *source) populateSchema() (err error) {
	var collections []string

	s.schema = schema.NewDatabaseSchema()

	// Get database name.
	stmt := sqlgen.Statement{
		Type: sqlgen.Select,
		Columns: sqlgen.JoinColumns(
			sqlgen.RawValue(`CURRENT_DATABASE()`),
		),
	}

	var row *sqlx.Row

	if row, err = s.QueryRow(stmt); err != nil {
		return err
	}

	if err = row.Scan(&s.schema.Name); err != nil {
		return err
	}

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

func (s *source) tableExists(names ...string) error {
	var stmt sqlgen.Statement
	var err error
	var rows *sqlx.Rows

	for i := range names {

		if s.schema.HasTable(names[i]) {
			// We already know this table exists.
			continue
		}

		stmt = sqlgen.Statement{
			Type:  sqlgen.Select,
			Table: sqlgen.TableWithName(`information_schema.tables`),
			Columns: sqlgen.JoinColumns(
				sqlgen.ColumnWithName(`table_name`),
			),
			Where: sqlgen.WhereConditions(
				&sqlgen.ColumnValue{
					Column:   sqlgen.ColumnWithName(`table_catalog`),
					Operator: `=`,
					Value:    sqlPlaceholder,
				},
				&sqlgen.ColumnValue{
					Column:   sqlgen.ColumnWithName(`table_name`),
					Operator: `=`,
					Value:    sqlPlaceholder,
				},
			),
		}

		if rows, err = s.Query(stmt, s.schema.Name, names[i]); err != nil {
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
		Type:  sqlgen.Select,
		Table: sqlgen.TableWithName(`information_schema.columns`),
		Columns: sqlgen.JoinColumns(
			sqlgen.ColumnWithName(`column_name`),
			sqlgen.ColumnWithName(`data_type`),
		),
		Where: sqlgen.WhereConditions(
			&sqlgen.ColumnValue{
				Column:   sqlgen.ColumnWithName(`table_catalog`),
				Operator: `=`,
				Value:    sqlPlaceholder,
			},
			&sqlgen.ColumnValue{
				Column:   sqlgen.ColumnWithName(`table_name`),
				Operator: `=`,
				Value:    sqlPlaceholder,
			},
		),
	}

	var rows *sqlx.Rows
	var err error

	if rows, err = s.Query(stmt, s.schema.Name, tableName); err != nil {
		return nil, err
	}

	defer rows.Close()

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

func (s *source) getPrimaryKey(tableName string) ([]string, error) {
	tableSchema := s.schema.Table(tableName)

	if len(tableSchema.PrimaryKey) != 0 {
		return tableSchema.PrimaryKey, nil
	}

	// Getting primary key. See https://github.com/upper/db/issues/24.
	stmt := sqlgen.Statement{
		Type:  sqlgen.Select,
		Table: sqlgen.TableWithName(`pg_index, pg_class, pg_attribute`),
		Columns: sqlgen.JoinColumns(
			sqlgen.ColumnWithName(`pg_attribute.attname`),
		),
		Where: sqlgen.WhereConditions(
			sqlgen.RawValue(`pg_class.oid = '`+tableName+`'::regclass`),
			sqlgen.RawValue(`indrelid = pg_class.oid`),
			sqlgen.RawValue(`pg_attribute.attrelid = pg_class.oid`),
			sqlgen.RawValue(`pg_attribute.attnum = ANY(pg_index.indkey)`),
			sqlgen.RawValue(`indisprimary`),
		),
		OrderBy: &sqlgen.OrderBy{
			SortColumns: sqlgen.JoinSortColumns(
				&sqlgen.SortColumn{
					Column: sqlgen.ColumnWithName(`attname`),
					Order:  sqlgen.Ascendent,
				},
			),
		},
	}

	var rows *sqlx.Rows
	var err error

	if rows, err = s.Query(stmt); err != nil {
		return nil, err
	}

	tableSchema.PrimaryKey = make([]string, 0, 1)

	for rows.Next() {
		var key string
		if err = rows.Scan(&key); err != nil {
			return nil, err
		}
		tableSchema.PrimaryKey = append(tableSchema.PrimaryKey, key)
	}

	return tableSchema.PrimaryKey, nil
}
