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
	"strconv"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq" // PostgreSQL driver.
	"upper.io/db"
	"upper.io/db/util/schema"
	"upper.io/db/util/sqlgen"
	"upper.io/db/util/sqlutil"
	"upper.io/db/util/sqlutil/tx"
)

var (
	sqlPlaceholder = sqlgen.RawValue(`?`)
)

type database struct {
	connURL db.ConnectionURL
	session *sqlx.DB
	tx      *sqltx.Tx
	schema  *schema.DatabaseSchema
}

type tx struct {
	*sqltx.Tx
	*database
}

var (
	_ = db.Database(&database{})
	_ = db.Tx(&tx{})
)

type columnSchemaT struct {
	Name     string `db:"column_name"`
	DataType string `db:"data_type"`
}

// Driver returns the underlying *sqlx.DB instance.
func (d *database) Driver() interface{} {
	return d.session
}

// Open attempts to connect to the database server using already stored settings.
func (d *database) Open() error {
	var err error

	// Before db.ConnectionURL we used a unified db.Settings struct. This
	// condition checks for that type and provides backwards compatibility.
	if settings, ok := d.connURL.(db.Settings); ok {

		conn := ConnectionURL{
			User:     settings.User,
			Password: settings.Password,
			Address:  db.HostPort(settings.Host, uint(settings.Port)),
			Database: settings.Database,
			Options: map[string]string{
				"sslmode": "disable",
			},
		}

		d.connURL = conn
	}

	if d.session, err = sqlx.Open(`postgres`, d.connURL.String()); err != nil {
		return err
	}

	d.session.Mapper = sqlutil.NewMapper()

	if err = d.populateSchema(); err != nil {
		return err
	}

	return nil
}

// Clone returns a cloned db.Database session, this is typically used for
// transactions.
func (d *database) Clone() (db.Database, error) {
	return d.clone()
}

func (d *database) clone() (*database, error) {
	src := new(database)
	src.Setup(d.connURL)

	if err := src.Open(); err != nil {
		return nil, err
	}

	return src, nil
}

// Ping checks whether a connection to the database is still alive by pinging
// it, establishing a connection if necessary.
func (d *database) Ping() error {
	return d.session.Ping()
}

// Close terminates the current database session.
func (d *database) Close() error {
	if d.session != nil {
		return d.session.Close()
	}
	return nil
}

// Collection returns a table by name.
func (d *database) Collection(names ...string) (db.Collection, error) {
	var err error

	if len(names) == 0 {
		return nil, db.ErrMissingCollectionName
	}

	if d.tx != nil {
		if d.tx.Done() {
			return nil, sql.ErrTxDone
		}
	}

	col := &table{
		database: d,
	}

	col.Tables = names

	for _, name := range names {
		chunks := strings.SplitN(name, ` `, 2)

		if len(chunks) == 0 {
			return nil, db.ErrMissingCollectionName
		}

		tableName := chunks[0]

		if err := d.tableExists(tableName); err != nil {
			return nil, err
		}

		if col.Columns, err = d.tableColumns(tableName); err != nil {
			return nil, err
		}
	}

	return col, nil
}

// Collections returns a list of non-system tables from the database.
func (d *database) Collections() (collections []string, err error) {

	tablesInSchema := len(d.schema.Tables)

	// Is schema already populated?
	if tablesInSchema > 0 {
		// Pulling table names from schema.
		return d.schema.Tables, nil
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
	if rows, err = d.Query(stmt); err != nil {
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
		d.schema.AddTable(name)

		// Adding table to collections array.
		collections = append(collections, name)
	}

	return collections, nil
}

// Use changes the active database.
func (d *database) Use(database string) (err error) {
	var conn ConnectionURL

	if conn, err = ParseURL(d.connURL.String()); err != nil {
		return err
	}

	conn.Database = database

	d.connURL = conn

	return d.Open()
}

// Drop removes all tables from the current database.
func (d *database) Drop() error {
	_, err := d.Query(sqlgen.Statement{
		Type:     sqlgen.DropDatabase,
		Database: sqlgen.DatabaseWithName(d.schema.Name),
	})
	return err
}

// Setup stores database settings.
func (d *database) Setup(connURL db.ConnectionURL) error {
	d.connURL = connURL
	return d.Open()
}

// Name returns the name of the database.
func (d *database) Name() string {
	return d.schema.Name
}

// Transaction starts a transaction block and returns a db.Tx struct that can
// be used to issue transactional queries.
func (d *database) Transaction() (db.Tx, error) {
	var err error
	var clone *database
	var sqlTx *sqlx.Tx

	if clone, err = d.clone(); err != nil {
		return nil, err
	}

	if sqlTx, err = clone.session.Beginx(); err != nil {
		return nil, err
	}

	clone.tx = sqltx.New(sqlTx)

	return tx{Tx: clone.tx, database: clone}, nil
}

// Exec compiles and executes a statement that does not return any rows.
func (d *database) Exec(stmt sqlgen.Statement, args ...interface{}) (sql.Result, error) {
	var query string
	var res sql.Result
	var err error
	var start, end int64

	start = time.Now().UnixNano()

	defer func() {
		end = time.Now().UnixNano()
		sqlutil.Log(query, args, err, start, end)
	}()

	if d.session == nil {
		return nil, db.ErrNotConnected
	}

	query = stmt.Compile(template.Template)

	l := len(args)
	for i := 0; i < l; i++ {
		query = strings.Replace(query, `?`, fmt.Sprintf(`$%d`, i+1), 1)
	}

	if d.tx != nil {
		res, err = d.tx.Exec(query, args...)
	} else {
		res, err = d.session.Exec(query, args...)
	}

	return res, err
}

// Query compiles and executes a statement that returns rows.
func (d *database) Query(stmt sqlgen.Statement, args ...interface{}) (*sqlx.Rows, error) {
	var rows *sqlx.Rows
	var query string
	var err error
	var start, end int64

	start = time.Now().UnixNano()

	defer func() {
		end = time.Now().UnixNano()
		sqlutil.Log(query, args, err, start, end)
	}()

	if d.session == nil {
		return nil, db.ErrNotConnected
	}

	query = stmt.Compile(template.Template)

	l := len(args)
	for i := 0; i < l; i++ {
		query = strings.Replace(query, `?`, fmt.Sprintf(`$%d`, i+1), 1)
	}

	if d.tx != nil {
		rows, err = d.tx.Queryx(query, args...)
	} else {
		rows, err = d.session.Queryx(query, args...)
	}

	return rows, err
}

// QueryRow compiles and executes a statement that returns at most one row.
func (d *database) QueryRow(stmt sqlgen.Statement, args ...interface{}) (*sqlx.Row, error) {
	var query string
	var row *sqlx.Row
	var err error
	var start, end int64

	start = time.Now().UnixNano()

	defer func() {
		end = time.Now().UnixNano()
		sqlutil.Log(query, args, err, start, end)
	}()

	if d.session == nil {
		return nil, db.ErrNotConnected
	}

	query = stmt.Compile(template.Template)

	l := len(args)
	for i := 0; i < l; i++ {
		query = strings.Replace(query, `?`, `$`+strconv.Itoa(i+1), 1)
	}

	if d.tx != nil {
		row = d.tx.QueryRowx(query, args...)
	} else {
		row = d.session.QueryRowx(query, args...)
	}

	return row, err
}

// populateSchema looks up for the table info in the database and populates its
// schema for internal use.
func (d *database) populateSchema() (err error) {
	var collections []string

	d.schema = schema.NewDatabaseSchema()

	// Get database name.
	stmt := sqlgen.Statement{
		Type: sqlgen.Select,
		Columns: sqlgen.JoinColumns(
			sqlgen.RawValue(`CURRENT_DATABASE()`),
		),
	}

	var row *sqlx.Row

	if row, err = d.QueryRow(stmt); err != nil {
		return err
	}

	if err = row.Scan(&d.schema.Name); err != nil {
		return err
	}

	if collections, err = d.Collections(); err != nil {
		return err
	}

	for i := range collections {
		if _, err = d.Collection(collections[i]); err != nil {
			return err
		}
	}

	return err
}

func (d *database) tableExists(names ...string) error {
	var stmt sqlgen.Statement
	var err error
	var rows *sqlx.Rows

	for i := range names {

		if d.schema.HasTable(names[i]) {
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

		if rows, err = d.Query(stmt, d.schema.Name, names[i]); err != nil {
			return db.ErrCollectionDoesNotExist
		}

		defer rows.Close()

		if !rows.Next() {
			return db.ErrCollectionDoesNotExist
		}
	}

	return nil
}

func (d *database) tableColumns(tableName string) ([]string, error) {

	// Making sure this table is allocated.
	tableSchema := d.schema.Table(tableName)

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

	if rows, err = d.Query(stmt, d.schema.Name, tableName); err != nil {
		return nil, err
	}

	defer rows.Close()

	tableFields := []columnSchemaT{}

	if err = sqlutil.FetchRows(rows, &tableFields); err != nil {
		return nil, err
	}

	d.schema.TableInfo[tableName].Columns = make([]string, 0, len(tableFields))

	for i := range tableFields {
		d.schema.TableInfo[tableName].Columns = append(d.schema.TableInfo[tableName].Columns, tableFields[i].Name)
	}

	return d.schema.TableInfo[tableName].Columns, nil
}

func (d *database) getPrimaryKey(tableName string) ([]string, error) {
	tableSchema := d.schema.Table(tableName)

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

	if rows, err = d.Query(stmt); err != nil {
		return nil, err
	}

	defer rows.Close()

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
