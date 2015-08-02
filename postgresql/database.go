// Copyright (c) 2012-2015 The upper.io/db authors. All rights reserved.
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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq" // PostgreSQL driver.
	"upper.io/cache"
	"upper.io/db"
	"upper.io/db/util/adapter"
	"upper.io/db/util/schema"
	"upper.io/db/util/sqlgen"
	"upper.io/db/util/sqlutil"
	"upper.io/db/util/sqlutil/tx"
)

var (
	sqlPlaceholder = sqlgen.RawValue(`?`)
)

type database struct {
	connURL          db.ConnectionURL
	session          *sqlx.DB
	tx               *sqltx.Tx
	schema           *schema.DatabaseSchema
	cachedStatements *cache.Cache
	collections      map[string]*table
	collectionsMu    sync.Mutex
}

type tx struct {
	*sqltx.Tx
	*database
}

type cachedStatement struct {
	*sqlx.Stmt
	query string
}

var (
	_ = db.Database(&database{})
	_ = db.Tx(&tx{})
)

type columnSchemaT struct {
	Name     string `db:"column_name"`
	DataType string `db:"data_type"`
}

func (d *database) prepareStatement(stmt *sqlgen.Statement) (p *sqlx.Stmt, query string, err error) {
	if d.session == nil {
		return nil, "", db.ErrNotConnected
	}

	pc, ok := d.cachedStatements.ReadRaw(stmt)

	if ok {
		ps := pc.(*cachedStatement)
		p = ps.Stmt
		query = ps.query
	} else {
		query = compileAndReplacePlaceholders(stmt)

		if d.tx != nil {
			p, err = d.tx.Preparex(query)
		} else {
			p, err = d.session.Preparex(query)
		}

		if err != nil {
			return nil, "", err
		}

		d.cachedStatements.Write(stmt, &cachedStatement{p, query})
	}

	return p, query, nil
}

func compileAndReplacePlaceholders(stmt *sqlgen.Statement) (query string) {
	buf := stmt.Compile(template.Template)

	j := 1
	for i := range buf {
		if buf[i] == '?' {
			query = query + "$" + strconv.Itoa(j)
			j++
		} else {
			query = query + string(buf[i])
		}
	}

	return query
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

	d.cachedStatements = cache.NewCache()

	d.collections = make(map[string]*table)

	if d.schema == nil {
		if err = d.populateSchema(); err != nil {
			return err
		}
	}

	return nil
}

// Clone returns a cloned db.Database session, this is typically used for
// transactions.
func (d *database) Clone() (db.Database, error) {
	return d.clone()
}

func (d *database) clone() (*database, error) {
	clone := &database{
		schema: d.schema,
	}
	if err := clone.Setup(d.connURL); err != nil {
		return nil, err
	}
	return clone, nil
}

// Ping checks whether a connection to the database is still alive by pinging
// it, establishing a connection if necessary.
func (d *database) Ping() error {
	return d.session.Ping()
}

// Close terminates the current database session.
func (d *database) Close() error {
	if d.session != nil {
		d.cachedStatements.Clear()
		return d.session.Close()
	}
	return nil
}

// C returns a collection interface.
func (d *database) C(names ...string) db.Collection {
	if len(names) == 0 {
		return &adapter.NonExistentCollection{Err: db.ErrMissingCollectionName}
	}

	if c, ok := d.collections[sqlutil.HashTableNames(names)]; ok {
		return c
	}

	c, err := d.Collection(names...)
	if err != nil {
		return &adapter.NonExistentCollection{Err: err}
	}
	return c
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

	col := &table{database: d}
	col.T.Tables = names
	col.T.Mapper = d.session.Mapper

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

	// Saving the collection for C().
	d.collectionsMu.Lock()
	d.collections[sqlutil.HashTableNames(names)] = col
	d.collectionsMu.Unlock()

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
	stmt := &sqlgen.Statement{
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

	collections = []string{}

	var name string

	for rows.Next() {
		// Getting table name.
		if err = rows.Scan(&name); err != nil {
			rows.Close()
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
func (d *database) Use(name string) (err error) {
	var conn ConnectionURL

	if conn, err = ParseURL(d.connURL.String()); err != nil {
		return err
	}

	conn.Database = name

	d.connURL = conn

	d.schema = nil

	return d.Open()
}

// Drop removes all tables from the current database.
func (d *database) Drop() error {
	_, err := d.Query(&sqlgen.Statement{
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
	var sqlTx *sqlx.Tx
	var clone *database

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
func (d *database) Exec(stmt *sqlgen.Statement, args ...interface{}) (sql.Result, error) {
	var query string
	var p *sqlx.Stmt
	var err error

	if db.Debug {
		var start, end int64
		start = time.Now().UnixNano()

		defer func() {
			end = time.Now().UnixNano()
			sqlutil.Log(query, args, err, start, end)
		}()
	}

	if p, query, err = d.prepareStatement(stmt); err != nil {
		return nil, err
	}

	return p.Exec(args...)
}

// Query compiles and executes a statement that returns rows.
func (d *database) Query(stmt *sqlgen.Statement, args ...interface{}) (*sqlx.Rows, error) {
	var query string
	var p *sqlx.Stmt
	var err error

	if db.Debug {
		var start, end int64
		start = time.Now().UnixNano()

		defer func() {
			end = time.Now().UnixNano()
			sqlutil.Log(query, args, err, start, end)
		}()
	}

	if p, query, err = d.prepareStatement(stmt); err != nil {
		return nil, err
	}

	return p.Queryx(args...)
}

// QueryRow compiles and executes a statement that returns at most one row.
func (d *database) QueryRow(stmt *sqlgen.Statement, args ...interface{}) (*sqlx.Row, error) {
	var query string
	var p *sqlx.Stmt
	var err error

	if db.Debug {
		var start, end int64
		start = time.Now().UnixNano()

		defer func() {
			end = time.Now().UnixNano()
			sqlutil.Log(query, args, err, start, end)
		}()
	}

	if p, query, err = d.prepareStatement(stmt); err != nil {
		return nil, err
	}

	return p.QueryRowx(args...), nil
}

// populateSchema looks up for the table info in the database and populates its
// schema for internal use.
func (d *database) populateSchema() (err error) {
	var collections []string

	d.schema = schema.NewDatabaseSchema()

	// Get database name.
	stmt := &sqlgen.Statement{
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
	var stmt *sqlgen.Statement
	var err error
	var rows *sqlx.Rows

	for i := range names {

		if d.schema.HasTable(names[i]) {
			// We already know this table exists.
			continue
		}

		stmt = &sqlgen.Statement{
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

		if !rows.Next() {
			rows.Close()
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

	stmt := &sqlgen.Statement{
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

	tableFields := []columnSchemaT{}

	if err = sqlutil.FetchRows(rows, &tableFields); err != nil {
		rows.Close()
		return nil, err
	}

	rows.Close()

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
	stmt := &sqlgen.Statement{
		Type:  sqlgen.Select,
		Table: sqlgen.TableWithName(`pg_index, pg_class, pg_attribute`),
		Columns: sqlgen.JoinColumns(
			sqlgen.ColumnWithName(`pg_attribute.attname`),
		),
		Where: sqlgen.WhereConditions(
			sqlgen.RawValue(`pg_class.oid = '"`+tableName+`"'::regclass`),
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

	tableSchema.PrimaryKey = make([]string, 0, 1)

	for rows.Next() {
		var key string
		if err = rows.Scan(&key); err != nil {
			rows.Close()
			return nil, err
		}
		tableSchema.PrimaryKey = append(tableSchema.PrimaryKey, key)
	}

	return tableSchema.PrimaryKey, nil
}
