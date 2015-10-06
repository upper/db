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
	"upper.io/db/builder"
	"upper.io/db/util/adapter"
	"upper.io/db/util/schema"
	"upper.io/db/util/sqlgen"
	"upper.io/db/util/sqlutil"
	"upper.io/db/util/sqlutil/tx"
)

type database struct {
	connURL          db.ConnectionURL
	session          *sqlx.DB
	tx               *sqltx.Tx
	schema           *schema.DatabaseSchema
	cachedStatements *cache.Cache
	collections      map[string]*table
	collectionsMu    sync.Mutex
	builder          db.QueryBuilder
}

type cachedStatement struct {
	*sqlx.Stmt
	query string
}

var waitForConnMu sync.Mutex

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
			return nil, query, err
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

	connFn := func(d **database) (err error) {
		(*d).session, err = sqlx.Open(`postgres`, (*d).connURL.String())
		return
	}

	if err := waitForConnection(func() error { return connFn(&d) }); err != nil {
		return err
	}

	d.builder = builder.NewBuilder(d, template.Template)

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
		if d.tx != nil && !d.tx.Done() {
			d.tx.Rollback()
		}
		d.cachedStatements.Clear()
		return d.session.Close()
	}
	return nil
}

// C returns a collection interface.
func (d *database) C(name string) db.Collection {
	if c, ok := d.collections[name]; ok {
		return c
	}

	c, err := d.Collection(name)
	if err != nil {
		return &adapter.NonExistentCollection{Err: err}
	}
	return c
}

// Collection returns the table that matches the given name.
func (d *database) Collection(name string) (db.Collection, error) {
	if d.tx != nil {
		if d.tx.Done() {
			return nil, sql.ErrTxDone
		}
	}

	if err := d.TableExists(name); err != nil {
		return nil, err
	}

	col := newTable(d, name)

	d.collectionsMu.Lock()
	d.collections[name] = col
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

	// Querying table names.
	q := d.Builder().Select("table_name").
		From("information_schema.tables").
		Where("table_schema = ?", "public")

	var row struct {
		TableName string `db:"table_name"`
	}

	iter := q.Iterator()
	for iter.Next(&row) {
		d.schema.AddTable(row.TableName)
		collections = append(collections, row.TableName)
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

	connFn := func(sqlTx **sqlx.Tx) (err error) {
		*sqlTx, err = clone.session.Beginx()
		return
	}

	if err := waitForConnection(func() error { return connFn(&sqlTx) }); err != nil {
		return nil, err
	}

	clone.tx = sqltx.New(sqlTx)
	return &tx{Tx: clone.tx, database: clone}, nil
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
	q := d.Builder().Select(db.Raw{"CURRENT_DATABASE() AS name"})

	var row struct {
		Name string `db:"name"`
	}

	if err := q.Iterator().One(&row); err != nil {
		return err
	}

	d.schema.Name = row.Name

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

func (d *database) TableExists(name string) error {
	if !d.schema.HasTable(name) {
		q := d.Builder().Select("table_name").
			From("information_schema.tables").
			Where("table_catalog = ? AND table_name = ?", d.schema.Name, name)

		var row map[string]string

		if err := q.Iterator().One(&row); err != nil {
			return db.ErrCollectionDoesNotExist
		}
	}

	return nil
}

func (d *database) TableColumns(tableName string) ([]string, error) {

	tableSchema := d.schema.Table(tableName)

	if len(tableSchema.Columns) > 0 {
		return tableSchema.Columns, nil
	}

	q := d.Builder().Select("column_name", "data_type").
		From("information_schema.columns").
		Where("table_catalog = ? AND table_name = ?", d.schema.Name, tableName)

	var rows []columnSchemaT

	if err := q.Iterator().All(&rows); err != nil {
		return nil, err
	}

	d.schema.TableInfo[tableName].Columns = make([]string, 0, len(rows))

	for i := range rows {
		d.schema.TableInfo[tableName].Columns = append(d.schema.TableInfo[tableName].Columns, rows[i].Name)
	}

	return d.schema.TableInfo[tableName].Columns, nil
}

func (d *database) TablePrimaryKey(tableName string) ([]string, error) {
	tableSchema := d.schema.Table(tableName)

	if len(tableSchema.PrimaryKey) != 0 {
		return tableSchema.PrimaryKey, nil
	}

	tableSchema.PrimaryKey = make([]string, 0, 1)

	q := d.Builder().Select("pg_attribute.attname AS pkey").
		From("pg_index", "pg_class", "pg_attribute").
		Where(`
			pg_class.oid = '"` + tableName + `"'::regclass
			AND indrelid = pg_class.oid
			AND pg_attribute.attrelid = pg_class.oid
			AND pg_attribute.attnum = ANY(pg_index.indkey)
			AND indisprimary
		`).OrderBy("pkey")

	iter := q.Iterator()

	var row struct {
		Key string `db:"pkey"`
	}

	for iter.Next(&row) {
		tableSchema.PrimaryKey = append(tableSchema.PrimaryKey, row.Key)
	}

	return tableSchema.PrimaryKey, nil
}

// Builder returns a custom query builder.
func (d *database) Builder() db.QueryBuilder {
	return d.builder
}

// waitForConnection tries to execute the connectFn function, if connectFn
// returns an error, then waitForConnection will keep trying until connectFn
// returns nil. Maximum waiting time is 5s after having acquired the lock.
func waitForConnection(connectFn func() error) error {
	// This lock ensures first-come, first-served and prevents opening too many
	// file descriptors.
	waitForConnMu.Lock()
	defer waitForConnMu.Unlock()

	// Minimum waiting time.
	waitTime := time.Millisecond * 10

	// Waitig 5 seconds for a successful connection.
	for timeStart := time.Now(); time.Now().Sub(timeStart) < time.Second*5; {
		if err := connectFn(); err != nil {
			if strings.Contains(err.Error(), `too many clients`) || strings.Contains(err.Error(), `remaining connection slots are reserved`) {
				// Sleep and try again if, and only if, the server replied with a "too
				// many clients" error.
				time.Sleep(waitTime)
				if waitTime < time.Millisecond*500 {
					// Wait a bit more next time.
					waitTime = waitTime * 2
				}
				continue
			}
			// Return any other error immediately.
			return err
		}
		return nil
	}

	return db.ErrGivingUpTryingToConnect
}
