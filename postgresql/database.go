// Copyright (c) 2012-present The upper.io/db authors. All rights reserved.
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
	"strings"
	"sync"

	_ "github.com/lib/pq" // PostgreSQL driver.
	"upper.io/db.v2"
	"upper.io/db.v2/internal/sqladapter"
	"upper.io/db.v2/sqlbuilder"
	"upper.io/db.v2/sqlbuilder/exql"
)

// Database represents a SQL database.
type Database interface {
	db.Database
	builder.Builder

	UseTransaction(tx *sql.Tx) (Tx, error)
	NewTransaction() (Tx, error)

	Transaction(fn func(tx Tx) error) error
}

// database is the actual implementation of Database
type database struct {
	sqladapter.BaseDatabase // Leveraged by sqladapter
	builder.Builder

	txMu sync.Mutex
	tx   sqladapter.Tx

	connURL db.ConnectionURL
}

var (
	_ = sqladapter.Database(&database{})
	_ = db.Database(&database{})
)

// newDatabase binds *database with sqladapter and the SQL builer.
func newDatabase(settings db.ConnectionURL) (*database, error) {
	d := &database{
		connURL: settings,
	}
	return d, nil
}

// Open stablishes a new connection to a SQL server.
func Open(settings db.ConnectionURL) (Database, error) {
	d, err := newDatabase(settings)
	if err != nil {
		return nil, err
	}
	if err := d.Open(settings); err != nil {
		return nil, err
	}
	return d, nil
}

// New wraps the given *sql.DB session and creates a new db session.
func New(sess *sql.DB) (Database, error) {
	d, err := newDatabase(nil)
	if err != nil {
		return nil, err
	}

	// Binding with sqladapter's logic.
	d.BaseDatabase = sqladapter.NewBaseDatabase(d)

	// Binding with builder.
	b, err := builder.New(d.BaseDatabase, template)
	if err != nil {
		return nil, err
	}
	d.Builder = b

	if err := d.BaseDatabase.BindSession(sess); err != nil {
		return nil, err
	}
	return d, nil
}

// ConnectionURL returns this database's ConnectionURL.
func (d *database) ConnectionURL() db.ConnectionURL {
	return d.connURL
}

// Open attempts to open a connection to the database server.
func (d *database) Open(connURL db.ConnectionURL) error {
	if connURL == nil {
		return db.ErrMissingConnURL
	}
	d.connURL = connURL
	return d.open()
}

// UseTransaction makes the adapter use the given transaction.
func (d *database) UseTransaction(sqlTx *sql.Tx) (Tx, error) {
	if sqlTx == nil { // No transaction given.
		d.txMu.Lock()
		currentTx := d.tx
		d.txMu.Unlock()
		if currentTx != nil {
			return &tx{Tx: currentTx}, nil
		}
		// Create a new transaction.
		return d.NewTransaction()
	}

	d.txMu.Lock()
	defer d.txMu.Unlock()

	if err := d.BindTx(sqlTx); err != nil {
		return nil, err
	}
	d.tx = sqladapter.NewTx(d)
	return &tx{Tx: d.tx}, nil
}

// NewTransaction starts a transaction block.
func (d *database) NewTransaction() (Tx, error) {
	nTx, err := d.NewLocalTransaction()
	if err != nil {
		return nil, err
	}
	return &tx{Tx: nTx}, nil
}

// Collections returns a list of non-system tables from the database.
func (d *database) Collections() (collections []string, err error) {
	q := d.Builder.Select("table_name").
		From("information_schema.tables").
		Where("table_schema = ?", "public")

	iter := q.Iterator()
	defer iter.Close()

	for iter.Next() {
		var tableName string
		if err := iter.Scan(&tableName); err != nil {
			return nil, err
		}
		collections = append(collections, tableName)
	}

	return collections, nil
}

func (d *database) open() error {
	// Binding with sqladapter's logic.
	d.BaseDatabase = sqladapter.NewBaseDatabase(d)

	// Binding with builder.
	b, err := builder.New(d.BaseDatabase, template)
	if err != nil {
		return err
	}
	d.Builder = b

	connFn := func() error {
		sess, err := sql.Open("postgres", d.ConnectionURL().String())
		if err == nil {
			return d.BaseDatabase.BindSession(sess)
		}
		return err
	}

	if err := d.BaseDatabase.WaitForConnection(connFn); err != nil {
		return err
	}

	return nil
}

func (d *database) clone() (*database, error) {
	clone, err := newDatabase(d.connURL)
	if err != nil {
		return nil, err
	}

	if err := clone.open(); err != nil {
		return nil, err
	}

	return clone, nil
}

// CompileStatement allows sqladapter to compile the given statement into the
// format PostgreSQL expects.
func (d *database) CompileStatement(stmt *exql.Statement) string {
	return sqladapter.ReplaceWithDollarSign(stmt.Compile(template))
}

// Err allows sqladapter to translate some known errors into generic errors.
func (d *database) Err(err error) error {
	if err != nil {
		s := err.Error()
		if strings.Contains(s, `too many clients`) || strings.Contains(s, `remaining connection slots are reserved`) || strings.Contains(s, `too many open`) {
			return db.ErrTooManyClients
		}
	}
	return err
}

// NewLocalCollection allows sqladapter create a local db.Collection.
func (d *database) NewLocalCollection(name string) db.Collection {
	return newTable(d, name)
}

// Transaction creates a transaction and passes it to the given function, if
// if the function returns no error then the transaction is commited.
func (d *database) Transaction(fn func(tx Tx) error) error {
	tx, err := d.NewTransaction()
	if err != nil {
		return err
	}
	defer tx.Close()
	if err := fn(tx); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

// NewLocalTransaction allows sqladapter start a transaction block.
func (d *database) NewLocalTransaction() (sqladapter.Tx, error) {
	clone, err := d.clone()
	if err != nil {
		return nil, err
	}

	clone.txMu.Lock()
	defer clone.txMu.Unlock()

	connFn := func() error {
		sqlTx, err := clone.BaseDatabase.Session().Begin()
		if err == nil {
			return clone.BindTx(sqlTx)
		}
		return err
	}

	if err := d.BaseDatabase.WaitForConnection(connFn); err != nil {
		return nil, err
	}

	clone.tx = sqladapter.NewTx(clone)

	return sqladapter.NewTx(clone), nil
}

// FindDatabaseName allows sqladapter look up the database's name.
func (d *database) FindDatabaseName() (string, error) {
	q := d.Builder.Select(db.Raw("CURRENT_DATABASE() AS name"))

	iter := q.Iterator()
	defer iter.Close()

	if iter.Next() {
		var name string
		err := iter.Scan(&name)
		return name, err
	}

	return "", iter.Err()
}

// TableExists allows sqladapter check whether a table exists and returns an
// error in case it doesn't.
func (d *database) TableExists(name string) error {
	q := d.Builder.Select("table_name").
		From("information_schema.tables").
		Where("table_catalog = ? AND table_name = ?", d.BaseDatabase.Name(), name)

	iter := q.Iterator()
	defer iter.Close()

	if iter.Next() {
		var name string
		if err := iter.Scan(&name); err != nil {
			return err
		}
		return nil
	}
	return db.ErrCollectionDoesNotExist
}

// FindTablePrimaryKeys allows sqladapter find a table's primary keys.
func (d *database) FindTablePrimaryKeys(tableName string) ([]string, error) {
	q := d.Builder.Select("pg_attribute.attname AS pkey").
		From("pg_index", "pg_class", "pg_attribute").
		Where(`
			pg_class.oid = '"` + tableName + `"'::regclass
			AND indrelid = pg_class.oid
			AND pg_attribute.attrelid = pg_class.oid
			AND pg_attribute.attnum = ANY(pg_index.indkey)
			AND indisprimary
		`).OrderBy("pkey")

	iter := q.Iterator()
	defer iter.Close()

	pk := []string{}

	for iter.Next() {
		var k string
		if err := iter.Scan(&k); err != nil {
			return nil, err
		}
		pk = append(pk, k)
	}

	return pk, nil
}
