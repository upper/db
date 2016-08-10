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

package mysql

import (
	"strings"
	"sync"

	"database/sql"

	_ "github.com/go-sql-driver/mysql" // MySQL driver.
	"upper.io/db.v2"
	"upper.io/db.v2/internal/sqladapter"
	"upper.io/db.v2/internal/sqladapter/exql"
	"upper.io/db.v2/lib/sqlbuilder"
)

// database is the actual implementation of Database
type database struct {
	sqladapter.BaseDatabase // Leveraged by sqladapter
	sqlbuilder.Builder

	connURL db.ConnectionURL
	txMu    sync.Mutex
}

var (
	_ = sqlbuilder.Database(&database{})
)

// newDatabase binds *database with sqladapter and the SQL builer.
func newDatabase(settings db.ConnectionURL) (*database, error) {
	d := &database{
		connURL: settings,
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

// NewTx starts a transaction block.
func (d *database) NewTx() (sqlbuilder.Tx, error) {
	nTx, err := d.NewLocalTransaction()
	if err != nil {
		return nil, err
	}
	return &tx{DatabaseTx: nTx}, nil
}

// Collections returns a list of non-system tables from the database.
func (d *database) Collections() (collections []string, err error) {
	q := d.Select("table_name").
		From("information_schema.tables").
		Where("table_schema = ?", d.BaseDatabase.Name())

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

	// Binding with sqlbuilder.
	b, err := sqlbuilder.WithSession(d.BaseDatabase, template)
	if err != nil {
		return err
	}
	d.Builder = b

	connFn := func() error {
		sess, err := sql.Open("mysql", d.ConnectionURL().String())
		if err == nil {
			sess.SetConnMaxLifetime(connMaxLifetime)
			sess.SetMaxIdleConns(maxIdleConns)
			sess.SetMaxOpenConns(maxOpenConns)
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

	clone.BaseDatabase = sqladapter.NewBaseDatabase(clone)

	b, err := sqlbuilder.WithSession(clone.BaseDatabase, template)
	if err != nil {
		return nil, err
	}
	clone.Builder = b

	clone.BaseDatabase.BindSession(d.BaseDatabase.Session())
	return clone, nil
}

// CompileStatement allows sqladapter to compile the given statement into the
// format MySQL expects.
func (d *database) CompileStatement(stmt *exql.Statement) string {
	return stmt.Compile(template)
}

// Err allows sqladapter to translate some known errors into generic errors.
func (d *database) Err(err error) error {
	if err != nil {
		// This error is not exported so we have to check it by its string value.
		s := err.Error()
		if strings.Contains(s, `many connections`) {
			return db.ErrTooManyClients
		}
	}
	return err
}

// NewLocalCollection allows sqladapter create a local db.Collection.
func (d *database) NewLocalCollection(name string) db.Collection {
	return newTable(d, name)
}

// Tx creates a transaction and passes it to the given function, if if the
// function returns no error then the transaction is commited.
func (d *database) Tx(fn func(tx sqlbuilder.Tx) error) error {
	return sqladapter.RunTx(d, fn)
}

// NewLocalTransaction allows sqladapter start a transaction block.
func (d *database) NewLocalTransaction() (sqladapter.DatabaseTx, error) {
	clone, err := d.clone()
	if err != nil {
		return nil, err
	}

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

	return sqladapter.NewTx(clone), nil
}

// FindDatabaseName allows sqladapter look up the database's name.
func (d *database) FindDatabaseName() (string, error) {
	q := d.Select(db.Raw("DATABASE() AS name"))

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
	q := d.Select("table_name").
		From("information_schema.tables").
		Where("table_schema = ? AND table_name = ?", d.BaseDatabase.Name(), name)

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
	q := d.Select("k.column_name").
		From("information_schema.table_constraints AS t").
		Join("information_schema.key_column_usage AS k").
		Using("constraint_name", "table_schema", "table_name").
		Where(`
			t.constraint_type = 'primary key'
			AND t.table_schema = ?
			AND t.table_name = ?
		`, d.BaseDatabase.Name(), tableName).
		OrderBy("k.ordinal_position")

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
