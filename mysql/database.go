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

package mysql

import (
	"strings"

	"database/sql"

	_ "github.com/go-sql-driver/mysql" // MySQL driver.
	"upper.io/db.v2"
	"upper.io/db.v2/builder/sqlgen"
	"upper.io/db.v2/internal/sqladapter"
)

type database struct {
	*sqladapter.BaseDatabase
}

var _ = db.Database(&database{})

// CompileAndReplacePlaceholders compiles the given statement into an string
// and replaces each generic placeholder with the placeholder the driver
// expects (if any).
func (d *database) CompileAndReplacePlaceholders(stmt *sqlgen.Statement) (query string) {
	return stmt.Compile(d.Template())
}

// Err translates some known errors into generic errors.
func (d *database) Err(err error) error {
	if err != nil {
		s := err.Error()
		if strings.Contains(s, `many connections`) {
			return db.ErrTooManyClients
		}
	}
	return err
}

func (d *database) open() error {
	var sess *sql.DB

	connFn := func(sess **sql.DB) (err error) {
		*sess, err = sql.Open("mysql", d.ConnectionURL().String())
		return
	}

	if err := d.WaitForConnection(func() error { return connFn(&sess) }); err != nil {
		return err
	}

	return d.Bind(sess)
}

// Open attempts to open a connection to the database server.
func (d *database) Open(connURL db.ConnectionURL) error {
	d.BaseDatabase = sqladapter.NewDatabase(d, connURL, template())
	return d.open()
}

// Clone creates a new database connection with the same settings as the
// original.
func (d *database) Clone() (db.Database, error) {
	return d.clone()
}

// NewTable returns a db.Collection.
func (d *database) NewTable(name string) db.Collection {
	return newTable(d, name)
}

// Collections returns a list of non-system tables from the database.
func (d *database) Collections() (collections []string, err error) {

	q := d.Builder().Select("table_name").
		From("information_schema.tables").
		Where("table_schema = ?", d.Schema().Name())

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

// Transaction starts a transaction block and returns a db.Tx struct that can
// be used to issue transactional queries.
func (d *database) Transaction() (db.Tx, error) {
	var err error
	var sqlTx *sql.Tx
	var clone *database

	if clone, err = d.clone(); err != nil {
		return nil, err
	}

	connFn := func(sqlTx **sql.Tx) (err error) {
		*sqlTx, err = clone.Session().Begin()
		return
	}

	if err := d.WaitForConnection(func() error { return connFn(&sqlTx) }); err != nil {
		return nil, err
	}

	clone.BindTx(sqlTx)

	return &sqladapter.TxDatabase{Database: clone, Tx: clone.Tx()}, nil
}

// PopulateSchema looks up for the table info in the database and populates its
// schema for internal use.
func (d *database) PopulateSchema() error {
	schema := d.NewSchema()

	q := d.Builder().Select(db.Raw("DATABASE() AS name"))

	iter := q.Iterator()
	defer iter.Close()

	if iter.Next() {
		var name string
		err := iter.Scan(&name)
		schema.SetName(name)
		return err
	}
	return iter.Err()
}

// TableExists checks whether a table exists and returns an error in case it doesn't.
func (d *database) TableExists(name string) error {
	q := d.Builder().Select("table_name").
		From("information_schema.tables").
		Where("table_schema = ? AND table_name = ?", d.Schema().Name(), name)

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

// TablePrimaryKey returns all primary keys from the given table.
func (d *database) TablePrimaryKey(tableName string) ([]string, error) {
	tableSchema := d.Schema().Table(tableName)

	pk := tableSchema.PrimaryKeys()
	if pk != nil {
		return pk, nil
	}

	pk = []string{}

	q := d.Builder().Select("k.column_name").
		From("information_schema.table_constraints AS t").
		Join("information_schema.key_column_usage AS k").
		Using("constraint_name", "table_schema", "table_name").
		Where(`
			t.constraint_type = 'primary key'
			AND t.table_schema = ?
			AND t.table_name = ?
		`, d.Schema().Name(), tableName).
		OrderBy("k.ordinal_position")

	iter := q.Iterator()
	defer iter.Close()

	for iter.Next() {
		var k string
		if err := iter.Scan(&k); err != nil {
			return nil, err
		}
		pk = append(pk, k)
	}

	tableSchema.SetPrimaryKeys(pk)

	return pk, nil
}

func (d *database) clone() (*database, error) {
	clone := &database{}
	clone.BaseDatabase = d.BaseDatabase.Clone(clone)
	if err := clone.open(); err != nil {
		return nil, err
	}
	return clone, nil
}
