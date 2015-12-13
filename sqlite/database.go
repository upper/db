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

package sqlite

import (
	"errors"
	"fmt"
	"sync/atomic"

	"database/sql"

	_ "github.com/mattn/go-sqlite3" // SQLite3 driver.
	"upper.io/builder/sqlbuilder"
	"upper.io/builder/sqlgen"
	template "upper.io/builder/template/sqlite"
	"upper.io/db.v2"
	"upper.io/db.v2/internal/sqladapter"
	"upper.io/db.v2/internal/sqlutil/tx"
)

type database struct {
	*sqladapter.BaseDatabase
	columns map[string][]columnSchemaT
}

var (
	fileOpenCount       int32
	errTooManyOpenFiles = errors.New(`Too many open database files.`)
)

type columnSchemaT struct {
	Name string `db:"name"`
	PK   int    `db:"pk"`
}

var _ = db.Database(&database{})

const (
	// If we try to open lots of sessions cgo will panic without a warning, this
	// artificial limit was added to prevent that panic.
	maxOpenFiles = 100
)

// CompileAndReplacePlaceholders compiles the given statement into an string
// and replaces each generic placeholder with the placeholder the driver
// expects (if any).
func (d *database) CompileAndReplacePlaceholders(stmt *sqlgen.Statement) (query string) {
	return stmt.Compile(d.Template())
}

// Err translates some known errors into generic errors.
func (d *database) Err(err error) error {
	if err != nil {
		if err == errTooManyOpenFiles {
			return db.ErrTooManyClients
		}
	}
	return err
}

// Open attempts to open a connection to the database server.
func (d *database) Open() error {
	var sess *sql.DB

	openFn := func(sess **sql.DB) (err error) {
		openFiles := atomic.LoadInt32(&fileOpenCount)

		if openFiles < maxOpenFiles {
			*sess, err = sql.Open(`sqlite3`, d.ConnectionURL().String())

			if err == nil {
				atomic.AddInt32(&fileOpenCount, 1)
			}
			return
		}

		return errTooManyOpenFiles

	}

	if err := d.WaitForConnection(func() error { return openFn(&sess) }); err != nil {
		return err
	}

	return d.Bind(sess)
}

// Setup configures the adapter.
func (d *database) Setup(connURL db.ConnectionURL) error {
	d.BaseDatabase = sqladapter.NewDatabase(d, connURL, template.Template())
	return d.Open()
}

// Use changes the active database.
func (d *database) Use(name string) (err error) {
	var conn ConnectionURL
	if conn, err = ParseURL(d.ConnectionURL().String()); err != nil {
		return err
	}
	conn.Database = name
	if d.BaseDatabase != nil {
		d.Close()
	}
	return d.Setup(conn)
}

func (d *database) Close() error {
	if d.Session() != nil {
		if atomic.AddInt32(&fileOpenCount, -1) < 0 {
			return errors.New(`Close() without Open()?`)
		}
		return d.BaseDatabase.Close()
	}
	return nil
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

	if len(d.Schema().Tables) == 0 {
		q := d.Builder().Select("tbl_name").
			From("sqlite_master").
			Where("type = ?", "table")

		iter := q.Iterator()
		defer iter.Close()

		if iter.Err() != nil {
			return nil, iter.Err()
		}

		for iter.Next() {
			var tableName string
			if err := iter.Scan(&tableName); err != nil {
				return nil, err
			}
			d.Schema().AddTable(tableName)
		}
	}

	return d.Schema().Tables, nil
}

// Drop removes all tables from the current database.
func (d *database) Drop() error {
	stmt := &sqlgen.Statement{
		Type:     sqlgen.DropDatabase,
		Database: sqlgen.DatabaseWithName(d.Schema().Name),
	}
	if _, err := d.Builder().Exec(stmt); err != nil {
		return err
	}
	return nil
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

	return &sqltx.Database{Database: clone, Tx: clone.Tx()}, nil
}

// PopulateSchema looks up for the table info in the database and populates its
// schema for internal use.
func (d *database) PopulateSchema() (err error) {
	var collections []string

	d.NewSchema()

	var connURL ConnectionURL
	if connURL, err = ParseURL(d.ConnectionURL().String()); err != nil {
		return err
	}

	d.Schema().Name = connURL.Database

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

// TableExists checks whether a table exists and returns an error in case it doesn't.
func (d *database) TableExists(name string) error {
	if d.Schema().HasTable(name) {
		return nil
	}

	q := d.Builder().Select("tbl_name").
		From("sqlite_master").
		Where("type = 'table' AND tbl_name = ?", name)

	iter := q.Iterator()
	defer iter.Close()

	if iter.Next() {
		var tableName string
		if err := iter.Scan(&tableName); err != nil {
			return err
		}
	} else {
		return db.ErrCollectionDoesNotExist
	}

	return nil
}

// TableColumns returns all columns from the given table.
func (d *database) TableColumns(tableName string) ([]string, error) {
	s := d.Schema()

	if len(s.Table(tableName).Columns) == 0 {

		stmt := sqlgen.RawSQL(fmt.Sprintf(`PRAGMA TABLE_INFO('%s')`, tableName))

		rows, err := d.Builder().Query(stmt)
		if err != nil {
			return nil, err
		}

		if d.columns == nil {
			d.columns = make(map[string][]columnSchemaT)
		}

		columns := []columnSchemaT{}

		if err := sqlbuilder.NewIterator(rows).All(&columns); err != nil {
			return nil, err
		}

		d.columns[tableName] = columns

		s.TableInfo[tableName].Columns = make([]string, 0, len(columns))

		for _, col := range d.columns[tableName] {
			s.TableInfo[tableName].Columns = append(s.TableInfo[tableName].Columns, col.Name)
		}
	}

	return s.Table(tableName).Columns, nil
}

// TablePrimaryKey returns all primary keys from the given table.
func (d *database) TablePrimaryKey(tableName string) ([]string, error) {
	tableSchema := d.Schema().Table(tableName)

	d.TableColumns(tableName)

	maxValue := -1

	for i := range d.columns[tableName] {
		if d.columns[tableName][i].PK > 0 && d.columns[tableName][i].PK > maxValue {
			maxValue = d.columns[tableName][i].PK
		}
	}

	if maxValue > 0 {
		tableSchema.PrimaryKey = make([]string, maxValue)

		for i := range d.columns[tableName] {
			if d.columns[tableName][i].PK > 0 {
				tableSchema.PrimaryKey[d.columns[tableName][i].PK-1] = d.columns[tableName][i].Name
			}
		}
	}

	return tableSchema.PrimaryKey, nil
}

func (d *database) clone() (*database, error) {
	clone := &database{}
	clone.BaseDatabase = d.BaseDatabase.Clone(clone)
	if err := clone.Open(); err != nil {
		return nil, err
	}
	return clone, nil
}
