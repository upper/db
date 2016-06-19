// Copyright (c) 2012-2016 The upper.io/db authors. All rights reserved.
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
	"errors"
	"sync/atomic"

	_ "github.com/cznic/ql/driver" // QL driver
	"upper.io/db.v2"
	"upper.io/db.v2/sqlbuilder"
	"upper.io/db.v2/sqlbuilder/exql"
	"upper.io/db.v2/internal/sqladapter"
)

// Database represents a SQL database.
type Database interface {
	db.Database
	builder.Builder

	NewTransaction() (Tx, error)
}

// database is the actual implementation of Database
type database struct {
	sqladapter.BaseDatabase // Leveraged by sqladapter
	builder.Builder

	connURL db.ConnectionURL
}

var (
	fileOpenCount       int32
	errTooManyOpenFiles       = errors.New(`Too many open database files.`)
	maxOpenFiles        int32 = 5
)

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

// CleanUp cleans up the session.
func (d *database) CleanUp() error {
	if atomic.AddInt32(&fileOpenCount, -1) < 0 {
		return errors.New(`Close() without Open()?`)
	}
	return nil
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
	q := d.Builder.Select("Name").
		From("__Table")

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

	openFn := func() error {
		openFiles := atomic.LoadInt32(&fileOpenCount)
		if openFiles < maxOpenFiles {
			sess, err := sql.Open("ql", d.ConnectionURL().String())
			if err == nil {
				if err := d.BaseDatabase.BindSession(sess); err != nil {
					return err
				}
				atomic.AddInt32(&fileOpenCount, 1)
				return nil
			}
			return err
		}
		return errTooManyOpenFiles
	}

	if err := d.BaseDatabase.WaitForConnection(openFn); err != nil {
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
// format SQLite expects.
func (d *database) CompileStatement(stmt *exql.Statement) string {
	return sqladapter.ReplaceWithDollarSign(stmt.Compile(template))
}

// Err allows sqladapter to translate some known errors into generic errors.
func (d *database) Err(err error) error {
	if err != nil {
		if err == errTooManyOpenFiles {
			return db.ErrTooManyClients
		}
	}
	return err
}

// StatementExec wraps the statement to execute around a transaction.
func (d *database) StatementExec(stmt *sql.Stmt, args ...interface{}) (sql.Result, error) {
	if d.BaseDatabase.Tx() == nil {
		var tx *sql.Tx
		var res sql.Result
		var err error

		if tx, err = d.Session().Begin(); err != nil {
			return nil, err
		}

		s := tx.Stmt(stmt)

		if res, err = s.Exec(args...); err != nil {
			return nil, err
		}

		if err = tx.Commit(); err != nil {
			return nil, err
		}

		return res, err
	}
	return stmt.Exec(args...)
}

// NewLocalCollection allows sqladapter create a local db.Collection.
func (d *database) NewLocalCollection(name string) db.Collection {
	return newTable(d, name)
}

// NewLocalTransaction allows sqladapter start a transaction block.
func (d *database) NewLocalTransaction() (sqladapter.Tx, error) {
	clone, err := d.clone()
	if err != nil {
		return nil, err
	}

	openFn := func() error {
		sqlTx, err := clone.BaseDatabase.Session().Begin()
		if err == nil {
			return clone.BindTx(sqlTx)
		}
		return err
	}

	if err := d.BaseDatabase.WaitForConnection(openFn); err != nil {
		return nil, err
	}

	return sqladapter.NewTx(clone), nil
}

// FindDatabaseName allows sqladapter look up the database's name.
func (d *database) FindDatabaseName() (string, error) {
	connURL, err := ParseURL(d.ConnectionURL().String())
	if err != nil {
		return "", err
	}
	return connURL.Database, nil
}

// TableExists allows sqladapter check whether a table exists and returns an
// error in case it doesn't.
func (d *database) TableExists(name string) error {
	q := d.Builder.Select("Name").
		From("__Table").
		Where("Name == ?", name)

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
	return nil, nil
}
