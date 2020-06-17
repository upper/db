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

// Package sqlite wraps the github.com/lib/sqlite SQLite driver. See
// https://github.com/upper/db/adapter/sqlite for documentation, particularities and
// usage examples.
package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	_ "github.com/mattn/go-sqlite3" // SQLite3 driver.
	db "github.com/upper/db"
	"github.com/upper/db/internal/sqladapter"
	"github.com/upper/db/internal/sqladapter/compat"
	"github.com/upper/db/internal/sqladapter/exql"
	"github.com/upper/db/sqlbuilder"
)

// database is the actual implementation of Database
type database struct {
}

var (
	fileOpenCount       int32
	errTooManyOpenFiles       = errors.New(`Too many open database files.`)
	maxOpenFiles        int32 = 100
)

func newSession(settings db.ConnectionURL) sqladapter.Session {
	return sqladapter.NewSession(settings, &database{})
}

func (*database) Template() *exql.Template {
	return template
}

/*
// CleanUp cleans up the session.
func (d *database) CleanUp() error {
	if atomic.AddInt32(&fileOpenCount, -1) < 0 {
		return errors.New(`Close() without Open()?`)
	}
	return nil
}
*/

func (*database) Open(sess sqladapter.Session, dsn string) (*sql.DB, error) {
	return sql.Open("sqlite3", dsn)
}

func (*database) Collections(sess sqladapter.Session) (collections []string, err error) {
	q := sess.Select("tbl_name").
		From("sqlite_master").
		Where("type = ?", "table")

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

/*
func (d *database) open() error {
	// Binding with sqladapter's logic.
	d.BaseDatabase = sqladapter.NewBaseDatabase(d)

	// Binding with sqlbuilder.
	d.SQLBuilder = sqlbuilder.WithSession(d.BaseDatabase, template)

	openFn := func() error {
		openFiles := atomic.LoadInt32(&fileOpenCount)
		if openFiles < maxOpenFiles {
			sess, err := sql.Open("sqlite3", d.ConnectionURL().String())
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
*/

func (*database) CompileStatement(sess sqladapter.Session, stmt *exql.Statement, args []interface{}) (string, []interface{}) {
	compiled, err := stmt.Compile(template)
	if err != nil {
		panic(err.Error())
	}
	return sqlbuilder.Preprocess(compiled, args)
}

func (*database) Err(sess sqladapter.Session, err error) error {
	if err != nil {
		if err == errTooManyOpenFiles {
			return db.ErrTooManyClients
		}
	}
	return err
}

func (*database) StatementExec(sess sqladapter.Session, ctx context.Context, query string, args ...interface{}) (res sql.Result, err error) {
	//d.mu.Lock()
	//defer d.mu.Unlock()

	if sess.Transaction() != nil {
		return compat.ExecContext(sess.Driver().(*sql.Tx), ctx, query, args)
	}

	sqlTx, err := compat.BeginTx(sess.Driver().(*sql.DB), ctx, sess.TxOptions())
	if err != nil {
		return nil, err
	}

	if res, err = compat.ExecContext(sqlTx, ctx, query, args); err != nil {
		return nil, err
	}

	if err = sqlTx.Commit(); err != nil {
		return nil, err
	}

	return res, err
}

func (*database) NewCollection() sqladapter.AdapterCollection {
	return &collectionAdapter{}
}

func (*database) LookupName(sess sqladapter.Session) (string, error) {
	connURL, err := ParseURL(sess.ConnectionURL().String())
	if err != nil {
		return "", err
	}
	return connURL.Database, nil
}

func (*database) TableExists(sess sqladapter.Session, name string) error {
	q := sess.Select("tbl_name").
		From("sqlite_master").
		Where("type = 'table' AND tbl_name = ?", name)

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

func (*database) PrimaryKeys(sess sqladapter.Session, tableName string) ([]string, error) {
	pk := make([]string, 0, 1)

	stmt := exql.RawSQL(fmt.Sprintf("PRAGMA TABLE_INFO('%s')", tableName))

	rows, err := sess.Query(stmt)
	if err != nil {
		return nil, err
	}

	columns := []struct {
		Name string `db:"name"`
		PK   int    `db:"pk"`
	}{}

	if err := sqlbuilder.NewIterator(rows).All(&columns); err != nil {
		return nil, err
	}

	maxValue := -1

	for _, column := range columns {
		if column.PK > 0 && column.PK > maxValue {
			maxValue = column.PK
		}
	}

	if maxValue > 0 {
		for _, column := range columns {
			if column.PK > 0 {
				pk = append(pk, column.Name)
			}
		}
	}

	return pk, nil
}
