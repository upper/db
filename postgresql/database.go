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
	"strconv"
	"strings"

	"database/sql"

	_ "github.com/lib/pq" // PostgreSQL driver.
	"upper.io/db.v2"
	"upper.io/db.v2/builder"
	"upper.io/db.v2/builder/exql"
	"upper.io/db.v2/internal/sqladapter"
)

type database struct {
	sqladapter.BaseDatabase
	b       builder.Builder
	connURL db.ConnectionURL
}

type Database interface {
	sqladapter.Database
}

var (
	_ = sqladapter.Database(&database{})
)

func newDatabase(settings db.ConnectionURL) (*database, error) {
	d := &database{
		connURL: settings,
	}

	d.BaseDatabase = sqladapter.NewBaseDatabase(d)

	b, err := builder.New(d.BaseDatabase, template)
	if err != nil {
		return nil, err
	}

	d.b = b

	return d, nil
}

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

func (d *database) ConnectionURL() db.ConnectionURL {
	return d.connURL
}

func (d *database) Builder() builder.Builder {
	return d.b
}

// CompileStatement compiles the given statement into an string
// and replaces each generic placeholder with the placeholder the driver
// expects (if any).
func (d *database) CompileStatement(stmt *exql.Statement) (query string) {
	buf := stmt.Compile(template())

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

// Err translates some known errors into generic errors.
func (d *database) Err(err error) error {
	if err != nil {
		s := err.Error()
		if strings.Contains(s, `too many clients`) || strings.Contains(s, `remaining connection slots are reserved`) {
			return db.ErrTooManyClients
		}
	}
	return err
}

func (d *database) Template() *exql.Template {
	return template()
}

func (d *database) open() error {
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

// Open attempts to open a connection to the database server.
func (d *database) Open(connURL db.ConnectionURL) error {
	if connURL == nil {
		return db.ErrMissingConnURL
	}
	return d.open()
}

// Clone creates a new database connection with the same settings as the
// original.
func (d *database) Clone() (db.Database, error) {
	return d.clone()
}

// NewCollection returns a db.Collection.
func (d *database) NewCollection(name string) db.Collection {
	return newTable(d, name)
}

// Collections returns a list of non-system tables from the database.
func (d *database) Collections() (collections []string, err error) {
	q := d.Builder().Select("table_name").
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

// Transaction starts a transaction block and returns a db.Tx struct that can
// be used to issue transactional queries.
func (d *database) Transaction() (db.Tx, error) {
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

	return clone, nil
}

func (d *database) FindDatabaseName() (string, error) {
	q := d.Builder().Select(db.Raw("CURRENT_DATABASE() AS name"))

	iter := q.Iterator()
	defer iter.Close()

	if iter.Next() {
		var name string
		err := iter.Scan(&name)
		return name, err
	}

	return "", iter.Err()
}

// TableExists checks whether a table exists and returns an error in case it doesn't.
func (d *database) TableExists(name string) error {
	q := d.Builder().Select("table_name").
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

func (d *database) FindTablePrimaryKeys(tableName string) ([]string, error) {
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
