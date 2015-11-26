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
	"upper.io/builder/sqlgen"
	template "upper.io/builder/template/postgresql"
	"upper.io/db.v2"
	"upper.io/db.v2/internal/sqladapter"
	"upper.io/db.v2/internal/sqlutil/tx"
)

type database struct {
	*sqladapter.BaseDatabase
}

var _ = db.Database(&database{})

// CompileAndReplacePlaceholders compiles the given statement into an string
// and replaces each generic placeholder with the placeholder the driver
// expects (if any).
func (d *database) CompileAndReplacePlaceholders(stmt *sqlgen.Statement) (query string) {
	buf := stmt.Compile(d.Template())

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
		if strings.Contains(s, `relation`) && strings.Contains(s, `does not exist`) {
			return db.ErrCollectionDoesNotExist
		}
	}
	return err
}

// Open attempts to open a connection to the database server.
func (d *database) Open() error {
	var sess *sql.DB

	connFn := func(sess **sql.DB) (err error) {
		*sess, err = sql.Open("postgres", d.ConnectionURL().String())
		return
	}

	if err := d.WaitForConnection(func() error { return connFn(&sess) }); err != nil {
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

	q := d.Builder().Select(db.Raw("CURRENT_DATABASE() AS name"))

	var dbName string

	iter := q.Iterator()
	defer iter.Close()

	if iter.Next() {
		if err := iter.Scan(&dbName); err != nil {
			return err
		}
	} else {
		return iter.Err()
	}

	d.Schema().Name = dbName

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

	q := d.Builder().Select("table_name").
		From("information_schema.tables").
		Where("table_catalog = ? AND table_name = ?", d.Schema().Name, name)

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

		q := d.Builder().Select("column_name").
			From("information_schema.columns").
			Where("table_catalog = ? AND table_name = ?", d.Schema().Name, tableName)

		var rows []struct {
			Name string `db:"column_name"`
		}

		if err := q.Iterator().All(&rows); err != nil {
			return nil, err
		}

		s.TableInfo[tableName].Columns = make([]string, 0, len(rows))

		for i := range rows {
			s.TableInfo[tableName].Columns = append(s.TableInfo[tableName].Columns, rows[i].Name)
		}
	}

	return s.Table(tableName).Columns, nil
}

// TablePrimaryKey returns all primary keys from the given table.
func (d *database) TablePrimaryKey(tableName string) ([]string, error) {
	s := d.Schema()

	ts := s.Table(tableName)

	if len(ts.PrimaryKey) != 0 {
		return ts.PrimaryKey, nil
	}

	ts.PrimaryKey = make([]string, 0, 1)

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

	for iter.Next() {
		var pKey string
		if err := iter.Scan(&pKey); err != nil {
			return nil, err
		}
		ts.PrimaryKey = append(ts.PrimaryKey, pKey)
	}

	return ts.PrimaryKey, nil
}

func (d *database) clone() (*database, error) {
	clone := &database{}
	clone.BaseDatabase = d.BaseDatabase.Clone(clone)
	if err := clone.Open(); err != nil {
		return nil, err
	}
	return clone, nil
}
