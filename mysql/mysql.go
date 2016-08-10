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

package mysql // import "upper.io/db.v2/mysql"

import (
	"database/sql"
	"time"

	"upper.io/db.v2"

	"upper.io/db.v2/internal/sqladapter"
	"upper.io/db.v2/lib/sqlbuilder"
)

var (
	connMaxLifetime = db.DefaultConnMaxLifetime
	maxIdleConns    = db.DefaultMaxIdleConns
	maxOpenConns    = db.DefaultMaxOpenConns
)

const sqlDriver = `mysql`

// Adapter is the public name of the adapter.
const Adapter = sqlDriver

func init() {
	sqlbuilder.RegisterAdapter(Adapter, &sqlbuilder.AdapterFuncMap{
		New:   New,
		NewTx: NewTx,
		Open:  Open,
	})
}

// Open stablishes a new connection with the SQL server.
func Open(settings db.ConnectionURL) (sqlbuilder.Database, error) {
	d, err := newDatabase(settings)
	if err != nil {
		return nil, err
	}
	if err := d.Open(settings); err != nil {
		return nil, err
	}
	return d, nil
}

// NewTx returns a transaction session.
func NewTx(sqlTx *sql.Tx) (sqlbuilder.Tx, error) {
	d, err := newDatabase(nil)
	if err != nil {
		return nil, err
	}

	// Binding with sqladapter's logic.
	d.BaseDatabase = sqladapter.NewBaseDatabase(d)

	// Binding with sqlbuilder.
	b, err := sqlbuilder.WithSession(d.BaseDatabase, template)
	if err != nil {
		return nil, err
	}
	d.Builder = b

	if err := d.BaseDatabase.BindTx(sqlTx); err != nil {
		return nil, err
	}

	newTx := sqladapter.NewTx(d)
	return &tx{DatabaseTx: newTx}, nil
}

// New wraps the given *sql.DB session and creates a new db session.
func New(sess *sql.DB) (sqlbuilder.Database, error) {
	d, err := newDatabase(nil)
	if err != nil {
		return nil, err
	}

	// Binding with sqladapter's logic.
	d.BaseDatabase = sqladapter.NewBaseDatabase(d)

	// Binding with sqlbuilder.
	b, err := sqlbuilder.WithSession(d.BaseDatabase, template)
	if err != nil {
		return nil, err
	}
	d.Builder = b

	if err := d.BaseDatabase.BindSession(sess); err != nil {
		return nil, err
	}
	return d, nil
}

// SetConnMaxLifetime sets the default value to be passed to
// db.SetConnMaxLifetime.
func SetConnMaxLifetime(d time.Duration) {
	connMaxLifetime = d
}

// SetMaxIdleConns sets the default value to be passed to db.SetMaxOpenConns.
func SetMaxIdleConns(n int) {
	if n < 0 {
		n = 0
	}
	maxIdleConns = n
}

// SetMaxOpenConns sets the default value to be passed to db.SetMaxOpenConns.
// If the value of maxIdleConns is >= 0 and maxOpenConns is less than
// maxIdleConns, then maxIdleConns will be reduced to match maxOpenConns.
func SetMaxOpenConns(n int) {
	if n < 0 {
		n = 0
	}
	if n > maxIdleConns {
		maxIdleConns = n
	}
	maxOpenConns = n
}
