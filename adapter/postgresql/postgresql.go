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

package postgresql // import "github.com/upper/db/adapter/postgresql"

import (
	"database/sql"

	db "github.com/upper/db"
	"github.com/upper/db/internal/sqladapter"
	"github.com/upper/db/sqlbuilder"
)

// Adapter is the unique name that you can use to refer to this adapter.
const Adapter = `postgresql`

type postgresqlAdapter struct {
}

func (postgresqlAdapter) Open(dsn db.ConnectionURL) (db.Session, error) {
	return Open(dsn)
}

func (postgresqlAdapter) NewTx(sqlTx *sql.Tx) (sqlbuilder.Tx, error) {
	return NewTx(sqlTx)
}

func (postgresqlAdapter) New(sqlDB *sql.DB) (sqlbuilder.Session, error) {
	return New(sqlDB)
}

func init() {
	db.RegisterAdapter(Adapter, sqlbuilder.Adapter(&postgresqlAdapter{}))
}

func Open(connURL db.ConnectionURL) (sqlbuilder.Session, error) {
	sess := newSession(connURL)
	if err := sess.Open(); err != nil {
		return nil, err
	}
	return sess, nil
}

func NewTx(sqlTx *sql.Tx) (sqlbuilder.Tx, error) {
	tx, err := sqladapter.NewTx(&database{}, sqlTx)
	if err != nil {
		return nil, err
	}
	return tx, nil
}

func New(sqlDB *sql.DB) (sqlbuilder.Session, error) {
	sess := newSession(nil)
	if err := sess.BindDB(sqlDB); err != nil {
		return nil, err
	}
	return sess, nil
}
