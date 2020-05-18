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

package ql // import "github.com/upper/db/adapter/ql"

import (
	"database/sql"
	db "github.com/upper/db"
	"github.com/upper/db/sqlbuilder"
)

// Adapter is the public name of the adapter.
const Adapter = `ql`

type qlAdapter struct {
}

func (qlAdapter) Open(dsn db.ConnectionURL) (db.Database, error) {
	return Open(dsn)
}

func (qlAdapter) NewTx(sqlTx *sql.Tx) (sqlbuilder.Tx, error) {
	return NewTx(sqlTx)
}

func (qlAdapter) New(sqlDB *sql.DB) (sqlbuilder.Database, error) {
	return New(sqlDB)
}

func init() {
	db.RegisterAdapter(Adapter, sqlbuilder.Adapter(&qlAdapter{}))
}
