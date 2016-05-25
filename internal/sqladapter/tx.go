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

package sqladapter

import (
	"database/sql"
	"sync/atomic"
)

// Tx represents a database session within a transaction.
type Tx interface {
	Database
	BaseTx
}

// BaseTx defines methods to be implemented by a transaction.
type BaseTx interface {
	Commit() error
	Rollback() error
	Committed() bool
}

type txWrapper struct {
	Database
	BaseTx
}

// NewTx creates a database session within a transaction.
func NewTx(db Database) Tx {
	return &txWrapper{
		Database: db,
		BaseTx:   db.Tx(),
	}
}

func newTxWrapper(db Database) Tx {
	return &txWrapper{
		Database: db,
		BaseTx:   db.Tx(),
	}
}

type sqlTx struct {
	*sql.Tx
	committed atomic.Value
}

func newTx(tx *sql.Tx) BaseTx {
	return &sqlTx{Tx: tx}
}

func (t *sqlTx) Committed() bool {
	committed := t.committed.Load()
	if committed != nil {
		return true
	}
	return false
}

func (t *sqlTx) Commit() (err error) {
	if err = t.Tx.Commit(); err == nil {
		t.committed.Store(struct{}{})
	}
	return err
}

func (t *txWrapper) Commit() error {
	defer t.Database.Close()
	return t.BaseTx.Commit()
}

func (t *txWrapper) Rollback() error {
	defer t.Database.Close()
	return t.BaseTx.Rollback()
}

var (
	_ = BaseTx(&sqlTx{})
)
