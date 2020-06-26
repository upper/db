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

package sqladapter

import (
	"context"
	"database/sql"
	"sync/atomic"

	db "github.com/upper/db"
	"github.com/upper/db/sqlbuilder"
)

// SessionTx represents a database session within a transaction.
type SessionTx interface {
	Session

	BaseTx
}

// BaseTx provides logic for methods that can be shared across all SQL
// adapters.
type BaseTx interface {
	db.Tx

	// Committed returns true if the transaction was already commited.
	Committed() bool
}

type sessionTx struct {
	Session
	BaseTx
}

// NewSessionTx creates a database session within a transaction.
func NewSessionTx(sess Session) SessionTx {
	newSessionTx := &sessionTx{
		Session: sess,
		BaseTx:  sess.Transaction(),
	}
	return newSessionTx
}

type baseTx struct {
	*sql.Tx
	committed atomic.Value
}

func newBaseTx(tx *sql.Tx) BaseTx {
	return &baseTx{Tx: tx}
}

func (b *baseTx) Committed() bool {
	committed := b.committed.Load()
	return committed != nil
}

func (b *baseTx) Commit() (err error) {
	err = b.Tx.Commit()
	if err != nil {
		return err
	}
	b.committed.Store(struct{}{})
	return nil
}

func (w *sessionTx) Commit() error {
	defer w.Session.Close() // Automatic close on commit.
	return w.BaseTx.Commit()
}

func (w *sessionTx) Rollback() error {
	defer w.Session.Close() // Automatic close on rollback.
	return w.BaseTx.Rollback()
}

// TxContext creates a transaction context and runs fn within it.
func TxContext(ctx context.Context, sess sqlbuilder.Session, fn func(tx sqlbuilder.Tx) error) error {
	tx, err := sess.NewTx(ctx)
	if err != nil {
		return err
	}

	defer tx.Close()
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

var (
	_ = BaseTx(&baseTx{})
	_ = SessionTx(&sessionTx{})
)

type txWrapper struct {
	SessionTx
}

var (
	_ = sqlbuilder.Tx(&txWrapper{})
)

func (t *txWrapper) WithContext(ctx context.Context) sqlbuilder.Tx {
	newTx := *t
	newTx.SessionTx.SetContext(ctx)
	return &newTx
}
