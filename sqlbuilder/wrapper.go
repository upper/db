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

package sqlbuilder

import (
	"context"
	"database/sql"

	db "github.com/upper/db"
)

// Tx represents a transaction on a SQL database. A transaction is like a
// regular Session except it has two extra methods: Commit and Rollback.
//
// A transaction needs to be committed (with Commit) to make changes permanent,
// changes can be discarded before committing by rolling back (with Rollback).
// After either committing or rolling back a transaction it can not longer be
// used and it's automatically closed.
type Tx interface {
	// All db.Session methods are available on transaction sessions. They will
	// run on the same transaction.
	db.Session

	// All SQLBuilder methods are available on transaction sessions. They will
	// run on the same transaction.
	SQLBuilder

	// db.Tx adds Commit and Rollback methods to the transaction.
	db.Tx

	// Context returns the context used as default for queries on this transaction.
	// If no context has been set, a default context.Background() is returned.
	Context() context.Context

	// WithContext returns a copy of the transaction that uses the given context
	// as default. Copies are safe to use concurrently but they're backed by the
	// same *sql.Tx, so any copy may commit or rollback the parent transaction.
	WithContext(context.Context) Tx

	// SetTxOptions sets the default TxOptions that is going to be used for new
	// transactions created in the session.
	SetTxOptions(sql.TxOptions)

	// TxOptions returns the defaultx TxOptions.
	TxOptions() *sql.TxOptions
}

// Session represents a SQL database.
type Session interface {
	// All db.Session methods are available on this session.
	db.Session

	// All SQLBuilder methods are available on this session.
	SQLBuilder

	// NewTx creates and returns a transaction that runs on the given context.
	// If a nil context is given, then the transaction will use the session's
	// default context.  The user is responsible for committing or rolling back
	// the session.
	NewTx(ctx context.Context) (Tx, error)

	// Tx creates a transaction block on the default database context and passes
	// it to the function fn. If fn returns no error the transaction is commited,
	// else the transaction is rolled back. After being commited or rolled back
	// the transaction is closed automatically.
	Tx(fn func(sess Tx) error) error

	// TxContext creates a transaction block on the given context and passes it to
	// the function fn. If fn returns no error the transaction is commited, else
	// the transaction is rolled back. After being commited or rolled back the
	// transaction is closed automatically.
	TxContext(ctx context.Context, fn func(sess Tx) error) error

	// Context returns the context used as default for queries on this session
	// and for new transactions.  If no context has been set, a default
	// context.Background() is returned.
	Context() context.Context

	// WithContext returns a copy of the session that uses the given context as
	// default. Copies are safe to use concurrently but they're backed by the
	// same *sql.DB. You may close a copy at any point but that won't close the
	// parent session.
	WithContext(context.Context) Session

	// SetTxOptions sets the default TxOptions that is going to be used for new
	// transactions created in the session.
	SetTxOptions(sql.TxOptions)

	// TxOptions returns the defaultx TxOptions.
	TxOptions() *sql.TxOptions
}

// Adapter represents a SQL adapter.
type Adapter interface {
	// New wraps an active *sql.DB session and returns a SQLBuilder database.  The
	// adapter needs to be imported to the blank namespace in order for it to be
	// used here.
	//
	// This method is internally used by upper-db to create a builder backed by the
	// given database.  You may want to use your adapter's New function instead of
	// this one.
	New(*sql.DB) (Session, error)

	// NewTx wraps an active *sql.Tx transation and returns a SQLBuilder
	// transaction.  The adapter needs to be imported to the blank namespace in
	// order for it to be used.
	//
	// This method is internally used by upper-db to create a builder backed by the
	// given transaction.  You may want to use your adapter's NewTx function
	// instead of this one.
	NewTx(*sql.Tx) (Tx, error)

	// Open opens a SQL database.
	OpenDSN(db.ConnectionURL) (Session, error)
}

type dbAdapter struct {
	Adapter
}

func (d *dbAdapter) Open(conn db.ConnectionURL) (db.Session, error) {
	sess, err := d.Adapter.OpenDSN(conn)
	if err != nil {
		return nil, err
	}
	return sess.(db.Session), nil
}

func NewCompatAdapter(adapter Adapter) db.Adapter {
	return &dbAdapter{adapter}
}
