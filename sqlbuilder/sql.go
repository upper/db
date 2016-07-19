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

package builder

import (
	"database/sql"
	"fmt"
	"sync"

	"upper.io/db.v2"
)

var (
	sqlAdapters   map[string]*SQLAdapterFuncMap
	sqlAdaptersMu sync.RWMutex
)

func init() {
	sqlAdapters = make(map[string]*SQLAdapterFuncMap)
}

// SQLCommon holds common methods for SQL databases.
type SQLCommon interface {
	db.Database
	SQLBuilder
}

// SQLTx represents transaction on a SQL database. Transactions can only accept
// intructions until being commited or rolled back, they become useless
// afterwards and are automatically closed.
type SQLTx interface {
	SQLCommon
	db.Tx
}

// SQLDatabase represents a Database which is capable of both creating
// transactions and use SQL builder methods.
type SQLDatabase interface {
	SQLCommon

	// NewTx returns a new session that lives within a transaction. This session
	// is completely independent from its parent.
	NewTx() (SQLTx, error)

	// Tx creates a new transaction that is passed as context to the fn function.
	// The fn function defines a transaction operation.  If the fn function
	// returns nil, the transaction is commited, otherwise the transaction is
	// rolled back.  The transaction session is closed after the function exists,
	// regardless of the error value returned by fn.
	Tx(fn func(sess SQLTx) error) error
}

type SQLAdapterFuncMap struct {
	New   func(sqlDB *sql.DB) (SQLDatabase, error)
	NewTx func(sqlTx *sql.Tx) (SQLTx, error)
	Open  func(settings db.ConnectionURL) (SQLDatabase, error)
}

// RegisterSQLAdapter registers a SQL database adapter. This function must be
// called from adapter packages upon initialization. RegisterSQLAdapter calls
// RegisterAdapter automatically.
func RegisterSQLAdapter(name string, adapter *SQLAdapterFuncMap) {
	sqlAdaptersMu.Lock()
	defer sqlAdaptersMu.Unlock()

	if name == "" {
		panic(`Missing adapter name`)
	}
	if _, ok := sqlAdapters[name]; ok {
		panic(`db.RegisterSQLAdapter() called twice for adapter: ` + name)
	}
	sqlAdapters[name] = adapter

	db.RegisterAdapter(name, &db.AdapterFuncMap{
		Open: func(settings db.ConnectionURL) (db.Database, error) {
			return adapter.Open(settings)
		},
	})
}

// SQLAdapter returns SQL database functions.
func SQLAdapter(name string) SQLAdapterFuncMap {
	sqlAdaptersMu.RLock()
	defer sqlAdaptersMu.RUnlock()

	if fn, ok := sqlAdapters[name]; ok {
		return *fn
	}
	return missingSQLAdapter(name)
}

func SQLOpen(adapter string, settings db.ConnectionURL) (SQLDatabase, error) {
	return SQLAdapter(adapter).Open(settings)
}

func SQLNew(adapter string, sqlDB *sql.DB) (SQLDatabase, error) {
	return SQLAdapter(adapter).New(sqlDB)
}

func SQLNewTx(adapter string, sqlTx *sql.Tx) (SQLTx, error) {
	return SQLAdapter(adapter).NewTx(sqlTx)
}

func missingSQLAdapter(name string) SQLAdapterFuncMap {
	err := fmt.Errorf("upper: Missing SQL adapter %q, forgot to import?", name)
	return SQLAdapterFuncMap{
		New: func(*sql.DB) (SQLDatabase, error) {
			return nil, err
		},
		NewTx: func(*sql.Tx) (SQLTx, error) {
			return nil, err
		},
		Open: func(db.ConnectionURL) (SQLDatabase, error) {
			return nil, err
		},
	}
}
