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
	"database/sql"
	"fmt"
	"sync"

	"upper.io/db.v2"
)

var (
	adapters   map[string]*AdapterFuncMap
	adaptersMu sync.RWMutex
)

func init() {
	adapters = make(map[string]*AdapterFuncMap)
}

// Backend holds common methods for SQL databases.
type Backend interface {
	db.Database
	Builder
}

// Tx represents transaction on a SQL database. Transactions can only accept
// intructions until being commited or rolled back, they become useless
// afterwards and are automatically closed.
type Tx interface {
	Backend
	db.Tx
}

// Database represents a Database which is capable of both creating
// transactions and use SQL builder methods.
type Database interface {
	Backend

	// NewTx returns a new session that lives within a transaction. This session
	// is completely independent from its parent.
	NewTx() (Tx, error)

	// Tx creates a new transaction that is passed as context to the fn function.
	// The fn function defines a transaction operation.  If the fn function
	// returns nil, the transaction is commited, otherwise the transaction is
	// rolled back.  The transaction session is closed after the function exists,
	// regardless of the error value returned by fn.
	Tx(fn func(sess Tx) error) error
}

type AdapterFuncMap struct {
	New   func(sqlDB *sql.DB) (Database, error)
	NewTx func(sqlTx *sql.Tx) (Tx, error)
	Open  func(settings db.ConnectionURL) (Database, error)
}

// RegisterAdapter registers a SQL database adapter. This function must be
// called from adapter packages upon initialization. RegisterAdapter calls
// RegisterAdapter automatically.
func RegisterAdapter(name string, adapter *AdapterFuncMap) {
	adaptersMu.Lock()
	defer adaptersMu.Unlock()

	if name == "" {
		panic(`Missing adapter name`)
	}
	if _, ok := adapters[name]; ok {
		panic(`db.RegisterAdapter() called twice for adapter: ` + name)
	}
	adapters[name] = adapter

	db.RegisterAdapter(name, &db.AdapterFuncMap{
		Open: func(settings db.ConnectionURL) (db.Database, error) {
			return adapter.Open(settings)
		},
	})
}

// adapter returns SQL database functions.
func adapter(name string) AdapterFuncMap {
	adaptersMu.RLock()
	defer adaptersMu.RUnlock()

	if fn, ok := adapters[name]; ok {
		return *fn
	}
	return missingAdapter(name)
}

func Open(adapterName string, settings db.ConnectionURL) (Database, error) {
	return adapter(adapterName).Open(settings)
}

func New(adapterName string, sqlDB *sql.DB) (Database, error) {
	return adapter(adapterName).New(sqlDB)
}

func NewTx(adapterName string, sqlTx *sql.Tx) (Tx, error) {
	return adapter(adapterName).NewTx(sqlTx)
}

func missingAdapter(name string) AdapterFuncMap {
	err := fmt.Errorf("upper: Missing SQL adapter %q, forgot to import?", name)
	return AdapterFuncMap{
		New: func(*sql.DB) (Database, error) {
			return nil, err
		},
		NewTx: func(*sql.Tx) (Tx, error) {
			return nil, err
		},
		Open: func(db.ConnectionURL) (Database, error) {
			return nil, err
		},
	}
}
