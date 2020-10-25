// Copyright (c) 2012-today The upper.io/db authors. All rights reserved.
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

package mockdb

import (
	"context"
	"database/sql"
	"strings"
	"sync"

	"github.com/DATA-DOG/go-sqlmock"
	db "github.com/upper/db/v4"
	"github.com/upper/db/v4/internal/sqladapter"
	"github.com/upper/db/v4/internal/sqlbuilder"
)

// Adapter is the internal name of the adapter.
const Adapter = "mockdb"

var registeredAdapter = sqladapter.RegisterAdapter(Adapter, &database{})

// Open establishes a connection to the database server and returns a
// sqlbuilder.Session instance (which is compatible with db.Session).
func Open(connURL db.ConnectionURL) (db.Session, error) {
	return registeredAdapter.OpenDSN(connURL)
}

// NewTx creates a sqlbuilder.Tx instance by wrapping a *sql.Tx value.
func NewTx(sqlTx *sql.Tx) (sqlbuilder.Tx, error) {
	return registeredAdapter.NewTx(sqlTx)
}

// New creates a sqlbuilder.Sesion instance by wrapping a *sql.DB value.
func New(sqlDB *sql.DB) (db.Session, error) {
	return registeredAdapter.New(sqlDB)
}

type MockDB struct {
	sess    sqladapter.Session
	ctx     context.Context
	db      *sql.DB
	connURL ConnectionURL
	mock    sqlmock.Sqlmock

	collections sync.Map
}

func Mock(sess db.Session) *MockDB {
	s, ok := loadSession(sess.(sqladapter.Session))
	if !ok {
		panic("adapter not registered")
	}
	return s
}

func (m *MockDB) getCollection(name string) (*MockCollection, bool) {
	c, ok := m.collections.Load(strings.ToLower(name))
	if !ok {
		return nil, false
	}
	return c.(*MockCollection), true
}

func (m *MockDB) Collection(name string) *MockCollection {
	mockCollection, ok := m.getCollection(name)
	if !ok {
		mockCollection = &MockCollection{
			db:          m,
			primaryKeys: []string{},
		}
		m.collections.Store(strings.ToLower(name), mockCollection)
	}
	return mockCollection
}

func (m *MockDB) Ping(err error) *MockDB {
	if err == nil {
		m.mock.ExpectPing()
	} else {
		m.mock.ExpectPing().
			WillReturnError(err)
	}
	return m
}

func (m *MockDB) Reset() error {
	sqlDB, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	if err != nil {
		return err
	}
	m.db = sqlDB
	m.mock = mock
	m.collections = sync.Map{}

	mock.ExpectPing()
	if err := m.sess.BindDB(m.db); err != nil {
		return err
	}
	return nil
}

type MockCollection struct {
	db *MockDB

	insert func(interface{}) (interface{}, error)
	findFn func(conds ...interface{}) (result []interface{}, err error)

	primaryKeys []string
}

func (c *MockCollection) PrimaryKeys(primaryKeys []string) *MockCollection {
	c.primaryKeys = primaryKeys
	return c
}

func (c *MockCollection) Insert(fn func(interface{}) (interface{}, error)) *MockCollection {
	c.db.mock.ExpectBegin()
	c.insert = fn
	return c
}

func (c *MockCollection) Get(findFn func(cond ...interface{}) ([]interface{}, error)) *MockCollection {
	c.findFn = findFn
	return c
}
