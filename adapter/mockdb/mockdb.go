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
	"database/sql/driver"
	"fmt"
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
	name = strings.ToLower(name)
	mockCollection, ok := m.getCollection(name)
	if !ok {
		mockCollection = &MockCollection{
			db:          m,
			name:        name,
			primaryKeys: []string{},
		}
		m.collections.Store(name, mockCollection)
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

func (m *MockDB) Tx(txFn func(m *MockDB) error) {
	m.mock.ExpectBegin()
	err := txFn(m)
	if err != nil {
		m.mock.ExpectRollback().
			WillReturnError(err)
		return
	}
	m.mock.ExpectCommit()
}

func (m *MockDB) Reset() error {
	sqlDB, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	if err != nil {
		return err
	}

	m.db = sqlDB
	m.mock = mock
	m.collections = sync.Map{}

	mock.MatchExpectationsInOrder(false)
	mock.ExpectPing()
	if err := m.sess.BindDB(m.db); err != nil {
		return err
	}
	return nil
}

type MockCollection struct {
	db   *MockDB
	name string

	insertFn func(interface{}) (int64, error)
	findFn   func(conds ...interface{}) (result []interface{}, err error)
	deleteFn func() (int64, error)

	primaryKeys []string
}

func (c *MockCollection) PrimaryKeys(primaryKeys []string) *MockCollection {
	c.primaryKeys = primaryKeys
	return c
}

func (c *MockCollection) Delete(fn func() (int64, error)) *MockCollection {
	c.deleteFn = fn
	return c
}

func (c *MockCollection) Insert(fn func(interface{}) (int64, error)) *MockCollection {
	c.insertFn = fn
	return c
}

func (c *MockCollection) Get(findFn func(cond ...interface{}) ([]interface{}, error)) *MockCollection {
	c.findFn = findFn
	return c
}

type MockResult struct {
	res *sqladapter.Result
	c   *MockCollection
}

func (r *MockResult) Limit(limit int) db.Result {
	r.res.Limit(limit)
	return r
}

func (r *MockResult) Offset(offset int) db.Result {
	r.res.Offset(offset)
	return r
}

func (r *MockResult) OrderBy(columns ...interface{}) db.Result {
	r.res.OrderBy(columns...)
	return r
}

func (r *MockResult) Select(columns ...interface{}) db.Result {
	r.res.Select(columns...)
	return r
}

func (r *MockResult) And(conds ...interface{}) db.Result {
	r.res.And(conds...)
	return r
}

func (r *MockResult) GroupBy(columns ...interface{}) db.Result {
	r.res.GroupBy(columns...)
	return r
}

func (r *MockResult) Delete() error {
	if r.c.deleteFn == nil {
		return db.ErrNoMoreRows
	}

	rowsAffected, err := r.c.deleteFn()

	expectExec := r.c.db.mock.ExpectExec(
		fmt.Sprintf(`DELETE FROM %q`, r.c.name),
	)

	if err != nil {
		expectExec.WillReturnError(err)
	}

	expectExec.WithArgs(argumentsToValues(r.res.Arguments())...)
	expectExec.WillReturnResult(sqlmock.NewResult(0, rowsAffected))

	return r.res.Delete()
}

func (r *MockResult) Update(record interface{}) error {
	return r.res.Update(record)
}

func (r *MockResult) Count() (uint64, error) {
	return r.res.Count()
}

func (r *MockResult) Exists() (bool, error) {
	return r.res.Exists()
}

func (r *MockResult) Next(item interface{}) bool {
	return r.res.Next(item)
}

func (r *MockResult) Err() error {
	return r.res.Err()
}

func (r *MockResult) One(item interface{}) error {
	if r.c.findFn == nil {
		return db.ErrNoMoreRows
	}

	items, err := r.c.findFn(nil)
	if err != nil {
		return err
	}

	columns := []string{}
	rows := []map[string]interface{}{}
	for i := range items {
		names, values, err := sqlbuilder.Map(items[i], nil)
		if err != nil {
			return err
		}
		row := map[string]interface{}{}
		for i := range names {
			if !inSlice(columns, names[i]) {
				columns = append(columns, names[i])
			}
			row[names[i]] = values[i]
		}
		rows = append(rows, row)
	}

	mockRows := sqlmock.NewRows(columns)

	for i := range rows {
		row := []driver.Value{}
		for _, column := range columns {
			row = append(row, rows[i][column])
		}
		mockRows.AddRow(row...)
	}

	r.c.db.mock.ExpectQuery(
		fmt.Sprintf("SELECT .+ FROM %q", r.c.name)).
		WithArgs(argumentsToValues(r.res.Arguments())...).
		WillReturnRows(mockRows)

	return r.res.One(item)
}

func (r *MockResult) All(items interface{}) error {
	return r.res.All(items)
}

func (r *MockResult) Paginate(pageSize uint) db.Result {
	r.res.Paginate(pageSize)
	return r
}

func (r *MockResult) Page(pageNumber uint) db.Result {
	r.res.Page(pageNumber)
	return r
}

func (r *MockResult) Cursor(column string) db.Result {
	r.res.Cursor(column)
	return r
}

func (r *MockResult) NextPage(cursor interface{}) db.Result {
	r.res.NextPage(cursor)
	return r
}

func (r *MockResult) String() string {
	return r.res.String()
}

func (r *MockResult) PrevPage(cursor interface{}) db.Result {
	r.res.PrevPage(cursor)
	return r
}

func (r *MockResult) TotalPages() (uint, error) {
	return r.res.TotalPages()
}

func (r *MockResult) TotalEntries() (uint64, error) {
	return r.res.TotalEntries()
}

func (r *MockResult) Close() error {
	return r.res.Close()
}

var _ = db.Result(&MockResult{})
