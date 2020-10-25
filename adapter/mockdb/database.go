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
	"database/sql"
	"errors"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/upper/db/v4"
	"github.com/upper/db/v4/internal/sqladapter"
	"github.com/upper/db/v4/internal/sqladapter/exql"
	"sync"
)

var errMissingSession = errors.New("no such session")

var (
	sessionsMu sync.RWMutex
	sessions   = map[string]*MockDB{}
)

type database struct {
}

func (*database) Collections(sess sqladapter.Session) (collections []string, err error) {
	sessionsMu.RLock()
	defer sessionsMu.RUnlock()

	s, ok := loadSession(sess)
	if !ok {
		return nil, errMissingSession
	}

	names := []string{}
	s.collections.Range(func(key, value interface{}) bool {
		names = append(names, key.(string))
		return true
	})
	return names, nil
}

func (*database) NewCollection() sqladapter.CollectionAdapter {
	return &collectionAdapter{}
}

func (d *database) LookupName(sess sqladapter.Session) (string, error) {
	sessionsMu.RLock()
	defer sessionsMu.RUnlock()

	s, ok := loadSession(sess)
	if !ok {
		return "", errMissingSession
	}

	return s.connURL.Database, nil
}

func (d *database) OpenDSN(sess sqladapter.Session, dsn string) (*sql.DB, error) {
	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		return nil, err
	}

	connURL, err := ParseURL(dsn)
	if err != nil {
		return nil, err
	}

	sessionsMu.Lock()
	storeSession(sess, &MockDB{
		db:          sqlDB,
		mock:        mock,
		connURL:     connURL,
		collections: sync.Map{},
	})
	sessionsMu.Unlock()

	return sqlDB, nil
}

func (*database) PrimaryKeys(sess sqladapter.Session, tableName string) ([]string, error) {
	sessionsMu.RLock()
	defer sessionsMu.RUnlock()

	s, ok := loadSession(sess)
	if !ok {
		return nil, db.ErrNotConnected
	}

	c, ok := s.getCollection(tableName)
	if !ok {
		return nil, db.ErrCollectionDoesNotExist
	}

	return c.primaryKeys, nil
}

func (*database) TableExists(sess sqladapter.Session, name string) error {
	sessionsMu.RLock()
	defer sessionsMu.RUnlock()

	s, ok := loadSession(sess)
	if !ok {
		return db.ErrNotConnected
	}

	_, ok = s.getCollection(name)
	if !ok {
		return db.ErrCollectionDoesNotExist
	}

	return nil
}

func (*database) Template() *exql.Template {
	return template
}

func loadSession(sess sqladapter.Session) (*MockDB, bool) {
	s, ok := sessions[sess.ConnectionURL().String()]
	return s, ok
}

func storeSession(sess sqladapter.Session, mockDB *MockDB) {
	sessions[sess.ConnectionURL().String()] = mockDB
}

func loadCollection(col sqladapter.Collection) (*MockCollection, bool) {
	return Mock(col.Session()).getCollection(col.Name())
}
