// Copyright (c) 2012-2015 The upper.io/db authors. All rights reserved.
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

package mongo // import "upper.io/db.v2/mongo"

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"gopkg.in/mgo.v2"
	"upper.io/builder"
	"upper.io/db.v2"
)

// Adapter holds the name of the mongodb adapter.
const Adapter = `mongo`

var connTimeout = time.Second * 5

// Source represents a MongoDB database.
type Source struct {
	name          string
	connURL       db.ConnectionURL
	session       *mgo.Session
	database      *mgo.Database
	version       []int
	collections   map[string]*Collection
	collectionsMu sync.Mutex
}

func init() {
	db.Register(Adapter, &Source{})
}

// Name returns the name of the database.
func (s *Source) Name() string {
	return s.name
}

// Setup stores database settings and opens a connection to a database.
func (s *Source) Setup(connURL db.ConnectionURL) error {
	s.connURL = connURL
	return s.Open()
}

// Clone returns a cloned db.Database session.
func (s *Source) Clone() (db.Database, error) {
	clone := &Source{
		name:        s.name,
		connURL:     s.connURL,
		session:     s.session.Copy(),
		database:    s.database,
		version:     s.version,
		collections: map[string]*Collection{},
	}
	return clone, nil
}

// Transaction should support transactions, but it doesn't as MongoDB
// currently does not support them.
func (s *Source) Transaction() (db.Tx, error) {
	return nil, db.ErrUnsupported
}

// Ping checks whether a connection to the database is still alive by pinging
// it, establishing a connection if necessary.
func (s *Source) Ping() error {
	return s.session.Ping()
}

// Driver returns the underlying *mgo.Session instance.
func (s *Source) Driver() interface{} {
	return s.session
}

// Open attempts to connect to the database server using already stored
// settings.
func (s *Source) Open() error {
	var err error

	// Before db.ConnectionURL we used a unified db.Settings struct. This
	// condition checks for that type and provides backwards compatibility.
	if settings, ok := s.connURL.(db.Settings); ok {
		var addr string

		if settings.Host != "" {
			if settings.Port > 0 {
				addr = fmt.Sprintf("%s:%d", settings.Host, settings.Port)
			} else {
				addr = settings.Host
			}
		} else {
			addr = settings.Socket
		}

		conn := ConnectionURL{
			User:     settings.User,
			Password: settings.Password,
			Address:  db.ParseAddress(addr),
			Database: settings.Database,
		}

		// Replace original s.connURL
		s.connURL = conn
	}

	if s.session, err = mgo.DialWithTimeout(s.connURL.String(), connTimeout); err != nil {
		return err
	}

	s.collections = map[string]*Collection{}
	s.database = s.session.DB("")

	return nil
}

// Close terminates the current database session.
func (s *Source) Close() error {
	if s.session != nil {
		s.session.Close()
	}
	return nil
}

// Use changes the active database.
func (s *Source) Use(database string) (err error) {
	var conn ConnectionURL

	if conn, err = ParseURL(s.connURL.String()); err != nil {
		return err
	}

	conn.Database = database

	s.connURL = conn

	return s.Open()
}

// Drop drops the current database.
func (s *Source) Drop() error {
	err := s.database.DropDatabase()
	return err
}

// Collections returns a list of non-system tables from the database.
func (s *Source) Collections() (cols []string, err error) {
	var rawcols []string
	var col string

	if rawcols, err = s.database.CollectionNames(); err != nil {
		return nil, err
	}

	cols = make([]string, 0, len(rawcols))

	for _, col = range rawcols {
		if !strings.HasPrefix(col, "system.") {
			cols = append(cols, col)
		}
	}

	return cols, nil
}

// C returns a collection interface.
func (s *Source) C(name string) db.Collection {
	if col, ok := s.collections[name]; ok {
		// We can safely ignore if the collection exists or not.
		return col
	}

	c, _ := s.Collection(name)
	return c
}

// Collection returns a collection by name.
func (s *Source) Collection(name string) (db.Collection, error) {
	var err error

	s.collectionsMu.Lock()
	defer s.collectionsMu.Unlock()

	var col *Collection
	var ok bool

	if col, ok = s.collections[name]; !ok {
		col = &Collection{
			parent:     s,
			collection: s.database.C(name),
		}
		s.collections[name] = col
	}

	if !col.Exists() {
		err = db.ErrCollectionDoesNotExist
	}

	return col, err
}

func (s *Source) versionAtLeast(version ...int) bool {
	// only fetch this once - it makes a db call
	if len(s.version) == 0 {
		buildInfo, err := s.database.Session.BuildInfo()
		if err != nil {
			return false
		}
		s.version = buildInfo.VersionArray
	}

	// Check major version first
	if s.version[0] > version[0] {
		return true
	}

	for i := range version {
		if i == len(s.version) {
			return false
		}
		if s.version[i] < version[i] {
			return false
		}
	}
	return true
}

func (s *Source) Builder() builder.Builder {
	return nil
}
