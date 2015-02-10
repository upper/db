// Copyright (c) 2012-2014 José Carlos Nieto, https://menteslibres.net/xiam
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

package mongo

import (
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"gopkg.in/mgo.v2"
	"upper.io/db"
)

const Adapter = `mongo`

var connTimeout = time.Second * 5

type Source struct {
	name     string
	connURL  db.ConnectionURL
	session  *mgo.Session
	database *mgo.Database
	version  []int
}

func debugEnabled() bool {
	if os.Getenv(db.EnvEnableDebug) != "" {
		return true
	}
	return false
}

func init() {
	db.Register(Adapter, &Source{})
}

func debugLogQuery(c *chunks) {
	log.Printf("Fields: %v\nLimit: %v\nOffset: %v\nSort: %v\nConditions: %v\n", c.Fields, c.Limit, c.Offset, c.Sort, c.Conditions)
}

// Returns the string name of the database.
func (s *Source) Name() string {
	return s.name
}

// Stores database settings.
func (s *Source) Setup(connURL db.ConnectionURL) error {
	s.connURL = connURL
	return s.Open()
}

func (s *Source) Clone() (db.Database, error) {
	clone := &Source{
		name:     s.name,
		connURL:  s.connURL,
		session:  s.session.Copy(),
		database: s.database,
		version:  s.version,
	}
	return clone, nil
}

func (s *Source) Transaction() (db.Tx, error) {
	return nil, db.ErrUnsupported
}

func (s *Source) Ping() error {
	return s.session.Ping()
}

// Returns the underlying *mgo.Session instance.
func (s *Source) Driver() interface{} {
	return s.session
}

// Attempts to connect to a database using the stored settings.
func (s *Source) Open() error {
	var err error

	// Before db.ConnectionURL we used a unified db.Settings struct. This
	// condition checks for that type and provides backwards compatibility.
	if settings, ok := s.connURL.(db.Settings); ok {
		var sAddr string

		if settings.Host != "" {
			if settings.Port > 0 {
				sAddr = fmt.Sprintf("%s:%d", settings.Host, settings.Port)
			} else {
				sAddr = settings.Host
			}
		} else {
			sAddr = settings.Socket
		}

		conn := ConnectionURL{
			User:     settings.User,
			Password: settings.Password,
			Address:  db.ParseAddress(sAddr),
			Database: settings.Database,
		}

		// Replace original s.connURL
		s.connURL = conn
	}

	if s.session, err = mgo.DialWithTimeout(s.connURL.String(), connTimeout); err != nil {
		return err
	}

	s.database = s.session.DB("")

	return nil
}

// Closes the current database session.
func (s *Source) Close() error {
	if s.session != nil {
		s.session.Close()
	}
	return nil
}

// Changes the active database.
func (s *Source) Use(database string) (err error) {
	var conn ConnectionURL

	if conn, err = ParseURL(s.connURL.String()); err != nil {
		return err
	}

	conn.Database = database

	s.connURL = conn

	return s.Open()
}

// Drops the currently active database.
func (s *Source) Drop() error {
	err := s.database.DropDatabase()
	return err
}

// Returns a slice of non-system collection names within the active
// database.
func (s *Source) Collections() (cols []string, err error) {
	var rawcols []string
	var col string

	if rawcols, err = s.database.CollectionNames(); err != nil {
		return nil, err
	}

	cols = make([]string, 0, len(rawcols))

	for _, col = range rawcols {
		if strings.HasPrefix(col, "system.") == false {
			cols = append(cols, col)
		}
	}

	return cols, nil
}

// Returns a collection instance by name.
func (s *Source) Collection(names ...string) (db.Collection, error) {
	var err error

	if len(names) > 1 {
		return nil, db.ErrUnsupported
	}

	name := names[0]

	col := &Collection{}
	col.parent = s
	col.collection = s.database.C(name)

	if col.Exists() == false {
		err = db.ErrCollectionDoesNotExist
	}

	return col, err
}

func (s *Source) VersionAtLeast(version ...int) bool {
	// only fetch this once - it makes a db call
	if len(s.version) == 0 {
		buildInfo, err := s.database.Session.BuildInfo()
		if err != nil {
			return false
		}
		s.version = buildInfo.VersionArray
	}

	for i := range version {
		if i == len(s.version) {
			return false
		}
		if s.version[i] < version[i] {
			return false
		}

		if s.version[i] > version[i] {
			return true
		}
	}
	return true
}
