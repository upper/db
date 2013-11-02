/*
  Copyright (c) 2012-2013 José Carlos Nieto, https://menteslibres.net/xiam

  Permission is hereby granted, free of charge, to any person obtaining
  a copy of this software and associated documentation files (the
  "Software"), to deal in the Software without restriction, including
  without limitation the rights to use, copy, modify, merge, publish,
  distribute, sublicense, and/or sell copies of the Software, and to
  permit persons to whom the Software is furnished to do so, subject to
  the following conditions:

  The above copyright notice and this permission notice shall be
  included in all copies or substantial portions of the Software.

  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
  NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
  LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
  OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
  WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

package mongo

import (
	"fmt"
	"labix.org/v2/mgo"
	"net/url"
	"time"
	"upper.io/db"
)

var Debug = false

const driverName = `mongo`

// Registers this driver.
func init() {
	db.Register(driverName, &Source{})
}

type Source struct {
	name     string
	config   db.Settings
	session  *mgo.Session
	database *mgo.Database
}

// Returns the string name of the database.
func (self *Source) Name() string {
	return self.name
}

// Stores database settings.
func (self *Source) Setup(config db.Settings) error {
	self.config = config
	return self.Open()
}

// Returns the underlying *mgo.Session instance.
func (self *Source) Driver() interface{} {
	return self.session
}

// Attempts to connect to a database using the stored settings.
func (self *Source) Open() error {
	var err error

	connURL := &url.URL{Scheme: `mongodb`}

	if self.config.Port == 0 {
		self.config.Port = 27017
	}

	if self.config.Host == "" {
		self.config.Host = `127.0.0.1`
	}

	connURL.Host = fmt.Sprintf(`%s:%d`, self.config.Host, self.config.Port)

	if self.config.User != "" {
		connURL.User = url.UserPassword(self.config.User, self.config.Password)
	}

	if self.config.Database != "" {
		connURL.Path = "/" + self.config.Database
	}

	self.session, err = mgo.DialWithTimeout(connURL.String(), 5*time.Second)

	if err != nil {
		return err
	}

	if self.config.Database != "" {
		self.Use(self.config.Database)
	}

	return nil
}

// Closes the current database session.
func (self *Source) Close() error {
	if self.session != nil {
		self.session.Close()
	}
	return nil
}

// Changes the active database.
func (self *Source) Use(database string) error {
	self.config.Database = database
	self.name = database
	self.database = self.session.DB(self.config.Database)
	return nil
}

// Starts a transaction block.
func (self *Source) Begin() error {
	// TODO:
	// MongoDB does not supports something like BEGIN and END statements.
	return nil
}

// Ends a transaction block.
func (self *Source) End() error {
	// TODO:
	// MongoDB does not supports something like BEGIN and END statements.
	return nil
}

// Drops the currently active database.
func (self *Source) Drop() error {
	err := self.database.DropDatabase()
	return err
}

// Returns a list of all tables within the currently active database.
func (self *Source) Collections() ([]string, error) {
	cols := []string{}
	rawcols, err := self.database.CollectionNames()
	if err != nil {
		return nil, err
	}
	for _, col := range rawcols {
		if col != "system.indexes" {
			cols = append(cols, col)
		}
	}
	return cols, nil
}

// Returns a collection instance by name.
func (self *Source) Collection(name string) (db.Collection, error) {
	var err error

	col := &Collection{}
	col.parent = self
	col.collection = self.database.C(name)

	col.DB = self
	col.SetName = name

	if col.Exists() == false {
		err = db.ErrCollectionDoesNotExists
	}

	return col, err
}
