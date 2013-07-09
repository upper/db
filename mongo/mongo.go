/*
  Copyright (c) 2012-2013 José Carlos Nieto, http://xiam.menteslibres.org/

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
	"menteslibres.net/gosexy/db"
	"net/url"
	"time"
)

var Debug = false

// Registers this driver.
func init() {
	db.Register("mongo", &Source{})
}

// Mongodb datasource.
type Source struct {
	name     string
	config   db.DataSource
	session  *mgo.Session
	database *mgo.Database
}

// Returns database name.
func (self *Source) Name() string {
	return self.name
}

// Returns a datasource session that is not yet connected to the database.
func Session(config db.DataSource) db.Database {
	self := &Source{}
	self.config = config
	return self
}

// Opens a connection.
func (self *Source) Setup(config db.DataSource) error {
	self.config = config
	return self.Open()
}

// Sets the active database.
func (self *Source) Use(database string) error {
	self.config.Database = database
	self.name = database
	self.database = self.session.DB(self.config.Database)
	return nil
}

/*
	Starts a transaction block.
*/
func (self *Source) Begin() error {
	// TODO:
	// MongoDB does not supports something like BEGIN and END statements.
	return nil
}

/*
	Ends a transaction block.
*/
func (self *Source) End() error {
	// TODO:
	// MongoDB does not supports something like BEGIN and END statements.
	return nil
}

// Returns a collection from the current database.
func (self *Source) Collection(name string) (db.Collection, error) {
	var err error

	col := &SourceCollection{}
	col.parent = self
	col.collection = self.database.C(name)

	col.DB = self
	col.SetName = name

	if col.Exists() == false {
		err = db.ErrCollectionDoesNotExists
	}

	return col, err
}

// Returns a collection from the current database. Panics if the collection does not exists.
func (self *Source) ExistentCollection(name string) db.Collection {
	col, err := self.Collection(name)
	if err != nil {
		panic(err.Error())
	}
	return col
}

// Returns the underlying driver (*mgo.Session).
func (self *Source) Driver() interface{} {
	return self.session
}

// Opens a connection to the datasource. See Session().
func (self *Source) Open() error {
	var err error

	connURL := &url.URL{Scheme: "mongodb"}

	if self.config.Port == 0 {
		self.config.Port = 27017
	}

	if self.config.Host == "" {
		self.config.Host = "127.0.0.1"
	}

	connURL.Host = fmt.Sprintf("%s:%d", self.config.Host, self.config.Port)

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

// Drops the active database and all its collections.
func (self *Source) Drop() error {
	err := self.database.DropDatabase()
	return err
}

// Closes the connection to the database.
func (self *Source) Close() error {
	if self.session != nil {
		self.session.Close()
	}
	return nil
}

// Returns the names of all collection in the current database.
func (self *Source) Collections() []string {
	cols := []string{}
	rawcols, _ := self.database.CollectionNames()
	for _, col := range rawcols {
		if col != "system.indexes" {
			cols = append(cols, col)
		}
	}
	return cols
}
