/*
  Copyright (c) 2014 José Carlos Nieto, https://menteslibres.net/xiam

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
	"errors"
	"fmt"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"upper.io/db"
)

type Result struct {
	c           *Collection
	queryChunks *chunks
	iter        *mgo.Iter
}

var (
	errUnknownSortValue = errors.New(`Unknown sort value "%s".`)
)

// Creates a *mgo.Iter we can use in Next(), All() or One().
func (self *Result) setCursor() error {
	if self.iter == nil {
		q, err := self.query()
		if err != nil {
			return err
		}
		self.iter = q.Iter()
	}
	return nil
}

func (self *Result) Where(terms ...interface{}) db.Result {
	self.queryChunks.Conditions = self.c.compileQuery(terms...)
	return self
}

// Determines the maximum limit of results to be returned.
func (self *Result) Limit(n uint) db.Result {
	self.queryChunks.Limit = int(n)
	return self
}

// Determines how many documents will be skipped before starting to grab
// results.
func (self *Result) Skip(n uint) db.Result {
	self.queryChunks.Offset = int(n)
	return self
}

// Determines sorting of results according to the provided names. Fields may be
// prefixed by - (minus) which means descending order, ascending order would be
// used otherwise.
func (self *Result) Sort(fields ...interface{}) db.Result {
	ss := make([]string, len(fields))
	for i, field := range fields {
		ss[i] = fmt.Sprintf(`%v`, field)
	}
	self.queryChunks.Sort = ss
	return self
}

// Retrieves only the given fields.
func (self *Result) Select(fields ...interface{}) db.Result {
	fieldslen := len(fields)
	self.queryChunks.Fields = make([]string, 0, fieldslen)
	for i := 0; i < fieldslen; i++ {
		self.queryChunks.Fields = append(self.queryChunks.Fields, fmt.Sprintf(`%v`, fields[i]))
	}
	return self
}

// Dumps all results into a pointer to an slice of structs or maps.
func (self *Result) All(dst interface{}) error {

	var err error

	err = self.setCursor()

	if err != nil {
		return err
	}

	err = self.iter.All(dst)

	if err != nil {
		return err
	}

	self.Close()

	return nil
}

// Used to group results that have the same value in the same column or
// columns.
func (self *Result) Group(fields ...interface{}) db.Result {
	self.queryChunks.GroupBy = fields
	return self
}

// Fetches only one result from the resultset.
func (self *Result) One(dst interface{}) error {
	var err error
	err = self.Next(dst)

	if err != nil {
		return err
	}

	self.Close()

	return nil
}

// Fetches the next result from the resultset.
func (self *Result) Next(dst interface{}) error {
	err := self.setCursor()

	if err != nil {
		return err
	}

	success := self.iter.Next(dst)

	if success == false {
		err := self.iter.Err()
		if err == nil {
			return db.ErrNoMoreRows
		}
		return err
	}

	return nil
}

// Removes the matching items from the collection.
func (self *Result) Remove() error {
	var err error
	_, err = self.c.collection.RemoveAll(self.queryChunks.Conditions)
	if err != nil {
		return err
	}
	return nil
}

// Closes the result set.
func (self *Result) Close() error {
	var err error
	if self.iter != nil {
		err = self.iter.Close()
		self.iter = nil
	}
	return err
}

// Updates matching items from the collection with values of the given map or
// struct.
func (self *Result) Update(src interface{}) error {
	var err error
	_, err = self.c.collection.UpdateAll(self.queryChunks.Conditions, map[string]interface{}{"$set": src})
	if err != nil {
		return err
	}
	return nil
}

func (self *Result) query() (*mgo.Query, error) {
	var err error

	q := self.c.collection.Find(self.queryChunks.Conditions)

	if self.queryChunks.GroupBy != nil {
		return nil, db.ErrUnsupported
	}

	if self.queryChunks.Offset > 0 {
		q = q.Skip(self.queryChunks.Offset)
	}

	if self.queryChunks.Limit > 0 {
		q = q.Limit(self.queryChunks.Limit)
	}

	if self.queryChunks.Fields != nil {
		sel := bson.M{}
		for _, field := range self.queryChunks.Fields {
			if field == `*` {
				break
			}
			sel[field] = true
		}
		q = q.Select(sel)
	}

	if len(self.queryChunks.Sort) > 0 {
		q.Sort(self.queryChunks.Sort...)
	}

	return q, err
}

// Counts matching elements.
func (self *Result) Count() (uint64, error) {
	q := self.c.collection.Find(self.queryChunks.Conditions)
	total, err := q.Count()
	return uint64(total), err
}
