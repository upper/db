/*
  Copyright (c) 2013 José Carlos Nieto, http://xiam.menteslibres.org/

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
	"labix.org/v2/mgo"
	"menteslibres.net/gosexy/db"
	"menteslibres.net/gosexy/db/util"
)

type Result struct {
	query      *mgo.Query
	collection *util.C
	iter       *mgo.Iter
	relations  []db.Relation
}

func (self *Result) All(dst interface{}) error {
	var err error

	err = self.query.All(dst)

	if err != nil {
		return err
	}

	// Fetching relations
	err = self.collection.FetchRelations(dst, self.relations, toInternal)

	if err != nil {
		return err
	}

	dst = toNative(dst)

	return nil
}

func (self *Result) Next(dst interface{}) error {

	if self.iter.Next(dst) == false {
		return db.ErrNoMoreRows
	}

	if self.iter.Err() != nil {
		return self.iter.Err()
	}

	self.collection.FetchRelation(dst, self.relations, toInternal)

	dst = toNative(dst)

	return nil
}

func (self *Result) One(dst interface{}) error {
	var err error

	err = self.query.One(dst)
	if err != nil {
		return err
	}

	err = self.collection.FetchRelation(dst, self.relations, toInternal)

	if err != nil {
		return err
	}

	dst = toNative(dst)

	return err
}

func (self *Result) Close() error {
	return nil
}
