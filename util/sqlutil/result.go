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

package sqlutil

import (
	"database/sql"
	"menteslibres.net/gosexy/db"
)

type Result struct {
	Rows      *sql.Rows
	Table     *T
	Relations []db.Relation
}

func (self *Result) FetchAll(dst interface{}, convertFn func(interface{}) interface{}) error {
	var err error
	defer self.Close()

	err = self.Table.FetchRows(dst, self.Rows)

	if err != nil {
		return err
	}

	err = self.Table.FetchRelations(dst, self.Relations, convertFn)

	if err != nil {
		return err
	}

	return nil
}

func (self *Result) FetchNext(dst interface{}, convertFn func(interface{}) interface{}) error {
	var err error

	err = self.Table.FetchRow(dst, self.Rows)

	if err != nil {
		return err
	}

	err = self.Table.FetchRelation(dst, self.Relations, convertFn)

	if err != nil {
		return err
	}

	return nil
}

func (self *Result) FetchOne(dst interface{}, convertFn func(interface{}) interface{}) error {
	defer self.Close()

	err := self.FetchNext(dst, convertFn)

	if err != nil {
		return err
	}

	return nil
}

func (self *Result) Close() error {
	err := self.Rows.Close()
	return err
}
