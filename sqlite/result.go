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

package sqlite

import (
	"database/sql"
	"fmt"
	"strings"
	"upper.io/db/util/sqlutil"
)

type counter struct {
	Total uint64 `field:"total"`
}

type Result struct {
	t           *Table
	queryChunks *sqlutil.QueryChunks
	sqlutil.Result
	// This is the main query cursor, for Next() and One().
	cursor *sql.Rows
}

func (self *Result) All(dst interface{}) error {
	return self.FetchAll(dst, toInternalInterface)
}

func (self *Result) Next(dst interface{}) error {

	var err error

	// We need a cursor, if the cursor does not exists yet then we create one.
	if self.cursor == nil {
		self.cursor, err = self.t.source.doQuery(
			// Mandatory SQL.
			fmt.Sprintf(
				`SELECT %s FROM '%s' WHERE %s`,
				// Fields.
				strings.Join(self.queryChunks.Fields, `, `),
				// Table name
				self.t.Name(),
				// Conditions
				self.queryChunks.Conditions,
			),
			// Arguments
			self.queryChunks.Arguments,
			// Optional SQL
			self.queryChunks.Sort,
			self.queryChunks.Limit,
			self.queryChunks.Offset,
		)
		if err != nil {
			return err
		}
	}

	// Fetching the next result from the cursor.
	err = self.FetchNextRow(dst, self.cursor, toInternalInterface)

	return err
}

func (self *Result) One(dst interface{}) error {
	return self.FetchOne(dst, toInternalInterface)
}

func (self *Result) Remove() error {
	return nil
}

func (self *Result) Update(terms interface{}) error {
	return nil
}

func (self *Result) Close() error {
	if self.cursor != nil {
		return self.cursor.Close()
	}
	return nil
}

func (self *Result) Count() (uint64, error) {

	rows, err := self.t.source.doQuery(
		fmt.Sprintf(
			`SELECT COUNT(1) AS total FROM '%s' WHERE %s`,
			self.t.Name(),
			self.queryChunks.Conditions,
		),
		self.queryChunks.Arguments,
	)

	if err != nil {
		return 0, err
	}

	dst := counter{}
	self.Table.FetchRow(&dst, rows)

	return dst.Total, nil
}
