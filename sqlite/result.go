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

func (self *Result) One(dst interface{}) error {
	var err error
	defer self.Close()
	err = self.Next(dst)
	if err != nil {
		return err
	}
	return nil
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

	if err != nil {
		self.Close()
	}

	return err
}

func (self *Result) Remove() error {
	var err error
	_, err = self.t.source.doExec(
		fmt.Sprintf(
			`DELETE FROM '%s' WHERE %s`,
			self.t.Name(),
			self.queryChunks.Conditions,
		),
		self.queryChunks.Arguments,
	)
	return err

}

func (self *Result) Update(values interface{}) error {

	ff, vv, err := self.t.FieldValues(values, toInternal)

	if err != nil {
		return err
	}

	total := len(ff)

	updateFields := make([]string, total)
	updateArgs := make([]string, total)

	for i := 0; i < total; i++ {
		updateFields[i] = fmt.Sprintf(`%s = ?`, ff[i])
		updateArgs[i] = vv[i]
	}

	_, err = self.t.source.doExec(
		fmt.Sprintf(
			`UPDATE '%s' SET %s WHERE %s`,
			self.t.Name(),
			strings.Join(updateFields, `, `),
			self.queryChunks.Conditions,
		),
		updateArgs,
		self.queryChunks.Arguments,
	)

	return err
}

func (self *Result) Close() error {
	var err error
	if self.cursor != nil {
		err = self.cursor.Close()
		self.cursor = nil
	}
	return err
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

	rows.Close()

	return dst.Total, nil
}
