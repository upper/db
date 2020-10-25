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
	"database/sql/driver"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/upper/db/v4"
	"github.com/upper/db/v4/internal/sqladapter"
	"github.com/upper/db/v4/internal/sqlbuilder"
)

type collectionAdapter struct {
}

func (*collectionAdapter) Insert(col sqladapter.Collection, item interface{}) (interface{}, error) {
	c, ok := loadCollection(col)
	if !ok {
		return nil, db.ErrInvalidCollection
	}

	if c.insert == nil {
		c.db.mock.ExpectRollback()
		return nil, db.ErrNotImplemented
	}

	id, err := c.insert(item)
	if err != nil {
		c.db.mock.ExpectRollback()
		return nil, err
	}

	rows := sqlmock.NewRows(col.PrimaryKeys()).AddRow(id)

	c.db.mock.ExpectQuery("SELECT").
		WithArgs(1).
		WillReturnRows(rows)

	c.db.mock.ExpectCommit()

	return id, nil
}

func (*collectionAdapter) Find(col sqladapter.Collection, res *sqladapter.Result, conds []interface{}) db.Result {
	c, ok := loadCollection(col)
	if !ok {
		return sqladapter.NewErrorResult(db.ErrInvalidCollection)
	}

	if c.findFn == nil {
		return res
	}

	items, err := c.findFn(conds...)
	if err != nil {
		return sqladapter.NewErrorResult(err)
	}

	columns := []string{}
	rows := []map[string]interface{}{}
	for i := range items {
		names, values, err := sqlbuilder.Map(items[i], nil)
		if err != nil {
			return sqladapter.NewErrorResult(err)
		}
		row := map[string]interface{}{}
		for i := range names {
			if !inSlice(columns, names[i]) {
				columns = append(columns, names[i])
			}
			row[names[i]] = values[i]
		}
		rows = append(rows, row)
	}

	mockRows := sqlmock.NewRows(columns)

	for i := range rows {
		row := []driver.Value{}
		for _, column := range columns {
			row = append(row, rows[i][column])
		}
		mockRows.AddRow(row...)
	}

	args := []driver.Value{}
	for _, arg := range res.Arguments() {
		args = append(args, arg)
	}

	c.db.mock.ExpectQuery("SELECT").
		WithArgs(args...).
		WillReturnRows(mockRows)

	return res
}

func inSlice(list []string, item string) bool {
	for i := range list {
		if list[i] == item {
			return true
		}
	}
	return false
}

var _ = interface {
	sqladapter.Finder
}(&collectionAdapter{})
