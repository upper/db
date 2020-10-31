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
	"fmt"
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

	if c.insertFn == nil {
		return nil, db.ErrNotImplemented
	}

	lastInsertID, err := c.insertFn(item)
	if err != nil {
		return nil, err
	}

	columnNames, columnValues, err := sqlbuilder.Map(item, nil)
	if err != nil {
		return nil, err
	}

	// Define query
	q := col.SQL().InsertInto(col.Name()).
		Columns(columnNames...).
		Values(columnValues...)

	// Set expectations
	expectedExec := c.db.mock.ExpectExec(
		fmt.Sprintf("INSERT INTO %q", col.Name()),
	).WithArgs(argumentsToValues(q.Arguments())...)

	result := sqlmock.NewResult(lastInsertID, 1)
	expectedExec.WillReturnResult(result)

	res, err := q.Exec()
	if err != nil {
		return nil, err
	}

	pKey := col.PrimaryKeys()
	lastID, err := res.LastInsertId()
	if err == nil && len(pKey) <= 1 {
		return lastID, nil
	}

	keyMap := db.Cond{}
	for i := range columnNames {
		for j := 0; j < len(pKey); j++ {
			if pKey[j] == columnNames[i] {
				keyMap[pKey[j]] = columnValues[i]
			}
		}
	}

	// There was an auto column among primary keys, let's search for it.
	if lastID > 0 {
		for j := 0; j < len(pKey); j++ {
			if keyMap[pKey[j]] == nil {
				keyMap[pKey[j]] = lastID
			}
		}
	}

	return keyMap, nil
}

func (*collectionAdapter) Find(col sqladapter.Collection, res *sqladapter.Result, conds []interface{}) db.Result {
	c, ok := loadCollection(col)
	if !ok {
		return sqladapter.NewErrorResult(db.ErrInvalidCollection)
	}

	if c.findFn == nil {
		return sqladapter.NewErrorResult(db.ErrNoMoreRows)
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

	c.db.mock.ExpectQuery(
		fmt.Sprintf("SELECT .+ FROM %q", col.Name())).
		WithArgs(argumentsToValues(res.Arguments())...).
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

func argumentsToValues(args []interface{}) []driver.Value {
	values := []driver.Value{}
	for i := range args {
		values = append(values, args[i])
	}
	return values
}

var _ = interface {
	sqladapter.Finder
}(&collectionAdapter{})
