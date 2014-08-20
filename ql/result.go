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

package ql

import (
	"database/sql"
	"fmt"
	"strings"

	"upper.io/db"
	"upper.io/db/util/sqlgen"
	"upper.io/db/util/sqlutil"
)

type counter_t struct {
	Total uint64 `db:"total"`
}

type result struct {
	table     *table
	cursor    *sql.Rows // This is the main query cursor. It starts as a nil value.
	limit     sqlgen.Limit
	offset    sqlgen.Offset
	columns   sqlgen.Columns
	where     sqlgen.Where
	orderBy   sqlgen.OrderBy
	groupBy   sqlgen.GroupBy
	arguments []interface{}
}

// Executes a SELECT statement that can feed Next(), All() or One().
func (self *result) setCursor() error {
	var err error
	// We need a cursor, if the cursor does not exists yet then we create one.
	if self.cursor == nil {
		self.cursor, err = self.table.source.doQuery(sqlgen.Statement{
			Type:    sqlgen.SqlSelect,
			Table:   sqlgen.Table{self.table.Name()},
			Columns: self.columns,
			Limit:   self.limit,
			Offset:  self.offset,
			Where:   self.where,
			OrderBy: self.orderBy,
			GroupBy: self.groupBy,
		}, self.arguments...)
	}
	return err
}

// Sets conditions for reducing the working set.
func (self *result) Where(terms ...interface{}) db.Result {
	self.where, self.arguments = whereValues(terms)
	return self
}

// Determines the maximum limit of results to be returned.
func (self *result) Limit(n uint) db.Result {
	self.limit = sqlgen.Limit(n)
	return self
}

// Determines how many documents will be skipped before starting to grab
// results.
func (self *result) Skip(n uint) db.Result {
	self.offset = sqlgen.Offset(n)
	return self
}

// Used to group results that have the same value in the same column or
// columns.
func (self *result) Group(fields ...interface{}) db.Result {

	groupByColumns := make(sqlgen.GroupBy, 0, len(fields))

	l := len(fields)
	for i := 0; i < l; i++ {
		switch value := fields[i].(type) {
		// Maybe other types?
		default:
			groupByColumns = append(groupByColumns, sqlgen.Column{value})
		}
	}

	self.groupBy = groupByColumns

	return self
}

// Determines sorting of results according to the provided names. Fields may be
// prefixed by - (minus) which means descending order, ascending order would be
// used otherwise.
func (self *result) Sort(fields ...interface{}) db.Result {

	sortColumns := make(sqlgen.SortColumns, 0, len(fields))

	l := len(fields)
	for i := 0; i < l; i++ {
		var sort sqlgen.SortColumn

		switch value := fields[i].(type) {
		case db.Raw:
			sort = sqlgen.SortColumn{
				sqlgen.Column{sqlgen.Raw{fmt.Sprintf(`%v`, value.Value)}},
				sqlgen.SqlSortAsc,
			}
		case string:
			if strings.HasPrefix(value, `-`) {
				// Explicit descending order.
				sort = sqlgen.SortColumn{
					sqlgen.Column{value[1:]},
					sqlgen.SqlSortDesc,
				}
			} else {
				// Ascending order.
				sort = sqlgen.SortColumn{
					sqlgen.Column{value},
					sqlgen.SqlSortAsc,
				}
			}
		}
		sortColumns = append(sortColumns, sort)
	}

	self.orderBy.SortColumns = sortColumns

	return self
}

// Retrieves only the given fields.
func (self *result) Select(fields ...interface{}) db.Result {
	self.columns = make(sqlgen.Columns, 0, len(fields))

	l := len(fields)
	for i := 0; i < l; i++ {
		switch value := fields[i].(type) {
		case db.Raw:
			self.columns = append(self.columns, sqlgen.Column{sqlgen.Raw{fmt.Sprintf(`%v`, value.Value)}})
		default:
			self.columns = append(self.columns, sqlgen.Column{value})
		}
	}

	return self
}

// Dumps all results into a pointer to an slice of structs or maps.
func (self *result) All(dst interface{}) error {
	var err error

	if self.cursor != nil {
		return db.ErrQueryIsPending
	}

	// Current cursor.
	err = self.setCursor()

	if err != nil {
		return err
	}

	defer self.Close()

	// Fetching all results within the cursor.
	err = self.table.fetchRows(self.cursor, dst)

	return err
}

// Fetches only one result from the resultset.
func (self *result) One(dst interface{}) error {
	var err error

	if self.cursor != nil {
		return db.ErrQueryIsPending
	}

	defer self.Close()

	err = self.Next(dst)

	return err
}

// Fetches the next result from the resultset.
func (self *result) Next(dst interface{}) error {

	var err error

	// Current cursor.
	err = self.setCursor()

	if err != nil {
		self.Close()
	}

	// Fetching the next result from the cursor.
	err = self.table.fetchRow(self.cursor, dst)

	if err != nil {
		self.Close()
	}

	return err
}

// Removes the matching items from the collection.
func (self *result) Remove() error {
	var err error
	_, err = self.table.source.doExec(sqlgen.Statement{
		Type:  sqlgen.SqlDelete,
		Table: sqlgen.Table{self.table.Name()},
		Where: self.where,
	}, self.arguments...)
	return err

}

// Updates matching items from the collection with values of the given map or
// struct.
func (self *result) Update(values interface{}) error {

	ff, vv, err := self.table.FieldValues(values, toInternal)

	total := len(ff)

	cvs := make(sqlgen.ColumnValues, 0, total)

	for i := 0; i < total; i++ {
		cvs = append(cvs, sqlgen.ColumnValue{sqlgen.Column{ff[i]}, "=", sqlPlaceholder})
	}

	vv = append(vv, self.arguments...)

	_, err = self.table.source.doExec(sqlgen.Statement{
		Type:         sqlgen.SqlUpdate,
		Table:        sqlgen.Table{self.table.Name()},
		ColumnValues: cvs,
		Where:        self.where,
	}, vv...)

	return err
}

// Closes the result set.
func (self *result) Close() error {
	var err error
	if self.cursor != nil {
		err = self.cursor.Close()
		self.cursor = nil
	}
	return err
}

// Counts matching elements.
func (self *result) Count() (uint64, error) {
	var count counter_t

	rows, err := self.table.source.doQuery(sqlgen.Statement{
		Type:  sqlgen.SqlSelectCount,
		Table: sqlgen.Table{self.table.Name()},
		Where: self.where,
	}, self.arguments...)

	if err != nil {
		return 0, err
	}

	defer rows.Close()
	if err = sqlutil.FetchRow(rows, &count); err != nil {
		return 0, err
	}

	return count.Total, nil
}
