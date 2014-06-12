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

package postgresql

import (
	"database/sql"
	"strings"
	"upper.io/db"
	"upper.io/db/util/sqlgen"
)

type counter_t struct {
	Total uint64 `db:"_t"`
}

type Result struct {
	table     *Table
	cursor    *sql.Rows // This is the main query cursor. It starts as a nil value.
	limit     sqlgen.Limit
	offset    sqlgen.Offset
	columns   sqlgen.Columns
	where     sqlgen.Where
	orderBy   sqlgen.OrderBy
	arguments []interface{}
}

// Executes a SELECT statement that can feed Next(), All() or One().
func (self *Result) setCursor() error {
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
		}, self.arguments...)
	}
	return err
}

// Determines the maximum limit of results to be returned.
func (self *Result) Limit(n uint) db.Result {
	self.limit = sqlgen.Limit(n)
	return self
}

// Determines how many documents will be skipped before starting to grab
// results.
func (self *Result) Skip(n uint) db.Result {
	self.offset = sqlgen.Offset(n)
	return self
}

// Determines sorting of results according to the provided names. Fields may be
// prefixed by - (minus) which means descending order, ascending order would be
// used otherwise.
func (self *Result) Sort(fields ...string) db.Result {

	sortColumns := make(sqlgen.SortColumns, 0, len(fields))

	for _, field := range fields {
		var sort sqlgen.SortColumn

		if strings.HasPrefix(field, `-`) {
			// Explicit descending order.
			sort = sqlgen.SortColumn{
				sqlgen.Column{field[1:]},
				sqlgen.SqlSortDesc,
			}
		} else {
			// Ascending order.
			sort = sqlgen.SortColumn{
				sqlgen.Column{field},
				sqlgen.SqlSortAsc,
			}
		}

		sortColumns = append(sortColumns, sort)
	}

	self.orderBy.SortColumns = sortColumns

	return self
}

// Retrieves only the given fields.
func (self *Result) Select(fields ...string) db.Result {
	self.columns = make(sqlgen.Columns, 0, len(fields))

	l := len(fields)
	for i := 0; i < l; i++ {
		self.columns = append(self.columns, sqlgen.Column{fields[i]})
	}

	return self
}

// Dumps all results into a pointer to an slice of structs or maps.
func (self *Result) All(dst interface{}) error {
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
	err = self.table.T.FetchRows(dst, self.cursor)

	return err
}

// Fetches only one result from the resultset.
func (self *Result) One(dst interface{}) error {
	var err error

	if self.cursor != nil {
		return db.ErrQueryIsPending
	}

	defer self.Close()

	err = self.Next(dst)

	return err
}

// Fetches the next result from the resultset.
func (self *Result) Next(dst interface{}) error {

	var err error

	// Current cursor.
	err = self.setCursor()

	if err != nil {
		self.Close()
	}

	// Fetching the next result from the cursor.
	err = self.table.T.FetchRow(dst, self.cursor)

	if err != nil {
		self.Close()
	}

	return err
}

// Removes the matching items from the collection.
func (self *Result) Remove() error {
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
func (self *Result) Update(values interface{}) error {

	ff, vv, err := self.table.FieldValues(values, toInternal)

	total := len(ff)

	cvs := make(sqlgen.ColumnValues, 0, total)

	for i := 0; i < total; i++ {
		cvs = append(cvs, sqlgen.ColumnValue{sqlgen.Column{ff[i]}, "=", sqlPlaceholder})
	}

	_, err = self.table.source.doExec(sqlgen.Statement{
		Type:         sqlgen.SqlUpdate,
		Table:        sqlgen.Table{self.table.Name()},
		ColumnValues: cvs,
	}, vv...)

	return err
}

// Closes the result set.
func (self *Result) Close() error {
	var err error
	if self.cursor != nil {
		err = self.cursor.Close()
		self.cursor = nil
	}
	return err
}

// Counting the elements that will be returned.
func (self *Result) Count() (uint64, error) {

	rows, err := self.table.source.doQuery(sqlgen.Statement{
		Type:   sqlgen.SqlSelectCount,
		Table:  sqlgen.Table{self.table.Name()},
		Where:  self.where,
		Limit:  self.limit,
		Offset: self.offset,
	}, self.arguments...)

	if err != nil {
		return 0, err
	}

	defer rows.Close()

	dst := counter_t{}
	self.table.T.FetchRow(&dst, rows)

	return dst.Total, nil
}
