/*
  Copyright (c) 2012-2014 José Carlos Nieto, https://menteslibres.net/xiam

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

package postgresql

import (
	"database/sql"
	"fmt"
	"strings"
	"upper.io/db"
	"upper.io/db/util/sqlgen"
)

type counter struct {
	Total uint64 `field:"total"`
}

type Result struct {
	table      *Table
	cursor     *sql.Rows // This is the main query cursor. It starts as a nil value.
	stmt       sqlgen.Statement
	arguments  []interface{}
	limit      sqlgen.Limit
	offset     sqlgen.Offset
	conditions sqlgen.Where
	orderBy    sqlgen.OrderBy
}

// Executes a SELECT statement that can feed Next(), All() or One().
func (self *Result) setCursor() error {
	var err error
	// We need a cursor, if the cursor does not exists yet then we create one.
	if self.cursor == nil {
		self.cursor, err = self.table.source.doQuery(sqlgen.Statement{
			Type:   sqlgen.SqlSelect,
			Table:  sqlgen.Table{self.table.Name()},
			Limit:  self.limit,
			Offset: self.offset,
			Where:  self.conditions,
		}, self.arguments)
		/*
			self.cursor, err = self.table.source.doQuery(
				// Mandatory SQL.
				fmt.Sprintf(
					`SELECT %s FROM %s WHERE %s`,
					// Fields.
					strings.Join(self.queryChunks.Fields, `, `),
					// Table name
					self.table.Name(),
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
		*/
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

	/*
		sort := make([]string, 0, len(fields))

		for _, field := range fields {
			if strings.HasPrefix(field, `-`) == true {
				sort = append(sort, field[1:]+` DESC`)
			} else {
				sort = append(sort, field+` ASC`)
			}
		}

		self.queryChunks.Sort = `ORDER BY ` + strings.Join(sort, `, `)

		return self
	*/
}

// Retrieves only the given fields.
func (self *Result) Select(fields ...string) db.Result {
	self.stmt.Columns = make(sqlgen.Columns, 0, len(fields))

	l := len(fields)
	for i := 0; i < l; i++ {
		self.stmt.Columns = append(self.stmt.Columns, sqlgen.Column{fields[i]})
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
		Where: self.conditions,
	}, self.arguments)
	/*
		_, err = self.table.source.doExec(
			fmt.Sprintf(
				`DELETE FROM "%s" WHERE %s`,
				self.table.Name(),
				self.queryChunks.Conditions,
			),
			self.queryChunks.Arguments,
		)
	*/
	return err

}

// Updates matching items from the collection with values of the given map or
// struct.
func (self *Result) Update(values interface{}) error {

	ff, vv, err := self.table.FieldValues(values, toInternal)

	if err != nil {
		return err
	}

	total := len(ff)

	updateFields := make([]string, total)
	updateArgs := make([]interface{}, total)

	for i := 0; i < total; i++ {
		updateFields[i] = fmt.Sprintf(`%s = ?`, ff[i])
		updateArgs[i] = vv[i]
	}

	_, err = self.table.source.doExec(
		fmt.Sprintf(
			`UPDATE "%s" SET %s WHERE %s`,
			self.table.Name(),
			strings.Join(updateFields, `, `),
			self.queryChunks.Conditions,
		),
		updateArgs,
		self.queryChunks.Arguments,
	)

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

// Counts matching elements.
func (self *Result) Count() (uint64, error) {

	rows, err := self.table.source.doQuery(sqlgen.Statement{
		Type:  sqlgen.SqlSelectCount,
		Table: sqlgen.Table{self.table.Name()},
		Where: self.conditions,
	}, self.arguments)

	/*
		rows, err := self.table.source.doQuery(
			fmt.Sprintf(
				`SELECT COUNT(1) AS total FROM %s WHERE %s`,
				self.table.Name(),
				self.queryChunks.Conditions,
			),
			self.queryChunks.Arguments,
		)
	*/

	if err != nil {
		return 0, err
	}

	defer rows.Close()

	dst := counter{}
	self.table.T.FetchRow(&dst, rows)

	return dst.Total, nil
}
