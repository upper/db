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

package sqlite

import (
	"database/sql"
	"fmt"
	"strings"

	"upper.io/db"
	"upper.io/db/util/sqlgen"
	"upper.io/db/util/sqlutil"
)

type counter struct {
	Total uint64 `db:"_t"`
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
func (r *result) setCursor() error {
	var err error
	// We need a cursor, if the cursor does not exists yet then we create one.
	if r.cursor == nil {
		r.cursor, err = r.table.source.doQuery(sqlgen.Statement{
			Type:    sqlgen.SqlSelect,
			Table:   sqlgen.Table{r.table.Name()},
			Columns: r.columns,
			Limit:   r.limit,
			Offset:  r.offset,
			Where:   r.where,
			OrderBy: r.orderBy,
			GroupBy: r.groupBy,
		}, r.arguments...)
	}
	return err
}

// Sets conditions for reducing the working set.
func (r *result) Where(terms ...interface{}) db.Result {
	r.where, r.arguments = whereValues(terms)
	return r
}

// Determines the maximum limit of results to be returned.
func (r *result) Limit(n uint) db.Result {
	r.limit = sqlgen.Limit(n)
	return r
}

// Determines how many documents will be skipped before starting to grab
// results.
func (r *result) Skip(n uint) db.Result {
	r.offset = sqlgen.Offset(n)
	return r
}

// Used to group results that have the same value in the same column or
// columns.
func (r *result) Group(fields ...interface{}) db.Result {

	groupByColumns := make(sqlgen.GroupBy, 0, len(fields))

	l := len(fields)
	for i := 0; i < l; i++ {
		switch value := fields[i].(type) {
		// Maybe other types?
		default:
			groupByColumns = append(groupByColumns, sqlgen.Column{value})
		}
	}

	r.groupBy = groupByColumns

	return r
}

// Determines sorting of results according to the provided names. Fields may be
// prefixed by - (minus) which means descending order, ascending order would be
// used otherwise.
func (r *result) Sort(fields ...interface{}) db.Result {

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

	r.orderBy.SortColumns = sortColumns

	return r
}

// Retrieves only the given fields.
func (r *result) Select(fields ...interface{}) db.Result {

	r.columns = make(sqlgen.Columns, 0, len(fields))

	l := len(fields)
	for i := 0; i < l; i++ {
		var col sqlgen.Column
		switch value := fields[i].(type) {
		case db.Func:
			v := interfaceArgs(value.Args)
			var s string
			if len(v) == 0 {
				s = fmt.Sprintf(`%s()`, value.Name)
			} else {
				ss := make([]string, 0, len(v))
				for j := range v {
					ss = append(ss, fmt.Sprintf(`%v`, v[j]))
				}
				s = fmt.Sprintf(`%s(%s)`, value.Name, strings.Join(ss, `, `))
			}
			col = sqlgen.Column{sqlgen.Raw{s}}
		case db.Raw:
			col = sqlgen.Column{sqlgen.Raw{fmt.Sprintf(`%v`, value.Value)}}
		default:
			col = sqlgen.Column{value}
		}
		r.columns = append(r.columns, col)
	}

	return r
}

// Dumps all results into a pointer to an slice of structs or maps.
func (r *result) All(dst interface{}) error {
	var err error

	if r.cursor != nil {
		return db.ErrQueryIsPending
	}

	// Current cursor.
	err = r.setCursor()

	if err != nil {
		return err
	}

	defer r.Close()

	// Fetching all results within the cursor.
	err = sqlutil.FetchRows(r.cursor, dst)

	return err
}

// Fetches only one result from the resultset.
func (r *result) One(dst interface{}) error {
	var err error

	if r.cursor != nil {
		return db.ErrQueryIsPending
	}

	defer r.Close()

	err = r.Next(dst)

	return err
}

// Fetches the next result from the resultset.
func (r *result) Next(dst interface{}) error {

	var err error

	// Current cursor.
	err = r.setCursor()

	if err != nil {
		r.Close()
	}

	// Fetching the next result from the cursor.
	err = sqlutil.FetchRow(r.cursor, dst)

	if err != nil {
		r.Close()
	}

	return err
}

// Removes the matching items from the collection.
func (r *result) Remove() error {
	var err error
	_, err = r.table.source.doExec(sqlgen.Statement{
		Type:  sqlgen.SqlDelete,
		Table: sqlgen.Table{r.table.Name()},
		Where: r.where,
	}, r.arguments...)
	return err

}

// Updates matching items from the collection with values of the given map or
// struct.
func (r *result) Update(values interface{}) error {

	ff, vv, err := r.table.FieldValues(values, toInternal)

	total := len(ff)

	cvs := make(sqlgen.ColumnValues, 0, total)

	for i := 0; i < total; i++ {
		cvs = append(cvs, sqlgen.ColumnValue{sqlgen.Column{ff[i]}, "=", sqlPlaceholder})
	}

	vv = append(vv, r.arguments...)

	_, err = r.table.source.doExec(sqlgen.Statement{
		Type:         sqlgen.SqlUpdate,
		Table:        sqlgen.Table{r.table.Name()},
		ColumnValues: cvs,
		Where:        r.where,
	}, vv...)

	return err
}

// Closes the result set.
func (r *result) Close() error {
	var err error
	if r.cursor != nil {
		err = r.cursor.Close()
		r.cursor = nil
	}
	return err
}

// Counting the elements that will be returned.
func (r *result) Count() (uint64, error) {
	var count counter

	rows, err := r.table.source.doQuery(sqlgen.Statement{
		Type:  sqlgen.SqlSelectCount,
		Table: sqlgen.Table{r.table.Name()},
		Where: r.where,
	}, r.arguments...)

	if err != nil {
		return 0, err
	}

	defer rows.Close()
	if err = sqlutil.FetchRow(rows, &count); err != nil {
		return 0, err
	}

	return count.Total, nil
}
