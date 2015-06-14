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

package result

import (
	"fmt"
	"strings"

	"github.com/jmoiron/sqlx"
	"upper.io/db"
	"upper.io/db/util/sqlgen"
	"upper.io/db/util/sqlutil"
)

var (
	sqlPlaceholder = sqlgen.RawValue(`?`)
)

type counter struct {
	Total uint64 `db:"_t"`
}

type Result struct {
	table     DataProvider
	cursor    *sqlx.Rows // This is the main query cursor. It starts as a nil value.
	limit     sqlgen.Limit
	offset    sqlgen.Offset
	columns   sqlgen.Columns
	where     sqlgen.Where
	orderBy   sqlgen.OrderBy
	groupBy   sqlgen.GroupBy
	arguments []interface{}
	template  *sqlutil.TemplateWithUtils
}

// NewResult creates and results a new result set on the given table, this set
// is limited by the given sqlgen.Where conditions.
func NewResult(template *sqlutil.TemplateWithUtils, p DataProvider, where sqlgen.Where, arguments []interface{}) *Result {
	return &Result{
		table:     p,
		where:     where,
		arguments: arguments,
		template:  template,
	}
}

// Executes a SELECT statement that can feed Next(), All() or One().
func (r *Result) setCursor() error {
	var err error
	// We need a cursor, if the cursor does not exists yet then we create one.
	if r.cursor == nil {
		r.cursor, err = r.table.Query(&sqlgen.Statement{
			Type:    sqlgen.Select,
			Table:   sqlgen.TableWithName(r.table.Name()),
			Columns: &r.columns,
			Limit:   r.limit,
			Offset:  r.offset,
			Where:   &r.where,
			OrderBy: &r.orderBy,
			GroupBy: &r.groupBy,
		}, r.arguments...)
	}
	return err
}

// Sets conditions for reducing the working set.
func (r *Result) Where(terms ...interface{}) db.Result {
	r.where, r.arguments = r.template.ToWhereWithArguments(terms)
	return r
}

// Determines the maximum limit of results to be returned.
func (r *Result) Limit(n uint) db.Result {
	r.limit = sqlgen.Limit(n)
	return r
}

// Determines how many documents will be skipped before starting to grab
// results.
func (r *Result) Skip(n uint) db.Result {
	r.offset = sqlgen.Offset(n)
	return r
}

// Used to group results that have the same value in the same column or
// columns.
func (r *Result) Group(fields ...interface{}) db.Result {
	var columns []sqlgen.Fragment

	for i := range fields {
		switch v := fields[i].(type) {
		case string:
			columns = append(columns, sqlgen.ColumnWithName(v))
		case sqlgen.Fragment:
			columns = append(columns, v)
		}
	}

	r.groupBy = *sqlgen.GroupByColumns(columns...)

	return r
}

// Determines sorting of results according to the provided names. Fields may be
// prefixed by - (minus) which means descending order, ascending order would be
// used otherwise.
func (r *Result) Sort(fields ...interface{}) db.Result {

	var sortColumns sqlgen.SortColumns

	for i := range fields {
		var sort *sqlgen.SortColumn

		switch value := fields[i].(type) {
		case db.Raw:
			sort = &sqlgen.SortColumn{
				Column: sqlgen.RawValue(fmt.Sprintf(`%v`, value.Value)),
				Order:  sqlgen.Ascendent,
			}
		case string:
			if strings.HasPrefix(value, `-`) {
				// Explicit descending order.
				sort = &sqlgen.SortColumn{
					Column: sqlgen.ColumnWithName(value[1:]),
					Order:  sqlgen.Descendent,
				}
			} else {
				// Ascending order.
				sort = &sqlgen.SortColumn{
					Column: sqlgen.ColumnWithName(value),
					Order:  sqlgen.Ascendent,
				}
			}
		}
		sortColumns.Columns = append(sortColumns.Columns, sort)
	}

	r.orderBy.SortColumns = &sortColumns

	return r
}

// Retrieves only the given fields.
func (r *Result) Select(fields ...interface{}) db.Result {

	r.columns = sqlgen.Columns{}

	for i := range fields {
		var col sqlgen.Fragment
		switch value := fields[i].(type) {
		case db.Func:
			v := r.template.ToInterfaceArguments(value.Args)
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
			col = sqlgen.RawValue(s)
		case db.Raw:
			col = sqlgen.RawValue(fmt.Sprintf(`%v`, value.Value))
		default:
			col = sqlgen.ColumnWithName(fmt.Sprintf(`%v`, value))
		}
		r.columns.Columns = append(r.columns.Columns, col)
	}

	return r
}

// Dumps all results into a pointer to an slice of structs or maps.
func (r *Result) All(dst interface{}) error {
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
func (r *Result) One(dst interface{}) error {
	var err error

	if r.cursor != nil {
		return db.ErrQueryIsPending
	}

	defer r.Close()

	err = r.Next(dst)

	return err
}

// Fetches the next result from the resultset.
func (r *Result) Next(dst interface{}) (err error) {

	if err = r.setCursor(); err != nil {
		r.Close()
		return err
	}

	if err = sqlutil.FetchRow(r.cursor, dst); err != nil {
		r.Close()
		return err
	}

	return nil
}

// Removes the matching items from the collection.
func (r *Result) Remove() error {
	var err error

	_, err = r.table.Exec(&sqlgen.Statement{
		Type:  sqlgen.Delete,
		Table: sqlgen.TableWithName(r.table.Name()),
		Where: &r.where,
	}, r.arguments...)

	return err

}

// Updates matching items from the collection with values of the given map or
// struct.
func (r *Result) Update(values interface{}) error {

	ff, vv, err := r.table.FieldValues(values)
	if err != nil {
		return err
	}

	cvs := new(sqlgen.ColumnValues)

	for i := range ff {
		cvs.ColumnValues = append(cvs.ColumnValues, &sqlgen.ColumnValue{Column: sqlgen.ColumnWithName(ff[i]), Operator: r.template.AssignmentOperator, Value: sqlPlaceholder})
	}

	vv = append(vv, r.arguments...)

	_, err = r.table.Exec(&sqlgen.Statement{
		Type:         sqlgen.Update,
		Table:        sqlgen.TableWithName(r.table.Name()),
		ColumnValues: cvs,
		Where:        &r.where,
	}, vv...)

	return err
}

// Closes the result set.
func (r *Result) Close() (err error) {
	if r.cursor != nil {
		err = r.cursor.Close()
		r.cursor = nil
	}
	return err
}

// Counts the elements within the main conditions of the set.
func (r *Result) Count() (uint64, error) {
	var count counter

	row, err := r.table.QueryRow(&sqlgen.Statement{
		Type:  sqlgen.Count,
		Table: sqlgen.TableWithName(r.table.Name()),
		Where: &r.where,
	}, r.arguments...)

	if err != nil {
		return 0, err
	}

	err = row.Scan(&count.Total)
	if err != nil {
		return 0, err
	}

	return count.Total, nil
}
