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
	"fmt"
	"reflect"
	"strings"
	"time"

	"menteslibres.net/gosexy/to"
	"upper.io/db"
	"upper.io/db/util/sqlgen"
	"upper.io/db/util/sqlutil"
)

const defaultOperator = `=`

type table struct {
	sqlutil.T
	source *source
	names  []string
}

func whereValues(term interface{}) (where sqlgen.Where, args []interface{}) {

	args = []interface{}{}

	switch t := term.(type) {
	case []interface{}:
		l := len(t)
		where = make(sqlgen.Where, 0, l)
		for _, cond := range t {
			w, v := whereValues(cond)
			args = append(args, v...)
			where = append(where, w...)
		}
	case db.And:
		and := make(sqlgen.And, 0, len(t))
		for _, cond := range t {
			k, v := whereValues(cond)
			args = append(args, v...)
			and = append(and, k...)
		}
		where = append(where, and)
	case db.Or:
		or := make(sqlgen.Or, 0, len(t))
		for _, cond := range t {
			k, v := whereValues(cond)
			args = append(args, v...)
			or = append(or, k...)
		}
		where = append(where, or)
	case db.Raw:
		if s, ok := t.Value.(string); ok == true {
			where = append(where, sqlgen.Raw{s})
		}
	case db.Cond:
		k, v := conditionValues(t)
		args = append(args, v...)
		for _, kk := range k {
			where = append(where, kk)
		}
	}

	return where, args
}

func interfaceArgs(value interface{}) (args []interface{}) {

	if value == nil {
		return nil
	}

	value_v := reflect.ValueOf(value)

	switch value_v.Type().Kind() {
	case reflect.Slice:
		var i, total int

		total = value_v.Len()
		if total > 0 {
			args = make([]interface{}, total)

			for i = 0; i < total; i++ {
				args[i] = toInternal(value_v.Index(i).Interface())
			}

			return args
		}
		return nil
	default:
		args = []interface{}{toInternal(value)}
	}

	return args
}

func conditionValues(cond db.Cond) (columnValues sqlgen.ColumnValues, args []interface{}) {

	args = []interface{}{}

	for column, value := range cond {
		var columnValue sqlgen.ColumnValue

		// Guessing operator from input, or using a default one.
		column := strings.TrimSpace(column)
		chunks := strings.SplitN(column, ` `, 2)

		columnValue.Column = sqlgen.Column{chunks[0]}

		if len(chunks) > 1 {
			columnValue.Operator = chunks[1]
		} else {
			columnValue.Operator = defaultOperator
		}

		switch value := value.(type) {
		case db.Func:
			// Catches functions.
			value_i := interfaceArgs(value.Args)
			columnValue.Operator = value.Name

			if value_i == nil {
				// A function with no arguments.
				columnValue.Value = sqlgen.Value{sqlgen.Raw{`()`}}
			} else {
				// A function with one or more arguments.
				columnValue.Value = sqlgen.Value{sqlgen.Raw{fmt.Sprintf(`(?%s)`, strings.Repeat(`, ?`, len(value_i)-1))}}
			}

			args = append(args, value_i...)
		default:
			// Catches everything else.
			value_i := interfaceArgs(value)
			l := len(value_i)
			if value_i == nil || l == 0 {
				// Nil value given.
				columnValue.Value = sqlgen.Value{sqlgen.Raw{`NULL`}}
			} else {
				if l > 1 {
					// Array value given.
					columnValue.Value = sqlgen.Value{sqlgen.Raw{fmt.Sprintf(`(?%s)`, strings.Repeat(`, ?`, len(value_i)-1))}}
				} else {
					// Single value given.
					columnValue.Value = sqlPlaceholder
				}
				args = append(args, value_i...)
			}
		}

		columnValues = append(columnValues, columnValue)
	}

	return columnValues, args
}

func (self *table) Find(terms ...interface{}) db.Result {
	where, arguments := whereValues(terms)

	result := &result{
		table:     self,
		where:     where,
		arguments: arguments,
	}

	return result
}

func (self *table) tableN(i int) string {
	if len(self.names) > i {
		chunks := strings.SplitN(self.names[i], " ", 2)
		if len(chunks) > 0 {
			return chunks[0]
		}
	}
	return ""
}

// Deletes all the rows within the collection.
func (self *table) Truncate() error {

	_, err := self.source.doExec(sqlgen.Statement{
		Type:  sqlgen.SqlTruncate,
		Table: sqlgen.Table{self.tableN(0)},
	})

	if err != nil {
		return err
	}

	return nil
}

// Appends an item (map or struct) into the collection.
func (self *table) Append(item interface{}) (interface{}, error) {

	cols, vals, err := self.FieldValues(item, toInternal)

	var columns sqlgen.Columns
	var values sqlgen.Values

	for _, col := range cols {
		columns = append(columns, sqlgen.Column{col})
	}

	for i := 0; i < len(vals); i++ {
		values = append(values, sqlPlaceholder)
	}

	// Error ocurred, stop appending.
	if err != nil {
		return nil, err
	}

	var extra string

	//if _, ok := self.ColumnTypes[self.PrimaryKey]; ok == true {
	//	extra = fmt.Sprintf(`RETURNING %s`, self.PrimaryKey)
	//}

	row, err := self.source.doQueryRow(sqlgen.Statement{
		Type:    sqlgen.SqlInsert,
		Table:   sqlgen.Table{self.tableN(0)},
		Columns: columns,
		Values:  values,
		Extra:   sqlgen.Extra(extra),
	}, vals...)

	if err != nil {
		return nil, err
	}

	var id int64

	if err = row.Scan(&id); err != nil {
		if err == sql.ErrNoRows {
			// Can't tell the row's id. Maybe there isn't any?
			return nil, nil
		}
		// Other kind of error.
		return nil, err
	}

	return id, nil
}

// Returns true if the collection exists.
func (self *table) Exists() bool {
	if err := self.source.tableExists(self.names...); err != nil {
		return false
	}
	return true
}

func (self *table) Name() string {
	return strings.Join(self.names, `, `)
}

// Converts a Go value into internal database representation.
func toInternal(val interface{}) interface{} {
	switch t := val.(type) {
	case []byte:
		return string(t)
	case time.Time:
		return t.Format(DateFormat)
	case time.Duration:
		return fmt.Sprintf(TimeFormat, int(t/time.Hour), int(t/time.Minute%60), int(t/time.Second%60), t%time.Second/time.Millisecond)
	case bool:
		if t == true {
			return `1`
		}
		return `0`
	}
	return to.String(val)
}
