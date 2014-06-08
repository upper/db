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
	"menteslibres.net/gosexy/to"
	"reflect"
	"strings"
	"time"
	"upper.io/db"
	"upper.io/db/util/sqlgen"
	"upper.io/db/util/sqlutil"
)

// Represents a PostgreSQL table.
type Table struct {
	*sqlutil.T
	source *Source
	names  []string
}

const defaultOperator = `=`

func conditionValue(cond db.Cond) (columnValues sqlgen.ColumnValues, args []interface{}) {

	l := len(cond)

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
			if value_i == nil {
				// A function with no arguments.
				columnValue.Value = sqlgen.Value{sqlgen.Raw{fmt.Sprintf(`%s()`, value.Name)}}
			} else {
				// A function with one or more arguments.
				columnValue.Value = sqlgen.Value{sqlgen.Raw{fmt.Sprintf(`%s(?%s)`, value.Name, strings.Repeat(`?`, len(value_i)-1))}}
			}
		default:
			// Catches everything else.
			value_i := interfaceArgs(value)
			if value_i == nil {
				// Nil value given.
				columnValue.Value = sqlgen.Value{sqlgen.Raw{`NULL`}}
			} else {
				// Another kind of value given.
				columnValue.Value = sqlgen.Value{sqlgen.Raw{fmt.Sprintf(`(?%s)`, strings.Repeat(`?`, len(value_i)-1))}}
				args = append(args, value_i...)
			}
		}

		columnValues = append(columnValues, columnValue)
	}

	return columnValues, args
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
			and = append(and, k)
		}
		where = append(where, and)
	case db.Or:
		or := make(sqlgen.Or, 0, len(t))
		for _, cond := range t {
			k, v := whereValues(cond)
			args = append(args, v...)
			or = append(or, k)
		}
		where = append(where, or)
	case db.Cond:
		k, v := conditionValue(t)
		args = append(args, v...)
		where = append(where, k)
	}

	return where, args
}

func (self *Table) Find(terms ...interface{}) db.Result {
	var arguments []interface{}

	stmt := sqlgen.Statement{}

	stmt.Where, arguments = whereValues(terms)

	result := &Result{
		table:     self,
		cursor:    nil,
		stmt:      stmt,
		arguments: arguments,
	}

	return result
}

func (self *Table) tableN(i int) string {
	if len(self.names) > i {
		chunks := strings.SplitN(self.names[i], " ", 2)
		if len(chunks) > 0 {
			return chunks[0]
		}
	}
	return ""
}

// Deletes all the rows within the collection.
func (self *Table) Truncate() error {

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
func (self *Table) Append(item interface{}) (interface{}, error) {

	cols, vals, err := self.FieldValues(item, toInternal)

	var columns sqlgen.Columns
	var values sqlgen.Values

	for _, col := range cols {
		columns = append(columns, sqlgen.Column{col})
	}

	for _, val := range vals {
		values = append(values, sqlgen.Value{val})
	}

	// Error ocurred, stop appending.
	if err != nil {
		return nil, err
	}

	var extra string

	if _, ok := self.ColumnTypes[self.PrimaryKey]; ok == true {
		extra = fmt.Sprintf(`RETURNING %s`, self.PrimaryKey)
	}

	row, err := self.source.doQueryRow(sqlgen.Statement{
		Type:    sqlgen.SqlInsert,
		Table:   sqlgen.Table{self.tableN(0)},
		Columns: columns,
		Values:  values,
		Extra:   sqlgen.Extra(extra),
	})

	/*
		row, err := self.source.doQueryRow(
			fmt.Sprintf(`INSERT INTO %s`, self.tableN(0)),
			sqlFields(fields),
			`VALUES`,
			sqlValues(values),
			tail,
		)
	*/

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
func (self *Table) Exists() bool {
	if err := self.source.tableExists(self.names...); err != nil {
		return false
	}
	return true
}

func (self *Table) Name() string {
	return strings.Join(self.names, `, `)
}

func toInternalInterface(val interface{}) interface{} {
	return toInternal(val)
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
		} else {
			return `0`
		}
	}
	return to.String(val)
}

// Convers a database representation (after auto-conversion) into a Go value.
func toNative(val interface{}) interface{} {
	return val
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
		} else {
			return nil
		}
	default:
		args = []interface{}{toInternal(value)}
	}

	return args
}
