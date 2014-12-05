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
	"fmt"
	"reflect"
	"strings"

	"upper.io/db"
	"upper.io/db/util/sqlgen"
	"upper.io/db/util/sqlutil"
)

const defaultOperator = `==`

type table struct {
	sqlutil.T
	columnTypes map[string]reflect.Kind
	source      *source
	names       []string
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
	case db.Constrainer:
		k, v := conditionValues(t.Constraint())
		args = append(args, v...)
		for _, kk := range k {
			where = append(where, kk)
		}
	default:
		panic(fmt.Sprintf(db.ErrUnknownConditionType.Error(), reflect.TypeOf(t)))
	}

	return where, args
}

func interfaceArgs(value interface{}) (args []interface{}) {

	if value == nil {
		return nil
	}

	v := reflect.ValueOf(value)

	switch v.Type().Kind() {
	case reflect.Slice:
		var i, total int

		total = v.Len()
		if total > 0 {
			args = make([]interface{}, total)

			for i = 0; i < total; i++ {
				args[i] = v.Index(i).Interface()
			}

			return args
		}
		return nil
	default:
		args = []interface{}{value}
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
			v := interfaceArgs(value.Args)
			columnValue.Operator = value.Name

			if v == nil {
				// A function with no arguments.
				columnValue.Value = sqlgen.Value{sqlgen.Raw{`()`}}
			} else {
				// A function with one or more arguments.
				columnValue.Value = sqlgen.Value{sqlgen.Raw{fmt.Sprintf(`(?%s)`, strings.Repeat(`, ?`, len(v)-1))}}
			}

			args = append(args, v...)
		default:
			// Catches everything else.
			v := interfaceArgs(value)
			l := len(v)
			if v == nil || l == 0 {
				// Nil value given.
				columnValue.Value = sqlgen.Value{sqlgen.Raw{`NULL`}}
			} else {
				if l > 1 {
					// Array value given.
					columnValue.Value = sqlgen.Value{sqlgen.Raw{fmt.Sprintf(`(?%s)`, strings.Repeat(`, ?`, len(v)-1))}}
				} else {
					// Single value given.
					columnValue.Value = sqlPlaceholder
				}
				args = append(args, v...)
			}
		}

		columnValues = append(columnValues, columnValue)
	}

	return columnValues, args
}

func (t *table) Find(terms ...interface{}) db.Result {
	where, arguments := whereValues(terms)

	result := &result{
		table:     t,
		where:     where,
		arguments: arguments,
	}

	return result
}

func (t *table) tableN(i int) string {
	if len(t.names) > i {
		chunks := strings.SplitN(t.names[i], " ", 2)
		if len(chunks) > 0 {
			return chunks[0]
		}
	}
	return ""
}

// Deletes all the rows within the collection.
func (t *table) Truncate() error {

	_, err := t.source.doExec(sqlgen.Statement{
		Type:  sqlgen.SqlTruncate,
		Table: sqlgen.Table{t.tableN(0)},
	})

	if err != nil {
		return err
	}

	return nil
}

// Appends an item (map or struct) into the collection.
func (t *table) Append(item interface{}) (interface{}, error) {

	cols, vals, err := t.FieldValues(item, toInternal)

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

	res, err := t.source.doExec(sqlgen.Statement{
		Type:    sqlgen.SqlInsert,
		Table:   sqlgen.Table{t.tableN(0)},
		Columns: columns,
		Values:  values,
	}, vals...)

	if err != nil {
		return nil, err
	}

	var id int64
	id, _ = res.LastInsertId()

	// Does the item satisfy the db.ID interface?
	if setter, ok := item.(db.IDSetter); ok {
		if err := setter.SetID(map[string]interface{}{"id": id}); err != nil {
			return nil, err
		}
	}

	return id, nil
}

// Returns true if the collection exists.
func (t *table) Exists() bool {
	if err := t.source.tableExists(t.names...); err != nil {
		return false
	}
	return true
}

func (t *table) Name() string {
	return strings.Join(t.names, `, `)
}

func toInternal(v interface{}) interface{} {
	return v
}
