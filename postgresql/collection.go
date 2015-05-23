// Copyright (c) 2012-2015 José Carlos Nieto, https://menteslibres.net/xiam
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

	"github.com/jmoiron/sqlx"
	"upper.io/db"
	"upper.io/db/util/sqlgen"
	"upper.io/db/util/sqlutil"
)

const defaultOperator = `=`

type table struct {
	sqlutil.T
	source     *source
	primaryKey string
	names      []string
}

func whereValues(term interface{}) (where sqlgen.Where, args []interface{}) {
	args = []interface{}{}

	switch t := term.(type) {
	case []interface{}:
		for i := range t {
			w, v := whereValues(t[i])
			args = append(args, v...)
			where.Conditions = append(where.Conditions, w.Conditions...)
		}
		return
	case db.And:
		var op sqlgen.And
		for i := range t {
			k, v := whereValues(t[i])
			args = append(args, v...)
			op.Conditions = append(op.Conditions, k.Conditions...)
		}
		where.Conditions = append(where.Conditions, &op)
		return
	case db.Or:
		var op sqlgen.Or
		for i := range t {
			w, v := whereValues(t[i])
			args = append(args, v...)
			op.Conditions = append(op.Conditions, w.Conditions...)
		}
		where.Conditions = append(where.Conditions, &op)
		return
	case db.Raw:
		if s, ok := t.Value.(string); ok {
			where.Conditions = append(where.Conditions, sqlgen.RawValue(s))
		}
		return
	case db.Cond:
		cv, v := columnValues(t)
		args = append(args, v...)
		for i := range cv.ColumnValues {
			where.Conditions = append(where.Conditions, cv.ColumnValues[i])
		}
		return
	case db.Constrainer:
		cv, v := columnValues(t.Constraint())
		args = append(args, v...)
		for i := range cv.ColumnValues {
			where.Conditions = append(where.Conditions, cv.ColumnValues[i])
		}
		return
	}

	panic(fmt.Sprintf(db.ErrUnknownConditionType.Error(), term))
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

func columnValues(cond db.Cond) (columnValues sqlgen.ColumnValues, args []interface{}) {
	args = []interface{}{}

	for column, value := range cond {
		columnValue := sqlgen.ColumnValue{}

		// Guessing operator from input, or using a default one.
		column := strings.TrimSpace(column)
		chunks := strings.SplitN(column, ` `, 2)

		columnValue.Column = sqlgen.ColumnWithName(chunks[0])

		if len(chunks) > 1 {
			columnValue.Operator = chunks[1]
		} else {
			columnValue.Operator = defaultOperator
		}

		switch value := value.(type) {
		case db.Func:
			v := interfaceArgs(value.Args)
			columnValue.Operator = value.Name

			if v == nil {
				// A function with no arguments.
				columnValue.Value = sqlgen.RawValue(`()`)
			} else {
				// A function with one or more arguments.
				columnValue.Value = sqlgen.RawValue(fmt.Sprintf(`(?%s)`, strings.Repeat(`, ?`, len(v)-1)))
			}

			args = append(args, v...)
		default:
			v := interfaceArgs(value)

			l := len(v)
			if v == nil || l == 0 {
				// Nil value given.
				columnValue.Value = sqlgen.RawValue(psqlNull)
			} else {
				if l > 1 {
					// Array value given.
					columnValue.Value = sqlgen.RawValue(fmt.Sprintf(`(?%s)`, strings.Repeat(`, ?`, len(v)-1)))
				} else {
					// Single value given.
					columnValue.Value = sqlPlaceholder
				}
				args = append(args, v...)
			}
		}

		columnValues.ColumnValues = append(columnValues.ColumnValues, &columnValue)
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
		Type:  sqlgen.Truncate,
		Table: sqlgen.TableWithName(t.tableN(0)),
	})

	if err != nil {
		return err
	}

	return nil
}

// Appends an item (map or struct) into the collection.
func (t *table) Append(item interface{}) (interface{}, error) {

	cols, vals, err := t.FieldValues(item)

	if err != nil {
		return nil, err
	}

	columns := new(sqlgen.Columns)

	columns.Columns = make([]sqlgen.Fragment, 0, len(cols))
	for i := range cols {
		columns.Columns = append(columns.Columns, sqlgen.ColumnWithName(cols[i]))
	}

	values := new(sqlgen.Values)
	var arguments []interface{}

	arguments = make([]interface{}, 0, len(vals))
	values.Values = make([]sqlgen.Fragment, 0, len(vals))

	for i := range vals {
		switch v := vals[i].(type) {
		case *sqlgen.Value:
			// Adding value.
			values.Values = append(values.Values, v)
		case sqlgen.Value:
			// Adding value.
			values.Values = append(values.Values, &v)
		default:
			// Adding both value and placeholder.
			values.Values = append(values.Values, sqlPlaceholder)
			arguments = append(arguments, v)
		}
	}

	var pKey []string

	if pKey, err = t.source.getPrimaryKey(t.tableN(0)); err != nil {
		if err != sql.ErrNoRows {
			// Can't tell primary key.
			return nil, err
		}
	}

	stmt := sqlgen.Statement{
		Type:    sqlgen.Insert,
		Table:   sqlgen.TableWithName(t.tableN(0)),
		Columns: columns,
		Values:  values,
	}

	// No primary keys defined.
	if len(pKey) == 0 {
		var res sql.Result

		if res, err = t.source.doExec(stmt, arguments...); err != nil {
			return nil, err
		}

		// Attempt to use LastInsertId() (probably won't work, but the exec()
		// succeeded, so the error from LastInsertId() is ignored).
		lastID, _ := res.LastInsertId()

		return lastID, nil
	}

	var rows *sqlx.Rows

	// A primary key was found.
	stmt.Extra = sqlgen.Extra(fmt.Sprintf(`RETURNING "%s"`, strings.Join(pKey, `", "`)))
	if rows, err = t.source.doQuery(stmt, arguments...); err != nil {
		return nil, err
	}

	defer rows.Close()

	keyMap := map[string]interface{}{}
	if err := sqlutil.FetchRow(rows, &keyMap); err != nil {
		return nil, err
	}

	// Does the item satisfy the db.IDSetter interface?
	if setter, ok := item.(db.IDSetter); ok {
		if err := setter.SetID(keyMap); err != nil {
			return nil, err
		}
		return nil, nil
	}

	// The IDSetter interface does not match, look for another interface match.
	if len(keyMap) == 1 {
		id := keyMap[pKey[0]]

		// Matches db.Int64IDSetter
		if setter, ok := item.(db.Int64IDSetter); ok {
			if err = setter.SetID(id.(int64)); err != nil {
				return nil, err
			}
			return nil, nil
		}

		// Matches db.Uint64IDSetter
		if setter, ok := item.(db.Uint64IDSetter); ok {
			if err = setter.SetID(uint64(id.(int64))); err != nil {
				return nil, err
			}
			return nil, nil
		}

		// No interface matched, falling back to old behaviour.
		return id.(int64), nil
	}

	// More than one key, no interface matched, let's return a map.
	return keyMap, nil
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
