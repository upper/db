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
	"fmt"
	"reflect"
	"strings"
	"time"

	"database/sql"

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
				args[i] = toInternal(v.Index(i).Interface())
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

func (c *table) Find(terms ...interface{}) db.Result {
	where, arguments := whereValues(terms)

	result := &result{
		table:     c,
		where:     where,
		arguments: arguments,
	}

	return result
}

func (c *table) tableN(i int) string {
	if len(c.names) > i {
		chunks := strings.SplitN(c.names[i], " ", 2)
		if len(chunks) > 0 {
			return chunks[0]
		}
	}
	return ""
}

// Deletes all the rows within the collection.
func (c *table) Truncate() error {

	_, err := c.source.doExec(sqlgen.Statement{
		Type:  sqlgen.SqlTruncate,
		Table: sqlgen.Table{c.tableN(0)},
	})

	if err != nil {
		return err
	}

	return nil
}

// Appends an item (map or struct) into the collection.
func (c *table) Append(item interface{}) (interface{}, error) {

	var pKey []string
	var columns sqlgen.Columns
	var values sqlgen.Values
	var arguments []interface{}

	cols, vals, err := c.FieldValues(item, toInternal)

	// Error ocurred, stop appending.
	if err != nil {
		return nil, err
	}

	columns = make(sqlgen.Columns, 0, len(cols))
	for i := range cols {
		columns = append(columns, sqlgen.Column{cols[i]})
	}

	arguments = make([]interface{}, 0, len(vals))
	values = make(sqlgen.Values, 0, len(vals))
	for i := range vals {
		switch v := vals[i].(type) {
		case sqlgen.Value:
			// Adding value.
			values = append(values, v)
		default:
			// Adding both value and placeholder.
			values = append(values, sqlPlaceholder)
			arguments = append(arguments, v)
		}
	}

	if pKey, err = c.source.getPrimaryKey(c.tableN(0)); err != nil {
		if err != sql.ErrNoRows {
			// Can't tell primary key.
			return nil, err
		}
	}

	stmt := sqlgen.Statement{
		Type:    sqlgen.SqlInsert,
		Table:   sqlgen.Table{c.tableN(0)},
		Columns: columns,
		Values:  values,
	}

	var res sql.Result
	if res, err = c.source.doExec(stmt, arguments...); err != nil {
		return nil, err
	}

	// We have a single key.
	if len(pKey) <= 1 {
		// Attempt to use LastInsertId() to get our ID.
		id, _ := res.LastInsertId()
		if id > 0 {
			if setter, ok := item.(db.Int64IDSetter); ok {
				if err := setter.SetID(id); err != nil {
					return nil, err
				}
			}
			if setter, ok := item.(db.Uint64IDSetter); ok {
				if err := setter.SetID(uint64(id)); err != nil {
					return nil, err
				}
			}
		}
		return id, nil
	}

	// There is no "RETURNING" in SQLite, so we have to return the values that
	// were given for constructing the composite key.
	keyMap := make(map[string]interface{})

	for i := range cols {
		for j := 0; j < len(pKey); j++ {
			if pKey[j] == cols[i] {
				keyMap[pKey[j]] = vals[i]
			}
		}
	}

	// Does the item satisfy the db.IDSetter interface?
	if setter, ok := item.(db.IDSetter); ok {
		if err := setter.SetID(keyMap); err != nil {
			return nil, err
		}
		return nil, nil
	}

	// Backwards compatibility (int64).
	if len(keyMap) == 1 {
		if numericID, ok := keyMap[pKey[0]].(int64); ok {
			return numericID, nil
		}
	}

	return keyMap, nil
}

// Returns true if the collection exists.
func (c *table) Exists() bool {
	if err := c.source.tableExists(c.names...); err != nil {
		return false
	}
	return true
}

func (c *table) Name() string {
	return strings.Join(c.names, `, `)
}

// Converts a Go value into internal database representation.
func toInternal(val interface{}) interface{} {
	switch t := val.(type) {
	case db.Marshaler:
		return t
	case []byte:
		return string(t)
	case *time.Time:
		if t == nil || t.IsZero() {
			return sqlgen.Value{sqlgen.Raw{sqlNull}}
		}
		return t.Format(DateFormat)
	case time.Time:
		if t.IsZero() {
			return sqlgen.Value{sqlgen.Raw{sqlNull}}
		}
		return t.Format(DateFormat)
	case time.Duration:
		return fmt.Sprintf(TimeFormat, int(t/time.Hour), int(t/time.Minute%60), int(t/time.Second%60), t%time.Second/time.Millisecond)
	case sql.NullBool:
		if t.Valid {
			if t.Bool {
				return toInternal(t.Bool)
			}
			return false
		}
		return sqlgen.Value{sqlgen.Raw{sqlNull}}
	case sql.NullFloat64:
		if t.Valid {
			if t.Float64 != 0.0 {
				return toInternal(t.Float64)
			}
			return float64(0)
		}
		return sqlgen.Value{sqlgen.Raw{sqlNull}}
	case sql.NullInt64:
		if t.Valid {
			if t.Int64 != 0 {
				return toInternal(t.Int64)
			}
			return 0
		}
		return sqlgen.Value{sqlgen.Raw{sqlNull}}
	case sql.NullString:
		if t.Valid {
			return toInternal(t.String)
		}
		return sqlgen.Value{sqlgen.Raw{sqlNull}}
	case bool:
		if t == true {
			return `1`
		}
		return `0`
	}

	return to.String(val)
}
