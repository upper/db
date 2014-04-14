/*
  Copyright (c) 2012-2013 José Carlos Nieto, https://menteslibres.net/xiam

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

package sqlite

import (
	"fmt"
	"menteslibres.net/gosexy/to"
	"reflect"
	"strings"
	"time"
	"upper.io/db"
	"upper.io/db/util/sqlutil"
)

// Represents a SQLite table.
type Table struct {
	source *Source
	sqlutil.T
}

// Creates a filter with the given terms.
func (self *Table) Find(terms ...interface{}) db.Result {

	queryChunks := sqlutil.NewQueryChunks()

	// No specific fields given.
	if len(queryChunks.Fields) == 0 {
		queryChunks.Fields = []string{`*`}
	}

	// Compiling conditions
	queryChunks.Conditions, queryChunks.Arguments = self.compileConditions(terms)

	if queryChunks.Conditions == "" {
		queryChunks.Conditions = `1 = 1`
	}

	// Creating a result handler.
	result := &Result{
		self,
		queryChunks,
		nil,
	}

	return result
}

// Transforms conditions into arguments for sql.Exec/sql.Query
func (self *Table) compileConditions(term interface{}) (string, []interface{}) {
	sql := []string{}
	args := []interface{}{}

	switch t := term.(type) {
	case []interface{}:
		for i := range t {
			rsql, rargs := self.compileConditions(t[i])
			if rsql != "" {
				sql = append(sql, rsql)
				args = append(args, rargs...)
			}
		}
		if len(sql) > 0 {
			return `(` + strings.Join(sql, ` AND `) + `)`, args
		}
	case db.Or:
		for i := range t {
			rsql, rargs := self.compileConditions(t[i])
			if rsql != "" {
				sql = append(sql, rsql)
				args = append(args, rargs...)
			}
		}
		if len(sql) > 0 {
			return `(` + strings.Join(sql, ` OR `) + `)`, args
		}
	case db.And:
		for i := range t {
			rsql, rargs := self.compileConditions(t[i])
			if rsql != "" {
				sql = append(sql, rsql)
				args = append(args, rargs...)
			}
		}
		if len(sql) > 0 {
			return `(` + strings.Join(sql, ` AND `) + `)`, args
		}
	case db.Cond:
		return self.compileStatement(t)
	}

	return "", args
}

func (self *Table) compileStatement(cond db.Cond) (string, []interface{}) {

	total := len(cond)

	str := make([]string, 0, total)
	arg := make([]interface{}, 0, total)

	// Walking over conditions
	for field, value := range cond {
		// Removing leading or trailing spaces.
		field = strings.TrimSpace(field)

		chunks := strings.SplitN(field, ` `, 2)

		// Default operator.
		op := `=`

		if len(chunks) > 1 {
			// User has defined a different operator.
			op = chunks[1]
		}

		switch value := value.(type) {
		case db.Func:
			value_i := interfaceArgs(value.Args)
			if value_i == nil {
				str = append(str, fmt.Sprintf(`%s %s ()`, chunks[0], value.Name))
			} else {
				str = append(str, fmt.Sprintf(`%s %s (?%s)`, chunks[0], value.Name, strings.Repeat(`,?`, len(value_i)-1)))
				arg = append(arg, value_i...)
			}
		default:
			value_i := interfaceArgs(value)
			if value_i == nil {
				str = append(str, fmt.Sprintf(`%s %s ()`, chunks[0], op))
			} else {
				str = append(str, fmt.Sprintf(`%s %s (?%s)`, chunks[0], op, strings.Repeat(`,?`, len(value_i)-1)))
				arg = append(arg, value_i...)
			}
		}
	}

	switch len(str) {
	case 1:
		return str[0], arg
	case 0:
		return "", []interface{}{}
	}

	return `(` + strings.Join(str, ` AND `) + `)`, arg
}

// Deletes all the rows within the collection.
func (self *Table) Truncate() error {

	_, err := self.source.doExec(
		fmt.Sprintf(`DELETE FROM '%s'`, self.Name()),
	)

	return err
}

// Appends an item (map or struct) into the collection.
func (self *Table) Append(item interface{}) (interface{}, error) {

	fields, values, err := self.FieldValues(item, toInternal)

	// Error ocurred, stop appending.
	if err != nil {
		return nil, err
	}

	res, err := self.source.doExec(
		fmt.Sprintf(`INSERT INTO '%s'`, self.Name()),
		sqlFields(fields),
		`VALUES`,
		sqlValues(values),
	)

	// Error ocurred, stop appending.
	if err != nil {
		return nil, err
	}

	// Last inserted ID could be zero too.
	id, _ := res.LastInsertId()

	return id, nil
}

// Returns true if the collection exists.
func (self *Table) Exists() bool {
	rows, err := self.source.doQuery(
		fmt.Sprintf(`
			SELECT name
				FROM sqlite_master
				WHERE type = 'table' AND name = '%s'
			`,
			self.Name(),
		),
	)

	if err != nil {
		return false
	}

	defer rows.Close()

	return rows.Next()
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
