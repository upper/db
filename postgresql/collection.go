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
	"reflect"
	"strings"
	"time"

	"menteslibres.net/gosexy/to"
	"upper.io/db"
	"upper.io/db/util/sqlutil"
)

type table struct {
	source *source
	sqlutil.T
}

func (tbl *table) Find(terms ...interface{}) db.Result {
	queryChunks := sqlutil.NewQueryChunks()

	// No specific fields given.
	if len(queryChunks.Fields) == 0 {
		queryChunks.Fields = []string{`*`}
	}

	// Compiling conditions
	queryChunks.Conditions, queryChunks.Arguments = tbl.compileConditions(terms)

	if queryChunks.Conditions == "" {
		queryChunks.Conditions = `1 = 1`
	}

	// Creating a result handler.
	res := &result{
		tbl,
		queryChunks,
		nil,
	}

	return res
}

func (tbl *table) compileConditions(term interface{}) (string, []interface{}) {
	sql := []string{}
	args := []interface{}{}

	switch t := term.(type) {
	case []interface{}:
		for i := range t {
			rsql, rargs := tbl.compileConditions(t[i])
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
			rsql, rargs := tbl.compileConditions(t[i])
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
			rsql, rargs := tbl.compileConditions(t[i])
			if rsql != "" {
				sql = append(sql, rsql)
				args = append(args, rargs...)
			}
		}
		if len(sql) > 0 {
			return `(` + strings.Join(sql, ` AND `) + `)`, args
		}
	case db.Cond:
		return tbl.compileStatement(t)
	}

	return "", args
}

func (tbl *table) compileStatement(cond db.Cond) (string, []interface{}) {
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
			valueI := interfaceArgs(value.Args)
			if valueI == nil {
				str = append(str, fmt.Sprintf(`%s %s ()`, chunks[0], value.Name))
			} else {
				str = append(str, fmt.Sprintf(`%s %s (?%s)`, chunks[0], value.Name, strings.Repeat(`,?`, len(valueI)-1)))
				arg = append(arg, valueI...)
			}
		default:
			valueI := interfaceArgs(value)
			if valueI == nil {
				str = append(str, fmt.Sprintf(`%s %s ()`, chunks[0], op))
			} else {
				str = append(str, fmt.Sprintf(`%s %s (?%s)`, chunks[0], op, strings.Repeat(`,?`, len(valueI)-1)))
				arg = append(arg, valueI...)
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

func (tbl *table) Truncate() error {
	_, err := tbl.source.doExec(
		fmt.Sprintf(`TRUNCATE TABLE "%s"`, tbl.Name()),
	)

	return err
}

func (tbl *table) Append(item interface{}) (interface{}, error) {
	fields, values, err := tbl.FieldValues(item, toInternal)

	// Error ocurred, stop appending.
	if err != nil {
		return nil, err
	}

	tail := ""

	if _, ok := tbl.ColumnTypes[tbl.PrimaryKey]; ok == true {
		tail = fmt.Sprintf(`RETURNING %s`, tbl.PrimaryKey)
	}

	row, err := tbl.source.doQueryRow(
		fmt.Sprintf(`INSERT INTO "%s"`, tbl.Name()),
		sqlFields(fields),
		`VALUES`,
		sqlValues(values),
		tail,
	)

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

func (tbl *table) Exists() bool {
	rows, err := tbl.source.doQuery(
		fmt.Sprintf(`
				SELECT table_name
					FROM information_schema.tables
				WHERE table_catalog = '%s' AND table_name = '%s'
		`,
			tbl.source.Name(),
			tbl.Name(),
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
		}
		return `0`
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

	valueV := reflect.ValueOf(value)

	switch valueV.Type().Kind() {
	case reflect.Slice:
		var i, total int

		total = valueV.Len()
		if total > 0 {
			args = make([]interface{}, total)

			for i = 0; i < total; i++ {
				args[i] = toInternal(valueV.Index(i).Interface())
			}

			return args
		}
		return nil
	default:
		args = []interface{}{toInternal(value)}
	}

	return args
}
