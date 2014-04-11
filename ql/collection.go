/*
  Copyright (c) 2014 José Carlos Nieto, https://menteslibres.net/xiam

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

package ql

import (
	"fmt"
	//"menteslibres.net/gosexy/to"
	"strings"
	//"time"
	"upper.io/db"
	"upper.io/db/util/sqlutil"
)

// Represents a QL table.
type Table struct {
	source *Source
	sqlutil.T
}

func mirrorFn(a interface{}) interface{} {
	return a
}

func (self *Table) Find(terms ...interface{}) db.Result {

	queryChunks := sqlutil.NewQueryChunks()

	// No specific fields given.
	if len(queryChunks.Fields) == 0 {
		queryChunks.Fields = []string{`*`}
	}

	// Compiling conditions
	queryChunks.Conditions, queryChunks.Arguments = self.compileConditions(terms)

	if queryChunks.Conditions == "" {
		queryChunks.Conditions = `1 == 1`
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

func (self *Table) compileStatement(where db.Cond) (string, []interface{}) {

	str := make([]string, len(where))
	arg := make([]interface{}, len(where))

	i := 0

	for key, _ := range where {
		key = strings.Trim(key, ` `)
		chunks := strings.SplitN(key, ` `, 2)

		op := `==`

		if len(chunks) > 1 {
			op = chunks[1]
		}

		str[i] = fmt.Sprintf(`%s %s ?`, chunks[0], op)
		arg[i] = where[key]

		i++
	}

	switch len(str) {
	case 1:
		return str[0], arg
	case 0:
		return "", nil
	}

	return `(` + strings.Join(str, ` AND `) + `)`, arg
}

// Deletes all the rows within the collection.
func (self *Table) Truncate() (err error) {

	_, err = self.source.doExec(
		fmt.Sprintf(`TRUNCATE TABLE %s`, self.Name()),
	)

	return err
}

// Appends an item (map or struct) into the collection.
func (self *Table) Append(item interface{}) (interface{}, error) {

	fields, values, err := self.FieldValues(item, mirrorFn)

	// Error ocurred, stop appending.
	if err != nil {
		return nil, err
	}

	res, err := self.source.doExec(
		fmt.Sprintf(`INSERT INTO %s`, self.Name()),
		sqlFields(fields),
		`VALUES`,
		sqlValues(values),
	)

	if err != nil {
		return nil, err
	}

	var id int64

	id, err = res.LastInsertId()

	if err != nil {
		return nil, err
	}

	return id, nil
}

// Returns true if the collection exists.
func (self *Table) Exists() bool {
	rows, err := self.source.doQuery(
		`SELECT Name
			FROM __Table
		WHERE Name == ?
		`,
		[]interface{}{self.Name()},
	)

	if err != nil {
		return false
	}

	defer rows.Close()

	return rows.Next()
}

/*
func toInternalInterface(val interface{}) interface{} {
	return toInternal(val)
}

// Converts a Go value into internal database representation.
func toInternal(val interface{}) string {

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
*/
