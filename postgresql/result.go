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
	"strings"

	"upper.io/db"
	"upper.io/db/util/sqlutil"
)

type counter struct {
	Total uint64 `field:"total"`
}

type result struct {
	table       *table
	queryChunks *sqlutil.QueryChunks
	// This is the main query cursor. It starts as a nil value.
	cursor *sql.Rows
}

// Executes a SELECT statement that can feed Next(), All() or One().
func (res *result) setCursor() error {
	var err error
	// We need a cursor, if the cursor does not exists yet then we create one.
	if res.cursor == nil {
		res.cursor, err = res.table.source.doQuery(
			// Mandatory SQL.
			fmt.Sprintf(
				`SELECT %s FROM "%s" WHERE %s`,
				// Fields.
				strings.Join(res.queryChunks.Fields, `, `),
				// Table name
				res.table.Name(),
				// Conditions
				res.queryChunks.Conditions,
			),
			// Arguments
			res.queryChunks.Arguments,
			// Optional SQL
			res.queryChunks.Sort,
			res.queryChunks.Limit,
			res.queryChunks.Offset,
		)
	}
	return err
}

func (res *result) Limit(n uint) db.Result {
	res.queryChunks.Limit = fmt.Sprintf(`LIMIT %d`, n)
	return res
}

func (res *result) Skip(n uint) db.Result {
	res.queryChunks.Offset = fmt.Sprintf(`OFFSET %d`, n)
	return res
}

func (res *result) Sort(fields ...string) db.Result {
	sort := make([]string, 0, len(fields))

	for _, field := range fields {
		if strings.HasPrefix(field, `-`) == true {
			sort = append(sort, field[1:]+` DESC`)
		} else {
			sort = append(sort, field+` ASC`)
		}
	}

	res.queryChunks.Sort = `ORDER BY ` + strings.Join(sort, `, `)

	return res
}

func (res *result) Select(fields ...string) db.Result {
	res.queryChunks.Fields = fields
	return res
}

func (res *result) All(dst interface{}) error {
	var err error

	if res.cursor != nil {
		return db.ErrQueryIsPending
	}

	// Current cursor.
	err = res.setCursor()

	if err != nil {
		return err
	}

	defer res.Close()

	// Fetching all results within the cursor.
	err = res.table.T.FetchRows(dst, res.cursor)

	return err
}

func (res *result) One(dst interface{}) error {
	var err error

	if res.cursor != nil {
		return db.ErrQueryIsPending
	}

	defer res.Close()

	err = res.Next(dst)

	return err
}

func (res *result) Next(dst interface{}) error {
	err := res.setCursor()
	if err != nil {
		res.Close()
		return err
	}

	err = res.table.T.FetchRow(dst, res.cursor)
	if err != nil {
		res.Close()
		return err
	}

	return nil
}

func (res *result) Remove() error {
	var err error
	_, err = res.table.source.doExec(
		fmt.Sprintf(
			`DELETE FROM "%s" WHERE %s`,
			res.table.Name(),
			res.queryChunks.Conditions,
		),
		res.queryChunks.Arguments,
	)
	return err

}

func (res *result) Update(values interface{}) error {

	ff, vv, err := res.table.FieldValues(values, toInternal)

	if err != nil {
		return err
	}

	total := len(ff)

	updateFields := make([]string, total)
	updateArgs := make([]interface{}, total)

	for i := 0; i < total; i++ {
		updateFields[i] = fmt.Sprintf(`%s = ?`, ff[i])
		updateArgs[i] = vv[i]
	}

	_, err = res.table.source.doExec(
		fmt.Sprintf(
			`UPDATE "%s" SET %s WHERE %s`,
			res.table.Name(),
			strings.Join(updateFields, `, `),
			res.queryChunks.Conditions,
		),
		updateArgs,
		res.queryChunks.Arguments,
	)

	return err
}

func (res *result) Close() error {
	var err error
	if res.cursor != nil {
		err = res.cursor.Close()
		res.cursor = nil
	}
	return err
}

func (res *result) Count() (uint64, error) {

	rows, err := res.table.source.doQuery(
		fmt.Sprintf(
			`SELECT COUNT(1) AS total FROM "%s" WHERE %s`,
			res.table.Name(),
			res.queryChunks.Conditions,
		),
		res.queryChunks.Arguments,
	)

	if err != nil {
		return 0, err
	}

	dst := counter{}
	res.table.T.FetchRow(&dst, rows)

	rows.Close()

	return dst.Total, nil
}
