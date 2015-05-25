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
	"strings"

	"github.com/jmoiron/sqlx"
	"upper.io/db"
	"upper.io/db/util/sqlgen"
	"upper.io/db/util/sqlutil"
	"upper.io/db/util/sqlutil/result"
)

type table struct {
	sqlutil.T
	*source
	primaryKey string
	names      []string
}

// Find creates a result set with the given conditions.
func (t *table) Find(terms ...interface{}) db.Result {
	where, arguments := sqlutil.ToWhereWithArguments(terms)
	return result.NewResult(t, where, arguments)
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

// Truncate deletes all rows within the table.
func (t *table) Truncate() error {
	_, err := t.source.Exec(sqlgen.Statement{
		Type:  sqlgen.Truncate,
		Table: sqlgen.TableWithName(t.tableN(0)),
	})

	if err != nil {
		return err
	}

	return nil
}

// Append inserts an item (map or struct) into the collection.
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

		if res, err = t.source.Exec(stmt, arguments...); err != nil {
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
	if rows, err = t.source.Query(stmt, arguments...); err != nil {
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

// Exists returns true if the collection exists.
func (t *table) Exists() bool {
	if err := t.source.tableExists(t.names...); err != nil {
		return false
	}
	return true
}

// Name returns the name of the table or tables that form the collection.
func (t *table) Name() string {
	return strings.Join(t.names, `, `)
}
