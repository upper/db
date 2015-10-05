// Copyright (c) 2012-2015 The upper.io/db authors. All rights reserved.
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
	"upper.io/db/builder"
	"upper.io/db/util/sqlgen"
	"upper.io/db/util/sqlutil"
	"upper.io/db/util/sqlutil/result"
)

type table struct {
	sqlutil.T
	*database
}

var _ = db.Collection(&table{})

// Find creates a result set with the given conditions.
func (t *table) Find(conds ...interface{}) db.Result {
	return result.NewResult(t.database.Builder(), t.Name(), conds)
}

// Truncate deletes all rows from the table.
func (t *table) Truncate() error {
	_, err := t.database.Exec(&sqlgen.Statement{
		Type:  sqlgen.Truncate,
		Table: sqlgen.TableWithName(t.MainTableName()),
	})
	if err != nil {
		return err
	}

	return nil
}

// Append inserts an item (map or struct) into the collection.
func (t *table) Append(item interface{}) (interface{}, error) {
	columnNames, columnValues, err := builder.Map(item)

	if err != nil {
		return nil, err
	}

	sqlgenCols, sqlgenVals, sqlgenArgs, err := template.ToColumnsValuesAndArguments(columnNames, columnValues)

	if err != nil {
		return nil, err
	}

	var pKey []string

	if pKey, err = t.database.getPrimaryKey(t.MainTableName()); err != nil {
		if err != sql.ErrNoRows {
			// Can't tell primary key.
			return nil, err
		}
	}

	stmt := &sqlgen.Statement{
		Type:    sqlgen.Insert,
		Table:   sqlgen.TableWithName(t.MainTableName()),
		Columns: sqlgenCols,
		Values:  sqlgenVals,
	}

	// No primary keys defined.
	if len(pKey) == 0 {
		var res sql.Result

		if res, err = t.database.Exec(stmt, sqlgenArgs...); err != nil {
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

	if rows, err = t.database.Query(stmt, sqlgenArgs...); err != nil {
		return nil, err
	}

	keyMap := map[string]interface{}{}
	if err := sqlutil.FetchRow(rows, &keyMap); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()

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
	if err := t.database.tableExists(t.Tables...); err != nil {
		return false
	}
	return true
}

// Name returns the name of the table or tables that form the collection.
func (t *table) Name() string {
	return strings.Join(t.Tables, `, `)
}
