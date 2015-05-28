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

package mysql

import (
	"database/sql"
	"strings"

	"upper.io/db"
	"upper.io/db/util/sqlgen"
	"upper.io/db/util/sqlutil"
	"upper.io/db/util/sqlutil/result"
)

type table struct {
	sqlutil.T
	*source
	names []string
}

var _ = db.Collection(&table{})

// tableN returns the nth name provided to the table.
func (t *table) tableN(i int) string {
	if len(t.names) > i {
		chunks := strings.SplitN(t.names[i], " ", 2)
		if len(chunks) > 0 {
			return chunks[0]
		}
	}
	return ""
}

// Find creates a result set with the given conditions.
func (t *table) Find(terms ...interface{}) db.Result {
	where, arguments := sqlutil.ToWhereWithArguments(terms)
	return result.NewResult(t, where, arguments)
}

// Truncate deletes all rows from the table.
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

// Appends an item (map or struct) into the collection.
func (t *table) Append(item interface{}) (interface{}, error) {
	var pKey []string

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

	var res sql.Result
	if res, err = t.source.Exec(stmt, arguments...); err != nil {
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

	// There is no "RETURNING" in MySQL, so we have to return the values that
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
func (t *table) Exists() bool {
	if err := t.source.tableExists(t.names...); err != nil {
		return false
	}
	return true
}

func (t *table) Name() string {
	return strings.Join(t.names, `, `)
}
