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

	"upper.io/db.v2/builder/sqlbuilder"
	"upper.io/db.v2/builder/sqlgen"
	"upper.io/db.v2"
	"upper.io/db.v2/internal/sqladapter"
)

type table struct {
	sqladapter.Collection
}

var _ = db.Collection(&table{})

// Truncate deletes all rows from the table.
func (t *table) Truncate() error {
	stmt := sqlgen.Statement{
		Type:  sqlgen.Truncate,
		Table: sqlgen.TableWithName(t.Name()),
	}

	if _, err := t.Database().Builder().Exec(&stmt); err != nil {
		return err
	}
	return nil
}

// Append inserts an item (map or struct) into the collection.
func (t *table) Append(item interface{}) (interface{}, error) {
	columnNames, columnValues, err := sqlbuilder.Map(item)
	if err != nil {
		return nil, err
	}

	var pKey []string
	if pKey, err = t.Database().TablePrimaryKey(t.Name()); err != nil {
		if err != sql.ErrNoRows {
			return nil, err
		}
	}

	q := t.Database().Builder().InsertInto(t.Name()).
		Columns(columnNames...).
		Values(columnValues...)

	if len(pKey) == 0 {
		// There is no primary key.
		var res sql.Result

		if res, err = q.Exec(); err != nil {
			return nil, err
		}

		// Attempt to use LastInsertId() (probably won't work, but the Exec()
		// succeeded, so we can safely ignore the error from LastInsertId()).
		lastID, _ := res.LastInsertId()

		return lastID, nil
	}

	// Asking the database to return the primary key after insertion.
	q.Returning(pKey...)

	var keyMap map[string]interface{}
	if err = q.Iterator().One(&keyMap); err != nil {
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

	// This was a compound key and no interface matched it, let's return a map.
	return keyMap, nil
}

func newTable(d *database, name string) *table {
	return &table{sqladapter.NewCollection(d, name)}
}
