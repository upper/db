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

package ql

import (
	"database/sql"

	"upper.io/builder/sqlbuilder"
	"upper.io/builder/sqlgen"
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

	q := t.Database().Builder().InsertInto(t.Name()).
		Columns(columnNames...).
		Values(columnValues...)

	var res sql.Result
	if res, err = q.Exec(); err != nil {
		return nil, err
	}

	var id int64
	id, _ = res.LastInsertId()

	// Does the item satisfy the db.ID interface?
	if setter, ok := item.(db.IDSetter); ok {
		if err := setter.SetID(map[string]interface{}{"id": id}); err != nil {
			return nil, err
		}
	}

	return id, nil
}

func newTable(d *database, name string) *table {
	return &table{sqladapter.NewCollection(d, name)}
}
