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

package ql

import (
	"database/sql"
	"reflect"
	"strings"

	"upper.io/v2/db"
	"upper.io/v2/db/util/sqlgen"
	"upper.io/v2/db/util/sqlutil"
	"upper.io/v2/db/util/sqlutil/result"
)

type table struct {
	sqlutil.T
	*database
	names       []string
	columnTypes map[string]reflect.Kind
}

var _ = db.Collection(&table{})

// Find creates a result set with the given conditions.
func (t *table) Find(terms ...interface{}) db.Result {
	where, arguments := template.ToWhereWithArguments(terms)
	return result.NewResult(template, t, where, arguments)
}

// Truncate deletes all rows from the table.
func (t *table) Truncate() error {
	_, err := t.database.Exec(sqlgen.Statement{
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

	columnNames, columnValues, err := t.FieldValues(item)

	if err != nil {
		return nil, err
	}

	sqlgenCols, sqlgenVals, sqlgenArgs, err := template.ToColumnsValuesAndArguments(columnNames, columnValues)

	if err != nil {
		return nil, err
	}

	stmt := sqlgen.Statement{
		Type:    sqlgen.Insert,
		Table:   sqlgen.TableWithName(t.MainTableName()),
		Columns: sqlgenCols,
		Values:  sqlgenVals,
	}

	var res sql.Result
	if res, err = t.database.Exec(stmt, sqlgenArgs...); err != nil {
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
