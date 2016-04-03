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
	"fmt"
	"reflect"

	"upper.io/db.v2"
	"upper.io/db.v2/builder/sqlbuilder"
	"upper.io/db.v2/builder/sqlgen"
	"upper.io/db.v2/internal/sqladapter"
)

type resultProxy struct {
	db.Result
	col db.Collection
}

func (r *resultProxy) Select(fields ...interface{}) db.Result {
	if len(fields) == 1 {
		if s, ok := fields[0].(string); ok && s == "*" {
			var columns []struct {
				Name string `db:"Name"`
			}
			err := r.col.(*table).Database().Builder().Select("Name").
				From("__Column").
				Where("TableName", r.col.Name()).
				Iterator().All(&columns)
			if err == nil {
				fields = make([]interface{}, 0, len(columns)+1)
				fields = append(fields, "id() as id")
				for _, column := range columns {
					fields = append(fields, column.Name)
				}
			}
		}
	}
	return r.Result.Select(fields...)
}

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

// InsertReturning inserts an item and updates the variable.
func (t *table) InsertReturning(item interface{}) error {
	if reflect.TypeOf(item).Kind() != reflect.Ptr {
		return fmt.Errorf("Expecting a pointer to map or string but got %T", item)
	}

	sess := db.Database(t.Database())

	if currTx := sess.(*database).BaseDatabase.Tx(); currTx == nil {
		// Not within a transaction, let's create one.
		tx, err := sess.Transaction()
		if err != nil {
			return err
		}
		sess = tx
	}

	var res db.Result

	col := sess.Collection(t.Name())

	id, err := col.Insert(item)
	if err != nil {
		goto cancel
	}
	if id == nil {
		err = fmt.Errorf("Insertion did not return any ID, aborted.")
		goto cancel
	}

	res = col.Find(id)
	if err = res.One(item); err != nil {
		goto cancel
	}

	if tx, ok := sess.(db.Tx); ok {
		// This is only executed if t.Database() was **not** a transaction and if
		// sess was created with sess.Transaction().
		return tx.Commit()
	}
	return err

cancel:
	// This goto label should only be used when we got an error within a
	// transaction and we don't want to continue.

	if tx, ok := sess.(db.Tx); ok {
		// This is only executed if t.Database() was **not** a transaction and if
		// sess was created with sess.Transaction().
		tx.Rollback()
	}
	return err
}

func (t *table) Find(conds ...interface{}) db.Result {
	if len(conds) == 1 {
		if id, ok := conds[0].(uint64); ok { // ID type.
			conds[0] = db.Cond{
				"id()": id,
			}
		}
		if id, ok := conds[0].(int64); ok { // ID type.
			conds[0] = db.Cond{
				"id()": id,
			}
		}
		if id, ok := conds[0].(int); ok { // ID type.
			conds[0] = db.Cond{
				"id()": id,
			}
		}
	}
	res := &resultProxy{t.Collection.Find(conds...), t}
	return res.Select("*")
}

// Insert inserts an item (map or struct) into the collection.
func (t *table) Insert(item interface{}) (interface{}, error) {
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
	return id, nil
}

func newTable(d *database, name string) *table {
	return &table{sqladapter.NewCollection(d, name)}
}
