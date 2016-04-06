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
	"reflect"

	"upper.io/db.v2"
	"upper.io/db.v2/builder"
	"upper.io/db.v2/builder/exql"
	"upper.io/db.v2/internal/sqladapter"
)

type table struct {
	sqladapter.Collection
	d *database
}

var _ = db.Collection(&table{})

// Truncate deletes all rows from the table.
func (t *table) Truncate() error {
	stmt := exql.Statement{
		Type:  exql.Truncate,
		Table: exql.TableWithName(t.Name()),
	}

	if _, err := t.d.Builder.Exec(&stmt); err != nil {
		return err
	}
	return nil
}

func (t *table) Find(conds ...interface{}) db.Result {
	if len(conds) == 1 {
		if id, ok := conds[0].(int64); ok { // ID type.
			conds[0] = db.Cond{
				"id": id,
			}
		}
	}
	return sqladapter.NewResult(t.d.Builder, t.Name(), conds)
}

// InsertReturning inserts an item and updates the variable.
func (t *table) InsertReturning(item interface{}) error {
	if reflect.TypeOf(item).Kind() != reflect.Ptr {
		return fmt.Errorf("Expecting a pointer to map or string but got %T", item)
	}

	sess := db.Database(t.d)

	if currTx := t.d.BaseDatabase.Tx(); currTx == nil {
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

// Insert inserts an item (map or struct) into the collection.
func (t *table) Insert(item interface{}) (interface{}, error) {
	columnNames, columnValues, err := builder.Map(item)
	if err != nil {
		return nil, err
	}

	var pKey []string
	if pKey, err = t.Database().TablePrimaryKey(t.Name()); err != nil {
		if err != sql.ErrNoRows {
			return nil, err
		}
	}

	q := t.d.InsertInto(t.Name()).
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

	var keyMap db.Cond
	if err = q.Iterator().One(&keyMap); err != nil {
		return nil, err
	}

	// The IDSetter interface does not match, look for another interface match.
	if len(keyMap) == 1 {
		return keyMap[pKey[0]], nil
	}

	// This was a compound key and no interface matched it, let's return a map.
	return keyMap, nil
}

func newTable(d *database, name string) *table {
	return &table{Collection: sqladapter.NewCollection(d, name), d: d}
}
