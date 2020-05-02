// Copyright (c) 2012-present The upper.io/db authors. All rights reserved.
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

	db "github.com/upper/db"
	"github.com/upper/db/internal/sqladapter"
	"github.com/upper/db/sqlbuilder"
)

// table is the actual implementation of a collection.
type table struct {
	sqladapter.BaseCollection // Leveraged by sqladapter

	d    *database
	name string
}

var (
	_ = sqladapter.Collection(&table{})
	_ = db.Collection(&table{})
)

// newTable binds *table with sqladapter.
func newTable(d *database, name string) *table {
	t := &table{
		name: name,
		d:    d,
	}
	t.BaseCollection = sqladapter.NewBaseCollection(t)
	return t
}

type resultProxy struct {
	db.Result
	t *table
}

func (r *resultProxy) Select(fields ...interface{}) db.Result {
	if len(fields) == 1 {
		if s, ok := fields[0].(string); ok && s == "*" {
			var columns []struct {
				Name string `db:"Name"`
			}
			err := r.t.d.Select("Name").
				From("__Column").
				Where("TableName", r.t.Name()).
				Iterator().All(&columns)
			if err == nil {
				fields = make([]interface{}, 0, len(columns)+1)
				fields = append(fields, "id() AS id")
				for _, column := range columns {
					fields = append(fields, column.Name)
				}
			}
		}
	}
	return r.Result.Select(fields...)
}

func (t *table) Name() string {
	return t.name
}

func (t *table) Database() sqladapter.Database {
	return t.d
}

func (t *table) FilterConds(conds ...interface{}) []interface{} {
	if len(conds) == 1 {
		switch conds[0].(type) {
		case int, int64, uint, uint64:
			// This is an special QL index, I'm not sure if it allows the user to
			// create special indexes with custom names.
			conds[0] = db.Cond{"id()": db.Eq(conds[0])}
		}
	}
	return conds
}

func (t *table) Find(conds ...interface{}) db.Result {
	res := &resultProxy{
		Result: t.BaseCollection.Find(conds...),
		t:      t,
	}
	return res.Select("*")
}

// Insert inserts an item (map or struct) into the collection.
func (t *table) Insert(item interface{}) (interface{}, error) {
	columnNames, columnValues, err := sqlbuilder.Map(item, nil)
	if err != nil {
		return nil, err
	}

	pKey := t.BaseCollection.PrimaryKeys()

	q := t.d.InsertInto(t.Name()).
		Columns(columnNames...).
		Values(columnValues...)

	var res sql.Result
	if res, err = q.Exec(); err != nil {
		return nil, err
	}

	if len(pKey) <= 1 {
		return res.LastInsertId()
	}

	keyMap := db.Cond{}

	for i := range columnNames {
		for j := 0; j < len(pKey); j++ {
			if pKey[j] == columnNames[i] {
				keyMap[pKey[j]] = columnValues[i]
			}
		}
	}

	return keyMap, nil
}
