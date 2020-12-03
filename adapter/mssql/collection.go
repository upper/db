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

package mssql

import (
	db "github.com/upper/db/v4"
	"github.com/upper/db/v4/internal/sqladapter"
	"github.com/upper/db/v4/internal/sqlbuilder"
)

type collectionAdapter struct {
	hasIdentityColumn *bool
}

func (adt *collectionAdapter) Insert(col sqladapter.Collection, item interface{}) (interface{}, error) {
	columnNames, columnValues, err := sqlbuilder.Map(item, nil)
	if err != nil {
		return nil, err
	}

	pKey, err := col.PrimaryKeys()
	if err != nil {
		return nil, err
	}

	var hasKeys bool
	for i := range columnNames {
		for j := 0; j < len(pKey); j++ {
			if pKey[j] == columnNames[i] {
				if columnValues[i] != nil {
					hasKeys = true
					break
				}
			}
		}
	}

	if hasKeys {
		if adt.hasIdentityColumn == nil {
			var hasIdentityColumn bool
			var identityColumns int

			row, err := col.SQL().QueryRow("SELECT COUNT(1) FROM sys.identity_columns WHERE OBJECT_NAME(object_id) = ?", col.Name())
			if err != nil {
				return nil, err
			}

			err = row.Scan(&identityColumns)
			if err != nil {
				return nil, err
			}

			if identityColumns > 0 {
				hasIdentityColumn = true
			}

			adt.hasIdentityColumn = &hasIdentityColumn
		}

		if *adt.hasIdentityColumn {
			_, err = col.SQL().Exec("SET IDENTITY_INSERT " + col.Name() + " ON")
			if err != nil {
				return nil, err
			}
			defer func() {
				_, _ = col.SQL().Exec("SET IDENTITY_INSERT " + col.Name() + " OFF")
			}()
		}
	}

	q := col.SQL().InsertInto(col.Name()).
		Columns(columnNames...).
		Values(columnValues...)

	if len(pKey) < 1 {
		_, err = q.Exec()
		if err != nil {
			return nil, err
		}
		return nil, nil
	}

	q = q.Returning(pKey...)

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
