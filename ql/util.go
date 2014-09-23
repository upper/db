// Copyright (c) 2012-2014 José Carlos Nieto, https://menteslibres.net/xiam
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

	"menteslibres.net/gosexy/to"
	"upper.io/db"
	"upper.io/db/util"
)

func (t *table) fetchRow(rows *sql.Rows, dst interface{}) error {
	var err error

	dstv := reflect.ValueOf(dst)

	if dstv.IsNil() || dstv.Kind() != reflect.Ptr {
		return db.ErrExpectingPointer
	}

	itemV := dstv.Elem()

	next := rows.Next()

	if next == false {
		if err = rows.Err(); err != nil {
			return err
		}
		return db.ErrNoMoreRows
	}

	var columns []string

	if columns, err = rows.Columns(); err != nil {
		return err
	}

	item, err := t.fetchResult(itemV.Type(), rows, columns)

	if err != nil {
		return err
	}

	itemV.Set(reflect.Indirect(item))

	return nil
}

func (t *table) fetchResult(itemT reflect.Type, rows *sql.Rows, columns []string) (item reflect.Value, err error) {
	expecting := len(columns)

	scanArgs := make([]interface{}, expecting)

	switch itemT.Kind() {
	case reflect.Struct:
		// Creating new value of the expected type.
		item = reflect.New(itemT)
		// Pairing each column with its index.
		for i, columnName := range columns {
			index := util.GetStructFieldIndex(itemT, columnName)
			if len(index) > 0 {
				destF := item.Elem().FieldByIndex(index)
				scanArgs[i] = destF.Addr().Interface()
			} else {
				var placeholder sql.RawBytes
				scanArgs[i] = &placeholder
			}
		}

		err = rows.Scan(scanArgs...)

		if err != nil {
			return item, err
		}
	case reflect.Map:
		values := make([]*sql.RawBytes, len(columns))
		for i := range columns {
			scanArgs[i] = &values[i]
		}
		err = rows.Scan(scanArgs...)

		if err == nil {
			item = reflect.MakeMap(itemT)
			for i, columnName := range columns {
				valS := string(*values[i])

				var vv reflect.Value

				if _, ok := t.columnTypes[columnName]; ok == true {
					v, _ := to.Convert(valS, t.columnTypes[columnName])
					vv = reflect.ValueOf(v)
				} else {
					v, _ := to.Convert(valS, reflect.String)
					vv = reflect.ValueOf(v)
				}

				vk := reflect.ValueOf(columnName)
				item.SetMapIndex(vk, vv)
			}
		}

		return item, err
	default:
		return item, db.ErrExpectingMapOrStruct
	}

	return item, nil
}

func (t *table) fetchRows(rows *sql.Rows, dst interface{}) error {
	var err error

	// Destination.
	dstv := reflect.ValueOf(dst)

	if dstv.IsNil() || dstv.Kind() != reflect.Ptr {
		return db.ErrExpectingPointer
	}

	if dstv.Elem().Kind() != reflect.Slice {
		return db.ErrExpectingSlicePointer
	}

	if dstv.Kind() != reflect.Ptr || dstv.Elem().Kind() != reflect.Slice || dstv.IsNil() {
		return db.ErrExpectingSliceMapStruct
	}

	slicev := dstv.Elem()
	itemT := slicev.Type().Elem()

	var columns []string

	if columns, err = rows.Columns(); err != nil {
		return err
	}

	for rows.Next() {

		item, err := t.fetchResult(itemT, rows, columns)

		if err != nil {
			return err
		}

		slicev = reflect.Append(slicev, reflect.Indirect(item))
	}

	rows.Close()

	dstv.Elem().Set(slicev)

	return nil
}
