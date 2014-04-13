/*
  Copyright (c) 2014 José Carlos Nieto, https://menteslibres.net/xiam

  Permission is hereby granted, free of charge, to any person obtaining
  a copy of this software and associated documentation files (the
  "Software"), to deal in the Software without restriction, including
  without limitation the rights to use, copy, modify, merge, publish,
  distribute, sublicense, and/or sell copies of the Software, and to
  permit persons to whom the Software is furnished to do so, subject to
  the following conditions:

  The above copyright notice and this permission notice shall be
  included in all copies or substantial portions of the Software.

  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
  NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
  LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
  OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
  WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

package ql

import (
	"database/sql"
	"reflect"
	"strings"
	"upper.io/db"
	"upper.io/db/util"
	"upper.io/db/util/sqlutil"
)

// Wrapper for sqlutil.T
type t struct {
	*sqlutil.T
}

func (self *t) qlFetchRow(dst interface{}, rows *sql.Rows) error {

	dstv := reflect.ValueOf(dst)

	if dstv.IsNil() || dstv.Kind() != reflect.Ptr {
		return db.ErrExpectingPointer
	}

	item_v := dstv.Elem()

	columns, err := sqlutil.GetRowColumns(rows)

	if err != nil {
		return err
	}

	next := rows.Next()

	if next == false {
		if err = rows.Err(); err != nil {
			return err
		}
		return db.ErrNoMoreRows
	}

	item, err := self.qlFetchResult(item_v.Type(), rows, columns)

	if err != nil {
		return err
	}

	item_v.Set(reflect.Indirect(item))

	return nil
}

func (self *t) qlFetchResult(item_t reflect.Type, rows *sql.Rows, columns []string) (item reflect.Value, err error) {
	expecting := len(columns)

	scanArgs := make([]interface{}, expecting)

	switch item_t.Kind() {
	case reflect.Struct:
		// Creating new value of the expected type.
		item = reflect.New(item_t)
		// Pairing each column with its index.
		for i, columnName := range columns {
			index := util.GetStructFieldIndex(item_t, columnName)
			if index != nil {
				dest_f := item.Elem().FieldByIndex(index)
				scanArgs[i] = dest_f.Addr().Interface()
			}
		}
	// case reflect.Map: // TODO
	default:
		return item, db.ErrUnsupportedDestination
		//return item, db.ErrExpectingMapOrStruct
	}

	err = rows.Scan(scanArgs...)

	if err != nil {
		return item, err
	}

	return item, nil
}

func (self *t) qlFetchRows(dst interface{}, rows *sql.Rows) error {

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

	columns, err := sqlutil.GetRowColumns(rows)

	if err != nil {
		return err
	}

	slicev := dstv.Elem()
	item_t := slicev.Type().Elem()

	for rows.Next() {

		item, err := self.qlFetchResult(item_t, rows, columns)

		if err != nil {
			return err
		}

		slicev = reflect.Append(slicev, reflect.Indirect(item))
	}

	rows.Close()

	dstv.Elem().Set(slicev)

	return nil
}
