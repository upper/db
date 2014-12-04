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

package sqlutil

import (
	"database/sql"
	"reflect"

	"menteslibres.net/gosexy/to"
	"upper.io/db"
	"upper.io/db/util"
)

// FetchRow receives a *sql.Rows value and tries to map all the rows into a
// single struct given by the pointer `dst`.
func FetchRow(rows *sql.Rows, dst interface{}) error {
	var columns []string
	var err error

	dstv := reflect.ValueOf(dst)

	if dstv.IsNil() || dstv.Kind() != reflect.Ptr {
		return db.ErrExpectingPointer
	}

	itemV := dstv.Elem()

	if columns, err = rows.Columns(); err != nil {
		return err
	}

	reset(dst)

	next := rows.Next()

	if next == false {
		if err = rows.Err(); err != nil {
			return err
		}
		return db.ErrNoMoreRows
	}

	item, err := fetchResult(itemV.Type(), rows, columns)

	if err != nil {
		return err
	}

	itemV.Set(reflect.Indirect(item))

	return nil
}

// FetchRows receives a *sql.Rows value and tries to map all the rows into a
// slice of structs given by the pointer `dst`.
func FetchRows(rows *sql.Rows, dst interface{}) error {
	var columns []string
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

	if columns, err = rows.Columns(); err != nil {
		return err
	}

	slicev := dstv.Elem()
	itemT := slicev.Type().Elem()

	reset(dst)

	for rows.Next() {

		item, err := fetchResult(itemT, rows, columns)

		if err != nil {
			return err
		}

		slicev = reflect.Append(slicev, reflect.Indirect(item))
	}

	rows.Close()

	dstv.Elem().Set(slicev)

	return nil
}

// indirect function taken from encoding/json/decode.go
//
// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
func indirect(v reflect.Value, decodingNull bool) (db.Unmarshaler, reflect.Value) {
	// If v is a named type and is addressable,
	// start with its address, so that if the type has pointer methods,
	// we find them.
	if v.Kind() != reflect.Ptr && v.Type().Name() != "" && v.CanAddr() {
		v = v.Addr()
	}
	for {
		// Load value from interface, but only if the result will be
		// usefully addressable.
		if v.Kind() == reflect.Interface && !v.IsNil() {
			e := v.Elem()
			if e.Kind() == reflect.Ptr && !e.IsNil() && (!decodingNull || e.Elem().Kind() == reflect.Ptr) {
				v = e
				continue
			}
		}

		if v.Kind() != reflect.Ptr {
			break
		}

		if v.Elem().Kind() != reflect.Ptr && decodingNull && v.CanSet() {
			break
		}
		if v.IsNil() {
			v.Set(reflect.New(v.Type().Elem()))
		}
		if v.Type().NumMethod() > 0 {
			if u, ok := v.Interface().(db.Unmarshaler); ok {
				return u, reflect.Value{}
			}
		}
		v = v.Elem()
	}
	return nil, v
}

func fetchResult(itemT reflect.Type, rows *sql.Rows, columns []string) (reflect.Value, error) {
	var item reflect.Value
	var err error

	switch itemT.Kind() {
	case reflect.Map:
		item = reflect.MakeMap(itemT)
	case reflect.Struct:
		item = reflect.New(itemT)
	default:
		return item, db.ErrExpectingMapOrStruct
	}

	expecting := len(columns)

	// Allocating results.
	values := make([]*sql.RawBytes, expecting)
	scanArgs := make([]interface{}, expecting)

	for i := range columns {
		scanArgs[i] = &values[i]
	}

	if err = rows.Scan(scanArgs...); err != nil {
		return item, err
	}

	// Range over row values.
	for i, value := range values {

		if value != nil {
			// Real column name
			column := columns[i]

			// Value as string.
			svalue := string(*value)

			var cv reflect.Value

			v, _ := to.Convert(svalue, reflect.String)
			cv = reflect.ValueOf(v)

			switch itemT.Kind() {
			// Destination is a map.
			case reflect.Map:
				if cv.Type() != itemT.Elem() {
					if itemT.Elem().Kind() == reflect.Interface {
						cv, _ = util.StringToType(svalue, cv.Type())
					} else {
						cv, _ = util.StringToType(svalue, itemT.Elem())
					}
				}
				if cv.IsValid() {
					item.SetMapIndex(reflect.ValueOf(column), cv)
				}
			// Destionation is a struct.
			case reflect.Struct:

				index := util.GetStructFieldIndex(itemT, column)

				if index == nil {
					continue
				} else {

					// Destination field.
					destf := item.Elem().FieldByIndex(index)

					if destf.IsValid() {

						if cv.Type() != destf.Type() {

							if destf.Type().Kind() != reflect.Interface {

								switch destf.Type() {
								case nullFloat64Type:
									nullFloat64 := sql.NullFloat64{}
									if svalue != `` {
										nullFloat64.Scan(svalue)
									}
									cv = reflect.ValueOf(nullFloat64)
								case nullInt64Type:
									nullInt64 := sql.NullInt64{}
									if svalue != `` {
										nullInt64.Scan(svalue)
									}
									cv = reflect.ValueOf(nullInt64)
								case nullBoolType:
									nullBool := sql.NullBool{}
									if svalue != `` {
										nullBool.Scan(svalue)
									}
									cv = reflect.ValueOf(nullBool)
								case nullStringType:
									nullString := sql.NullString{}
									nullString.Scan(svalue)
									cv = reflect.ValueOf(nullString)
								default:
									var decodingNull bool

									if svalue == "" {
										decodingNull = true
									}

									u, _ := indirect(destf, decodingNull)

									if u != nil {
										u.UnmarshalDB(svalue)

										if destf.Kind() == reflect.Interface || destf.Kind() == reflect.Ptr {
											cv = reflect.ValueOf(u)
										} else {
											cv = reflect.ValueOf(u).Elem()
										}

									} else {
										cv, _ = util.StringToType(svalue, destf.Type())
									}

								}
							}

						}

						// Copying value.
						if cv.IsValid() {
							destf.Set(cv)
						}

					}
				}

			}
		}
	}

	return item, nil
}
