/*
  Copyright (c) 2012-2013 José Carlos Nieto, http://xiam.menteslibres.org/

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

package sqlutil

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/gosexy/db"
	"github.com/gosexy/db/util"
	"github.com/gosexy/to"
	"reflect"
	"strings"
)

type T struct {
	ColumnTypes map[string]reflect.Kind
	util.C
}

type QueryChunks struct {
	Fields     []string
	Limit      string
	Offset     string
	Sort       string
	Relate     db.Relate
	RelateAll  db.RelateAll
	Relations  []db.Relation
	Conditions string
	Arguments  db.SqlArgs
}

func (self *T) ColumnLike(s string) string {
	for col, _ := range self.ColumnTypes {
		if util.CompareColumnToField(s, col) == true {
			return col
		}
	}
	return s
}

/*
	Copies *sql.Rows into the slice of maps or structs given by the pointer dst.
*/
func (self *T) FetchRows(dst interface{}, rows *sql.Rows) error {

	// Destination.
	dstv := reflect.ValueOf(dst)

	if dstv.Kind() != reflect.Ptr || dstv.Elem().Kind() != reflect.Slice || dstv.IsNil() {
		return errors.New("fetchRows expects a pointer to slice.")
	}

	// Column names.
	columns, err := rows.Columns()

	if err != nil {
		return err
	}

	// Column names to lower case.
	for i, _ := range columns {
		columns[i] = strings.ToLower(columns[i])
	}

	expecting := len(columns)

	slicev := dstv.Elem()
	itemt := slicev.Type().Elem()

	for rows.Next() {

		// Allocating results.
		values := make([]*sql.RawBytes, expecting)
		scanArgs := make([]interface{}, expecting)

		for i := range columns {
			scanArgs[i] = &values[i]
		}

		var item reflect.Value

		switch itemt.Kind() {
		case reflect.Map:
			item = reflect.MakeMap(itemt)
		case reflect.Struct:
			item = reflect.New(itemt)
		default:
			return fmt.Errorf("Don't know how to deal with %s, use either map or struct.", itemt.Kind())
		}

		err := rows.Scan(scanArgs...)

		if err != nil {
			return err
		}

		// Range over row values.
		for i, value := range values {
			if value != nil {
				column := columns[i]
				svalue := string(*value)

				var cv reflect.Value

				if _, ok := self.ColumnTypes[column]; ok == true {
					v, _ := to.Convert(string(*value), self.ColumnTypes[column])
					cv = reflect.ValueOf(v)
				} else {
					v, _ := to.Convert(string(*value), reflect.String)
					cv = reflect.ValueOf(v)
				}

				switch itemt.Kind() {
				// Destination is a map.
				case reflect.Map:
					if cv.Type().Kind() != itemt.Elem().Kind() {
						if itemt.Elem().Kind() != reflect.Interface {
							// Converting value.
							cv, _ = util.ConvertValue(svalue, itemt.Elem().Kind())
						}
					}
					if cv.IsValid() {
						item.SetMapIndex(reflect.ValueOf(column), cv)
					}
				// Destionation is a struct.
				case reflect.Struct:
					// Get appropriate column.
					f := func(s string) bool {
						return util.CompareColumnToField(s, column)
					}
					// Destination field.
					destf := item.Elem().FieldByNameFunc(f)
					if destf.IsValid() {
						if cv.Type().Kind() != destf.Type().Kind() {
							if destf.Type().Kind() != reflect.Interface {
								// Converting value.
								cv, _ = util.ConvertValue(svalue, destf.Type().Kind())
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

		slicev = reflect.Append(slicev, reflect.Indirect(item))
	}

	dstv.Elem().Set(slicev)

	return nil
}

func (self *T) FieldValues(item interface{}, convertFn func(interface{}) string) ([]string, []string, error) {

	fields := []string{}
	values := []string{}

	itemv := reflect.ValueOf(item)
	itemt := itemv.Type()

	switch itemt.Kind() {
	case reflect.Struct:
		nfields := itemv.NumField()
		values = make([]string, nfields)
		fields = make([]string, nfields)
		for i := 0; i < nfields; i++ {
			fields[i] = self.ColumnLike(itemt.Field(i).Name)
			values[i] = convertFn(itemv.Field(i).Interface())
		}
	case reflect.Map:
		nfields := itemv.Len()
		values = make([]string, nfields)
		fields = make([]string, nfields)
		mkeys := itemv.MapKeys()
		for i, keyv := range mkeys {
			valv := itemv.MapIndex(keyv)
			fields[i] = self.ColumnLike(to.String(keyv.Interface()))
			values[i] = convertFn(valv.Interface())
		}
	default:
		return nil, nil, fmt.Errorf("Expecting Struct or Map, received %v.", itemt.Kind())
	}

	return fields, values, nil
}

func NewQueryChunks() *QueryChunks {
	self := &QueryChunks{
		Relate:    make(db.Relate),
		RelateAll: make(db.RelateAll),
	}
	return self
}
