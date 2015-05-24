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

package sqlutil

import (
	"database/sql"
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"github.com/jmoiron/sqlx/reflectx"
	"upper.io/db"
)

var (
	reInvisibleChars       = regexp.MustCompile(`[\s\r\n\t]+`)
	reColumnCompareExclude = regexp.MustCompile(`[^a-zA-Z0-9]`)
)

var (
	nullInt64Type   = reflect.TypeOf(sql.NullInt64{})
	nullFloat64Type = reflect.TypeOf(sql.NullFloat64{})
	nullBoolType    = reflect.TypeOf(sql.NullBool{})
	nullStringType  = reflect.TypeOf(sql.NullString{})
)

// T type is commonly used by adapters to map database/sql values to Go values
// using FieldValues()
type T struct {
	Columns []string
	Mapper  *reflectx.Mapper
}

func (t *T) columnLike(s string) string {
	for _, name := range t.Columns {
		if normalizeColumn(s) == normalizeColumn(name) {
			return name
		}
	}
	return s
}

func (t *T) FieldValues(item interface{}) ([]string, []interface{}, error) {
	fields := []string{}
	values := []interface{}{}

	itemV := reflect.ValueOf(item)
	itemT := itemV.Type()

	if itemT.Kind() == reflect.Ptr {
		// Single derefence. Just in case user passed a pointer to struct instead of a struct.
		item = itemV.Elem().Interface()
		itemV = reflect.ValueOf(item)
		itemT = itemV.Type()
	}

	switch itemT.Kind() {

	case reflect.Struct:

		fieldMap := t.Mapper.TypeMap(itemT).Names
		nfields := len(fieldMap)

		values = make([]interface{}, 0, nfields)
		fields = make([]string, 0, nfields)

		for _, fi := range fieldMap {
			// log.Println("=>", fi.Name, fi.Options)

			fld := reflectx.FieldByIndexesReadOnly(itemV, fi.Index)
			if fld.Kind() == reflect.Ptr && fld.IsNil() {
				continue
			}

			var value interface{}
			if _, ok := fi.Options["stringarray"]; ok {
				value = StringArray(fld.Interface().([]string))
			} else if _, ok := fi.Options["int64array"]; ok {
				value = Int64Array(fld.Interface().([]int64))
			} else if _, ok := fi.Options["jsonb"]; ok {
				value = JsonbType{fld.Interface()}
			} else {
				value = fld.Interface()
			}

			if _, ok := fi.Options["omitempty"]; ok {
				if value == fi.Zero.Interface() {
					continue
				}
			}

			// TODO: columnLike stuff...?

			fields = append(fields, fi.Name)
			v, err := marshal(value)
			if err != nil {
				return nil, nil, err
			}
			values = append(values, v)
		}

	case reflect.Map:
		nfields := itemV.Len()
		values = make([]interface{}, nfields)
		fields = make([]string, nfields)
		mkeys := itemV.MapKeys()

		for i, keyV := range mkeys {
			valv := itemV.MapIndex(keyV)
			fields[i] = t.columnLike(fmt.Sprintf("%v", keyV.Interface()))

			v, err := marshal(valv.Interface())
			if err != nil {
				return nil, nil, err
			}

			values[i] = v
		}

	default:
		return nil, nil, db.ErrExpectingMapOrStruct
	}

	return fields, values, nil
}

func marshal(v interface{}) (interface{}, error) {
	if m, isMarshaler := v.(db.Marshaler); isMarshaler {
		var err error
		if v, err = m.MarshalDB(); err != nil {
			return nil, err
		}
	}
	return v, nil
}

func reset(data interface{}) error {
	// Resetting element.
	v := reflect.ValueOf(data).Elem()
	t := v.Type()
	z := reflect.Zero(t)
	v.Set(z)
	return nil
}

// normalizeColumn prepares a column for comparison against another column.
func normalizeColumn(s string) string {
	return strings.ToLower(reColumnCompareExclude.ReplaceAllString(s, ""))
}

// NewMapper creates a reflectx.Mapper
func NewMapper() *reflectx.Mapper {
	return reflectx.NewMapper("db")
}
