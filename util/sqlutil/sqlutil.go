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
	"reflect"
	"regexp"
	"strings"

	"github.com/jmoiron/sqlx/reflectx"

	"menteslibres.net/gosexy/to"

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

// NormalizeColumn prepares a column for comparison against another column.
func NormalizeColumn(s string) string {
	return strings.ToLower(reColumnCompareExclude.ReplaceAllString(s, ""))
}

// T type is commonly used by adapters to map database/sql values to Go values
// using FieldValues()
type T struct {
	Columns []string
	Mapper  *reflectx.Mapper
}

func (t *T) columnLike(s string) string {
	for _, name := range t.Columns {
		if NormalizeColumn(s) == NormalizeColumn(name) {
			return name
		}
	}
	return s
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

		fieldMap := t.Mapper.TypeMap(itemT).FieldMap()
		nfields := len(fieldMap)

		values = make([]interface{}, 0, nfields)
		fields = make([]string, 0, nfields)

		for _, fi := range fieldMap {
			value := reflectx.FieldByIndexesReadOnly(itemV, fi.Index).Interface()

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
			fields[i] = t.columnLike(to.String(keyV.Interface()))

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

func reset(data interface{}) error {
	// Resetting element.
	v := reflect.ValueOf(data).Elem()
	t := v.Type()
	z := reflect.Zero(t)
	v.Set(z)
	return nil
}

// NewMapper creates a reflectx.Mapper
func NewMapper() *reflectx.Mapper {
	return reflectx.NewMapper("db")
}
