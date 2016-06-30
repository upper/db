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

package sqlutil

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strings"
	// "crypto/md5"

	"upper.io/db"
	"upper.io/db/internal/reflectx"
)

var mapper = reflectx.NewMapper("db")

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
	PrimaryKey []string
	Columns    []string
	Tables     []string // Holds table names.
}

type fieldValue struct {
	fields []string
	values []interface{}
}

func (fv *fieldValue) Len() int {
	return len(fv.fields)
}

func (fv *fieldValue) Swap(i, j int) {
	fv.fields[i], fv.fields[j] = fv.fields[j], fv.fields[i]
	fv.values[i], fv.values[j] = fv.values[j], fv.values[i]
}

func (fv *fieldValue) Less(i, j int) bool {
	return fv.fields[i] < fv.fields[j]
}

func (t *T) FieldValues(item interface{}) ([]string, []interface{}, error) {
	var fv fieldValue

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

		fieldMap := mapper.TypeMap(itemT).Names
		nfields := len(fieldMap)

		fv.values = make([]interface{}, 0, nfields)
		fv.fields = make([]string, 0, nfields)

		for _, fi := range fieldMap {
			fld := reflectx.FieldByIndexesReadOnly(itemV, fi.Index)
			if fld.Kind() == reflect.Ptr && fld.IsNil() {
				continue
			}

			var value interface{}
			if _, ok := fi.Options["stringarray"]; ok {
				value = stringArray(fld.Interface().([]string))
			} else if _, ok := fi.Options["int64array"]; ok {
				value = int64Array(fld.Interface().([]int64))
			} else if _, ok := fi.Options["jsonb"]; ok {
				value = jsonbType{fld.Interface()}
			} else {
				value = fld.Interface()
			}

			if _, ok := fi.Options["omitempty"]; ok {
				if value == fi.Zero.Interface() {
					continue
				}
			}

			fv.fields = append(fv.fields, fi.Name)
			v, err := marshal(value)
			if err != nil {
				return nil, nil, err
			}
			fv.values = append(fv.values, v)
		}

	case reflect.Map:
		nfields := itemV.Len()
		fv.values = make([]interface{}, nfields)
		fv.fields = make([]string, nfields)
		mkeys := itemV.MapKeys()

		for i, keyV := range mkeys {
			valv := itemV.MapIndex(keyV)
			fv.fields[i] = fmt.Sprintf("%v", keyV.Interface())

			v, err := marshal(valv.Interface())
			if err != nil {
				return nil, nil, err
			}

			fv.values[i] = v
		}
	default:
		return nil, nil, errors.New("Expecting pointer to either map or struct")
	}

	sort.Sort(&fv)

	return fv.fields, fv.values, nil
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

// MainTableName returns the name of the first table.
func (t *T) MainTableName() string {
	return t.NthTableName(0)
}

// NthTableName returns the table name at index i.
func (t *T) NthTableName(i int) string {
	if len(t.Tables) > i {
		chunks := strings.SplitN(t.Tables[i], " ", 2)
		if len(chunks) > 0 {
			return chunks[0]
		}
	}
	return ""
}

// HashTableNames returns a unique string for the given array of tables.
func HashTableNames(names []string) string {
	return strings.Join(names, "|")
	// I think we don't really need to do this, the strings.Join already provides a unique string per array.
	// return fmt.Sprintf("%x", md5.Sum([]byte(strings.Join(names, "|"))))
}
