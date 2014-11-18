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
	"regexp"

	"menteslibres.net/gosexy/to"
	"upper.io/db"
	"upper.io/db/util"
)

var (
	reInvisibleChars = regexp.MustCompile(`[\s\r\n\t]+`)
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
}

func (t *T) columnLike(s string) string {
	for _, name := range t.Columns {
		if util.NormalizeColumn(s) == util.NormalizeColumn(name) {
			return name
		}
	}
	return s
}

func marshal(v interface{}) (interface{}, error) {
	m, isM := v.(db.Marshaler)

	if isM {
		var err error
		if v, err = m.MarshalDB(); err != nil {
			return nil, err
		}
	}

	return v, nil
}

// FieldValues accepts a map or a struct and splits them into an array of
// columns and values.
func (t *T) FieldValues(item interface{}, convertFn func(interface{}) interface{}) ([]string, []interface{}, error) {

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

		nfields := itemV.NumField()

		values = make([]interface{}, 0, nfields)
		fields = make([]string, 0, nfields)

		for i := 0; i < nfields; i++ {

			field := itemT.Field(i)

			if field.PkgPath != `` {
				// Field is unexported.
				continue
			}

			if field.Anonymous {
				// It's an anonymous field. Let's skip it unless it has an explicit
				// `db` tag.
				if field.Tag.Get(`db`) == `` {
					continue
				}
			}

			// Field options.
			fieldName, fieldOptions := util.ParseTag(field.Tag.Get(`db`))

			// Deprecated `field` tag.
			if deprecatedField := field.Tag.Get(`field`); deprecatedField != `` {
				fieldName = deprecatedField
			}

			// Deprecated `omitempty` tag.
			if deprecatedOmitEmpty := field.Tag.Get(`omitempty`); deprecatedOmitEmpty != `` {
				fieldOptions[`omitempty`] = true
			}

			// Deprecated `inline` tag.
			if deprecatedInline := field.Tag.Get(`inline`); deprecatedInline != `` {
				fieldOptions[`inline`] = true
			}

			// Skipping field
			if fieldName == `-` {
				continue
			}

			// Trying to match field name.

			// Explicit JSON or BSON options.
			if fieldName == `` && fieldOptions[`bson`] {
				// Using name from the BSON tag.
				fieldName, _ = util.ParseTag(field.Tag.Get(`bson`))
			}

			if fieldName == `` && fieldOptions[`bson`] {
				// Using name from the JSON tag.
				fieldName, _ = util.ParseTag(field.Tag.Get(`bson`))
			}

			// Still don't have a match? try to match againt JSON.
			if fieldName == `` {
				fieldName, _ = util.ParseTag(field.Tag.Get(`json`))
			}

			// Still don't have a match? try to match againt BSON.
			if fieldName == `` {
				fieldName, _ = util.ParseTag(field.Tag.Get(`bson`))
			}

			// Nothing works, trying to match by name.
			if fieldName == `` {
				fieldName = t.columnLike(field.Name)
			}

			// Processing tag options.
			value := itemV.Field(i).Interface()

			if fieldOptions[`omitempty`] == true {
				zero := reflect.Zero(reflect.TypeOf(value)).Interface()
				if value == zero {
					continue
				}
			}

			if fieldOptions[`inline`] == true {
				infields, invalues, inerr := t.FieldValues(value, convertFn)
				if inerr != nil {
					return nil, nil, inerr
				}
				fields = append(fields, infields...)
				values = append(values, invalues...)
			} else {
				fields = append(fields, fieldName)
				v, err := marshal(convertFn(value))

				if err != nil {
					return nil, nil, err
				}

				values = append(values, v)
			}
		}
	case reflect.Map:
		nfields := itemV.Len()
		values = make([]interface{}, nfields)
		fields = make([]string, nfields)
		mkeys := itemV.MapKeys()

		for i, keyV := range mkeys {
			valv := itemV.MapIndex(keyV)
			fields[i] = t.columnLike(to.String(keyV.Interface()))

			v, err := marshal(convertFn(valv.Interface()))

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
