// Copyright (c) 2012-2016 The upper.io/db.v1 authors. All rights reserved.
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

package util

import (
	"reflect"
	"regexp"
	"strings"
	"time"

	"menteslibres.net/gosexy/to"
)

var reColumnCompareExclude = regexp.MustCompile(`[^a-zA-Z0-9]`)

var (
	durationType = reflect.TypeOf(time.Duration(0))
	timeType     = reflect.TypeOf(time.Time{})
	ptimeType    = reflect.TypeOf(&time.Time{})
)

type tagOptions map[string]bool

func parseTagOptions(s string) tagOptions {
	opts := make(tagOptions)
	chunks := strings.Split(s, `,`)
	for _, chunk := range chunks {
		opts[strings.TrimSpace(chunk)] = true
	}
	return opts
}

// ParseTag splits a struct tag into comma separated chunks. The first chunk is
// returned as a string value, remaining chunks are considered enabled options.
func ParseTag(tag string) (string, tagOptions) {
	// Based on http://golang.org/src/pkg/encoding/json/tags.go
	if i := strings.Index(tag, `,`); i != -1 {
		return tag[:i], parseTagOptions(tag[i+1:])
	}
	return tag, parseTagOptions(``)
}

// GetStructFieldIndex returns the struct field index for a given column name
// or nil, if no column matches.
func GetStructFieldIndex(t reflect.Type, columnName string) []int {

	n := t.NumField()

	for i := 0; i < n; i++ {

		field := t.Field(i)

		if field.PkgPath != `` {
			// Field is unexported.
			continue
		}

		// Attempt to use db:`column_name`
		fieldName, fieldOptions := ParseTag(field.Tag.Get(`db`))

		// Deprecated `field` tag.
		if deprecatedField := field.Tag.Get(`field`); deprecatedField != `` {
			fieldName = deprecatedField
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
			fieldName, _ = ParseTag(field.Tag.Get(`bson`))
		}

		if fieldName == `` && fieldOptions[`bson`] {
			// Using name from the JSON tag.
			fieldName, _ = ParseTag(field.Tag.Get(`bson`))
		}

		// Still don't have a match? try to match againt JSON.
		if fieldName == `` {
			fieldName, _ = ParseTag(field.Tag.Get(`json`))
		}

		// Still don't have a match? try to match againt BSON.
		if fieldName == `` {
			fieldName, _ = ParseTag(field.Tag.Get(`bson`))
		}

		// Attempt to match field name.
		if fieldName == columnName {
			return []int{i}
		}

		// Nothing works, trying to match by name.
		if fieldName == `` {
			if NormalizeColumn(field.Name) == NormalizeColumn(columnName) {
				return []int{i}
			}
		}

		// Inline option.
		if fieldOptions[`inline`] == true {
			index := GetStructFieldIndex(field.Type, columnName)
			if index != nil {
				res := append([]int{i}, index...)
				return res
			}
		}
	}
	// No match.
	return nil
}

// StringToType converts a string value into another type.
func StringToType(src string, dstt reflect.Type) (srcv reflect.Value, err error) {

	// Is destination a pointer?
	if dstt.Kind() == reflect.Ptr {
		if src == "" {
			return
		}
	}

	switch dstt {
	case durationType:
		srcv = reflect.ValueOf(to.Duration(src))
	case timeType:
		srcv = reflect.ValueOf(to.Time(src))
	case ptimeType:
		t := to.Time(src)
		srcv = reflect.ValueOf(&t)
	default:
		return StringToKind(src, dstt.Kind())
	}
	return srcv, nil
}

// StringToKind converts a string into a kind.
func StringToKind(src string, dstk reflect.Kind) (reflect.Value, error) {
	var srcv reflect.Value

	// Destination type.
	switch dstk {
	case reflect.Interface:
		// Destination is interface, nuff said.
		srcv = reflect.ValueOf(src)
	default:
		cv, err := to.Convert(src, dstk)
		if err != nil {
			return srcv, nil
		}
		srcv = reflect.ValueOf(cv)
	}

	return srcv, nil
}

// NormalizeColumn prepares a column for comparison against another column.
func NormalizeColumn(s string) string {
	return strings.ToLower(reColumnCompareExclude.ReplaceAllString(s, ""))
}
