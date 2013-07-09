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

package util

import (
	"fmt"
	"menteslibres.net/gosexy/db"
	"menteslibres.net/gosexy/to"
	"reflect"
	"regexp"
	"strings"
	"time"
)

var extRelationPattern = regexp.MustCompile(`\{(.+)\}`)
var columnCompareExclude = regexp.MustCompile(`[^a-zA-Z0-9]`)

var durationType = reflect.TypeOf(time.Duration(0))
var timeType = reflect.TypeOf(time.Time{})

type C struct {
	DB      db.Database
	SetName string
}

func (self *C) RelationCollection(name string, terms db.On) (db.Collection, error) {

	var err error
	var col db.Collection

	for _, v := range terms {

		switch t := v.(type) {
		case db.Collection:
			col = t
		}
	}

	if col == nil {
		col, err = self.DB.Collection(name)
		if err != nil || col == nil {
			return nil, fmt.Errorf("Failed relation %s: %s", name, err.Error())
		}
	}

	return col, nil
}

func columnCompare(s string) string {
	return strings.ToLower(columnCompareExclude.ReplaceAllString(s, ""))
}

/*
	Returns the most appropriate struct field index for a given column name.

	If no column matches returns nil.
*/
func GetStructFieldIndex(t reflect.Type, columnName string) []int {

	n := t.NumField()

	columnNameLower := columnCompare(columnName)

	for i := 0; i < n; i++ {

		field := t.Field(i)

		// Field is exported.
		if field.PkgPath == "" {

			tag := field.Tag

			// Tag: field:"columnName"
			fieldName := tag.Get("field")

			if fieldName != "" {
				if fieldName == columnName {
					return []int{i}
				}
			}

			// Simply matching column to name.
			fieldNameLower := columnCompare(field.Name)

			if fieldNameLower == columnNameLower {
				return []int{i}
			}

			// Tag: inline:bool
			if tag.Get("inline") == "true" {
				index := GetStructFieldIndex(field.Type, columnName)
				if index != nil {
					res := append([]int{i}, index...)
					return res
				}
			}

		}

	}

	// No match.
	return nil
}

/*
	Returns true if a table column looks like a struct field.
*/
func CompareColumnToField(s, c string) bool {
	return columnCompare(s) == columnCompare(c)
}

/*
	Returns the table name as a string.
*/
func (self *C) Name() string {
	return self.SetName
}

func fetchItemRelations(itemv reflect.Value, relations []db.Relation, convertFn func(interface{}) interface{}) error {
	var err error

	itemk := itemv.Type().Kind()

	for _, relation := range relations {

		terms := make([]interface{}, len(relation.On))

		for j, term := range relation.On {
			switch t := term.(type) {
			// Just waiting for db.Cond statements.
			case db.Cond:
				for k, v := range t {
					switch s := v.(type) {
					case string:
						matches := extRelationPattern.FindStringSubmatch(s)
						if len(matches) > 1 {
							extkey := matches[1]
							var val reflect.Value
							switch itemk {
							case reflect.Struct:
								index := GetStructFieldIndex(itemv.Type(), extkey)
								if index == nil {
									continue
								} else {
									val = itemv.FieldByIndex(index)
								}
							case reflect.Map:
								val = itemv.MapIndex(reflect.ValueOf(extkey))
							}
							if val.IsValid() {
								term = db.Cond{k: convertFn(val.Interface())}
							}
						}
					}
				}
			case db.Collection:
				relation.Collection = t
			}
			terms[j] = term
		}

		keyv := reflect.ValueOf(relation.Name)

		switch itemk {
		case reflect.Struct:
			var val reflect.Value

			index := GetStructFieldIndex(itemv.Type(), relation.Name)

			if index == nil {
				continue
			} else {
				val = itemv.FieldByIndex(index)
			}

			if val.IsValid() {
				var res db.Result

				res, err = relation.Collection.Query(terms...)

				if err != nil {
					return err
				}

				p := reflect.New(val.Type())
				q := p.Interface()

				if relation.All == true {
					err = res.All(q)
				} else {
					err = res.One(q)
				}

				if err != nil {
					return err
				}

				val.Set(reflect.Indirect(p))

			}
		case reflect.Map:
			var err error
			var res db.Result
			var p reflect.Value

			res, err = relation.Collection.Query(terms...)

			if err != nil {
				return err
			}

			// Executing external query.
			if relation.All == true {
				var items []map[string]interface{}
				err = res.All(&items)
				p = reflect.ValueOf(items)
			} else {
				var item map[string]interface{}
				err = res.One(&item)
				p = reflect.ValueOf(item)
			}

			if err != nil {
				return err
			}

			itemv.SetMapIndex(keyv, p)
		}

	}

	return nil
}

func (self *C) FetchRelation(dst interface{}, relations []db.Relation, convertFn func(interface{}) interface{}) error {
	var err error

	if relations == nil {
		return nil
	}

	dstv := reflect.ValueOf(dst)

	if dstv.Kind() != reflect.Ptr || dstv.IsNil() {
		return db.ErrExpectingPointer
	}

	err = fetchItemRelations(dstv.Elem(), relations, convertFn)

	if err != nil {
		return err
	}

	return nil
}

func (self *C) FetchRelations(dst interface{}, relations []db.Relation, convertFn func(interface{}) interface{}) error {
	var err error

	if relations == nil {
		return nil
	}

	err = ValidateSliceDestination(dst)

	dstv := reflect.ValueOf(dst)
	itemv := dstv.Elem()

	// Iterate over results.
	for i := 0; i < dstv.Elem().Len(); i++ {
		item := itemv.Index(i)
		err = fetchItemRelations(item, relations, convertFn)
		if err != nil {
			return err
		}
	}

	return nil
}

func ValidateSliceDestination(dst interface{}) error {

	var dstv reflect.Value
	var itemv reflect.Value
	var itemk reflect.Kind

	// Checking input
	dstv = reflect.ValueOf(dst)

	if dstv.IsNil() || dstv.Kind() != reflect.Ptr {
		return db.ErrExpectingPointer
	}

	if dstv.Elem().Kind() != reflect.Slice {
		return db.ErrExpectingSlicePointer
	}

	itemv = dstv.Elem()
	itemk = itemv.Type().Elem().Kind()

	if itemk != reflect.Struct && itemk != reflect.Map {
		return db.ErrExpectingSliceMapStruct
	}

	return nil
}

func StringToType(src string, dstt reflect.Type) (reflect.Value, error) {
	var srcv reflect.Value
	switch dstt {
	case durationType:
		srcv = reflect.ValueOf(to.Duration(src))
	case timeType:
		// Destination is time.Time
		srcv = reflect.ValueOf(to.Time(src))
	default:
		return StringToKind(src, dstt.Kind())
	}
	return srcv, nil
}

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
