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
	"errors"
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

/*
	Returns true if a table column looks like a struct field.
*/
func CompareColumnToField(s, c string) bool {
	s = columnCompareExclude.ReplaceAllString(s, "")
	c = columnCompareExclude.ReplaceAllString(c, "")
	return strings.ToLower(s) == strings.ToLower(c)
}

/*
	Returns the table name as a string.
*/
func (self *C) Name() string {
	return self.SetName
}

func Fetch(dst interface{}, item db.Item) error {

	/*
		At this moment it is not possible to create a slice of a given element
		type: https://code.google.com/p/go/issues/detail?id=2339

		When it gets available this function should change, it must rely on
		FetchAll() the same way Find() relies on FindAll().
	*/

	dstv := reflect.ValueOf(dst)

	if dstv.Kind() != reflect.Ptr || dstv.IsNil() {
		return fmt.Errorf("Fetch() expects a pointer.")
	}

	el := dstv.Elem().Type()

	switch el.Kind() {
	case reflect.Struct:
		for column, _ := range item {
			f := func(s string) bool {
				return CompareColumnToField(s, column)
			}
			v := dstv.Elem().FieldByNameFunc(f)
			if v.IsValid() {
				v.Set(reflect.ValueOf(item[column]))
			}
		}
	case reflect.Map:
		dstv.Elem().Set(reflect.ValueOf(item))
	default:
		return fmt.Errorf("Expecting a pointer to map or struct, got %s.", el.Kind())
	}

	return nil
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
								f := func(s string) bool {
									return CompareColumnToField(s, extkey)
								}
								val = itemv.FieldByNameFunc(f)
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

			f := func(s string) bool {
				return CompareColumnToField(s, relation.Name)
			}

			val := itemv.FieldByNameFunc(f)

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
		return errors.New("Expecting a pointer.")
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

	if dstv.Kind() != reflect.Ptr || dstv.IsNil() || dstv.Elem().Kind() != reflect.Slice {
		return errors.New("Expecting a pointer to slice.")
	}

	itemv = dstv.Elem()
	itemk = itemv.Type().Elem().Kind()

	if itemk != reflect.Struct && itemk != reflect.Map {
		return errors.New("Expecting a pointer to slice of maps or structs.")
	}

	return nil
}

func ConvertValue(src string, dstk reflect.Kind) (reflect.Value, error) {
	var srcv reflect.Value

	// Destination type.
	switch dstk {
	case reflect.Interface:
		// Destination is interface, nuff said.
		srcv = reflect.ValueOf(src)
	case durationType.Kind():
		// Destination is time.Duration
		srcv = reflect.ValueOf(to.Duration(src))
	case timeType.Kind():
		// Destination is time.Time
		srcv = reflect.ValueOf(to.Time(src))
	default:
		// Destination is of an unknown type.
		cv, _ := to.Convert(src, dstk)
		srcv = reflect.ValueOf(cv)
	}

	return srcv, nil
}
