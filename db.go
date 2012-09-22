/*
  Copyright (c) 2012 José Carlos Nieto, http://xiam.menteslibres.org/

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

package db

import (
	"fmt"
	"github.com/gosexy/sugar"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Handles conditions and operators in an expression.
//
// Examples:
//
// Cond { "age": 18 } // Means the condition is to have the "age" field equal to 18.
//
// Cond { "age $lt": 18 } // $lt is a MongoDB operator, if you're using MongoDB, means that you want the "age" field to be lower than 18.
//
// Cond { "age >=": 18 } // >= is a SQL operator, if you're using SQL, means that you want the "age" field to be mayor or equal to 18.
type Cond map[string]interface{}

// Handles "And", "Or" and "Cond" types in an expression.
//
// Example:
//
// And (
//   Cond { "name": "Peter" },
//   Cond { "last_name": "Parker "},
// )
type And []interface{}

// Handles "And", "Or" and "Cond" types.
//
// Example:
//
// Or (
//   Cond { "year": 2012 },
//   Cond { "year": 1987 },
// )
type Or []interface{}

// Determines the order of returned Items in Find() or FindAll() expressions.
//
// Example:
//
// Sort { "age": -1 } // If using MongoDB, means sort by age in descending order.
//
// Sort { "age": "ASC" } // If using SQL, means sort by age in ascending order.
type Sort map[string]interface{}

// Determines how the matched item or items are going to change in Update() and UpdateAll() expressions.
//
// Example:
//
// Modify {
//  "$inc": {
//    "counter": 1
//  }
// }
type Modify map[string]interface{}

// Specifies relations with external collections, the specific relation with the parent expression can be determined with
// the name of field on the external collection plus the name of the referred parent column between brackets, however this can be only
// used along with Cond keytypes.
//
// Example:
//
// On {
//   db.Collection("external"),
//   Cond { "external_key": "{parent_value}" }, // Relation exists where the "external_key" field is equal to the parent's "parent_value".
// }
type On []interface{}

// Specifies a one-to-one relation in Find() and FindAll() expressions. It consists of a name and an On keytype.
//
// You can use the same keytypes you would use in a normal Find() and FindAll() expressions besides a Collection, you can also use
// other nested Relate and RelateAll statements. If no Collection is given, the one with the relation name will be tried.
//
// Example:
//
// Relate {
//   "father": On {
//     db.Collection("people"),
//     Cond { "gender": "man" },
//     Cond { "id": "{parent_id}" },
//   }
// }
type Relate map[string]On

// Specifies a one-to-many relation in Find() and FindAll() expressions. It consists of a name and an On keytype.
//
// You can use the same keytypes you would use in a normal Find() and FindAll() expressions besides a Collection, you can also use
// other nested Relate and RelateAll statements. If no Collection is given, the one with the relation name will be tried.
//
// Example:
//
// RelateAll {
//   "children": On {
//     db.Collection("people"),
//     Cond { "age $lt": 12 },
//     Cond { "parent_id": "{_id}" },
//   }
// }
type RelateAll map[string]On

// Limits the number of results a FindAll() expression returns.
//
// Example:
//
//   Limit(10)
type Limit uint

// Specifies how many matched results will be skipped in a FindAll() expression before returning.
//
// Example:
//
//   Offset(10)
type Offset uint

// Determines new values for the fields on the matched item or items in Update() and UpdateAll() expressions.
//
// Example:
//
// Set {
//   "name": "New Name"
// }
type Set map[string]interface{}

// Determines new values for the fields on the matched item or items in Update() and UpdateAll() expressions, if no item is found,
// a new one will be created.
//
// Example:
//
// Upsert {
//   "name": "New Name"
// }
type Upsert map[string]interface{}

// Rows from a result.
type Item map[string]interface{}

type Id string

// Connection and authentication data.
type DataSource struct {
	Host     string
	Port     int
	Database string
	User     string
	Password string
}

// Database methods.
type Database interface {
	Driver() interface{}

	Open() error
	Close() error

	Collection(string) Collection
	Collections() []string

	Use(string) error
	Drop() error
}

// Collection methods.
type Collection interface {
	Append(...interface{}) ([]Id, error)

	Count(...interface{}) (int, error)

	Find(...interface{}) Item
	FindAll(...interface{}) []Item

	Update(...interface{}) error

	Remove(...interface{}) error

	Truncate() error
}

// Specifies which fields to return in a query.
type Fields []string

// Specifies single or multiple requests in FindAll() expressions.
type MultiFlag bool
type SqlValues []string
type SqlArgs []string

// Returns the item value as a string.
func (item Item) GetString(name string) string {
	return fmt.Sprintf("%v", item[name])
}

// Returns the item value as a Go date.
func (item Item) GetDate(name string) time.Time {
	date := time.Date(0, time.January, 0, 0, 0, 0, 0, time.UTC)

	switch item[name].(type) {
	case time.Time:
		date = item[name].(time.Time)
	case string:
		var matched bool
		value := item[name].(string)

		matched, _ = regexp.MatchString(`^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$`, value)

		if matched {
			date, _ = time.Parse("2006-01-02 15:04:05", value)
		}
	}
	return date
}

// Returns the item value as a Go duration.
func (item Item) GetDuration(name string) time.Duration {
	duration, _ := time.ParseDuration("0h0m0s")

	switch item[name].(type) {
	case time.Duration:
		duration = item[name].(time.Duration)
	case string:
		var matched bool
		var re *regexp.Regexp
		value := item[name].(string)

		matched, _ = regexp.MatchString(`^\d{2}:\d{2}:\d{2}$`, value)

		if matched {
			re, _ = regexp.Compile(`^(\d{2}):(\d{2}):(\d{2})$`)
			all := re.FindAllStringSubmatch(value, -1)

			formatted := fmt.Sprintf("%sh%sm%ss", all[0][1], all[0][2], all[0][3])
			duration, _ = time.ParseDuration(formatted)
		}
	}
	return duration
}

// Returns the item value as a Tuple.
func (item Item) GetTuple(name string) sugar.Tuple {
	tuple := sugar.Tuple{}

	switch item[name].(type) {
	case map[string]interface{}:
		for k, _ := range item[name].(map[string]interface{}) {
			tuple[k] = item[name].(map[string]interface{})[k]
		}
	case sugar.Tuple:
		tuple = item[name].(sugar.Tuple)
	}

	return tuple
}

// Returns the item value as an array.
func (item Item) GetList(name string) sugar.List {
	list := sugar.List{}

	switch item[name].(type) {
	case []interface{}:
		list = make(sugar.List, len(item[name].([]interface{})))

		for k, _ := range item[name].([]interface{}) {
			list[k] = item[name].([]interface{})[k]
		}
	}

	return list
}

// Returns the item value as an integer.
func (item Item) GetInt(name string) int64 {
	i, _ := strconv.ParseInt(fmt.Sprintf("%v", item[name]), 10, 64)
	return i
}

// Returns the item value as a floating point number.
func (item Item) GetFloat(name string) float64 {
	f, _ := strconv.ParseFloat(fmt.Sprintf("%v", item[name]), 64)
	return f
}

// Returns the item value as a boolean.
func (item Item) GetBool(name string) bool {

	if item[name] == nil {
		return false
	}

	switch item[name].(type) {
	default:
		b := strings.ToLower(fmt.Sprintf("%v", item[name]))
		if b == "" || b == "0" || b == "false" {
			return false
		}
	}

	return true
}
