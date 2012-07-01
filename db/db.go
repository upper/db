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

// Handles conditions and operators in an expression.
//
// Examples:
//
// Where { "age": 18 } // Means the condition is to have the "age" field equal to 18.
//
// Where { "age $lt": 18 } // $lt is a MongoDB operator, if you're using MongoDB, means that you want the "age" field to be lower than 18.
//
// Where { "age >=": 18 } // >= is a SQL operator, if you're using SQL, means that you want the "age" field to be mayor or equal to 18.
type Where map[string] interface{}

// Handles "And", "Or" and "Where" types in an expression.
//
// Example:
//
// And (
//   Where { "name": "Peter" },
//   Where { "last_name": "Parker "},
// )
type And []interface{}

// Handles "And", "Or" and "Where" types.
//
// Example:
//
// Or (
//   Where { "year": 2012 },
//   Where { "year": 1987 },
// )
type Or []interface{}

// Determines the order of returned Items in Find() or FindAll() expressions.
//
// Example:
//
// Sort { "age": -1 } // If using MongoDB, means sort by age in descending order.
//
// Sort { "age": "ASC" } // If using SQL, means sort by age in ascending order.
type Sort map[string] interface{}

// Determines how the matched item or items are going to change in Update() and UpdateAll() expressions.
//
// Example:
//
// Modify {
//  "$inc": {
//    "counter": 1
//  }
// }
type Modify map[string] interface{}

// Specifies relations with external collections, the specific relation with the parent expression can be determined with
// the name of field on the external collection plus the name of the referred parent column between brackets, however this can be only
// used along with Where keytypes.
//
// Example:
//
// On {
//   db.Collection("external"),
//   Where { "external_key": "{parent_value}" }, // Relation exists where the "external_key" field is equal to the parent's "parent_value".
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
//     Where { "gender": "man" },
//     Where { "id": "{parent_id}" },
//   }
// }
type Relate map[string] On

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
//     Where { "age $lt": 12 },
//     Where { "parent_id": "{_id}" },
//   }
// }
type RelateAll map[string] On

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
type Set map[string] interface{}

// Determines new values for the fields on the matched item or items in Update() and UpdateAll() expressions, if no item is found,
// a new one will be created.
//
// Example:
//
// Upsert {
//   "name": "New Name"
// }
type Upsert map[string] interface{}

// Rows from a result.
type Item map[string] interface {}

// Connection and authentication data.
type DataSource struct {
  Host string
  Port int
  Database string
  User string
  Password string
}

// Database methods.
type Database interface {
  Connect() error

  Collection(string) Collection
  Collections() []string

  Use(string) error
  Drop() error
}

// Collection methods.
type Collection interface {
  Append(...interface{}) bool

  Count(...interface{}) int

  Find(...interface{}) Item
  FindAll(...interface{}) []Item

  Update(...interface{}) bool
  UpdateAll(...interface{}) bool

  Remove(...interface{}) bool
  RemoveAll(...interface{}) bool

  Truncate() bool
}

// Specifies single or multiple requests in FindAll() expressions.
type multiFlag bool

// Specifies which fields to return in a query.
type Fields []string
