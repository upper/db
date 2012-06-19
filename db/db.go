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

// "Where" is a keytype that can handle conditions and operators in an expression.
// Examples:
// Where { "age": 18 } // Means the condition is to have the "age" field equal to 18.
// Where { "age $lt": 18 } // $lt is a MongoDB operator, if you're using MongoDB, means that you want the "age" field to be lower than 18.
// Where { "age >=": 18 } // >= is a SQL operator, if you're using SQL, means that you want the "age" field to be mayor or equal to 18.
type Where map[string] interface{}

// "And" is a keytype that can handle "And", "Or" and "Where" types in an expression.
// Example:
// And (
//   Where { "name": "Peter" },
//   Where { "last_name": "Parker "},
// )
type And []interface{}

// "Or" is a keytype that can handle "And", "Or" and "Where" types.
// Example:
// Or (
//   Where { "year": 2012 },
//   Where { "year": 1987 },
// )
type Or []interface{}

// "Sort" is a keytype for determining the order of the returning Items in Find() or FindAll() expressions.
// Example:
// Sort { "age": -1 } // If using MongoDB, means sort by age in descending order.
// Sort { "age": "ASC" } // If using SQL, means sort by age in ascending order.
type Sort map[string] interface{}

// "Modify" is a keytype that determine values that are going to change in Update() and UpdateAll() expressions.
// Example:
// Modify {
//   "name": "New Name"
// }
type Modify map[string] interface{}

// "On" is a keytype that specifies relations with external collections, the specific relation with the parent expression can be
// determined with the name of field on the external collection plus the name of the referred parent column between brackets, 
// however this can be only used along with Where keytypes.
// Example:
// On {
//   db.Collection("external"),
//   Where { "external_key": "{parent_value}" }, // Relation exists where the "external_key" field is equal to the parent's "parent_value".
// } 
type On []interface{}

// "Relate" is a keytype that specifies a one-to-one relation in Find() and FindAll() expressions. It consists of a name and an On keytype.
// You can use the same keytypes you would use in a normal Find() and FindAll() expressions besides a Collection, you can also use 
// other nested Relate and RelateAll statements. If no Collection is given, the one with the relation name will be tried.
// Example: 
// Relate {
//   "father": On {
//     db.Collection("people"),
//     Where { "gender": "man" },
//     Where { "id": "{parent_id}" },
//   }
// }
type Relate map[string] On

// "RelateAll" is a keytype that specifies a one-to-many relation in Find() and FindAll() expressions. It consists of a name and an On keytype.
// You can use the same keytypes you would use in a normal Find() and FindAll() expressions besides a Collection, you can also use 
// other nested Relate and RelateAll statements. If no Collection is given, the one with the relation name will be tried.
// Example: 
// RelateAll {
//   "children": On {
//     db.Collection("people"),
//     Where { "age $lt": 12 },
//     Where { "parent_id": "{_id}" },
//   }
// }
type RelateAll map[string] On

// "Limit" is a keytype that limits the number of results a FindAll() expression returns.
// Example:
//   Limit(10)
type Limit uint

// "Offset" is a keytype that specifies how many matched results will be skipped in a FindAll() expression before returning.
// Example:
//   Offset(10)
type Offset uint

type Set map[string] interface{}
type Upsert map[string] interface{}

type Item map[string] interface {}

type DataSource struct {
  Host string
  Port int
  Database string
  User string
  Password string
}

type Database interface {
  Connect() error
  Use() error
  Collection()
  Drop() bool
  Collections() []string
}


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

type Multi bool
type CountFlag bool
