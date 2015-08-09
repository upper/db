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

package db

import (
	"fmt"
)

// Raw holds chunks of data to be passed to the database without any filtering.
// Use with care.
//
// When using `db.Raw{}`, the developer is responsible of providing a sanitized
// instruction to the database.
//
// The `db.Raw{}` expression is allowed as element on `db.Cond{}`, `db.And{}`,
// `db.Or{}` expressions and as argument on `db.Result.Select()` and
// `db.Collection.Find()` methods.
//
// Example:
//
//	// SQL: SOUNDEX('Hello')
//	Raw{"SOUNDEX('Hello')"}
type Raw struct {
	Value interface{}
}

// String returns a string representation of the passed raw value.
func (r Raw) String() string {
	if r.Value == nil {
		return ""
	}
	if v, ok := r.Value.(string); ok {
		return v
	}
	return fmt.Sprintf("%v", r.Value)
}
