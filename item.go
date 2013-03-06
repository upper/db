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
	"github.com/gosexy/to"
	"time"
)

/*
	These methods have been deprecated, you could use github.com/gosexy/to for
	conversion sugar
*/

// Deprecated: Returns the item value as a string.
func (self *Item) GetString(key string) string {
	return to.String((*self)[key])
}

// Deprecated: Returns the item value as a Go date.
func (self *Item) GetDate(key string) time.Time {
	return to.Time((*self)[key])
}

// Deprecated: Returns the item value as a Go duration.
func (self *Item) GetDuration(key string) time.Duration {
	return to.Duration((*self)[key])
}

// Deprecated: Returns the item value as a map[string] interface{}.
func (self *Item) GetMap(key string) map[string]interface{} {
	return to.Map((*self)[key])
}

// Deprecated: Returns the item value as a []interface{}.
func (self *Item) GetList(key string) []interface{} {
	return to.List((*self)[key])
}

// Deprecated: Returns the item value as an integer.
func (self *Item) GetInt(key string) int64 {
	return to.Int64((*self)[key])
}

// Deprecated: Returns the item value as an integer.
func (self *Item) GetUint(key string) uint64 {
	return to.Uint64((*self)[key])
}

// Deprecated: Returns the item value as a floating point number.
func (self *Item) GetFloat(key string) float64 {
	return to.Float64((*self)[key])
}

// Deprecated: Returns the item value as a boolean.
func (self *Item) GetBool(key string) bool {
	return to.Bool((*self)[key])
}
