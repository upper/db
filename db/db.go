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

type Where map[string] interface{}
type And []interface{}
type Or []interface{}
type Sort map[string] interface{}
type Modify map[string] interface{}

type Relate map[string] interface{}
type On []interface{}

type Multi bool
type CountFlag bool

type Limit uint
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

