/*
  Copyright (c) 2012-2014 José Carlos Nieto, https://menteslibres.net/xiam

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
	"reflect"
)

// Registered wrappers.
var wrappers = make(map[string]Database)

// Registers a database wrapper with a unique name.
func Register(name string, driver Database) {

	if name == "" {
		panic("Missing wrapper name.")
	}

	if _, ok := wrappers[name]; ok != false {
		panic("Register called twice for driver " + name)
	}

	wrappers[name] = driver
}

// Configures a connection to a database using the named adapter and the given
// settings.
func Open(name string, settings Settings) (Database, error) {

	driver, ok := wrappers[name]
	if ok == false {
		// Using panic instead of returning error because attemping to use an
		// nonexistent adapter will never result in a successful connection,
		// therefore should be considered a developer's mistake and must be catched
		// at compilation time.
		panic(fmt.Sprintf("Open: Unknown adapter %s. (see: https://upper.io/db#database-adapters)", name))
	}

	// Creating a new connection everytime Open() is called.
	driverType := reflect.ValueOf(driver).Elem().Type()
	newAdapter := reflect.New(driverType).Interface().(Database)

	// Setting up the connection.
	err := newAdapter.Setup(settings)
	if err != nil {
		return nil, err
	}

	return newAdapter, nil
}
