// Copyright (c) 2012-2014 José Carlos Nieto, https://menteslibres.net/xiam
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
	"reflect"
)

// This map holds a copy of all registered adapters.
var wrappers = make(map[string]Database)

// The db.Register() function is provided for database adapters. Using
// db.Register() an adapter can make itself available by the provided name.
// The adapter name must not be an empty string and the driver must not be nil,
// otherwise db.Register() will panic.
func Register(name string, adapter Database) {

	if name == `` {
		panic(`Missing adapter name.`)
	}

	if _, ok := wrappers[name]; ok != false {
		panic(`db.Register() called twice for adapter: ` + name)
	}

	wrappers[name] = adapter
}

// Configures a database sessions using the given adapter and the given
// settings.
func Open(name string, settings Settings) (Database, error) {

	driver, ok := wrappers[name]
	if ok == false {
		// Using panic instead of returning error because attemping to use an
		// nonexistent adapter will never result in a successful connection.
		panic(fmt.Sprintf(`Open: Unknown adapter %s. (see: https://upper.io/db#database-adapters)`, name))
	}

	// Creating a new connection everytime Open() is called.
	driverType := reflect.ValueOf(driver).Elem().Type()
	newAdapter := reflect.New(driverType).Interface().(Database)

	// Setting up the connection.
	if err := newAdapter.Setup(settings); err != nil {
		return nil, err
	}

	return newAdapter, nil
}
