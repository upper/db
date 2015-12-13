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
	"reflect"
)

// This map holds a copy of all registered adapters.
var wrappers = make(map[string]Database)

// Register associates an adapter's name with a type. Panics if the adapter
// name is empty or the adapter is nil.
func Register(name string, adapter Database) {

	if name == `` {
		panic(`Missing adapter name.`)
	}

	if _, ok := wrappers[name]; ok != false {
		panic(`db.Register() called twice for adapter: ` + name)
	}

	wrappers[name] = adapter
}

// Open configures a database session using the given adapter's name and the
// provided settings.
func Open(adapter string, conn ConnectionURL) (Database, error) {

	driver, ok := wrappers[adapter]

	if ok == false {
		// Using panic instead of returning error because attemping to use an
		// adapter that does not exists will never result in success.
		panic(fmt.Sprintf(`Open: Unknown adapter %s. (see: https://upper.io/db.v2#database-adapters)`, adapter))
	}

	// Creating a new connection everytime Open() is called.
	driverType := reflect.ValueOf(driver).Elem().Type()
	newAdapter := reflect.New(driverType).Interface().(Database)

	// Setting up the connection.
	if err := newAdapter.Setup(conn); err != nil {
		return nil, err
	}

	return newAdapter, nil
}
