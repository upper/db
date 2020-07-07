// Copyright (c) 2012-present The upper.io/db authors. All rights reserved.
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

// Session is an interface that defines methods for database adapters.
type Session interface {
	// ConnectionURL returns the DSN that was used to set up the adapter.
	ConnectionURL() ConnectionURL

	// Name returns the name of the database.
	Name() string

	// Ping returns an error if the DBMS could not be reached.
	Ping() error

	// Collection receives a table name and returns a collection reference. The
	// information retrieved from a collection is cached.
	Collection(name string) Collection

	// Collections returns a collection reference of all non system tables on the
	// database.
	Collections() ([]Collection, error)

	Save(model Model) error
	Get(model Model, id interface{}) error

	// Reset resets all the caching mechanisms the adapter is using.
	Reset()

	// Close terminates the currently active connection to the DBMS and clears
	// all caches.
	Close() error

	// Driver returns the underlying driver of the adapter as an interface.
	//
	// In order to actually use the driver, the `interface{}` value needs to be
	// casted into the appropriate type.
	//
	// Example:
	//  internalSQLDriver := sess.Driver().(*sql.DB)
	Driver() interface{}

	Settings
}
