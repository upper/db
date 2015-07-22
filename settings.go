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

// Settings holds database connection and authentication data.  Not all fields
// are mandatory, if any field is skipped, the database adapter will either try
// to use database defaults or return an error. Refer to the specific adapter
// to see which fields are required.
//
// Example:
//
// 	db.Settings{
// 		Host: "127.0.0.1",
// 		Database: "tests",
// 		User: "john",
// 		Password: "doe",
// 	}
type Settings struct {
	// Database server hostname or IP. This field is ignored if using unix
	// sockets or if the database does not require a connection to any host
	// (SQLite, QL).
	Host string
	// Database server port. This field is ignored if using unix sockets or if
	// the database does not require a connection to any host (SQLite, QL). If
	// not provided, the default database port is tried.
	Port int
	// Name of the database. You can also use a filename if the database supports
	// opening a raw file (SQLite, QL).
	Database string
	// Username for authentication, if required.
	User string
	// Password for authentication, if required.
	Password string
	// A path to a UNIX socket file. Leave blank if you rather use host and port.
	Socket string
	// Database charset. You can leave this field blank to use the default
	// database charset.
	Charset string
}

// String is provided for backwards compatibility.
func (s Settings) String() string {
	return ""
}
