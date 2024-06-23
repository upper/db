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

package ql

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConnectionURL(t *testing.T) {
	c := ConnectionURL{}

	assert.Zero(t, c.String())

	// Adding a database name.
	c.Database = "file://myfilename"
	absoluteName, _ := filepath.Abs(c.Database)

	assert.Equal(t, "file://"+absoluteName, c.String())

	// Adding an option.
	c.Options = map[string]string{
		"cache": "foobar",
		"mode":  "ro",
	}

	assert.Equal(t, "file://"+absoluteName+"?cache=foobar&mode=ro", c.String())

	// Setting another database.
	c.Database = "/another/database"
	assert.Equal(t, `file:///another/database?cache=foobar&mode=ro`, c.String())
}

func TestParseConnectionURL(t *testing.T) {
	var u ConnectionURL
	var s string
	var err error

	s = "file://mydatabase.db"
	u, err = ParseURL(s)
	assert.NoError(t, err)
	assert.Equal(t, "mydatabase.db", u.Database)

	s = "file:///path/to/my/database.db?mode=ro&cache=foobar"
	u, err = ParseURL(s)
	assert.NoError(t, err)
	assert.Equal(t, "/path/to/my/database.db", u.Database)

	s = "memory:///path/to/my/database.db?mode=ro&cache=foobar"
	u, err = ParseURL(s)
	assert.NoError(t, err)
	assert.Equal(t, "/path/to/my/database.db", u.Database)

	assert.Equal(t, "foobar", u.Options["cache"])
	assert.Equal(t, "ro", u.Options["mode"])

	s = "http://example.org"
	u, err = ParseURL(s)
	assert.Error(t, err)
	assert.Zero(t, u.Database)
}
