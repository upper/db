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

package sqlite

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConnectionURL(t *testing.T) {

	c := ConnectionURL{}

	assert.Equal(t, "", c.String(), "Expecting default connectiong string to be empty")

	// Adding a database name.
	c.Database = "myfilename"

	absoluteName, _ := filepath.Abs(c.Database)

	assert.Equal(t, "file://"+absoluteName+"?_busy_timeout=10000", c.String())

	// Adding an option.
	c.Options = map[string]string{
		"cache": "foobar",
		"mode":  "ro",
	}

	assert.Equal(t, "file://"+absoluteName+"?_busy_timeout=10000&cache=foobar&mode=ro", c.String())

	// Setting another database.
	c.Database = "/another/database"

	assert.Equal(t, "file:///another/database?_busy_timeout=10000&cache=foobar&mode=ro", c.String())
}

func TestParseConnectionURL(t *testing.T) {
	var u ConnectionURL
	var s string
	var err error

	s = "file://mydatabase.db"

	u, err = ParseURL(s)
	require.NoError(t, err)

	assert.Equal(t, "mydatabase.db", u.Database)

	assert.Equal(t, "shared", u.Options["cache"])

	s = "file:///path/to/my/database.db?_busy_timeout=10000&mode=ro&cache=foobar"

	u, err = ParseURL(s)
	require.NoError(t, err)

	assert.Equal(t, "/path/to/my/database.db", u.Database)

	assert.Equal(t, "foobar", u.Options["cache"])
	assert.Equal(t, "ro", u.Options["mode"])

	s = "http://example.org"
	_, err = ParseURL(s)
	require.Error(t, err)
}
