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

package mongo

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConnectionURL(t *testing.T) {
	c := ConnectionURL{}

	// Default connection string is only the protocol.
	assert.Equal(t, "", c.String(), "Expecting default connectiong string to be empty")

	// Adding a database name.
	c.Database = "myfilename"
	assert.Equal(t, "mongodb://myfilename", c.String())

	// Adding an option.
	c.Options = map[string]string{
		"cache": "foobar",
		"mode":  "ro",
	}

	// Adding username and password
	c.User = "user"
	c.Password = "pass"

	// Setting host.
	c.Host = "localhost"

	assert.Equal(t, "mongodb://user:pass@localhost/myfilename?cache=foobar&mode=ro", c.String())

	// Setting host and port.
	c.Host = "localhost:27017"

	assert.Equal(t, "mongodb://user:pass@localhost:27017/myfilename?cache=foobar&mode=ro", c.String())

	// Setting cluster.
	c.Host = "localhost,1.2.3.4,example.org:1234"

	assert.Equal(t, "mongodb://user:pass@localhost,1.2.3.4,example.org:1234/myfilename?cache=foobar&mode=ro", c.String())

	// Setting another database.
	c.Database = "another_database"

	assert.Equal(t, "mongodb://user:pass@localhost,1.2.3.4,example.org:1234/another_database?cache=foobar&mode=ro", c.String())
}

func TestParseConnectionURL(t *testing.T) {
	var u ConnectionURL
	var s string
	var err error

	s = "mongodb:///mydatabase"

	u, err = ParseURL(s)
	require.NoError(t, err)

	assert.Equal(t, "mydatabase", u.Database)

	s = "mongodb://user:pass@localhost,1.2.3.4,example.org:1234/another_database?cache=foobar&mode=ro"

	u, err = ParseURL(s)
	require.NoError(t, err)

	assert.Equal(t, "another_database", u.Database)
	assert.Equal(t, "foobar", u.Options["cache"])
	assert.Equal(t, "ro", u.Options["mode"])
	assert.Equal(t, "user", u.User)
	assert.Equal(t, "pass", u.Password)
	assert.Equal(t, "localhost,1.2.3.4,example.org:1234", u.Host)

	s = "http://example.org"
	_, err = ParseURL(s)
	require.Error(t, err)
}
