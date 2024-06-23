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

package mssql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConnectionURL(t *testing.T) {
	c := ConnectionURL{}

	// Zero value equals to an empty string.
	assert.Equal(t, "", c.String(), "Expecting default connectiong string to be empty")

	// Adding a database name.
	c.Database = "mydbname"
	assert.Equal(t, "sqlserver://127.0.0.1?database=mydbname", c.String())

	// Adding an option.
	c.Options = map[string]string{
		"connection timeout": "30",
		"param1":             "value1",
		"instance":           "instance1",
	}

	assert.Equal(t, "sqlserver://127.0.0.1/instance1?connection+timeout=30&database=mydbname&param1=value1", c.String())

	// Setting default options
	c.Options = nil

	// Setting user and password.
	c.User = "user"
	c.Password = "pass"

	assert.Equal(t, `sqlserver://user:pass@127.0.0.1?database=mydbname`, c.String())

	// Setting host.
	c.Host = "1.2.3.4:1433"
	assert.Equal(t, `sqlserver://user:pass@1.2.3.4:1433?database=mydbname`, c.String())
}

func TestParseConnectionURL(t *testing.T) {
	var u ConnectionURL
	var s string
	var err error

	s = "sqlserver://user:pass@127.0.0.1:1433?connection+timeout=30&database=mydbname&param1=value1"

	u, err = ParseURL(s)
	require.NoError(t, err)

	assert.Equal(t, "user", u.User)
	assert.Equal(t, "pass", u.Password)
	assert.Equal(t, "127.0.0.1:1433", u.Host)
	assert.Equal(t, "mydbname", u.Database)
}
