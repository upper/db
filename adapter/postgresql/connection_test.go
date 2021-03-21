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

package postgresql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConnectionURL(t *testing.T) {
	c := ConnectionURL{}

	// Default connection string is empty.
	assert.Equal(t, "", c.String(), "Expecting default connectiong string to be empty")

	// Adding a host with port.
	c.Host = "localhost:1234"
	assert.Equal(t, "host=localhost port=1234 sslmode=disable statement_cache_capacity=0", c.String())

	// Adding a host.
	c.Host = "localhost"
	assert.Equal(t, "host=localhost sslmode=disable statement_cache_capacity=0", c.String())

	// Adding a username.
	c.User = "Anakin"
	assert.Equal(t, `host=localhost sslmode=disable statement_cache_capacity=0 user=Anakin`, c.String())

	// Adding a password with special characters.
	c.Password = "Some Sort of ' Password"
	assert.Equal(t, `host=localhost password=Some\ Sort\ of\ \'\ Password sslmode=disable statement_cache_capacity=0 user=Anakin`, c.String())

	// Adding a port.
	c.Host = "localhost:1234"
	assert.Equal(t, `host=localhost password=Some\ Sort\ of\ \'\ Password port=1234 sslmode=disable statement_cache_capacity=0 user=Anakin`, c.String())

	// Adding a database.
	c.Database = "MyDatabase"
	assert.Equal(t, `dbname=MyDatabase host=localhost password=Some\ Sort\ of\ \'\ Password port=1234 sslmode=disable statement_cache_capacity=0 user=Anakin`, c.String())

	// Adding options.
	c.Options = map[string]string{
		"sslmode": "verify-full",
	}
	assert.Equal(t, `dbname=MyDatabase host=localhost password=Some\ Sort\ of\ \'\ Password port=1234 sslmode=verify-full statement_cache_capacity=0 user=Anakin`, c.String())
}

func TestParseConnectionURL(t *testing.T) {

	{
		s := "postgres://anakin:skywalker@localhost/jedis"
		u, err := ParseURL(s)
		assert.NoError(t, err)

		assert.Equal(t, "anakin", u.User)
		assert.Equal(t, "skywalker", u.Password)
		assert.Equal(t, "localhost", u.Host)
		assert.Equal(t, "jedis", u.Database)
		assert.Zero(t, u.Options["sslmode"], "Failed to parse SSLMode.")
	}

	{
		// case with port
		s := "postgres://anakin:skywalker@localhost:1234/jedis"
		u, err := ParseURL(s)
		assert.NoError(t, err)
		assert.Equal(t, "anakin", u.User)
		assert.Equal(t, "skywalker", u.Password)
		assert.Equal(t, "jedis", u.Database)
		assert.Equal(t, "localhost:1234", u.Host)
		assert.Zero(t, u.Options["sslmode"], "Failed to parse SSLMode.")
	}

	{
		s := "postgres://anakin:skywalker@localhost/jedis?sslmode=verify-full"
		u, err := ParseURL(s)
		assert.NoError(t, err)
		assert.Equal(t, "verify-full", u.Options["sslmode"])
	}

	{
		s := "user=anakin password=skywalker host=localhost dbname=jedis"
		u, err := ParseURL(s)
		assert.NoError(t, err)
		assert.Equal(t, "anakin", u.User)
		assert.Equal(t, "skywalker", u.Password)
		assert.Equal(t, "jedis", u.Database)
		assert.Equal(t, "localhost", u.Host)
		assert.Zero(t, u.Options["sslmode"], "Failed to parse SSLMode.")
	}

	{
		s := "user=anakin password=skywalker host=localhost dbname=jedis sslmode=verify-full"
		u, err := ParseURL(s)
		assert.NoError(t, err)
		assert.Equal(t, "verify-full", u.Options["sslmode"])
	}

	{
		s := "user=anakin password=skywalker host=localhost dbname=jedis sslmode=verify-full timezone=UTC"
		u, err := ParseURL(s)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(u.Options), "Expecting exactly two options")
		assert.Equal(t, "verify-full", u.Options["sslmode"])
		assert.Equal(t, "UTC", u.Options["timezone"])
	}
}
