// Copyright (c) 2012-today The upper.io/db authors. All rights reserved.
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

package mockdb

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestConnectionURL(t *testing.T) {
	c := ConnectionURL{}
	assert.NotEmpty(t, c.String())
	assert.Equal(t, "mockdb://mockdb", c.String())

	c.Database = "database"
	assert.Equal(t, "mockdb://database", c.String())

	c.Options = map[string]string{
		"cache": "foobar",
		"mode":  "ro",
	}
	assert.Equal(t, "mockdb://database?cache=foobar&mode=ro", c.String())
}

func TestParseConnectionURL(t *testing.T) {
	s := "mockdb://mydatabase.db"
	u, err := ParseURL(s)
	assert.NoError(t, err)
	assert.Equal(t, "mydatabase.db", u.Database)
}
