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

package sqlutil

import (
	"strings"
)

type T struct {
	Columns []string
	Tables  []string // Holds table names.
}

// MainTableName returns the name of the first table.
func (t *T) MainTableName() string {
	return t.NthTableName(0)
}

// NthTableName returns the table name at index i.
func (t *T) NthTableName(i int) string {
	if len(t.Tables) > i {
		chunks := strings.SplitN(t.Tables[i], " ", 2)
		if len(chunks) > 0 {
			return chunks[0]
		}
	}
	return ""
}

// HashTableNames returns a unique string for the given array of tables.
func HashTableNames(names []string) string {
	return strings.Join(names, "|")
	// I think we don't really need to do this, the strings.Join already provides a unique string per array.
	// return fmt.Sprintf("%x", md5.Sum([]byte(strings.Join(names, "|"))))
}
