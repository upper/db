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

package logger

import (
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"

	"upper.io/db.v2"
)

var (
	reInvisibleChars       = regexp.MustCompile(`[\s\r\n\t]+`)
	reColumnCompareExclude = regexp.MustCompile(`[^a-zA-Z0-9]`)
)

func init() {
	if os.Getenv(db.EnvEnableDebug) != "" {
		db.Debug = true
	}
}

type message struct {
	Query string
	Args  []interface{}
	Err   error
	Start int64
	End   int64
}

func (m *message) Print() {
	m.Query = reInvisibleChars.ReplaceAllString(m.Query, ` `)
	m.Query = strings.TrimSpace(m.Query)

	s := make([]string, 0, 3)

	if m.Query != "" {
		s = append(s, fmt.Sprintf(`Q: %s`, m.Query))
	}

	if len(m.Args) > 0 {
		s = append(s, fmt.Sprintf(`A: %v`, m.Args))
	}

	if m.Err != nil {
		s = append(s, fmt.Sprintf(`E: %q`, m.Err))
	}

	s = append(s, fmt.Sprintf(`T: %0.5fs`, float64(m.End-m.Start)/float64(1e9)))

	log.Printf("\n\t%s\n\n", strings.Join(s, "\n\t"))
}

// Log prints the given query to stdout in readable format.
func Log(query string, args []interface{}, err error, start int64, end int64) {
	if db.Debug {
		m := message{query, args, err, start, end}
		m.Print()
	}
}
