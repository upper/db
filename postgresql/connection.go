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

package postgresql

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"

	"github.com/lib/pq"
	"upper.io/db.v2"
)

// scanner implements a tokenizer for libpq-style option strings.
type scanner struct {
	s []rune
	i int
}

// Next returns the next rune.  It returns 0, false if the end of the text has
// been reached.
func (s *scanner) Next() (rune, bool) {
	if s.i >= len(s.s) {
		return 0, false
	}
	r := s.s[s.i]
	s.i++
	return r, true
}

// SkipSpaces returns the next non-whitespace rune.  It returns 0, false if the
// end of the text has been reached.
func (s *scanner) SkipSpaces() (rune, bool) {
	r, ok := s.Next()
	for unicode.IsSpace(r) && ok {
		r, ok = s.Next()
	}
	return r, ok
}

type values map[string]string

func (vs values) Set(k, v string) {
	vs[k] = v
}

func (vs values) Get(k string) (v string) {
	return vs[k]
}

func (vs values) Isset(k string) bool {
	_, ok := vs[k]
	return ok
}

// A typical PostgreSQL connection URL looks like:
//
// "postgres://bob:secret@1.2.3.4:5432/mydb?sslmode=verify-full"

const connectionScheme = `postgres`

// ConnectionURL implements a PostgreSQL connection struct.
type ConnectionURL struct {
	User     string
	Password string
	Address  db.Address
	Database string
	Options  map[string]string
}

var escaper = strings.NewReplacer(` `, `\ `, `'`, `\'`, `\`, `\\`)

func (c ConnectionURL) String() (s string) {
	u := make([]string, 0, 6)

	// TODO: This surely needs some sort of escaping.

	if c.User != "" {
		u = append(u, "user="+escaper.Replace(c.User))
	}

	if c.Password != "" {
		u = append(u, "password="+escaper.Replace(c.Password))
	}

	if c.Address != nil {
		if h, err := c.Address.Host(); err == nil {
			u = append(u, "host="+escaper.Replace(h))
		}

		if p, err := c.Address.Port(); err == nil {
			u = append(u, "port="+strconv.Itoa(int(p)))
		}
	}

	if c.Database != "" {
		u = append(u, "dbname="+escaper.Replace(c.Database))
	}

	// Is there actually any connection data?
	if len(u) == 0 {
		return ""
	}

	if c.Options == nil {
		c.Options = map[string]string{}
	}

	// If not present, SSL mode is assumed disabled.
	if sslMode, ok := c.Options["sslmode"]; !ok || sslMode == "" {
		c.Options["sslmode"] = "disable"
	}

	for k, v := range c.Options {
		u = append(u, escaper.Replace(k)+"="+escaper.Replace(v))
	}

	return strings.Join(u, " ")
}

// ParseURL parses s into a ConnectionURL struct.
func ParseURL(s string) (u ConnectionURL, err error) {
	o := make(values)

	if strings.HasPrefix(s, "postgres://") {
		s, err = pq.ParseURL(s)
		if err != nil {
			return u, err
		}
	}

	if err := parseOpts(s, o); err != nil {
		return u, err
	}

	u.User = o.Get("user")
	u.Password = o.Get("password")

	h := o.Get("host")
	p, _ := strconv.Atoi(o.Get("port"))

	if p > 0 {
		u.Address = db.HostPort(h, uint(p))
	} else {
		u.Address = db.Host(h)
	}

	u.Database = o.Get("dbname")

	u.Options = make(map[string]string)

	for k := range o {
		switch k {
		case "user", "password", "host", "port", "dbname":
			// Skip
		default:
			u.Options[k] = o[k]
		}
	}

	return u, err
}

// parseOpts parses the options from name and adds them to the values.
//
// The parsing code is based on conninfo_parse from libpq's fe-connect.c
func parseOpts(name string, o values) error {
	s := newScanner(name)

	for {
		var (
			keyRunes, valRunes []rune
			r                  rune
			ok                 bool
		)

		if r, ok = s.SkipSpaces(); !ok {
			break
		}

		// Scan the key
		for !unicode.IsSpace(r) && r != '=' {
			keyRunes = append(keyRunes, r)
			if r, ok = s.Next(); !ok {
				break
			}
		}

		// Skip any whitespace if we're not at the = yet
		if r != '=' {
			r, ok = s.SkipSpaces()
		}

		// The current character should be =
		if r != '=' || !ok {
			return fmt.Errorf(`missing "=" after %q in connection info string"`, string(keyRunes))
		}

		// Skip any whitespace after the =
		if r, ok = s.SkipSpaces(); !ok {
			// If we reach the end here, the last value is just an empty string as per libpq.
			o.Set(string(keyRunes), "")
			break
		}

		if r != '\'' {
			for !unicode.IsSpace(r) {
				if r == '\\' {
					if r, ok = s.Next(); !ok {
						return fmt.Errorf(`missing character after backslash`)
					}
				}
				valRunes = append(valRunes, r)

				if r, ok = s.Next(); !ok {
					break
				}
			}
		} else {
		quote:
			for {
				if r, ok = s.Next(); !ok {
					return fmt.Errorf(`unterminated quoted string literal in connection string`)
				}
				switch r {
				case '\'':
					break quote
				case '\\':
					r, _ = s.Next()
					fallthrough
				default:
					valRunes = append(valRunes, r)
				}
			}
		}

		o.Set(string(keyRunes), string(valRunes))
	}

	return nil
}

// newScanner returns a new scanner initialized with the option string s.
func newScanner(s string) *scanner {
	return &scanner{[]rune(s), 0}
}
