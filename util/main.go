// Copyright (c) 2012-2015 José Carlos Nieto, https://menteslibres.net/xiam
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

package util

import (
	"regexp"
	"strings"
)

var reColumnCompareExclude = regexp.MustCompile(`[^a-zA-Z0-9]`)

type tagOptions map[string]bool

func parseTagOptions(s string) tagOptions {
	opts := make(tagOptions)
	chunks := strings.Split(s, `,`)
	for _, chunk := range chunks {
		opts[strings.TrimSpace(chunk)] = true
	}
	return opts
}

// ParseTag splits a struct tag into comma separated chunks. The first chunk is
// returned as a string value, remaining chunks are considered enabled options.
func ParseTag(tag string) (string, tagOptions) {
	// Based on http://golang.org/src/pkg/encoding/json/tags.go
	if i := strings.Index(tag, `,`); i != -1 {
		return tag[:i], parseTagOptions(tag[i+1:])
	}
	return tag, parseTagOptions(``)
}

// NormalizeColumn prepares a column for comparison against another column.
func NormalizeColumn(s string) string {
	return strings.ToLower(reColumnCompareExclude.ReplaceAllString(s, ""))
}
