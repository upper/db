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

// Package schema provides basic information on relational database schemas.
package schema

import (
	"sync"
)

// DatabaseSchema represents a collection of tables.
type DatabaseSchema struct {
	name   string
	tables map[string]*TableSchema

	mu sync.RWMutex
}

// TableSchema represents a single table.
type TableSchema struct {
	pk []string

	mu sync.RWMutex
}

// NewDatabaseSchema creates and returns a database schema.
func NewDatabaseSchema() *DatabaseSchema {
	s := &DatabaseSchema{
		tables: make(map[string]*TableSchema),
	}
	return s
}

// Name returns the name of the database.
func (s *DatabaseSchema) Name() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.name
}

// Set name sets the name of the database.
func (s *DatabaseSchema) SetName(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.name = name
}

// Table retrives a table from the schema.
func (s *DatabaseSchema) Table(name string) *TableSchema {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.tables[name]; !ok {
		s.tables[name] = &TableSchema{}
	}

	return s.tables[name]
}

func (t *TableSchema) SetPrimaryKeys(pk []string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if len(pk) == 0 {
		t.pk = []string{} // if nil or empty array
		return
	}

	t.pk = pk
}

func (t *TableSchema) PrimaryKeys() []string {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.pk
}
