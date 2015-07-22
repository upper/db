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

// DatabaseSchema represents a collection of tables.
type DatabaseSchema struct {
	Name      string
	Alias     string
	Tables    []string
	TableInfo map[string]*TableSchema
}

// TableSchema represents a single table.
type TableSchema struct {
	PrimaryKey []string
	Alias      string
	Columns    []string
}

// NewDatabaseSchema creates and returns a database schema.
func NewDatabaseSchema() *DatabaseSchema {
	schema := new(DatabaseSchema)
	schema.Tables = []string{}
	schema.TableInfo = make(map[string]*TableSchema)
	return schema
}

// AddTable adds a table into the database schema. If the table does not exists
// it is created before being added.
func (d *DatabaseSchema) AddTable(name string) {
	if _, ok := d.TableInfo[name]; !ok {
		table := new(TableSchema)
		table.PrimaryKey = []string{}
		table.Columns = []string{}
		d.TableInfo[name] = table
		d.Tables = append(d.Tables, name)
	}
}

// Table retrives a table from the schema.
func (d *DatabaseSchema) Table(name string) *TableSchema {
	d.AddTable(name)
	return d.TableInfo[name]
}

// HasTable returns true if the given table is already defined within the
// schema.
func (d *DatabaseSchema) HasTable(name string) bool {
	if _, ok := d.TableInfo[name]; ok {
		return true
	}
	return false
}
