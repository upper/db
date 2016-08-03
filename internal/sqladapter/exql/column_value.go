package exql

import (
	"strings"
)

// ColumnValue represents a bundle between a column and a corresponding value.
type ColumnValue struct {
	Column   Fragment
	Operator string
	Value    Fragment
	hash     hash
}

type columnValueT struct {
	Column   string
	Operator string
	Value    string
}

// Hash returns a unique identifier for the struct.
func (c *ColumnValue) Hash() string {
	return c.hash.Hash(c)
}

// Compile transforms the ColumnValue into an equivalent SQL representation.
func (c *ColumnValue) Compile(layout *Template) (compiled string) {

	if z, ok := layout.Read(c); ok {
		return z
	}

	data := columnValueT{
		c.Column.Compile(layout),
		c.Operator,
		c.Value.Compile(layout),
	}

	compiled = mustParse(layout.ColumnValue, data)

	layout.Write(c, compiled)

	return
}

// ColumnValues represents an array of ColumnValue
type ColumnValues struct {
	ColumnValues []Fragment
	hash         hash
}

// JoinColumnValues returns an array of ColumnValue
func JoinColumnValues(values ...Fragment) *ColumnValues {
	return &ColumnValues{ColumnValues: values}
}

// Insert adds a column to the columns array.
func (c *ColumnValues) Insert(values ...Fragment) *ColumnValues {
	for _, f := range values {
		c.ColumnValues = append(c.ColumnValues, f)
	}
	c.hash.Reset()
	return c
}

// Hash returns a unique identifier for the struct.
func (c *ColumnValues) Hash() string {
	return c.hash.Hash(c)
}

// Compile transforms the ColumnValues into its SQL representation.
func (c *ColumnValues) Compile(layout *Template) (compiled string) {

	if z, ok := layout.Read(c); ok {
		return z
	}

	l := len(c.ColumnValues)

	out := make([]string, l)

	for i := range c.ColumnValues {
		out[i] = c.ColumnValues[i].Compile(layout)
	}

	compiled = strings.Join(out, layout.IdentifierSeparator)

	layout.Write(c, compiled)

	return
}
