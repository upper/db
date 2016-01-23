package sqlgen

import (
	"strings"
)

// Columns represents an array of Column.
type Columns struct {
	Columns []Fragment
	hash    MemHash
}

// Hash returns a unique identifier.
func (c *Columns) Hash() string {
	return c.hash.Hash(c)
}

// JoinColumns creates and returns an array of Column.
func JoinColumns(columns ...Fragment) *Columns {
	return &Columns{Columns: columns}
}

// OnConditions creates and retuens a new On.
func OnConditions(conditions ...Fragment) *On {
	return &On{Conditions: conditions}
}

// UsingColumns builds a Using from the given columns.
func UsingColumns(columns ...Fragment) *Using {
	return &Using{Columns: columns}
}

// Compile transforms the Columns into an equivalent SQL representation.
func (c *Columns) Compile(layout *Template) (compiled string) {

	if z, ok := layout.Read(c); ok {
		return z
	}

	l := len(c.Columns)

	if l > 0 {
		out := make([]string, l)

		for i := 0; i < l; i++ {
			out[i] = c.Columns[i].Compile(layout)
		}

		compiled = strings.Join(out, layout.IdentifierSeparator)
	}

	layout.Write(c, compiled)

	return
}
