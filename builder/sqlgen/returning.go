package sqlgen

import (
	"fmt"
)

// Returning represents a RETURNING clause.
type Returning struct {
	*Columns
	hash string
}

// Hash returns a unique identifier.
func (r Returning) Hash() string {
	if r.hash == "" {
		s := r.Columns.Hash()
		if s != "" {
			r.hash = fmt.Sprintf("Returning{%s}", s)
		}
	}
	return r.hash
}

// ReturningColumns creates and returns an array of Column.
func ReturningColumns(columns ...Fragment) *Returning {
	return &Returning{Columns: &Columns{Columns: columns}}
}

// Compile transforms the clause into its equivalent SQL representation.
func (r *Returning) Compile(layout *Template) (compiled string) {
	if z, ok := layout.Read(r); ok {
		return z
	}

	compiled = r.Columns.Compile(layout)

	layout.Write(r, compiled)

	return
}
