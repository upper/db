package sqlgen

import (
	"fmt"
)

var (
	_ = fmt.Stringer(&Raw{})
)

// Raw represents a value that is meant to be used in a query without escaping.
type Raw struct {
	Value string // Value should not be modified after assigned.
	hash  string
}

// RawValue creates and returns a new raw value.
func RawValue(v string) *Raw {
	return &Raw{Value: v}
}

// Hash returns a unique identifier.
func (r *Raw) Hash() string {
	if r.hash == "" {
		r.hash = `Raw{Value:"` + r.Value + `"}`
	}
	return r.hash
}

// Compile returns the raw value.
func (r *Raw) Compile(*Template) string {
	return r.Value
}

// String returns the raw value.
func (r *Raw) String() string {
	return r.Value
}
