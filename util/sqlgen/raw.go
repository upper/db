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

func NewRaw(v string) *Raw {
	return &Raw{Value: v}
}

func (r *Raw) Hash() string {
	if r.hash == "" {
		r.hash = `sqlgen.Raw{Value:"` + r.Value + `"}`
	}
	return r.hash
}

func (r *Raw) Compile(*Template) string {
	return r.Value
}

func (r *Raw) String() string {
	return r.Value
}
