package sqlgen

import (
	//"database/sql/driver"
	"fmt"
	//"log"
	"strings"
)

// ValueGroups represents an array of value groups.
type ValueGroups struct {
	Values []*Values
	hash   string
}

// Values represents an array of Value.
type Values struct {
	Values []Fragment
	hash   string
}

// Value represents an escaped SQL value.
type Value struct {
	V    interface{}
	hash string
}

// NewValue creates and returns a Value.
func NewValue(v interface{}) *Value {
	return &Value{V: v}
}

// NewValueGroup creates and returns an array of values.
func NewValueGroup(v ...Fragment) *Values {
	return &Values{Values: v}
}

// Hash returns a unique identifier.
func (v *Value) Hash() string {
	if v.hash == "" {
		switch t := v.V.(type) {
		case Fragment:
			v.hash = `Value(` + t.Hash() + `)`
		case string:
			v.hash = `Value(` + t + `)`
		default:
			v.hash = fmt.Sprintf(`Value(%v)`, v.V)
		}
	}
	return v.hash
}

// Compile transforms the Value into an equivalent SQL representation.
func (v *Value) Compile(layout *Template) (compiled string) {

	if z, ok := layout.Read(v); ok {
		return z
	}

	switch t := v.V.(type) {
	case Raw:
		compiled = t.Compile(layout)
	case Fragment:
		compiled = t.Compile(layout)
	default:
		compiled = mustParse(layout.ValueQuote, RawValue(fmt.Sprintf(`%v`, v.V)))
	}

	layout.Write(v, compiled)

	return
}

/*
func (v *Value) Scan(src interface{}) error {
	log.Println("Scan(", src, ") on", v.V)
	return nil
}

func (v *Value) Value() (driver.Value, error) {
	log.Println("Value() on", v.V)
	return v.V, nil
}
*/

// Hash returns a unique identifier.
func (vs *Values) Hash() string {
	if vs.hash == "" {
		hash := make([]string, len(vs.Values))
		for i := range vs.Values {
			hash[i] = vs.Values[i].Hash()
		}
		vs.hash = `Values(` + strings.Join(hash, `,`) + `)`
	}
	return vs.hash
}

// Compile transforms the Values into an equivalent SQL representation.
func (vs *Values) Compile(layout *Template) (compiled string) {
	if c, ok := layout.Read(vs); ok {
		return c
	}

	l := len(vs.Values)
	if l > 0 {
		chunks := make([]string, 0, l)
		for i := 0; i < l; i++ {
			chunks = append(chunks, vs.Values[i].Compile(layout))
		}
		compiled = mustParse(layout.ClauseGroup, strings.Join(chunks, layout.ValueSeparator))
	}
	layout.Write(vs, compiled)
	return
}

/*
func (vs Values) Scan(src interface{}) error {
	log.Println("Values.Scan(", src, ")")
	return nil
}

func (vs Values) Value() (driver.Value, error) {
	log.Println("Values.Value()")
	return vs, nil
}
*/

// Hash returns a unique identifier.
func (vg *ValueGroups) Hash() string {
	if vg.hash == "" {
		l := len(vg.Values)
		hashes := make([]string, l)
		for i := 0; i < l; i++ {
			hashes[i] = vg.Values[i].Hash()
		}
		vg.hash = fmt.Sprintf(`ValueGroups(%v)`, strings.Join(hashes, ", "))
	}
	return vg.hash
}

// Compile transforms the ValueGroups into an equivalent SQL representation.
func (vg *ValueGroups) Compile(layout *Template) (compiled string) {
	if c, ok := layout.Read(vg); ok {
		return c
	}

	l := len(vg.Values)
	if l > 0 {
		chunks := make([]string, 0, l)
		for i := 0; i < l; i++ {
			chunks = append(chunks, vg.Values[i].Compile(layout))
		}
		compiled = strings.Join(chunks, layout.ValueSeparator)
	}

	layout.Write(vg, compiled)
	return
}

// JoinValueGroups creates a new *ValueGroups object.
func JoinValueGroups(values ...*Values) *ValueGroups {
	return &ValueGroups{Values: values}
}
