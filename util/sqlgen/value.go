package sqlgen

import (
	"database/sql/driver"
	"fmt"
	"log"
	"strings"
)

type Values []Value

type Value struct {
	V    interface{}
	hash string
}

func NewValue(v interface{}) *Value {
	return &Value{V: v}
}

func (v *Value) Hash() string {
	if v.hash == "" {
		switch t := v.V.(type) {
		case cc:
			v.hash = `Value(` + t.Hash() + `)`
		case string:
			v.hash = `Value(` + t + `)`
		default:
			v.hash = fmt.Sprintf(`Value(%v)`, v.V)
		}
	}
	return v.hash
}

func (v *Value) Compile(layout *Template) (compiled string) {

	if z, ok := layout.Read(v); ok {
		return z
	}

	if raw, ok := v.V.(Raw); ok {
		compiled = raw.Compile(layout)
	} else if raw, ok := v.V.(cc); ok {
		compiled = raw.Compile(layout)
	} else {
		compiled = mustParse(layout.ValueQuote, NewRaw(fmt.Sprintf(`%v`, v.V)))
	}

	layout.Write(v, compiled)

	return
}

func (v *Value) Scan(src interface{}) error {
	log.Println("Scan(", src, ") on", v.V)
	return nil
}

func (v *Value) Value() (driver.Value, error) {
	log.Println("Value() on", v.V)
	return v.V, nil
}

func (vs Values) Hash() string {
	hash := make([]string, 0, len(vs))
	for i := range vs {
		hash = append(hash, vs[i].Hash())
	}
	return `Values(` + strings.Join(hash, `,`) + `)`
}

func (vs Values) Compile(layout *Template) (compiled string) {
	if c, ok := layout.Read(vs); ok {
		return c
	}

	l := len(vs)
	if l > 0 {
		chunks := make([]string, 0, l)
		for i := 0; i < l; i++ {
			chunks = append(chunks, vs[i].Compile(layout))
		}
		compiled = strings.Join(chunks, layout.ValueSeparator)
	}
	layout.Write(vs, compiled)
	return
}

func (vs Values) Scan(src interface{}) error {
	log.Println("Values.Scan(", src, ")")
	return nil
}

func (vs Values) Value() (driver.Value, error) {
	log.Println("Values.Value()")
	return vs, nil
}
