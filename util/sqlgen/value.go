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

func (self *Value) Hash() string {
	switch t := self.V.(type) {
	case cc:
		return `Value(` + t.Hash() + `)`
	case string:
		return `Value(` + t + `)`
	}
	return fmt.Sprintf(`Value(%v)`, self.V)
}

func (self *Value) Compile(layout *Template) (compiled string) {

	if z, ok := layout.Read(self); ok {
		return z
	}

	if raw, ok := self.V.(Raw); ok {
		compiled = raw.Value
	} else {
		compiled = mustParse(layout.ValueQuote, &Raw{Value: fmt.Sprintf(`%v`, self.V)})
	}

	layout.Write(self, compiled)

	return
}

func (self *Value) Scan(src interface{}) error {
	log.Println("Scan(", src, ") on", self.V)
	return nil
}

func (self *Value) Value() (driver.Value, error) {
	log.Println("Value() on", self.V)
	return self.V, nil
}

func (self Values) Hash() string {
	hash := make([]string, 0, len(self))
	for i := range self {
		hash = append(hash, self[i].Hash())
	}
	return `Values(` + strings.Join(hash, `,`) + `)`
}

func (self Values) Compile(layout *Template) (compiled string) {
	if c, ok := layout.Read(self); ok {
		return c
	}

	l := len(self)
	if l > 0 {
		chunks := make([]string, 0, l)
		for i := 0; i < l; i++ {
			chunks = append(chunks, self[i].Compile(layout))
		}
		compiled = strings.Join(chunks, layout.ValueSeparator)
	}
	layout.Write(self, compiled)
	return
}

func (self Values) Scan(src interface{}) error {
	log.Println("Values.Scan(", src, ")")
	return nil
}

func (self Values) Value() (driver.Value, error) {
	log.Println("Values.Value()")
	return self, nil
}
