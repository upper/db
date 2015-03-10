package sqlgen

import (
	"database/sql/driver"
	"fmt"
	"log"
	"strings"
)

type Values []Value

type Value struct {
	Val interface{}
}

func (self Value) Hash() string {
	switch t := self.Val.(type) {
	case cc:
		return `Value(` + t.Hash() + `)`
	case string:
		return `Value(` + t + `)`
	}
	return fmt.Sprintf(`Value(%v)`, self.Val)
}

func (self Value) Compile(layout *Template) (compiled string) {

	if c, ok := layout.Read(self); ok {
		return c
	}

	if raw, ok := self.Val.(Raw); ok {
		compiled = raw.Raw
	} else {
		compiled = mustParse(layout.ValueQuote, Raw{fmt.Sprintf(`%v`, self.Val)})
	}

	layout.Write(self, compiled)

	return
}

func (self Value) Scan(src interface{}) error {
	log.Println("Scan(", src, ") on", self.Val)
	return nil
}

func (self Value) Value() (driver.Value, error) {
	log.Println("Value() on", self.Val)
	return self.Val, nil
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
