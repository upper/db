package sqlgen

import (
	"fmt"
)

type Database struct {
	Value string
}

func (self Database) Hash() string {
	return `Database(` + self.Value + `)`
}

func (self Database) Compile(layout *Template) (compiled string) {
	if c, ok := layout.Read(self); ok {
		return c
	}

	compiled = mustParse(layout.IdentifierQuote, Raw{fmt.Sprintf(`%v`, self.Value)})

	layout.Write(self, compiled)

	return
}
