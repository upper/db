package sqlgen

import (
	"fmt"
)

type Database struct {
	Value string
}

func (self Database) Compile(layout *Template) string {
	return mustParse(layout.IdentifierQuote, Raw{fmt.Sprintf(`%v`, self.Value)})
}
