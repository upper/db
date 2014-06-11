package sqlgen

import (
	"fmt"
)

type Database struct {
	Value string
}

func (self Database) String() string {
	return mustParse(layout.IdentifierQuote, Raw{fmt.Sprintf(`%v`, self.Value)})
}
