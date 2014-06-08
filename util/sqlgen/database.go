package sqlgen

import (
	"fmt"
)

type Database struct {
	Value string
}

func (self Database) String() string {
	return mustParse(Layout.IdentifierQuote, Raw{fmt.Sprintf(`%v`, self.Value)})
}
