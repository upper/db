package sqlgen

import (
	"fmt"
)

type Table struct {
	v string
}

func (self Table) String() string {
	return mustParse(sqlIdentifierQuote, Raw{fmt.Sprintf(`%v`, self.v)})
}
