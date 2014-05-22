package sqlgen

import (
	"fmt"
)

type Value struct {
	v interface{}
}

func (self Value) String() string {
	if raw, ok := self.v.(Raw); ok {
		return raw.Raw
	}

	return mustParse(sqlEscape, Raw{fmt.Sprintf(`%v`, self.v)})
}
