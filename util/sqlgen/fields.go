package sqlgen

import (
	"strings"
)

type Columns struct {
	v []Column
}

func (self Columns) String() string {
	out := make([]string, len(self.v))

	for i := range self.v {
		out[i] = self.v[i].String()
	}

	return strings.Join(out, sqlColumnComma)
}

func (self Columns) Len() int {
	return len(self.v)
}
