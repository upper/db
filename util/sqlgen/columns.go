package sqlgen

import (
	"strings"
)

type Columns struct {
	v []Column
}

func (self Columns) String() string {
	if len(self.v) > 0 {
		out := make([]string, len(self.v))

		for i := range self.v {
			out[i] = self.v[i].String()
		}

		return strings.Join(out, sqlColumnComma)
	}
	return ""
}

func (self Columns) Len() int {
	return len(self.v)
}
