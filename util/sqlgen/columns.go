package sqlgen

import (
	"strings"
)

type Columns []Column

func (self Columns) String() string {
	l := len(self)

	if l > 0 {
		out := make([]string, l)

		for i := 0; i < l; i++ {
			out[i] = self[i].String()
		}

		return strings.Join(out, sqlColumnComma)
	}
	return ""
}

func (self Columns) Len() int {
	return len(self)
}
