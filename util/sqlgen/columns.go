package sqlgen

import (
	"strings"
)

type Columns []Column

func (self Columns) Compile(layout *Template) string {
	l := len(self)

	if l > 0 {
		out := make([]string, l)

		for i := 0; i < l; i++ {
			out[i] = self[i].Compile(layout)
		}

		return strings.Join(out, layout.IdentifierSeparator)
	}
	return ""
}
