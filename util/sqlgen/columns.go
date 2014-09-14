package sqlgen

import (
	"strings"
)

type Columns []Column

func (self Columns) Hash() string {
	hash := make([]string, 0, len(self))
	for i := range self {
		hash = append(hash, self[i].Hash())
	}
	return `Columns(` + strings.Join(hash, `,`) + `)`
}

func (self Columns) Compile(layout *Template) (compiled string) {

	if c, ok := layout.Read(self); ok {
		return c
	}

	l := len(self)

	if l > 0 {
		out := make([]string, l)

		for i := 0; i < l; i++ {
			out[i] = self[i].Compile(layout)
		}

		compiled = strings.Join(out, layout.IdentifierSeparator)
	}

	layout.Write(self, compiled)

	return
}
