package sqlgen

import (
	"fmt"
	"strings"
)

type Columns struct {
	Columns []Column
	hash    string
}

func (c *Columns) Hash() string {
	if c.hash == "" {
		s := make([]string, len(c.Columns))
		for i := range c.Columns {
			s[i] = c.Columns[i].Hash()
		}
		c.hash = fmt.Sprintf("sqlgen.Columns{Columns:{%s}}", strings.Join(s, ", "))
	}
	return c.hash
}

func NewColumns(columns ...Column) *Columns {
	return &Columns{Columns: columns}
}

func (c *Columns) Compile(layout *Template) (compiled string) {

	if z, ok := layout.Read(c); ok {
		return z
	}

	l := len(c.Columns)

	if l > 0 {
		out := make([]string, l)

		for i := 0; i < l; i++ {
			out[i] = c.Columns[i].Compile(layout)
		}

		compiled = strings.Join(out, layout.IdentifierSeparator)
	}

	layout.Write(c, compiled)

	return
}
