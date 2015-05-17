package sqlgen

import (
	"fmt"
	"strings"
)

type columnT struct {
	Name  string
	Alias string
}

type Column struct {
	Name interface{}
	hash string
}

func (c *Column) Hash() string {
	if c.hash == "" {
		var s string

		switch t := c.Name.(type) {
		case cc:
			s = t.Hash()
		case fmt.Stringer:
			s = t.String()
		case string:
			s = t
		default:
			s = fmt.Sprintf("%v", c.Name)
		}

		c.hash = fmt.Sprintf(`sqlgen.Column{Name:%q}`, s)
	}

	return c.hash
}

func (c *Column) Compile(layout *Template) (compiled string) {

	if z, ok := layout.Read(c); ok {
		return z
	}

	switch value := c.Name.(type) {
	case string:
		input := trimString(value)

		chunks := separateByAS(input)

		if len(chunks) == 1 {
			chunks = separateBySpace(input)
		}

		name := chunks[0]

		nameChunks := strings.SplitN(name, layout.ColumnSeparator, 2)

		for i := range nameChunks {
			nameChunks[i] = trimString(nameChunks[i])
			nameChunks[i] = mustParse(layout.IdentifierQuote, Raw{Value: nameChunks[i]})
		}

		name = strings.Join(nameChunks, layout.ColumnSeparator)

		var alias string

		if len(chunks) > 1 {
			alias = trimString(chunks[1])
			alias = mustParse(layout.IdentifierQuote, Raw{Value: alias})
		}

		compiled = mustParse(layout.ColumnAliasLayout, columnT{name, alias})
	case Raw:
		compiled = value.String()
	default:
		compiled = fmt.Sprintf("%v", c.Name)
	}

	layout.Write(c, compiled)

	return
}
