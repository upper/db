package sqlgen

import (
	"fmt"
	"strings"
)

type column_t struct {
	Name  string
	Alias string
}

type Column struct {
	Value interface{}
}

func (self Column) Hash() string {
	switch t := self.Value.(type) {
	case cc:
		return `Column(` + t.Hash() + `)`
	case string:
		return `Column(` + t + `)`
	}
	return fmt.Sprintf(`Column(%v)`, self.Value)
}

func (self Column) Compile(layout *Template) (compiled string) {

	if c, ok := layout.Read(self); ok {
		return c
	}

	switch value := self.Value.(type) {
	case string:
		// input := strings.TrimSpace(value)
		input := trimString(value)

		//chunks := reAliasSeparator.Split(input, 2)
		chunks := separateByAS(input)

		if len(chunks) == 1 {
			//chunks = reSpaceSeparator.Split(input, 2)
			chunks = separateBySpace(input)
		}

		name := chunks[0]

		nameChunks := strings.SplitN(name, layout.ColumnSeparator, 2)

		for i := range nameChunks {
			// nameChunks[i] = strings.TrimSpace(nameChunks[i])
			nameChunks[i] = trimString(nameChunks[i])
			nameChunks[i] = mustParse(layout.IdentifierQuote, Raw{nameChunks[i]})
		}

		name = strings.Join(nameChunks, layout.ColumnSeparator)

		var alias string

		if len(chunks) > 1 {
			// alias = strings.TrimSpace(chunks[1])
			alias = trimString(chunks[1])
			alias = mustParse(layout.IdentifierQuote, Raw{alias})
		}

		compiled = mustParse(layout.ColumnAliasLayout, column_t{name, alias})
	case Raw:
		compiled = value.String()
	default:
		compiled = fmt.Sprintf("%v", self.Value)
	}

	layout.Write(self, compiled)

	return
}
