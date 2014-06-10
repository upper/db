package sqlgen

import (
	"strings"
)

type column_t struct {
	Name  string
	Alias string
}

type Column struct {
	Value string
}

func (self Column) String() string {
	input := strings.TrimSpace(self.Value)

	chunks := reAliasSeparator.Split(input, 2)

	name := chunks[0]

	nameChunks := strings.SplitN(name, Layout.ColumnSeparator, 2)

	for i := range nameChunks {
		nameChunks[i] = strings.TrimSpace(nameChunks[i])
		nameChunks[i] = mustParse(Layout.IdentifierQuote, Raw{nameChunks[i]})
	}

	name = strings.Join(nameChunks, Layout.ColumnSeparator)

	var alias string

	if len(chunks) > 1 {
		alias = strings.TrimSpace(chunks[1])
		alias = mustParse(Layout.IdentifierQuote, Raw{alias})
	}

	return mustParse(Layout.ColumnAliasLayout, column_t{name, alias})
}
