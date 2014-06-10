package sqlgen

import (
	"regexp"
	"strings"
)

var (
	reTableSeparator = regexp.MustCompile(`\s*?,\s*?`)
	reAliasSeparator = regexp.MustCompile(`(?i:\s+AS\s+)`)
)

type table_t struct {
	Name  string
	Alias string
}

type Table struct {
	Name string
}

func quotedTableName(input string) string {
	input = strings.TrimSpace(input)

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

	return mustParse(Layout.TableAliasLayout, table_t{name, alias})
}

func (self Table) String() string {

	parts := reTableSeparator.Split(self.Name, -1)

	l := len(parts)

	for i := 0; i < l; i++ {
		parts[i] = quotedTableName(parts[i])
	}

	return strings.Join(parts, Layout.IdentifierSeparator)
}
