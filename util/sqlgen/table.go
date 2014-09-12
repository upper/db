package sqlgen

import (
	"regexp"
	"strings"
)

var (
	reTableSeparator = regexp.MustCompile(`\s*?,\s*?`)
	reAliasSeparator = regexp.MustCompile(`(?i:\s+AS\s+)`)
	reSpaceSeparator = regexp.MustCompile(`\s+`)
)

type table_t struct {
	Name  string
	Alias string
}

type Table struct {
	Name string
}

func quotedTableName(layout *Template, input string) string {
	input = strings.TrimSpace(input)

	chunks := reAliasSeparator.Split(input, 2)

	if len(chunks) == 1 {
		chunks = reSpaceSeparator.Split(input, 2)
	}

	name := chunks[0]

	nameChunks := strings.SplitN(name, layout.ColumnSeparator, 2)

	for i := range nameChunks {
		nameChunks[i] = strings.TrimSpace(nameChunks[i])
		nameChunks[i] = mustParse(layout.IdentifierQuote, Raw{nameChunks[i]})
	}

	name = strings.Join(nameChunks, layout.ColumnSeparator)

	var alias string

	if len(chunks) > 1 {
		alias = strings.TrimSpace(chunks[1])
		alias = mustParse(layout.IdentifierQuote, Raw{alias})
	}

	return mustParse(layout.TableAliasLayout, table_t{name, alias})
}

func (self Table) Hash() string {
	return self.Name
}

func (self Table) Compile(layout *Template) (compiled string) {

	// Splitting tables by a comma
	parts := reTableSeparator.Split(self.Name, -1)

	l := len(parts)

	for i := 0; i < l; i++ {
		parts[i] = quotedTableName(layout, parts[i])
	}

	compiled = strings.Join(parts, layout.IdentifierSeparator)

	return compiled
}
