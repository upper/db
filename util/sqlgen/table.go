package sqlgen

import (
	"fmt"
	"strings"
)

type table_t struct {
	Name  string
	Alias string
}

type Table struct {
	Name interface{}
}

func quotedTableName(layout *Template, input string) string {
	input = trimString(input)

	// chunks := reAliasSeparator.Split(input, 2)
	chunks := separateByAS(input)

	if len(chunks) == 1 {
		// chunks = reSpaceSeparator.Split(input, 2)
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

	return mustParse(layout.TableAliasLayout, table_t{name, alias})
}

func (self Table) Hash() string {
	switch t := self.Name.(type) {
	case cc:
		return `Table(` + t.Hash() + `)`
	case string:
		return `Table(` + t + `)`
	}
	return fmt.Sprintf(`Table(%v)`, self.Name)
}

func (self Table) Compile(layout *Template) (compiled string) {

	if c, ok := layout.Read(self); ok {
		return c
	}

	switch value := self.Name.(type) {
	case string:
		if self.Name == "" {
			return
		}

		// Splitting tables by a comma
		parts := separateByComma(value)

		l := len(parts)

		for i := 0; i < l; i++ {
			parts[i] = quotedTableName(layout, parts[i])
		}

		compiled = strings.Join(parts, layout.IdentifierSeparator)
	case Raw:
		compiled = value.String()
	}

	layout.Write(self, compiled)

	return
}
