package sqlgen

import (
	"strings"
)

type Table struct {
	Value string
}

func (self Table) String() string {
	chunks := strings.Split(self.Value, Layout.ColumnSeparator)

	for i := range chunks {
		chunks[i] = mustParse(Layout.IdentifierQuote, Raw{chunks[i]})
	}

	return strings.Join(chunks, Layout.ColumnSeparator)
}
