package sqlgen

import (
	"strings"
)

type Column struct {
	Value string
}

func (self Column) String() string {
	chunks := strings.Split(self.Value, Layout.ColumnSeparator)

	for i := range chunks {
		chunks[i] = mustParse(Layout.IdentifierQuote, Raw{chunks[i]})
	}

	return strings.Join(chunks, Layout.ColumnSeparator)
}
