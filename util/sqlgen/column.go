package sqlgen

import (
	"strings"
)

type Column struct {
	v string
}

func (self Column) String() string {
	chunks := strings.Split(self.v, sqlColumnSeparator)

	for i := range chunks {
		chunks[i] = mustParse(sqlEscape, Raw{chunks[i]})
	}

	return strings.Join(chunks, sqlColumnSeparator)
}
