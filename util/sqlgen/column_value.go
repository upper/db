package sqlgen

import (
	"strings"
)

type ColumnValue struct {
	Column
	Operator string
	Value
}

func (self ColumnValue) String() string {
	return mustParse(layout.ColumnValue, self)
}

type ColumnValues []ColumnValue

func (self ColumnValues) String() string {
	l := len(self)

	out := make([]string, l)

	for i := 0; i < l; i++ {
		out[i] = self[i].String()
	}

	return strings.Join(out, layout.IdentifierSeparator)
}
