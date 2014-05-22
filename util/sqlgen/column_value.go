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
	return mustParse(sqlColumnValue, self)
}

type ColumnValues struct {
	v []ColumnValue
}

func (self ColumnValues) String() string {
	out := make([]string, len(self.v))

	for i := range self.v {
		out[i] = self.v[i].String()
	}

	return strings.Join(out, sqlColumnComma)
}
