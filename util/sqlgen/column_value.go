package sqlgen

import (
	"strings"
)

type ColumnValue struct {
	Column
	Operator string
	Value
}

type columnValue_s struct {
	Column   string
	Operator string
	Value    string
}

func (self ColumnValue) Compile(layout *Template) string {
	data := columnValue_s{
		self.Column.Compile(layout),
		self.Operator,
		self.Value.Compile(layout),
	}
	return mustParse(layout.ColumnValue, data)
}

type ColumnValues []ColumnValue

func (self ColumnValues) Compile(layout *Template) string {
	l := len(self)

	out := make([]string, l)

	for i := 0; i < l; i++ {
		out[i] = self[i].Compile(layout)
	}

	return strings.Join(out, layout.IdentifierSeparator)
}
