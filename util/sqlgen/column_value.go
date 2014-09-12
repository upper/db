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

func (self ColumnValue) Compile(layout *Template) (compiled string) {

	if s, ok := layout.Cache(self); ok {
		return s
	}

	data := columnValue_s{
		self.Column.Compile(layout),
		self.Operator,
		self.Value.Compile(layout),
	}

	compiled = mustParse(layout.ColumnValue, data)

	return
}

type ColumnValues []ColumnValue

func (self ColumnValues) Compile(layout *Template) (compiled string) {

	/*
		if s, ok := layout.Cache(self); ok {
			return s
		}
	*/

	l := len(self)

	out := make([]string, l)

	for i := 0; i < l; i++ {
		out[i] = self[i].Compile(layout)
	}

	compiled = strings.Join(out, layout.IdentifierSeparator)

	return
}
