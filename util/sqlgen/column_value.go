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

func (self ColumnValue) Hash() string {
	return `ColumnValue(` + self.Column.Hash() + `;` + self.Operator + `;` + self.Value.Hash() + `)`
}

func (self ColumnValue) Compile(layout *Template) (compiled string) {

	if c, ok := layout.Read(self); ok {
		return c
	}

	data := columnValue_s{
		self.Column.Compile(layout),
		self.Operator,
		self.Value.Compile(layout),
	}

	compiled = mustParse(layout.ColumnValue, data)

	layout.Write(self, compiled)

	return
}

type ColumnValues []ColumnValue

func (self ColumnValues) Hash() string {
	hash := make([]string, 0, len(self))
	for i := range self {
		hash = append(hash, self[i].Hash())
	}
	return `ColumnValues(` + strings.Join(hash, `,`) + `)`
}

func (self ColumnValues) Compile(layout *Template) (compiled string) {

	if c, ok := layout.Read(self); ok {
		return c
	}

	l := len(self)

	out := make([]string, l)

	for i := 0; i < l; i++ {
		out[i] = self[i].Compile(layout)
	}

	compiled = strings.Join(out, layout.IdentifierSeparator)

	layout.Write(self, compiled)

	return
}
