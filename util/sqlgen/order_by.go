package sqlgen

import (
	"strings"
)

type SortColumn struct {
	Column
	Sort
}

type sortColumn_s struct {
	Column string
	Sort   string
}

type SortColumns []SortColumn

func (self SortColumn) Hash() string {
	return `SortColumn(` + self.Column.Hash() + `;` + self.Sort.Hash() + `)`
}

func (self SortColumns) Hash() string {
	hash := make([]string, 0, len(self))
	for i := range self {
		hash = append(hash, self[i].Hash())
	}
	return `SortColumns(` + strings.Join(hash, `,`) + `)`
}

func (self SortColumns) Compile(layout *Template) string {
	l := len(self)
	s := make([]string, 0, l)
	for i := 0; i < l; i++ {
		s = append(s, self[i].Compile(layout))
	}
	return strings.Join(s, layout.IdentifierSeparator)
}

func (self SortColumn) Compile(layout *Template) (compiled string) {

	if c, ok := layout.Read(self); ok {
		return c
	}

	data := sortColumn_s{
		Column: self.Column.Compile(layout),
		Sort:   self.Sort.Compile(layout),
	}

	compiled = mustParse(layout.SortByColumnLayout, data)

	layout.Write(self, compiled)
	return
}

type OrderBy struct {
	SortColumns
}

type orderBy_s struct {
	SortColumns string
}

func (self OrderBy) Hash() string {
	return `OrderBy(` + self.SortColumns.Hash() + `)`
}

func (self OrderBy) Compile(layout *Template) (compiled string) {

	if c, ok := layout.Read(self); ok {
		return c
	}

	if len(self.SortColumns) > 0 {
		data := orderBy_s{
			SortColumns: self.SortColumns.Compile(layout),
		}
		compiled = mustParse(layout.OrderByLayout, data)
	}

	layout.Write(self, compiled)

	return
}

type Sort uint8

const (
	SqlSortNone = iota
	SqlSortAsc
	SqlSortDesc
)

func (self Sort) Hash() string {
	switch self {
	case SqlSortAsc:
		return `Sort(1)`
	case SqlSortDesc:
		return `Sort(2)`
	}
	return `Sort(0)`
}

func (self Sort) Compile(layout *Template) string {
	switch self {
	case SqlSortAsc:
		return layout.AscKeyword
	case SqlSortDesc:
		return layout.DescKeyword
	}
	return ""
}
