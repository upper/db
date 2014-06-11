package sqlgen

import "strings"

type SortColumn struct {
	Column
	Sort
}

type SortColumns []SortColumn

func (self SortColumns) String() string {
	l := len(self)
	s := make([]string, 0, l)
	for i := 0; i < l; i++ {
		s = append(s, self[i].String())
	}
	return strings.Join(s, layout.IdentifierSeparator)
}

func (self SortColumn) String() string {
	return mustParse(layout.SortByColumnLayout, self)
}

type OrderBy struct {
	SortColumns
}

func (self OrderBy) String() string {
	if len(self.SortColumns) > 0 {
		return mustParse(layout.OrderByLayout, self)
	}
	return ""
}

type Sort uint8

const (
	SqlSortNone = iota
	SqlSortAsc
	SqlSortDesc
)

func (self Sort) String() string {
	switch self {
	case SqlSortAsc:
		return layout.AscKeyword
	case SqlSortDesc:
		return layout.DescKeyword
	}
	return ""
}
