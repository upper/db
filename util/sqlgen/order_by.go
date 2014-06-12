package sqlgen

import "strings"

type SortColumn struct {
	Column
	Sort
}

type sortColumn_s struct {
	Column string
	Sort   string
}

type SortColumns []SortColumn

func (self SortColumns) Compile(layout *Template) string {
	l := len(self)
	s := make([]string, 0, l)
	for i := 0; i < l; i++ {
		s = append(s, self[i].Compile(layout))
	}
	return strings.Join(s, layout.IdentifierSeparator)
}

func (self SortColumn) Compile(layout *Template) string {
	data := sortColumn_s{
		Column: self.Column.Compile(layout),
		Sort:   self.Sort.Compile(layout),
	}
	return mustParse(layout.SortByColumnLayout, data)
}

type OrderBy struct {
	SortColumns
}

type orderBy_s struct {
	SortColumns string
}

func (self OrderBy) Compile(layout *Template) string {
	if len(self.SortColumns) > 0 {
		data := orderBy_s{
			SortColumns: self.SortColumns.Compile(layout),
		}
		return mustParse(layout.OrderByLayout, data)
	}
	return ""
}

type Sort uint8

const (
	SqlSortNone = iota
	SqlSortAsc
	SqlSortDesc
)

func (self Sort) Compile(layout *Template) string {
	switch self {
	case SqlSortAsc:
		return layout.AscKeyword
	case SqlSortDesc:
		return layout.DescKeyword
	}
	return ""
}
