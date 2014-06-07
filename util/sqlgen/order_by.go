package sqlgen

type OrderBy struct {
	Columns
	Sort
}

func (self OrderBy) String() string {
	if self.Columns.Len() > 0 {
		return mustParse(Layout.OrderByLayout, self)
	}
	return ""
}

type Sort struct {
	v uint8
}

const (
	SqlSortNone = iota
	SqlSortAsc
	SqlSortDesc
)

func (self Sort) String() string {
	switch self.v {
	case SqlSortAsc:
		return Layout.AscKeyword
	case SqlSortDesc:
		return Layout.DescKeyword
	}
	return ""
}
