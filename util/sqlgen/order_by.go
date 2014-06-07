package sqlgen

type OrderBy struct {
	Columns
	Sort
}

func (self OrderBy) String() string {
	if self.Columns.Len() > 0 {
		return mustParse(sqlOrderByLayout, self)
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
		return sqlAscKeyword
	case SqlSortDesc:
		return sqlDescKeyword
	}
	return ""
}
