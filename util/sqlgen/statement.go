package sqlgen

type Statement struct {
	Type
	Table
	Database
	Limit
	Offset
	Columns
	Values
	ColumnValues
	OrderBy
	GroupBy
	Extra
	Where
}

type statement_s struct {
	Table    string
	Database string
	Limit
	Offset
	Columns      string
	Values       string
	ColumnValues string
	OrderBy      string
	GroupBy      string
	Extra        string
	Where        string
}

func (self *Statement) Compile(layout *Template) (compiled string) {

	data := statement_s{
		Table:        self.Table.Compile(layout),
		Database:     self.Database.Compile(layout),
		Limit:        self.Limit,
		Offset:       self.Offset,
		Columns:      self.Columns.Compile(layout),
		Values:       self.Values.Compile(layout),
		ColumnValues: self.ColumnValues.Compile(layout),
		OrderBy:      self.OrderBy.Compile(layout),
		GroupBy:      self.GroupBy.Compile(layout),
		Extra:        string(self.Extra),
		Where:        self.Where.Compile(layout),
	}

	switch self.Type {
	case SqlTruncate:
		compiled = mustParse(layout.TruncateLayout, data)
	case SqlDropTable:
		compiled = mustParse(layout.DropTableLayout, data)
	case SqlDropDatabase:
		compiled = mustParse(layout.DropDatabaseLayout, data)
	case SqlSelectCount:
		compiled = mustParse(layout.SelectCountLayout, data)
	case SqlSelect:
		compiled = mustParse(layout.SelectLayout, data)
	case SqlDelete:
		compiled = mustParse(layout.DeleteLayout, data)
	case SqlUpdate:
		compiled = mustParse(layout.UpdateLayout, data)
	case SqlInsert:
		compiled = mustParse(layout.InsertLayout, data)
	default:
		compiled = ""
	}

	return compiled
}
