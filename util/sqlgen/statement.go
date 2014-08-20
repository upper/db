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

func (self *Statement) Compile(layout *Template) string {

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
		return mustParse(layout.TruncateLayout, data)
	case SqlDropTable:
		return mustParse(layout.DropTableLayout, data)
	case SqlDropDatabase:
		return mustParse(layout.DropDatabaseLayout, data)
	case SqlSelectCount:
		return mustParse(layout.SelectCountLayout, data)
	case SqlSelect:
		return mustParse(layout.SelectLayout, data)
	case SqlDelete:
		return mustParse(layout.DeleteLayout, data)
	case SqlUpdate:
		return mustParse(layout.UpdateLayout, data)
	case SqlInsert:
		return mustParse(layout.InsertLayout, data)
	}
	return ""
}
