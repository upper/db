package sqlgen

import (
	"strconv"
)

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

func (self Statement) Hash() string {
	hash := `Statement(` +
		strconv.Itoa(int(self.Type)) + `;` +
		self.Table.Hash() + `;` +
		self.Database.Hash() + `;` +
		strconv.Itoa(int(self.Limit)) + `;` +
		strconv.Itoa(int(self.Offset)) + `;` +
		self.Columns.Hash() + `;` +
		self.Values.Hash() + `;` +
		self.ColumnValues.Hash() + `;` +
		self.OrderBy.Hash() + `;` +
		self.GroupBy.Hash() + `;` +
		string(self.Extra) + `;` +
		self.Where.Hash() +
		`)`
	return hash
}

func (self *Statement) Compile(layout *Template) (compiled string) {

	if c, ok := layout.Read(self); ok {
		return c
	}

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
	}

	layout.Write(self, compiled)

	return compiled
}
