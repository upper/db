package sqlgen

import (
	"strconv"
	"strings"
	"upper.io/cache"
)

type Statement struct {
	Type
	Table
	Database cc
	Limit
	Offset
	Columns cc
	Values
	ColumnValues cc
	OrderBy      cc
	GroupBy      cc
	Extra
	Where
}

type statementT struct {
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

func (layout *Template) compile(c cc) string {
	if c != nil {
		return c.Compile(layout)
	}
	return ""
}

func getHash(h cache.Hashable) string {
	if h != nil {
		return h.Hash()
	}
	return ""
}

func (self Statement) Hash() string {

	parts := strings.Join([]string{
		strconv.Itoa(int(self.Type)),
		self.Table.Hash(),
		getHash(self.Database),
		strconv.Itoa(int(self.Limit)),
		strconv.Itoa(int(self.Offset)),
		self.Columns.Hash(),
		self.Values.Hash(),
		getHash(self.ColumnValues),
		self.OrderBy.Hash(),
		getHash(self.GroupBy),
		string(self.Extra),
		self.Where.Hash(),
	}, ";")

	return `Statement(` + parts + `)`
}

func (self *Statement) Compile(layout *Template) (compiled string) {

	if c, ok := layout.Read(self); ok {
		return c
	}

	data := statementT{
		Table:        layout.compile(self.Table),
		Database:     layout.compile(self.Database),
		Limit:        self.Limit,
		Offset:       self.Offset,
		Columns:      layout.compile(self.Columns),
		Values:       layout.compile(self.Values),
		ColumnValues: layout.compile(self.ColumnValues),
		OrderBy:      layout.compile(self.OrderBy),
		GroupBy:      layout.compile(self.GroupBy),
		Extra:        string(self.Extra),
		Where:        layout.compile(self.Where),
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
