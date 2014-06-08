package sqlgen

import (
	"bytes"
	"text/template"
)

type Type uint

const (
	SqlTruncate = iota
	SqlDropTable
	SqlDropDatabase
	SqlSelectCount
	SqlInsert
	SqlSelect
	SqlUpdate
	SqlDelete
)

type (
	Limit  int
	Offset int
	Extra  string
)

func mustParse(text string, data interface{}) string {
	var b bytes.Buffer

	t := template.Must(template.New("").Parse(text))

	if err := t.Execute(&b, data); err != nil {
		panic("t.Execute: " + err.Error())
	}

	return b.String()
}

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
	Extra
	Where
}

func (self *Statement) Compile() string {
	switch self.Type {
	case SqlTruncate:
		return mustParse(Layout.TruncateLayout, self)
	case SqlDropTable:
		return mustParse(Layout.DropTableLayout, self)
	case SqlDropDatabase:
		return mustParse(Layout.DropDatabaseLayout, self)
	case SqlSelectCount:
		return mustParse(Layout.SelectCountLayout, self)
	case SqlSelect:
		return mustParse(Layout.SelectLayout, self)
	case SqlDelete:
		return mustParse(Layout.DeleteLayout, self)
	case SqlUpdate:
		return mustParse(Layout.UpdateLayout, self)
	case SqlInsert:
		return mustParse(Layout.InsertLayout, self)
	}
	return ""
}
