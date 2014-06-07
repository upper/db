package sqlgen

import (
	"bytes"
	"fmt"
	"text/template"
)

const (
	sqlColumnSeparator = `.`
	sqlColumnComma     = `, `
	sqlValueComma      = `, `
	sqlEscape          = `"{{.Raw}}"`

	sqlOrderByLayout = `
		{{if .Columns}}
			ORDER BY {{.Columns}} {{.Sort}}
		{{end}}
	`
	sqlSelectLayout = `
		SELECT

			{{if .Columns}}
				{{.Columns}}
			{{else}}
				*
			{{end}}

			FROM {{.Table}}

			{{if .Where}}
				WHERE {{.Where}}
			{{end}}

			{{.OrderBy}}

			{{if .Limit}}
				LIMIT {{.Limit}}
			{{end}}

			{{if .Offset}}
				OFFSET {{.Offset}}
			{{end}}
	`
	sqlDeleteLayout = `
		DELETE
			FROM {{.Table}}
			{{if .Where}}
				WHERE {{.Where}}
			{{end}}
	`
	sqlUpdateLayout = `
		UPDATE
			{{.Table}}
		SET {{.ColumnValues}}
			{{if .Where}}
				WHERE {{.Where}}
			{{end}}
	`

	sqlSelectCountLayout = `
		SELECT
			COUNT(1) AS _t
		FROM {{.Table}}
			{{if .Where}}
				WHERE {{.Where}}
			{{end}}
	`

	sqlInsertLayout = `
		INSERT INTO {{.Table}}
			({{.Columns}})
		VALUES
			({{.Values}})
	`

	sqlTruncateLayout = `
		TRUNCATE TABLE {{.Table}}
	`

	sqlDropDatabaseLayout = `
		DROP DATABASE {{.Database}}
	`

	sqlDropTableLayout = `
		DROP TABLE {{.Table}}
	`

	sqlAndKeyword      = `AND`
	sqlOrKeyword       = `OR`
	sqlDescKeyword     = `DESC`
	sqlAscKeyword      = `ASC`
	sqlDefaultOperator = `=`
	sqlConditionGroup  = `({{.}})`

	sqlColumnValue = `{{.Column}} {{.Operator}} {{.Value}}`
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
)

func mustParse(text string, data interface{}) string {
	var b bytes.Buffer

	t := template.Must(template.New("").Parse(text))

	if err := t.Execute(&b, data); err != nil {
		fmt.Printf("data: %v\n", data)
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
	Where
}

func (self *Statement) Compile() string {
	switch self.Type {
	case SqlTruncate:
		return mustParse(sqlTruncateLayout, self)
	case SqlDropTable:
		return mustParse(sqlDropTableLayout, self)
	case SqlDropDatabase:
		return mustParse(sqlDropDatabaseLayout, self)
	case SqlSelectCount:
		return mustParse(sqlSelectCountLayout, self)
	case SqlSelect:
		return mustParse(sqlSelectLayout, self)
	case SqlDelete:
		return mustParse(sqlDeleteLayout, self)
	case SqlUpdate:
		return mustParse(sqlUpdateLayout, self)
	case SqlInsert:
		return mustParse(sqlInsertLayout, self)
	}
	return ""
}
