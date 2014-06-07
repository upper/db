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
		SELECT {{.Columns}}

			FROM {{.Source}}

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
			FROM {{.Source}}
			{{if .Where}}
				WHERE {{.Where}}
			{{end}}
	`
	sqlUpdateLayout = `
		UPDATE
			{{.Source}}
		SET {{.ColumnValues}}
			{{if .Where}}
				WHERE {{.Where}}
			{{end}}
	`

	sqlSelectCountLayout = `
		SELECT
			COUNT(1) AS _t
		FROM {{.Source}}
			{{if .Where}}
				WHERE {{.Where}}
			{{end}}
	`

	sqlInsertLayout = `
		INSERT INTO {{.Source}}
			({{.Columns}})
		VALUES
			({{.Values}})
	`

	sqlTruncateLayout = `
		TRUNCATE TABLE {{.Source}}
	`

	sqlDropDatabaseLayout = `
		DROP DATABASE {{.Database}}
	`

	sqlDropTableLayout = `
		DROP TABLE {{.Source}}
	`

	sqlTautology       = `1 = 1`
	sqlAllFields       = `*`
	sqlAndKeyword      = `AND`
	sqlOrKeyword       = `OR`
	sqlDefaultOperator = `=`
	sqlDescKeyword     = `DESC`
	sqlAscKeyword      = `ASC`
	sqlConditionGroup  = `({{.}})`

	sqlColumnValue = `{{.Column}} {{.Operator}} {{.Value}}`

	sqlFunction = `{{.Function}}({{.Value}})`
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
	Source
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
