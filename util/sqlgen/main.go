package sqlgen

import (
	"bytes"
	"text/template"
)

const (
	sqlColumnSeparator     = `.`
	sqlIdentifierSeparator = `, `
	sqlIdentifierQuote     = `"{{.Raw}}"`
	sqlValueSeparator      = `, `
	sqlValueQuote          = `'{{.}}'`

	sqlAndKeyword      = `AND`
	sqlOrKeyword       = `OR`
	sqlNotKeyword      = `NOT`
	sqlDescKeyword     = `DESC`
	sqlAscKeyword      = `ASC`
	sqlDefaultOperator = `=`
	sqlClauseGroup     = `({{.}})`
	sqlClauseOperator  = ` {{.}} `
	sqlColumnValue     = `{{.Column}} {{.Operator}} {{.Value}}`

	sqlOrderByLayout = `
		{{if .Columns}}
			ORDER BY {{.Columns}} {{.Sort}}
		{{end}}
	`

	sqlWhereLayout = `
		{{if .Conds}}
			WHERE {{.Conds}}
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

			{{.Where}}

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
			{{.Where}}
	`
	sqlUpdateLayout = `
		UPDATE
			{{.Table}}
		SET {{.ColumnValues}}
			{{ .Where }}
	`

	sqlSelectCountLayout = `
		SELECT
			COUNT(1) AS _t
		FROM {{.Table}}
			{{.Where}}
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
