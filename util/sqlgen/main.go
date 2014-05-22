package sqlgen

import (
	"bytes"
	"fmt"
	"text/template"
)

const (
	sqlColumnSeparator = `.`
	sqlColumnComma     = `, `
	sqlEscape          = `"{{.Raw}}"`

	sqlSelectLayout = `
		SELECT {{.Fields}} FROM {{.Source}}
			{{if .Where}}
				WHERE {{.Where}}
			{{end}}
			{{if .OrderBy}}
				ORDER BY {{.OrderBy}} {{.Sort}}
			{{end}}
			{{if .Limit}}
				LIMIT {{.Limit}}
			{{end}}
			{{if .Offset}}
				OFFSET {{.Offset}}
			{{end}}
	`
	sqlDeleteLayout = `
		DELETE FROM {{.Source}}
			{{if .Where}}
				WHERE {{.Where}}
			{{end}}
	`
	sqlUpdateLayout = `
		UPDATE
			{{.Source}}
		SET {{.FieldValues}}
	`

	sqlCountLayout = `
		SELECT
			COUNT(1) AS total
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

	sqlTruncate = `
		TRUNCATE TABLE {{.Source}}
	`

	sqlDropDatabase = `
		DROP DATABASE {{.Database}}
	`

	sqlTautology       = `1 = 1`
	sqlAllFields       = `*`
	sqlAnd             = `AND`
	sqlOr              = `OR`
	sqlDefaultOperator = `=`

	sqlColumnValue = `{{.Column}} {{.Operator}} {{.Value}}`

	sqlFunction = `{{.Function}}({{.Value}})`
)

type (
	Fields      []string
	Source      string
	Limit       int
	Offset      int
	Sort        string
	OrderBy     string
	FieldValues map[string]string
	PrimaryKey  string
	Values      []string
	Operator    string
	Field       string
	Function    string
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

func (self *FieldValues) String() string {
	return ""
}

type Statement struct {
	Source
	Limit
	Offset
	Field
	Columns
	FieldValues
	PrimaryKey
	Values
	Function
}

func (self *Statement) String() string {
	return ""
}
