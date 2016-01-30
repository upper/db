// Copyright (c) 2012-2016 The upper.io/db.v1 authors. All rights reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining
// a copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to
// permit persons to whom the Software is furnished to do so, subject to
// the following conditions:
//
// The above copyright notice and this permission notice shall be
// included in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
// LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
// OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
// WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

package ql

const (
	qlColumnSeparator     = `.`
	qlIdentifierSeparator = `, `
	qlIdentifierQuote     = `{{.Raw}}`
	qlValueSeparator      = `, `
	qlValueQuote          = `"{{.}}"`
	qlAndKeyword          = `&&`
	qlOrKeyword           = `||`
	qlNotKeyword          = `!=`
	qlDescKeyword         = `DESC`
	qlAscKeyword          = `ASC`
	qlDefaultOperator     = `==`
	qlClauseGroup         = `({{.}})`
	qlClauseOperator      = ` {{.}} `
	qlColumnValue         = `{{.Column}} {{.Operator}} {{.Value}}`
	qlTableAliasLayout    = `{{.Name}}{{if .Alias}} AS {{.Alias}}{{end}}`
	qlColumnAliasLayout   = `{{.Name}}{{if .Alias}} AS {{.Alias}}{{end}}`
	qlSortByColumnLayout  = `{{.Column}} {{.Sort}}`

	qlOrderByLayout = `
		{{if .SortColumns}}
			ORDER BY {{.SortColumns}}
		{{end}}
	`

	qlWhereLayout = `
		{{if .Conds}}
			WHERE {{.Conds}}
		{{end}}
	`

	qlSelectLayout = `
		SELECT

			{{if .Columns}}
				{{.Columns}}
			{{else}}
				*
			{{end}}

			FROM {{.Table}}

			{{.Where}}

			{{.GroupBy}}

			{{.OrderBy}}

			{{if .Limit}}
				LIMIT {{.Limit}}
			{{end}}

			{{if .Offset}}
				OFFSET {{.Offset}}
			{{end}}
	`
	qlDeleteLayout = `
		DELETE
			FROM {{.Table}}
			{{.Where}}
	`
	qlUpdateLayout = `
		UPDATE
			{{.Table}}
		SET {{.ColumnValues}}
			{{ .Where }}
	`

	qlSelectCountLayout = `
		SELECT
			count(1) AS total
		FROM {{.Table}}
			{{.Where}}

			{{if .Limit}}
				LIMIT {{.Limit}}
			{{end}}

			{{if .Offset}}
				OFFSET {{.Offset}}
			{{end}}
	`

	qlInsertLayout = `
		INSERT INTO {{.Table}}
			({{.Columns}})
		VALUES
			({{.Values}})
		{{.Extra}}
	`

	qlTruncateLayout = `
		TRUNCATE TABLE {{.Table}}
	`

	qlDropDatabaseLayout = `
		DROP DATABASE {{.Database}}
	`

	qlDropTableLayout = `
		DROP TABLE {{.Table}}
	`

	qlGroupByLayout = `
		{{if .GroupColumns}}
			GROUP BY {{.GroupColumns}}
		{{end}}
	`
)
