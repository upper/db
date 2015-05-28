// Copyright (c) 2012-2015 José Carlos Nieto, https://menteslibres.net/xiam
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

package mysql

const (
	adapterColumnSeparator     = `.`
	adapterIdentifierSeparator = `, `
	adapterIdentifierQuote     = "`{{.Value}}`"
	adapterValueSeparator      = `, `
	adapterValueQuote          = `'{{.}}'`
	adapterAndKeyword          = `AND`
	adapterOrKeyword           = `OR`
	adapterNotKeyword          = `NOT`
	adapterDescKeyword         = `DESC`
	adapterAscKeyword          = `ASC`
	adapterDefaultOperator     = `=`
	adapterClauseGroup         = `({{.}})`
	adapterClauseOperator      = ` {{.}} `
	adapterColumnValue         = `{{.Column}} {{.Operator}} {{.Value}}`
	adapterTableAliasLayout    = `{{.Name}}{{if .Alias}} AS {{.Alias}}{{end}}`
	adapterColumnAliasLayout   = `{{.Name}}{{if .Alias}} AS {{.Alias}}{{end}}`
	adapterSortByColumnLayout  = `{{.Column}} {{.Order}}`

	adapterOrderByLayout = `
		{{if .SortColumns}}
			ORDER BY {{.SortColumns}}
		{{end}}
	`

	adapterWhereLayout = `
		{{if .Conds}}
			WHERE {{.Conds}}
		{{end}}
	`

	adapterSelectLayout = `
		SELECT

			{{if .Columns}}
				{{.Columns}}
			{{else}}
				*
			{{end}}

			{{if .Table}}
				FROM {{.Table}}
			{{end}}

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
	adapterDeleteLayout = `
		DELETE
			FROM {{.Table}}
			{{.Where}}
	`
	adapterUpdateLayout = `
		UPDATE
			{{.Table}}
		SET {{.ColumnValues}}
			{{ .Where }}
	`

	adapterSelectCountLayout = `
		SELECT
			COUNT(1) AS _t
		FROM {{.Table}}
			{{.Where}}

			{{if .Limit}}
				LIMIT {{.Limit}}
			{{end}}

			{{if .Offset}}
				OFFSET {{.Offset}}
			{{end}}
	`

	adapterInsertLayout = `
		INSERT INTO {{.Table}}
			({{.Columns}})
		VALUES
			({{.Values}})
		{{.Extra}}
	`

	adapterTruncateLayout = `
		TRUNCATE TABLE {{.Table}}
	`

	adapterDropDatabaseLayout = `
		DROP DATABASE {{.Database}}
	`

	adapterDropTableLayout = `
		DROP TABLE {{.Table}}
	`

	adapterGroupByLayout = `
		{{if .GroupColumns}}
			GROUP BY {{.GroupColumns}}
		{{end}}
	`
)
