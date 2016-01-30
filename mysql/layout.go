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

package mysql

const (
	mysqlColumnSeparator     = `.`
	mysqlIdentifierSeparator = `, `
	mysqlIdentifierQuote     = "`{{.Raw}}`"
	mysqlValueSeparator      = `, `
	mysqlValueQuote          = `'{{.}}'`
	mysqlAndKeyword          = `AND`
	mysqlOrKeyword           = `OR`
	mysqlNotKeyword          = `NOT`
	mysqlDescKeyword         = `DESC`
	mysqlAscKeyword          = `ASC`
	mysqlDefaultOperator     = `=`
	mysqlClauseGroup         = `({{.}})`
	mysqlClauseOperator      = ` {{.}} `
	mysqlColumnValue         = `{{.Column}} {{.Operator}} {{.Value}}`
	mysqlTableAliasLayout    = `{{.Name}}{{if .Alias}} AS {{.Alias}}{{end}}`
	mysqlColumnAliasLayout   = `{{.Name}}{{if .Alias}} AS {{.Alias}}{{end}}`
	mysqlSortByColumnLayout  = `{{.Column}} {{.Sort}}`

	mysqlOrderByLayout = `
		{{if .SortColumns}}
			ORDER BY {{.SortColumns}}
		{{end}}
	`

	mysqlWhereLayout = `
		{{if .Conds}}
			WHERE {{.Conds}}
		{{end}}
	`

	mysqlSelectLayout = `
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
	mysqlDeleteLayout = `
		DELETE
			FROM {{.Table}}
			{{.Where}}
	`
	mysqlUpdateLayout = `
		UPDATE
			{{.Table}}
		SET {{.ColumnValues}}
			{{ .Where }}
	`

	mysqlSelectCountLayout = `
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

	mysqlInsertLayout = `
		INSERT INTO {{.Table}}
			({{.Columns}})
		VALUES
			({{.Values}})
		{{.Extra}}
	`

	mysqlTruncateLayout = `
		TRUNCATE TABLE {{.Table}}
	`

	mysqlDropDatabaseLayout = `
		DROP DATABASE {{.Database}}
	`

	mysqlDropTableLayout = `
		DROP TABLE {{.Table}}
	`

	mysqlGroupByLayout = `
		{{if .GroupColumns}}
			GROUP BY {{.GroupColumns}}
		{{end}}
	`

	mysqlNull = `NULL`
)
