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

package postgresql

const (
	pgsqlColumnSeparator     = `.`
	pgsqlIdentifierSeparator = `, `
	pgsqlIdentifierQuote     = `"{{.Raw}}"`
	pgsqlValueSeparator      = `, `
	pgsqlValueQuote          = `'{{.}}'`
	pgsqlAndKeyword          = `AND`
	pgsqlOrKeyword           = `OR`
	pgsqlNotKeyword          = `NOT`
	pgsqlDescKeyword         = `DESC`
	pgsqlAscKeyword          = `ASC`
	pgsqlDefaultOperator     = `=`
	pgsqlClauseGroup         = `({{.}})`
	pgsqlClauseOperator      = ` {{.}} `
	pgsqlColumnValue         = `{{.Column}} {{.Operator}} {{.Value}}`
	pgsqlTableAliasLayout    = `{{.Name}}{{if .Alias}} AS {{.Alias}}{{end}}`
	pgsqlColumnAliasLayout   = `{{.Name}}{{if .Alias}} AS {{.Alias}}{{end}}`
	pgsqlSortByColumnLayout  = `{{.Column}} {{.Sort}}`

	pgsqlOrderByLayout = `
		{{if .SortColumns}}
			ORDER BY {{.SortColumns}}
		{{end}}
	`

	pgsqlWhereLayout = `
		{{if .Conds}}
			WHERE {{.Conds}}
		{{end}}
	`

	pgsqlSelectLayout = `
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
	pgsqlDeleteLayout = `
		DELETE
			FROM {{.Table}}
			{{.Where}}
	`
	pgsqlUpdateLayout = `
		UPDATE
			{{.Table}}
		SET {{.ColumnValues}}
			{{ .Where }}
	`

	pgsqlSelectCountLayout = `
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

	pgsqlInsertLayout = `
		INSERT INTO {{.Table}}
			({{.Columns}})
		VALUES
			({{.Values}})
		{{.Extra}}
	`

	pgsqlTruncateLayout = `
		TRUNCATE TABLE {{.Table}} RESTART IDENTITY
	`

	pgsqlDropDatabaseLayout = `
		DROP DATABASE {{.Database}}
	`

	pgsqlDropTableLayout = `
		DROP TABLE {{.Table}}
	`

	pgsqlGroupByLayout = `
		{{if .GroupColumns}}
			GROUP BY {{.GroupColumns}}
		{{end}}
	`

	psqlNull = `NULL`
)
