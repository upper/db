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

package sqlite

const (
	sqlColumnSeparator     = `.`
	sqlIdentifierSeparator = `, `
	sqlIdentifierQuote     = `"{{.Raw}}"`
	sqlValueSeparator      = `, `
	sqlValueQuote          = `'{{.}}'`
	sqlAndKeyword          = `AND`
	sqlOrKeyword           = `OR`
	sqlNotKeyword          = `NOT`
	sqlDescKeyword         = `DESC`
	sqlAscKeyword          = `ASC`
	sqlDefaultOperator     = `=`
	sqlClauseGroup         = `({{.}})`
	sqlClauseOperator      = ` {{.}} `
	sqlColumnValue         = `{{.Column}} {{.Operator}} {{.Value}}`
	sqlTableAliasLayout    = `{{.Name}}{{if .Alias}} AS {{.Alias}}{{end}}`
	sqlColumnAliasLayout   = `{{.Name}}{{if .Alias}} AS {{.Alias}}{{end}}`
	sqlSortByColumnLayout  = `{{.Column}} {{.Sort}}`

	sqlOrderByLayout = `
		{{if .SortColumns}}
			ORDER BY {{.SortColumns}}
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

			{{.GroupBy}}

			{{.OrderBy}}

			{{if .Limit}}
				LIMIT {{.Limit}}
			{{end}}

			{{if .Offset}}
				{{if not .Limit}}
					LIMIT -1 
				{{end}}
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

			{{if .Limit}}
				LIMIT {{.Limit}}
			{{end}}

			{{if .Offset}}
				{{if not .Limit}}
					LIMIT -1 
				{{end}}
				OFFSET {{.Offset}}
			{{end}}
	`

	sqlInsertLayout = `
		INSERT INTO {{.Table}}
			({{.Columns}})
		VALUES
			({{.Values}})
		{{.Extra}}
	`

	sqlTruncateLayout = `
		DELETE FROM {{.Table}}
	`

	sqlDropDatabaseLayout = `
		DROP DATABASE {{.Database}}
	`

	sqlDropTableLayout = `
		DROP TABLE {{.Table}}
	`

	sqlGroupByLayout = `
		{{if .GroupColumns}}
			GROUP BY {{.GroupColumns}}
		{{end}}
	`

	sqlNull = `NULL`
)
