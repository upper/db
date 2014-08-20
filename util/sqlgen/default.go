package sqlgen

const (
	defaultColumnSeparator     = `.`
	defaultIdentifierSeparator = `, `
	defaultIdentifierQuote     = `"{{.Raw}}"`
	defaultValueSeparator      = `, `
	defaultValueQuote          = `'{{.}}'`
	defaultAndKeyword          = `AND`
	defaultOrKeyword           = `OR`
	defaultNotKeyword          = `NOT`
	defaultDescKeyword         = `DESC`
	defaultAscKeyword          = `ASC`
	defaultDefaultOperator     = `=`
	defaultClauseGroup         = `({{.}})`
	defaultClauseOperator      = ` {{.}} `
	defaultColumnValue         = `{{.Column}} {{.Operator}} {{.Value}}`
	defaultTableAliasLayout    = `{{.Name}}{{if .Alias}} AS {{.Alias}}{{end}}`
	defaultColumnAliasLayout   = `{{.Name}}{{if .Alias}} AS {{.Alias}}{{end}}`
	defaultSortByColumnLayout  = `{{.Column}} {{.Sort}}`

	defaultOrderByLayout = `
		{{if .SortColumns}}
			ORDER BY {{.SortColumns}}
		{{end}}
	`

	defaultWhereLayout = `
		{{if .Conds}}
			WHERE {{.Conds}}
		{{end}}
	`

	defaultSelectLayout = `
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
	defaultDeleteLayout = `
		DELETE
			FROM {{.Table}}
			{{.Where}}
	`
	defaultUpdateLayout = `
		UPDATE
			{{.Table}}
		SET {{.ColumnValues}}
			{{ .Where }}
	`

	defaultSelectCountLayout = `
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

	defaultInsertLayout = `
		INSERT INTO {{.Table}}
			({{.Columns}})
		VALUES
			({{.Values}})
		{{.Extra}}
	`

	defaultTruncateLayout = `
		TRUNCATE TABLE {{.Table}}
	`

	defaultDropDatabaseLayout = `
		DROP DATABASE {{.Database}}
	`

	defaultDropTableLayout = `
		DROP TABLE {{.Table}}
	`

	defaultGroupByColumnLayout = `{{.Column}}`

	defaultGroupByLayout = `
		{{if .GroupColumns}}
			GROUP BY {{.GroupColumns}}
		{{end}}
	`
)

var defaultTemplate = &Template{
	defaultColumnSeparator,
	defaultIdentifierSeparator,
	defaultIdentifierQuote,
	defaultValueSeparator,
	defaultValueQuote,
	defaultAndKeyword,
	defaultOrKeyword,
	defaultNotKeyword,
	defaultDescKeyword,
	defaultAscKeyword,
	defaultDefaultOperator,
	defaultClauseGroup,
	defaultClauseOperator,
	defaultColumnValue,
	defaultTableAliasLayout,
	defaultColumnAliasLayout,
	defaultSortByColumnLayout,
	defaultWhereLayout,
	defaultOrderByLayout,
	defaultInsertLayout,
	defaultSelectLayout,
	defaultUpdateLayout,
	defaultDeleteLayout,
	defaultTruncateLayout,
	defaultDropDatabaseLayout,
	defaultDropTableLayout,
	defaultSelectCountLayout,
	defaultGroupByLayout,
}
