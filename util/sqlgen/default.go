package sqlgen

import (
	"upper.io/cache"
)

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
	ColumnSeparator:     defaultColumnSeparator,
	IdentifierSeparator: defaultIdentifierSeparator,
	IdentifierQuote:     defaultIdentifierQuote,
	ValueSeparator:      defaultValueSeparator,
	ValueQuote:          defaultValueQuote,
	AndKeyword:          defaultAndKeyword,
	OrKeyword:           defaultOrKeyword,
	NotKeyword:          defaultNotKeyword,
	DescKeyword:         defaultDescKeyword,
	AscKeyword:          defaultAscKeyword,
	DefaultOperator:     defaultDefaultOperator,
	ClauseGroup:         defaultClauseGroup,
	ClauseOperator:      defaultClauseOperator,
	ColumnValue:         defaultColumnValue,
	TableAliasLayout:    defaultTableAliasLayout,
	ColumnAliasLayout:   defaultColumnAliasLayout,
	SortByColumnLayout:  defaultSortByColumnLayout,
	WhereLayout:         defaultWhereLayout,
	OrderByLayout:       defaultOrderByLayout,
	InsertLayout:        defaultInsertLayout,
	SelectLayout:        defaultSelectLayout,
	UpdateLayout:        defaultUpdateLayout,
	DeleteLayout:        defaultDeleteLayout,
	TruncateLayout:      defaultTruncateLayout,
	DropDatabaseLayout:  defaultDropDatabaseLayout,
	DropTableLayout:     defaultDropTableLayout,
	SelectCountLayout:   defaultSelectCountLayout,
	GroupByLayout:       defaultGroupByLayout,
	Cache:               cache.NewCache(),
}
