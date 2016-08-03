package exql

import (
	"upper.io/db.v2/internal/cache"
)

const (
	defaultColumnSeparator     = `.`
	defaultIdentifierSeparator = `, `
	defaultIdentifierQuote     = `"{{.Value}}"`
	defaultValueSeparator      = `, `
	defaultValueQuote          = `'{{.}}'`
	defaultAndKeyword          = `AND`
	defaultOrKeyword           = `OR`
	defaultNotKeyword          = `NOT`
	defaultDescKeyword         = `DESC`
	defaultAscKeyword          = `ASC`
	defaultDefaultOperator     = `=`
	defaultAssignmentOperator  = `=`
	defaultClauseGroup         = `({{.}})`
	defaultClauseOperator      = ` {{.}} `
	defaultColumnValue         = `{{.Column}} {{.Operator}} {{.Value}}`
	defaultTableAliasLayout    = `{{.Name}}{{if .Alias}} AS {{.Alias}}{{end}}`
	defaultColumnAliasLayout   = `{{.Name}}{{if .Alias}} AS {{.Alias}}{{end}}`
	defaultSortByColumnLayout  = `{{.Column}} {{.Order}}`

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

	defaultUsingLayout = `
		{{if .Columns}}
			USING ({{.Columns}})
		{{end}}
	`

	defaultJoinLayout = `
		{{if .Table}}
			{{ if .On }}
				{{.Type}} JOIN {{.Table}}
				{{.On}}
			{{ else if .Using }}
				{{.Type}} JOIN {{.Table}}
				{{.Using}}
			{{ else if .Type | eq "CROSS" }}
				{{.Type}} JOIN {{.Table}}
			{{else}}
				NATURAL {{.Type}} JOIN {{.Table}}
			{{end}}
		{{end}}
	`

	defaultOnLayout = `
		{{if .Conds}}
			ON {{.Conds}}
		{{end}}
	`

	defaultSelectLayout = `
    SELECT

      {{if .Columns}}
        {{.Columns}}
      {{else}}
        *
      {{end}}

      {{if .Table}}
        FROM {{.Table}}
      {{end}}

      {{.Joins}}

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
    {{if .Limit}}
      LIMIT {{.Limit}}
    {{end}}
    {{if .Offset}}
      OFFSET {{.Offset}}
    {{end}}
	`
	defaultUpdateLayout = `
		UPDATE
			{{.Table}}
		SET {{.ColumnValues}}
			{{ .Where }}
	`

	defaultCountLayout = `
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
      {{if .Columns }}({{.Columns}}){{end}}
    VALUES
      {{.Values}}
		{{if .Returning}}
			RETURNING {{.Returning}}
		{{end}}
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
	AssignmentOperator:  defaultAssignmentOperator,
	ClauseGroup:         defaultClauseGroup,
	ClauseOperator:      defaultClauseOperator,
	ColumnValue:         defaultColumnValue,
	TableAliasLayout:    defaultTableAliasLayout,
	ColumnAliasLayout:   defaultColumnAliasLayout,
	SortByColumnLayout:  defaultSortByColumnLayout,
	WhereLayout:         defaultWhereLayout,
	OnLayout:            defaultOnLayout,
	UsingLayout:         defaultUsingLayout,
	JoinLayout:          defaultJoinLayout,
	OrderByLayout:       defaultOrderByLayout,
	InsertLayout:        defaultInsertLayout,
	SelectLayout:        defaultSelectLayout,
	UpdateLayout:        defaultUpdateLayout,
	DeleteLayout:        defaultDeleteLayout,
	TruncateLayout:      defaultTruncateLayout,
	DropDatabaseLayout:  defaultDropDatabaseLayout,
	DropTableLayout:     defaultDropTableLayout,
	CountLayout:         defaultCountLayout,
	GroupByLayout:       defaultGroupByLayout,
	Cache:               cache.NewCache(),
}
