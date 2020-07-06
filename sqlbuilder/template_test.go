package sqlbuilder

import (
	"github.com/upper/db/v4/internal/cache"
	"github.com/upper/db/v4/internal/sqladapter/exql"
)

const (
	defaultColumnSeparator     = `.`
	defaultIdentifierSeparator = `, `
	defaultIdentifierQuote     = `"{{.Value}}"`
	defaultValueSeparator      = `, `
	defaultValueQuote          = `'{{.}}'`
	defaultAndKeyword          = `AND`
	defaultOrKeyword           = `OR`
	defaultDescKeyword         = `DESC`
	defaultAscKeyword          = `ASC`
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
      {{if .Distinct}}
        DISTINCT
      {{end}}

      {{if defined .Columns}}
        {{.Columns | compile}}
      {{else}}
        *
      {{end}}

      {{if defined .Table}}
        FROM {{.Table | compile}}
      {{end}}

      {{.Joins | compile}}

      {{.Where | compile}}

      {{if defined .GroupBy}}
        {{.GroupBy | compile}}
      {{end}}

      {{.OrderBy | compile}}

      {{if .Limit}}
        LIMIT {{.Limit}}
      {{end}}

      {{if .Offset}}
        OFFSET {{.Offset}}
      {{end}}
  `
	defaultDeleteLayout = `
    DELETE
      FROM {{.Table | compile}}
      {{.Where | compile}}
  `
	defaultUpdateLayout = `
    UPDATE
      {{.Table | compile}}
    SET {{.ColumnValues | compile}}
      {{.Where | compile}}
  `

	defaultCountLayout = `
    SELECT
      COUNT(1) AS _t
    FROM {{.Table | compile}}
      {{.Where | compile}}

      {{if .Limit}}
        LIMIT {{.Limit}}
      {{end}}

      {{if .Offset}}
        OFFSET {{.Offset}}
      {{end}}
  `

	defaultInsertLayout = `
    INSERT INTO {{.Table | compile}}
      {{if defined .Columns }}({{.Columns | compile}}){{end}}
    VALUES
    {{if defined .Values}}
      {{.Values | compile}}
    {{else}}
      (default)
    {{end}}
    {{if defined .Returning}}
      RETURNING {{.Returning | compile}}
    {{end}}
  `

	defaultTruncateLayout = `
    TRUNCATE TABLE {{.Table | compile}}
  `

	defaultDropDatabaseLayout = `
    DROP DATABASE {{.Database | compile}}
  `

	defaultDropTableLayout = `
    DROP TABLE {{.Table | compile}}
  `

	defaultGroupByLayout = `
    {{if .GroupColumns}}
      GROUP BY {{.GroupColumns}}
    {{end}}
  `
)

var testTemplate = exql.Template{
	ColumnSeparator:     defaultColumnSeparator,
	IdentifierSeparator: defaultIdentifierSeparator,
	IdentifierQuote:     defaultIdentifierQuote,
	ValueSeparator:      defaultValueSeparator,
	ValueQuote:          defaultValueQuote,
	AndKeyword:          defaultAndKeyword,
	OrKeyword:           defaultOrKeyword,
	DescKeyword:         defaultDescKeyword,
	AscKeyword:          defaultAscKeyword,
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
