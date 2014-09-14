package sqlgen

import (
	"upper.io/cache"
)

type Template struct {
	ColumnSeparator     string
	IdentifierSeparator string
	IdentifierQuote     string
	ValueSeparator      string
	ValueQuote          string
	AndKeyword          string
	OrKeyword           string
	NotKeyword          string
	DescKeyword         string
	AscKeyword          string
	DefaultOperator     string
	ClauseGroup         string
	ClauseOperator      string
	ColumnValue         string
	TableAliasLayout    string
	ColumnAliasLayout   string
	SortByColumnLayout  string
	WhereLayout         string
	OrderByLayout       string
	InsertLayout        string
	SelectLayout        string
	UpdateLayout        string
	DeleteLayout        string
	TruncateLayout      string
	DropDatabaseLayout  string
	DropTableLayout     string
	SelectCountLayout   string
	GroupByLayout       string
	*cache.Cache
}
