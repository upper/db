package sqlgen

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
}

var layout = Template{
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
}
