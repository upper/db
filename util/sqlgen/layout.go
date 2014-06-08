package sqlgen

type layout struct {
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

var Layout = layout{
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
