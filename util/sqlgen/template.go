package sqlgen

import (
	"bytes"
	"text/template"
	"upper.io/cache"
)

type Type uint

const (
	SqlTruncate = iota
	SqlDropTable
	SqlDropDatabase
	SqlSelectCount
	SqlInsert
	SqlSelect
	SqlUpdate
	SqlDelete
)

type (
	Limit  int
	Offset int
	Extra  string
)

var (
	parsedTemplates = make(map[string]*template.Template)
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

func mustParse(text string, data interface{}) string {
	var b bytes.Buffer
	var ok bool

	if _, ok = parsedTemplates[text]; !ok {
		parsedTemplates[text] = template.Must(template.New("").Parse(text))
	}

	if err := parsedTemplates[text].Execute(&b, data); err != nil {
		panic("There was an error compiling the following template:\n" + text + "\nError was: " + err.Error())
	}

	return b.String()
}
