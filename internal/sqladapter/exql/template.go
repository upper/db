package exql

import (
	"bytes"
	"sync"
	"text/template"

	"upper.io/db.v2/internal/cache"
)

// Type is the type of SQL query the statement represents.
type Type uint

// Values for Type.
const (
	Truncate = Type(iota)
	DropTable
	DropDatabase
	Count
	Insert
	Select
	Update
	Delete

	SQL
)

type (
	// Limit represents the SQL limit in a query.
	Limit int
	// Offset represents the SQL offset in a query.
	Offset int
)

var (
	templateCache = templateMap{M: make(map[string]*template.Template)}
)

// Template is an SQL template.
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
	AssignmentOperator  string
	ClauseGroup         string
	ClauseOperator      string
	ColumnValue         string
	TableAliasLayout    string
	ColumnAliasLayout   string
	SortByColumnLayout  string
	WhereLayout         string
	OnLayout            string
	UsingLayout         string
	JoinLayout          string
	OrderByLayout       string
	InsertLayout        string
	SelectLayout        string
	UpdateLayout        string
	DeleteLayout        string
	TruncateLayout      string
	DropDatabaseLayout  string
	DropTableLayout     string
	CountLayout         string
	CTELayout           string
	GroupByLayout       string
	*cache.Cache
}

func mustParse(text string, data interface{}) string {
	var b bytes.Buffer
	var ok bool

	v, ok := templateCache.Get(text)
	if !ok {
		v = template.Must(template.New("").Parse(text))
		templateCache.Set(text, v)
	}

	if err := v.Execute(&b, data); err != nil {
		panic("There was an error compiling the following template:\n" + text + "\nError was: " + err.Error())
	}

	return b.String()
}

type templateMap struct {
	sync.RWMutex
	M map[string]*template.Template
}

func (m *templateMap) Get(k string) (*template.Template, bool) {
	m.RLock()
	defer m.RUnlock()
	v, ok := m.M[k]
	return v, ok
}

func (m *templateMap) Set(k string, v *template.Template) {
	m.Lock()
	defer m.Unlock()
	m.M[k] = v
}
