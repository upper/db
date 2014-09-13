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
	GroupByLayout       string
	cache               map[interface{}]string
	cachedTemplates     map[string]string
}

type cacheable interface {
	Hash() string
}

func (self *Template) SetCache(key interface{}, value string) {
	if self.cache == nil {
		self.cache = make(map[interface{}]string)
	}
	self.cache[key] = value
}

func (self *Template) Cache(key interface{}) (string, bool) {
	if self.cache != nil {
		if s, ok := self.cache[key]; ok {
			return s, true
		}
	}
	return "", false
}

func (self *Template) getCache(i cacheable) string {
	if self.cachedTemplates == nil {
		return ""
	}
	return self.cachedTemplates[i.Hash()]
}

func (self *Template) setCache(i cacheable, s string) {
	if self.cachedTemplates == nil {
		self.cachedTemplates = map[string]string{}
	}
	self.cachedTemplates[i.Hash()] = s
}

func (self *Template) isCached(i cacheable) bool {
	if self.cachedTemplates == nil {
		return false
	}
	if _, ok := self.cachedTemplates[i.Hash()]; ok {
		return true
	}
	return false
}
