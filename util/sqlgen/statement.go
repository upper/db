package sqlgen

import (
	"reflect"
	"strconv"
	"strings"

	"upper.io/cache"
)

// Statement represents different kinds of SQL statements.
type Statement struct {
	Type
	Table        Fragment
	Database     Fragment
	Columns      Fragment
	Values       Fragment
	ColumnValues Fragment
	OrderBy      Fragment
	GroupBy      Fragment
	Join         Fragment
	Where        Fragment
	Extra

	Limit
	Offset

	hash string
}

type statementT struct {
	Table        string
	Database     string
	Columns      string
	Values       string
	ColumnValues string
	OrderBy      string
	GroupBy      string
	Extra        string
	Where        string
	Join         string
	Limit
	Offset
}

func (layout *Template) doCompile(c Fragment) string {
	if c != nil && !reflect.ValueOf(c).IsNil() {
		return c.Compile(layout)
	}
	return ""
}

func getHash(h cache.Hashable) string {
	if h != nil && !reflect.ValueOf(h).IsNil() {
		return h.Hash()
	}
	return ""
}

// Hash returns a unique identifier.
func (s *Statement) Hash() string {
	if s.hash == "" {
		parts := strings.Join([]string{
			strconv.Itoa(int(s.Type)),
			getHash(s.Table),
			getHash(s.Database),
			strconv.Itoa(int(s.Limit)),
			strconv.Itoa(int(s.Offset)),
			getHash(s.Columns),
			getHash(s.Values),
			getHash(s.ColumnValues),
			getHash(s.OrderBy),
			getHash(s.GroupBy),
			string(s.Extra),
			getHash(s.Where),
			getHash(s.Join),
		}, ";")

		s.hash = `Statement{` + parts + `}`
	}
	return s.hash
}

// Compile transforms the Statement into an equivalent SQL query.
func (s *Statement) Compile(layout *Template) (compiled string) {

	if z, ok := layout.Read(s); ok {
		return z
	}

	data := statementT{
		Table:        layout.doCompile(s.Table),
		Database:     layout.doCompile(s.Database),
		Limit:        s.Limit,
		Offset:       s.Offset,
		Columns:      layout.doCompile(s.Columns),
		Values:       layout.doCompile(s.Values),
		ColumnValues: layout.doCompile(s.ColumnValues),
		OrderBy:      layout.doCompile(s.OrderBy),
		GroupBy:      layout.doCompile(s.GroupBy),
		Extra:        string(s.Extra),
		Where:        layout.doCompile(s.Where),
		Join:         layout.doCompile(s.Join),
	}

	switch s.Type {
	case Truncate:
		compiled = mustParse(layout.TruncateLayout, data)
	case DropTable:
		compiled = mustParse(layout.DropTableLayout, data)
	case DropDatabase:
		compiled = mustParse(layout.DropDatabaseLayout, data)
	case Count:
		compiled = mustParse(layout.CountLayout, data)
	case Select:
		compiled = mustParse(layout.SelectLayout, data)
	case Delete:
		compiled = mustParse(layout.DeleteLayout, data)
	case Update:
		compiled = mustParse(layout.UpdateLayout, data)
	case Insert:
		compiled = mustParse(layout.InsertLayout, data)
	default:
		panic("Unknown template type.")
	}

	layout.Write(s, compiled)

	return compiled
}
