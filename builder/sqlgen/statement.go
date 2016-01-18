package sqlgen

import (
	"reflect"

	"upper.io/db.v2/builder/cache"
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
	Joins        Fragment
	Where        Fragment
	Returning    Fragment

	Limit
	Offset

	rawSQL string

	hash MemHash
}

type statementT struct {
	Table        string
	Database     string
	Columns      string
	Values       string
	ColumnValues string
	OrderBy      string
	GroupBy      string
	Where        string
	Joins        string
	Returning    string
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

// Hash returns a unique identifier for the struct.
func (s *Statement) Hash() string {
	return s.hash.Hash(s)
}

// Compile transforms the Statement into an equivalent SQL query.
func (s *Statement) Compile(layout *Template) (compiled string) {
	if s.Type == rawSQL {
		// No need to hit the cache.
		return s.rawSQL
	}

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
		Where:        layout.doCompile(s.Where),
		Returning:    layout.doCompile(s.Returning),
		Joins:        layout.doCompile(s.Joins),
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

// RawSQL represents a raw SQL statement.
func RawSQL(s string) *Statement {
	return &Statement{
		Type:   rawSQL,
		rawSQL: s,
	}
}
