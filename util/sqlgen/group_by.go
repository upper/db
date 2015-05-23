package sqlgen

import (
	"fmt"
)

// GroupBy represents a SQL's "group by" statement.
type GroupBy struct {
	Columns Fragment
	hash    string
}

type groupByT struct {
	GroupColumns string
}

// Hash returns a unique identifier.
func (g *GroupBy) Hash() string {
	if g.hash == "" {
		if g.Columns != nil {
			g.hash = fmt.Sprintf(`GroupBy(%s)`, g.Columns.Hash())
		}
	}
	return g.hash
}

// GroupByColumns creates and returns a GroupBy with the given column.
func GroupByColumns(columns ...Fragment) *GroupBy {
	return &GroupBy{Columns: JoinColumns(columns...)}
}

// Compile transforms the GroupBy into an equivalent SQL representation.
func (g *GroupBy) Compile(layout *Template) (compiled string) {

	if c, ok := layout.Read(g); ok {
		return c
	}

	if g.Columns != nil {
		data := groupByT{
			GroupColumns: g.Columns.Compile(layout),
		}

		compiled = mustParse(layout.GroupByLayout, data)
	}

	layout.Write(g, compiled)

	return
}
