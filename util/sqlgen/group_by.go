package sqlgen

import (
	"fmt"
)

type GroupBy struct {
	Columns *Columns
	hash    string
}

type groupByT struct {
	GroupColumns string
}

func (g *GroupBy) Hash() string {
	if g.hash == "" {
		g.hash = fmt.Sprintf(`GroupBy(%s)`, g.Columns.Hash())
	}
	return g.hash
}

func NewGroupBy(columns ...Column) *GroupBy {
	return &GroupBy{Columns: NewColumns(columns...)}
}

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
