package sqlgen

type GroupBy Columns

type groupByT struct {
	GroupColumns string
}

func (g *GroupBy) Hash() string {
	c := Columns(*g)
	return `GroupBy(` + c.Hash() + `)`
}

func (g *GroupBy) Compile(layout *Template) (compiled string) {

	if c, ok := layout.Read(g); ok {
		return c
	}

	if len(g.Columns) > 0 {
		c := Columns(*g)

		data := groupByT{
			GroupColumns: c.Compile(layout),
		}

		compiled = mustParse(layout.GroupByLayout, data)
	}

	layout.Write(g, compiled)

	return
}
