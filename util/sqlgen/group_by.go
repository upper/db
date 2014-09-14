package sqlgen

type GroupBy Columns

type groupBy_s struct {
	GroupColumns string
}

func (self GroupBy) Hash() string {
	return `GroupBy(` + Columns(self).Hash() + `)`
}

func (self GroupBy) Compile(layout *Template) (compiled string) {

	if c, ok := layout.Read(self); ok {
		return c
	}

	if len(self) > 0 {

		data := groupBy_s{
			GroupColumns: Columns(self).Compile(layout),
		}

		compiled = mustParse(layout.GroupByLayout, data)
	}

	layout.Write(self, compiled)

	return
}
