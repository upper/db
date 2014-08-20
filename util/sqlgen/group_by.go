package sqlgen

type GroupBy Columns

type groupBy_s struct {
	GroupColumns string
}

func (self GroupBy) Compile(layout *Template) string {
	if len(self) > 0 {

		data := groupBy_s{
			GroupColumns: Columns(self).Compile(layout),
		}

		return mustParse(layout.GroupByLayout, data)
	}
	return ""
}
