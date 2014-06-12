package sqlgen

import (
	"strings"
)

type (
	Or    []interface{}
	And   []interface{}
	Where []interface{}
)

type conds struct {
	Conds string
}

func (self Or) Compile(layout *Template) string {
	return groupCondition(layout, self, mustParse(layout.ClauseOperator, layout.OrKeyword))
}

func (self And) Compile(layout *Template) string {
	return groupCondition(layout, self, mustParse(layout.ClauseOperator, layout.AndKeyword))
}

func (self Where) Compile(layout *Template) string {
	grouped := groupCondition(layout, self, mustParse(layout.ClauseOperator, layout.AndKeyword))
	if grouped != "" {
		return mustParse(layout.WhereLayout, conds{grouped})
	}
	return ""
}

func groupCondition(layout *Template, terms []interface{}, joinKeyword string) string {
	l := len(terms)

	chunks := make([]string, 0, l)

	if l > 0 {
		var i int
		for i = 0; i < l; i++ {
			switch v := terms[i].(type) {
			case ColumnValue:
				chunks = append(chunks, v.Compile(layout))
			case Or:
				chunks = append(chunks, v.Compile(layout))
			case And:
				chunks = append(chunks, v.Compile(layout))
			case Raw:
				chunks = append(chunks, v.String())
			}
		}
	}

	if len(chunks) > 0 {
		return mustParse(layout.ClauseGroup, strings.Join(chunks, joinKeyword))
	}

	return ""
}
