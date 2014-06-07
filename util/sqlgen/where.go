package sqlgen

import (
	"strings"
)

type (
	Or    []interface{}
	And   []interface{}
	Where []interface{}
)

func (self Or) String() string {
	return groupCondition(self, sqlOrKeyword)
}

func (self And) String() string {
	return groupCondition(self, sqlAndKeyword)
}

func (self Where) String() string {
	return groupCondition(self, sqlAndKeyword)
}

func groupCondition(terms []interface{}, joinKeyword string) string {
	l := len(terms)

	chunks := make([]string, 0, l)

	if l > 0 {
		var i int
		for i = 0; i < l; i++ {
			switch v := terms[i].(type) {
			case ColumnValue:
				chunks = append(chunks, v.String())
			case Or:
				chunks = append(chunks, v.String())
			case And:
				chunks = append(chunks, v.String())
			case Raw:
				chunks = append(chunks, v.String())
			}
		}
	}

	if len(chunks) > 0 {
		return mustParse(sqlConditionGroup, strings.Join(chunks, " "+joinKeyword+" "))
	}

	return ""
}
