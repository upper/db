package sqlgen

import (
	"strings"
)

type (
	Or    []cc
	And   []cc
	Where []cc
)

type conds struct {
	Conds string
}

func (self Or) Hash() string {
	hash := make([]string, 0, len(self))
	for i := range self {
		hash = append(hash, self[i].Hash())
	}
	return `Or(` + strings.Join(hash, `,`) + `)`
}

func (self Or) Compile(layout *Template) (compiled string) {
	if c, ok := layout.Read(self); ok {
		return c
	}

	compiled = groupCondition(layout, self, mustParse(layout.ClauseOperator, layout.OrKeyword))

	layout.Write(self, compiled)

	return
}

func (self And) Hash() string {
	hash := make([]string, 0, len(self))
	for i := range self {
		hash = append(hash, self[i].Hash())
	}
	return `And(` + strings.Join(hash, `,`) + `)`
}

func (self And) Compile(layout *Template) (compiled string) {
	if c, ok := layout.Read(self); ok {
		return c
	}

	compiled = groupCondition(layout, self, mustParse(layout.ClauseOperator, layout.AndKeyword))

	layout.Write(self, compiled)

	return
}

func (self Where) Hash() string {
	hash := make([]string, 0, len(self))
	for i := range self {
		hash = append(hash, self[i].Hash())
	}
	return `Where(` + strings.Join(hash, `,`) + `)`
}

func (self Where) Compile(layout *Template) (compiled string) {
	if c, ok := layout.Read(self); ok {
		return c
	}

	grouped := groupCondition(layout, self, mustParse(layout.ClauseOperator, layout.AndKeyword))

	if grouped != "" {
		compiled = mustParse(layout.WhereLayout, conds{grouped})
	}

	layout.Write(self, compiled)

	return
}

func groupCondition(layout *Template, terms []cc, joinKeyword string) string {
	l := len(terms)

	chunks := make([]string, 0, l)

	if l > 0 {
		for i := 0; i < l; i++ {
			chunks = append(chunks, terms[i].Compile(layout))
		}
	}

	if len(chunks) > 0 {
		return mustParse(layout.ClauseGroup, strings.Join(chunks, joinKeyword))
	}

	return ""
}
