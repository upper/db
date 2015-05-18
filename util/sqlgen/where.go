package sqlgen

import (
	"fmt"
	"strings"
)

type Or Where

type And Where

type Where struct {
	Conditions []cc
	hash       string
}

type conds struct {
	Conds string
}

func NewWhere(conditions ...cc) *Where {
	return &Where{Conditions: conditions}
}

func NewOr(conditions ...cc) *Or {
	return &Or{Conditions: conditions}
}

func NewAnd(conditions ...cc) *And {
	return &And{Conditions: conditions}
}

func (w *Where) Hash() string {
	if w.hash == "" {
		hash := make([]string, len(w.Conditions))
		for i := range w.Conditions {
			hash[i] = w.Conditions[i].Hash()
		}
		w.hash = fmt.Sprintf(`Where{%s}`, strings.Join(hash, `, `))
	}
	return w.hash
}

func (self *Or) Hash() string {
	w := Where(*self)
	return `Or(` + w.Hash() + `)`
}

func (self *And) Hash() string {
	w := Where(*self)
	return `Or(` + w.Hash() + `)`
}

func (self *Or) Compile(layout *Template) (compiled string) {

	if z, ok := layout.Read(self); ok {
		return z
	}

	compiled = groupCondition(layout, self.Conditions, mustParse(layout.ClauseOperator, layout.OrKeyword))

	layout.Write(self, compiled)

	return
}

func (self *And) Compile(layout *Template) (compiled string) {
	if c, ok := layout.Read(self); ok {
		return c
	}

	compiled = groupCondition(layout, self.Conditions, mustParse(layout.ClauseOperator, layout.AndKeyword))

	layout.Write(self, compiled)

	return
}

func (self *Where) Compile(layout *Template) (compiled string) {
	if c, ok := layout.Read(self); ok {
		return c
	}

	grouped := groupCondition(layout, self.Conditions, mustParse(layout.ClauseOperator, layout.AndKeyword))

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
