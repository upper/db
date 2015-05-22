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

func (o *Or) Hash() string {
	w := Where(*o)
	return `Or(` + w.Hash() + `)`
}

func (a *And) Hash() string {
	w := Where(*a)
	return `Or(` + w.Hash() + `)`
}

func (o *Or) Compile(layout *Template) (compiled string) {

	if z, ok := layout.Read(o); ok {
		return z
	}

	compiled = groupCondition(layout, o.Conditions, mustParse(layout.ClauseOperator, layout.OrKeyword))

	layout.Write(o, compiled)

	return
}

func (a *And) Compile(layout *Template) (compiled string) {
	if c, ok := layout.Read(a); ok {
		return c
	}

	compiled = groupCondition(layout, a.Conditions, mustParse(layout.ClauseOperator, layout.AndKeyword))

	layout.Write(a, compiled)

	return
}

func (w *Where) Compile(layout *Template) (compiled string) {
	if c, ok := layout.Read(w); ok {
		return c
	}

	grouped := groupCondition(layout, w.Conditions, mustParse(layout.ClauseOperator, layout.AndKeyword))

	if grouped != "" {
		compiled = mustParse(layout.WhereLayout, conds{grouped})
	}

	layout.Write(w, compiled)

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
