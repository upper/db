package sqlgen

import (
	"fmt"
	"strings"
)

// Or represents an SQL OR operator.
type Or Where

// And represents an SQL AND operator.
type And Where

// Where represents an SQL WHERE clause.
type Where struct {
	Conditions []cc
	hash       string
}

type conds struct {
	Conds string
}

// NewWhere creates and retuens a new Where.
func NewWhere(conditions ...cc) *Where {
	return &Where{Conditions: conditions}
}

// NewOr creates and returns a new Or.
func NewOr(conditions ...cc) *Or {
	return &Or{Conditions: conditions}
}

// NewAnd creates and returns a new And.
func NewAnd(conditions ...cc) *And {
	return &And{Conditions: conditions}
}

// Hash returns a unique identifier.
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

// Hash returns a unique identifier.
func (o *Or) Hash() string {
	w := Where(*o)
	return `Or(` + w.Hash() + `)`
}

// Hash returns a unique identifier.
func (a *And) Hash() string {
	w := Where(*a)
	return `Or(` + w.Hash() + `)`
}

// Compile transforms the Or into an equivalent SQL representation.
func (o *Or) Compile(layout *Template) (compiled string) {

	if z, ok := layout.Read(o); ok {
		return z
	}

	compiled = groupCondition(layout, o.Conditions, mustParse(layout.ClauseOperator, layout.OrKeyword))

	layout.Write(o, compiled)

	return
}

// Compile transforms the And into an equivalent SQL representation.
func (a *And) Compile(layout *Template) (compiled string) {
	if c, ok := layout.Read(a); ok {
		return c
	}

	compiled = groupCondition(layout, a.Conditions, mustParse(layout.ClauseOperator, layout.AndKeyword))

	layout.Write(a, compiled)

	return
}

// Compile transforms the Where into an equivalent SQL representation.
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
