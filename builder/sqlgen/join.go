package sqlgen

import (
	"fmt"
	"strings"
)

type innerJoinT struct {
	Type  string
	Table string
	On    string
	Using string
}

// Joins represents the union of different join conditions.
type Joins struct {
	Conditions []Fragment
	hash       string
}

// Hash returns a unique identifier.
func (j *Joins) Hash() string {
	if j.hash == "" {
		hash := make([]string, len(j.Conditions))
		for i := range j.Conditions {
			hash[i] = j.Conditions[i].Hash()
		}
		j.hash = fmt.Sprintf(`Join{%s}`, strings.Join(hash, `, `))
	}
	return j.hash
}

// Compile transforms the Where into an equivalent SQL representation.
func (j *Joins) Compile(layout *Template) (compiled string) {
	if c, ok := layout.Read(j); ok {
		return c
	}

	l := len(j.Conditions)

	chunks := make([]string, 0, l)

	if l > 0 {
		for i := 0; i < l; i++ {
			chunks = append(chunks, j.Conditions[i].Compile(layout))
		}
	}

	compiled = strings.Join(chunks, " ")

	layout.Write(j, compiled)

	return
}

// JoinConditions creates a Joins object.
func JoinConditions(joins ...*Join) *Joins {
	fragments := make([]Fragment, len(joins))
	for i := range fragments {
		fragments[i] = joins[i]
	}
	return &Joins{Conditions: fragments}
}

// Join represents a generic JOIN statement.
type Join struct {
	Type string
	*Table
	*On
	*Using
	hash string
}

// Hash returns a unique string given a JOIN.
func (j *Join) Hash() string {
	if j.hash == "" {
		if j.Table != nil && j.Table.Hash() != "" {
			j.hash = fmt.Sprintf(`Join{%s}`, strings.Join([]string{
				j.Type,
				j.Table.Hash(),
				getHash(j.On),
				getHash(j.Using),
			}, ", "))
		}
	}
	return j.hash
}

// Compile transforms the Join into its equivalent SQL representation.
func (j *Join) Compile(layout *Template) (compiled string) {

	if c, ok := layout.Read(j); ok {
		return c
	}

	if j.Table != nil {
		data := innerJoinT{
			Type:  j.Type,
			Table: j.Table.Compile(layout),
			On:    layout.doCompile(j.On),
			Using: layout.doCompile(j.Using),
		}
		compiled = mustParse(layout.JoinLayout, data)
	}

	layout.Write(j, compiled)

	return
}

// On represents JOIN conditions.
type On Where

// Hash returns a unique identifier.
func (o *On) Hash() string {
	if o.hash == "" {
		w := Where(*o)
		o.hash = fmt.Sprintf(`On{%s}`, w.Hash())
	}
	return o.hash
}

// Compile transforms the On into an equivalent SQL representation.
func (o *On) Compile(layout *Template) (compiled string) {
	if c, ok := layout.Read(o); ok {
		return c
	}

	grouped := groupCondition(layout, o.Conditions, mustParse(layout.ClauseOperator, layout.AndKeyword))

	if grouped != "" {
		compiled = mustParse(layout.OnLayout, conds{grouped})
	}

	layout.Write(o, compiled)

	return
}

// Using represents a USING function.
type Using Columns

type usingT struct {
	Columns string
}

// Hash returns a unique identifier.
func (u *Using) Hash() string {
	if u.hash == "" {
		c := Columns(*u)
		u.hash = fmt.Sprintf(`Using{%s}`, c.Hash())
	}
	return u.hash
}

// Compile transforms the Using into an equivalent SQL representation.
func (u *Using) Compile(layout *Template) (compiled string) {
	if u == nil {
		return ""
	}

	if c, ok := layout.Read(u); ok {
		return c
	}

	if len(u.Columns) > 0 {
		c := Columns(*u)
		data := usingT{
			Columns: c.Compile(layout),
		}
		compiled = mustParse(layout.UsingLayout, data)
	}

	layout.Write(u, compiled)

	return
}
