package exql

import "strings"

type CTE struct {
	Alias Fragment
	Statement
}

type cteT struct {
	Alias     Fragment
	Statement string
}

// Hash returns a unique identifier.
func (ct *CTE) Hash() string {
	return ct.hash.Hash(ct)
}

// Compile transforms the GroupBy into an equivalent SQL representation.
func (ct *CTE) Compile(layout *Template) (compiled string) {
	if z, ok := layout.Read(ct); ok {
		return z
	}

	data := cteT{
		Alias:     ct.Alias,
		Statement: ct.Statement.Compile(layout),
	}

	compiled = mustParse(layout.CTELayout, data)
	compiled = strings.TrimSpace(compiled)
	layout.Write(ct, compiled)
	return compiled
}
