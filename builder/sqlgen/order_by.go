package sqlgen

import (
	"fmt"
	"strings"
)

// Order represents the order in which SQL results are sorted.
type Order uint8

// Possible values for Order
const (
	DefaultOrder = Order(iota)
	Ascendent
	Descendent
)

// SortColumn represents the column-order relation in an ORDER BY clause.
type SortColumn struct {
	Column Fragment
	Order
	hash string
}

type sortColumnT struct {
	Column string
	Order  string
}

// SortColumns represents the columns in an ORDER BY clause.
type SortColumns struct {
	Columns []Fragment
	hash    string
}

// OrderBy represents an ORDER BY clause.
type OrderBy struct {
	SortColumns Fragment
	hash        string
}

type orderByT struct {
	SortColumns string
}

// JoinSortColumns creates and returns an array of column-order relations.
func JoinSortColumns(values ...Fragment) *SortColumns {
	return &SortColumns{Columns: values}
}

// JoinWithOrderBy creates an returns an OrderBy using the given SortColumns.
func JoinWithOrderBy(sc *SortColumns) *OrderBy {
	return &OrderBy{SortColumns: sc}
}

// Hash returns a unique identifier.
func (s *SortColumn) Hash() string {
	if s.hash == "" {
		s.hash = fmt.Sprintf(`SortColumn{Column:%s, Order:%s}`, s.Column.Hash(), s.Order.Hash())
	}
	return s.hash
}

// Compile transforms the SortColumn into an equivalent SQL representation.
func (s *SortColumn) Compile(layout *Template) (compiled string) {

	if c, ok := layout.Read(s); ok {
		return c
	}

	data := sortColumnT{
		Column: s.Column.Compile(layout),
		Order:  s.Order.Compile(layout),
	}

	compiled = mustParse(layout.SortByColumnLayout, data)

	layout.Write(s, compiled)

	return
}

// Hash returns a unique identifier.
func (s *SortColumns) Hash() string {
	if s.hash == "" {
		h := make([]string, len(s.Columns))
		for i := range s.Columns {
			h[i] = s.Columns[i].Hash()
		}
		s.hash = fmt.Sprintf(`SortColumns(%s)`, strings.Join(h, `, `))
	}
	return s.hash
}

// Compile transforms the SortColumns into an equivalent SQL representation.
func (s *SortColumns) Compile(layout *Template) (compiled string) {

	if z, ok := layout.Read(s); ok {
		return z
	}

	z := make([]string, len(s.Columns))

	for i := range s.Columns {
		z[i] = s.Columns[i].Compile(layout)
	}

	compiled = strings.Join(z, layout.IdentifierSeparator)

	layout.Write(s, compiled)

	return
}

// Hash returns a unique identifier.
func (s *OrderBy) Hash() string {
	if s.hash == "" {
		if s.SortColumns != nil {
			s.hash = `OrderBy(` + s.SortColumns.Hash() + `)`
		}
	}
	return s.hash
}

// Compile transforms the SortColumn into an equivalent SQL representation.
func (s *OrderBy) Compile(layout *Template) (compiled string) {

	if z, ok := layout.Read(s); ok {
		return z
	}

	if s.SortColumns != nil {
		data := orderByT{
			SortColumns: s.SortColumns.Compile(layout),
		}
		compiled = mustParse(layout.OrderByLayout, data)
	}

	layout.Write(s, compiled)

	return
}

// Hash returns a unique identifier.
func (s Order) Hash() string {
	switch s {
	case Ascendent:
		return `Order{ASC}`
	case Descendent:
		return `Order{DESC}`
	}
	return `Order{DEFAULT}`
}

// Compile transforms the SortColumn into an equivalent SQL representation.
func (s Order) Compile(layout *Template) string {
	switch s {
	case Ascendent:
		return layout.AscKeyword
	case Descendent:
		return layout.DescKeyword
	}
	return ""
}
