package sqlgen

import (
	"fmt"
	"strings"
)

type Order uint8

const (
	SqlOrderNone = Order(iota)
	SqlOrderAsc
	SqlOrderDesc
)

type SortColumn struct {
	Column
	Order
	hash string
}

type sortColumnT struct {
	Column string
	Order  string
}

type SortColumns struct {
	Columns []SortColumn
	hash    string
}

type OrderBy struct {
	SortColumns *SortColumns
	hash        string
}

type orderByT struct {
	SortColumns string
}

func NewSortColumns(values ...SortColumn) *SortColumns {
	return &SortColumns{Columns: values}
}

func NewOrderBy(sc *SortColumns) *OrderBy {
	return &OrderBy{SortColumns: sc}
}

func (s *SortColumn) Hash() string {
	if s.hash == "" {
		s.hash = fmt.Sprintf(`SortColumn{Column:%s, Order:%s}`, s.Column.Hash(), s.Order.Hash())
	}
	return s.hash
}

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

func (s *OrderBy) Hash() string {
	if s.hash == "" {
		s.hash = `OrderBy(` + s.SortColumns.Hash() + `)`
	}
	return s.hash
}

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

func (s Order) Hash() string {
	switch s {
	case SqlOrderAsc:
		return `Order{ASC}`
	case SqlOrderDesc:
		return `Order{DESC}`
	}
	return `Order{DEFAULT}`
}

func (s Order) Compile(layout *Template) string {
	switch s {
	case SqlOrderAsc:
		return layout.AscKeyword
	case SqlOrderDesc:
		return layout.DescKeyword
	}
	return ""
}
