package exql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOrderBy(t *testing.T) {
	o := JoinWithOrderBy(
		JoinSortColumns(
			&SortColumn{Column: &Column{Name: "foo"}},
		),
	)

	s := mustTrim(o.Compile(defaultTemplate))
	assert.Equal(t, `ORDER BY "foo"`, s)
}

func TestOrderByRaw(t *testing.T) {
	o := JoinWithOrderBy(
		JoinSortColumns(
			&SortColumn{Column: &Raw{Value: "CASE WHEN id IN ? THEN 0 ELSE 1 END"}},
		),
	)

	s := mustTrim(o.Compile(defaultTemplate))
	assert.Equal(t, `ORDER BY CASE WHEN id IN ? THEN 0 ELSE 1 END`, s)
}

func TestOrderByDesc(t *testing.T) {
	o := JoinWithOrderBy(
		JoinSortColumns(
			&SortColumn{Column: &Column{Name: "foo"}, Order: Order_Descendent},
		),
	)

	s := mustTrim(o.Compile(defaultTemplate))
	assert.Equal(t, `ORDER BY "foo" DESC`, s)
}

func BenchmarkOrderBy(b *testing.B) {
	for i := 0; i < b.N; i++ {
		JoinWithOrderBy(
			JoinSortColumns(
				&SortColumn{Column: &Column{Name: "foo"}},
			),
		)
	}
}

func BenchmarkOrderByHash(b *testing.B) {
	o := OrderBy{
		SortColumns: JoinSortColumns(
			&SortColumn{Column: &Column{Name: "foo"}},
		),
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		o.Hash()
	}
}

func BenchmarkCompileOrderByCompile(b *testing.B) {
	o := OrderBy{
		SortColumns: JoinSortColumns(
			&SortColumn{Column: &Column{Name: "foo"}},
		),
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = o.Compile(defaultTemplate)
	}
}

func BenchmarkCompileOrderByCompileNoCache(b *testing.B) {
	for i := 0; i < b.N; i++ {
		o := JoinWithOrderBy(
			JoinSortColumns(
				&SortColumn{Column: &Column{Name: "foo"}},
			),
		)
		_, _ = o.Compile(defaultTemplate)
	}
}

func BenchmarkCompileOrderCompile(b *testing.B) {
	o := Order_Descendent
	for i := 0; i < b.N; i++ {
		_, _ = o.Compile(defaultTemplate)
	}
}

func BenchmarkCompileOrderCompileNoCache(b *testing.B) {
	for i := 0; i < b.N; i++ {
		o := Order_Descendent
		_, _ = o.Compile(defaultTemplate)
	}
}

func BenchmarkSortColumnHash(b *testing.B) {
	s := &SortColumn{Column: &Column{Name: "foo"}}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Hash()
	}
}

func BenchmarkSortColumnCompile(b *testing.B) {
	s := &SortColumn{Column: &Column{Name: "foo"}}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = s.Compile(defaultTemplate)
	}
}

func BenchmarkSortColumnCompileNoCache(b *testing.B) {
	for i := 0; i < b.N; i++ {
		s := &SortColumn{Column: &Column{Name: "foo"}}
		_, _ = s.Compile(defaultTemplate)
	}
}

func BenchmarkSortColumnsHash(b *testing.B) {
	s := JoinSortColumns(
		&SortColumn{Column: &Column{Name: "foo"}},
		&SortColumn{Column: &Column{Name: "bar"}},
	)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Hash()
	}
}

func BenchmarkSortColumnsCompile(b *testing.B) {
	s := JoinSortColumns(
		&SortColumn{Column: &Column{Name: "foo"}},
		&SortColumn{Column: &Column{Name: "bar"}},
	)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = s.Compile(defaultTemplate)
	}
}

func BenchmarkSortColumnsCompileNoCache(b *testing.B) {
	for i := 0; i < b.N; i++ {
		s := JoinSortColumns(
			&SortColumn{Column: &Column{Name: "foo"}},
			&SortColumn{Column: &Column{Name: "bar"}},
		)
		_, _ = s.Compile(defaultTemplate)
	}
}
