package sqlgen

import (
	"fmt"
	"math/rand"
	"testing"
)

func BenchmarkColumn(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = Column{Name: "a"}
	}
}

func BenchmarkCompileColumnNoCache(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = (&Column{Name: "a"}).Compile(defaultTemplate)
	}
}

func BenchmarkValue(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = Value{"a"}
	}
}

func BenchmarkCompileValueNoCache(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = Value{"a"}.Compile(defaultTemplate)
	}
}

func BenchmarkValues(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = Values{{"a"}, {"b"}, {"c"}, {1}, {2}, {3}}
	}
}

func BenchmarkCompileValues(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = Values{{"a"}, {"b"}, {"c"}, {1}, {2}, {3}}.Compile(defaultTemplate)
	}
}

func BenchmarkValueRaw(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = Value{Raw{Value: "a"}}
	}
}

func BenchmarkColumnValue(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = ColumnValue{Column: Column{Name: "a"}, Operator: "=", Value: Value{Raw{Value: "7"}}}
	}
}

func BenchmarkCompileColumnValue(b *testing.B) {
	cv := ColumnValue{Column: Column{Name: "a"}, Operator: "=", Value: Value{Raw{Value: "7"}}}
	for i := 0; i < b.N; i++ {
		cv.Compile(defaultTemplate)
	}
}

func BenchmarkColumnValues(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = NewColumnValues(
			ColumnValue{Column: Column{Name: "a"}, Operator: "=", Value: Value{Raw{Value: "7"}}},
		)
	}
}

func BenchmarkCompileColumnValues(b *testing.B) {
	cv := NewColumnValues(ColumnValue{Column: Column{Name: "a"}, Operator: "=", Value: Value{Raw{Value: "7"}}})
	for i := 0; i < b.N; i++ {
		cv.Compile(defaultTemplate)
	}
}

func BenchmarkOrderBy(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = OrderBy{
			SortColumns: SortColumns{
				SortColumn{Column: Column{Name: "foo"}},
			},
		}
	}
}

func BenchmarkCompileOrderBy(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = OrderBy{
			SortColumns: SortColumns{
				SortColumn{Column: Column{Name: "foo"}},
			},
		}.Compile(defaultTemplate)
	}
}

func BenchmarkWhere(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = Where{
			&ColumnValue{Column: Column{Name: "baz"}, Operator: "=", Value: Value{99}},
		}
	}
}

func BenchmarkCompileWhere(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = Where{
			&ColumnValue{Column: Column{Name: "baz"}, Operator: "=", Value: Value{99}},
		}.Compile(defaultTemplate)
	}
}

func BenchmarkTable(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = Table{"foo"}
	}
}

func BenchmarkCompileTable(b *testing.B) {
	var t string
	for i := 0; i < b.N; i++ {
		t = Table{"foo"}.Compile(defaultTemplate)
		if t != `"foo"` {
			b.Fatal("Caching failed.")
		}
	}
}

func BenchmarkCompileRandomTable(b *testing.B) {
	var t string
	var m, n int
	var s, e string

	for i := 0; i < b.N; i++ {
		m, n = rand.Int(), rand.Int()
		s = fmt.Sprintf(`%s as %s`, m, n)
		e = fmt.Sprintf(`"%s" AS "%s"`, m, n)

		t = Table{s}.Compile(defaultTemplate)
		if t != e {
			b.Fatal()
		}
	}
}

func BenchmarkCompileSelect(b *testing.B) {
	var stmt Statement

	for i := 0; i < b.N; i++ {
		stmt = Statement{
			Type:  SqlSelectCount,
			Table: Table{"table_name"},
			Where: Where{
				&ColumnValue{Column: Column{Name: "a"}, Operator: "=", Value: Value{Raw{Value: "7"}}},
			},
		}
		_ = stmt.Compile(defaultTemplate)
	}
}
