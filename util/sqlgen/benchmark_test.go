package sqlgen

import (
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

/*
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
		_ = NewValue(Raw{Value: "a"}}
	}
}
*/

func BenchmarkColumnValue(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = ColumnValue{Column: Column{Name: "a"}, Operator: "=", Value: NewValue(Raw{Value: "7"})}
	}
}

func BenchmarkCompileColumnValue(b *testing.B) {
	cv := ColumnValue{Column: Column{Name: "a"}, Operator: "=", Value: NewValue(Raw{Value: "7"})}
	for i := 0; i < b.N; i++ {
		cv.Compile(defaultTemplate)
	}
}

func BenchmarkColumnValues(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = NewColumnValues(
			ColumnValue{Column: Column{Name: "a"}, Operator: "=", Value: NewValue(Raw{Value: "7"})},
		)
	}
}

func BenchmarkCompileColumnValues(b *testing.B) {
	cv := NewColumnValues(ColumnValue{Column: Column{Name: "a"}, Operator: "=", Value: NewValue(Raw{Value: "7"})})
	for i := 0; i < b.N; i++ {
		cv.Compile(defaultTemplate)
	}
}

func BenchmarkWhere(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = Where{
			&ColumnValue{Column: Column{Name: "baz"}, Operator: "=", Value: NewValue(99)},
		}
	}
}

func BenchmarkCompileWhere(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = Where{
			&ColumnValue{Column: Column{Name: "baz"}, Operator: "=", Value: NewValue(99)},
		}.Compile(defaultTemplate)
	}
}

func BenchmarkTable(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = NewTable("foo")
	}
}

func BenchmarkCompileSelect(b *testing.B) {
	var stmt Statement

	for i := 0; i < b.N; i++ {
		stmt = Statement{
			Type:  SqlSelectCount,
			Table: NewTable("table_name"),
			Where: Where{
				&ColumnValue{Column: Column{Name: "a"}, Operator: "=", Value: NewValue(Raw{Value: "7"})},
			},
		}
		_ = stmt.Compile(defaultTemplate)
	}
}
