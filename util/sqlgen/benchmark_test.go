package sqlgen

import (
	"testing"
)

func BenchmarkColumn(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = Column{"a"}
	}
}

func BenchmarkColumnNestedValue(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = Column{Value: "a"}
	}
}

func BenchmarkCompileColumn(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = Column{Value: "a"}.Compile(defaultTemplate)
	}
}

func BenchmarkValue(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = Value{"a"}
	}
}

func BenchmarkValueRaw(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = Value{Raw{"a"}}
	}
}

func BenchmarkColumnValue(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = ColumnValue{Column{"a"}, "=", Value{Raw{"7"}}}
	}
}

func BenchmarkTable(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = Table{"foo"}
	}
}

func BenchmarkCompileTable(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = Table{"foo"}.Compile(defaultTemplate)
	}
}

func BenchmarkWhere(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = Where{
			ColumnValue{Column{"a"}, "=", Value{Raw{"7"}}},
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
				ColumnValue{Column{"a"}, "=", Value{Raw{"7"}}},
			},
		}
		_ = stmt.Compile(defaultTemplate)
	}
}
