package sqlgen

import (
	"testing"
)

func BenchmarkCompileSelect(b *testing.B) {
	var stmt Statement

	for i := 0; i < b.N; i++ {
		stmt = Statement{
			Type:  SqlSelectCount,
			Table: NewTable("table_name"),
			Where: NewWhere(
				&ColumnValue{Column: Column{Name: "a"}, Operator: "=", Value: NewValue(Raw{Value: "7"})},
			),
		}
		_ = stmt.Compile(defaultTemplate)
	}
}
