package sqlgen

import (
	"testing"
)

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
		stmt.Compile(defaultTemplate)
	}

}
