package exql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWhereAnd(t *testing.T) {
	and := JoinWithAnd(
		&ColumnValue{Column: &Column{Name: "id"}, Operator: ">", Value: NewValue(&Raw{Value: "8"})},
		&ColumnValue{Column: &Column{Name: "id"}, Operator: "<", Value: NewValue(&Raw{Value: "99"})},
		&ColumnValue{Column: &Column{Name: "name"}, Operator: "=", Value: NewValue("John")},
	)

	s, err := and.Compile(defaultTemplate)
	assert.NoError(t, err)
	assert.Equal(t, `("id" > 8 AND "id" < 99 AND "name" = 'John')`, s)
}

func TestWhereOr(t *testing.T) {
	or := JoinWithOr(
		&ColumnValue{Column: &Column{Name: "id"}, Operator: "=", Value: NewValue(&Raw{Value: "8"})},
		&ColumnValue{Column: &Column{Name: "id"}, Operator: "=", Value: NewValue(&Raw{Value: "99"})},
	)

	s, err := or.Compile(defaultTemplate)
	assert.NoError(t, err)
	assert.Equal(t, `("id" = 8 OR "id" = 99)`, s)
}

func TestWhereAndOr(t *testing.T) {
	and := JoinWithAnd(
		&ColumnValue{Column: &Column{Name: "id"}, Operator: ">", Value: NewValue(&Raw{Value: "8"})},
		&ColumnValue{Column: &Column{Name: "id"}, Operator: "<", Value: NewValue(&Raw{Value: "99"})},
		&ColumnValue{Column: &Column{Name: "name"}, Operator: "=", Value: NewValue("John")},
		JoinWithOr(
			&ColumnValue{Column: &Column{Name: "last_name"}, Operator: "=", Value: NewValue("Smith")},
			&ColumnValue{Column: &Column{Name: "last_name"}, Operator: "=", Value: NewValue("Reyes")},
		),
	)

	s, err := and.Compile(defaultTemplate)
	assert.NoError(t, err)

	assert.Equal(t, `("id" > 8 AND "id" < 99 AND "name" = 'John' AND ("last_name" = 'Smith' OR "last_name" = 'Reyes'))`, s)
}

func TestWhereAndRawOrAnd(t *testing.T) {
	{
		where := WhereConditions(
			JoinWithAnd(
				&ColumnValue{Column: &Column{Name: "id"}, Operator: ">", Value: NewValue(2)},
				&ColumnValue{Column: &Column{Name: "id"}, Operator: "<", Value: NewValue(&Raw{Value: "77"})},
				&ColumnValue{Column: &Column{Name: "id"}, Operator: "<", Value: NewValue(1)},
			),
			&ColumnValue{Column: &Column{Name: "name"}, Operator: "=", Value: NewValue("John")},
			&Raw{Value: "city_id = 728"},
			JoinWithOr(
				&ColumnValue{Column: &Column{Name: "last_name"}, Operator: "=", Value: NewValue("Smith")},
				&ColumnValue{Column: &Column{Name: "last_name"}, Operator: "=", Value: NewValue("Reyes")},
			),
			JoinWithAnd(
				&ColumnValue{Column: &Column{Name: "age"}, Operator: ">", Value: NewValue(&Raw{Value: "18"})},
				&ColumnValue{Column: &Column{Name: "age"}, Operator: "<", Value: NewValue(&Raw{Value: "41"})},
			),
		)

		assert.Equal(t,
			`WHERE (("id" > '2' AND "id" < 77 AND "id" < '1') AND "name" = 'John' AND city_id = 728 AND ("last_name" = 'Smith' OR "last_name" = 'Reyes') AND ("age" > 18 AND "age" < 41))`,
			mustTrim(where.Compile(defaultTemplate)),
		)
	}

	{
		where := WhereConditions(
			JoinWithAnd(
				&ColumnValue{Column: &Column{Name: "id"}, Operator: ">", Value: NewValue(&Raw{Value: "8"})},
				&ColumnValue{Column: &Column{Name: "id"}, Operator: ">", Value: NewValue(&Raw{Value: "8"})},
				&ColumnValue{Column: &Column{Name: "id"}, Operator: "<", Value: NewValue(&Raw{Value: "99"})},
				&ColumnValue{Column: &Column{Name: "id"}, Operator: "<", Value: NewValue(1)},
			),
			&ColumnValue{Column: &Column{Name: "name"}, Operator: "=", Value: NewValue("John")},
			&Raw{Value: "city_id = 728"},
			JoinWithOr(
				&ColumnValue{Column: &Column{Name: "last_name"}, Operator: "=", Value: NewValue("Smith")},
				&ColumnValue{Column: &Column{Name: "last_name"}, Operator: "=", Value: NewValue("Reyes")},
			),
			JoinWithAnd(
				&ColumnValue{Column: &Column{Name: "age"}, Operator: ">", Value: NewValue(&Raw{Value: "18"})},
				&ColumnValue{Column: &Column{Name: "age"}, Operator: "<", Value: NewValue(&Raw{Value: "41"})},
			),
		)

		assert.Equal(t,
			`WHERE (("id" > 8 AND "id" > 8 AND "id" < 99 AND "id" < '1') AND "name" = 'John' AND city_id = 728 AND ("last_name" = 'Smith' OR "last_name" = 'Reyes') AND ("age" > 18 AND "age" < 41))`,
			mustTrim(where.Compile(defaultTemplate)),
		)
	}
}

func BenchmarkWhere(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = WhereConditions(
			&ColumnValue{Column: &Column{Name: "baz"}, Operator: "=", Value: NewValue(99)},
		)
	}
}

func BenchmarkCompileWhere(b *testing.B) {
	w := WhereConditions(
		&ColumnValue{Column: &Column{Name: "baz"}, Operator: "=", Value: NewValue(99)},
	)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = w.Compile(defaultTemplate)
	}
}

func BenchmarkCompileWhereNoCache(b *testing.B) {
	for i := 0; i < b.N; i++ {
		w := WhereConditions(
			&ColumnValue{Column: &Column{Name: "baz"}, Operator: "=", Value: NewValue(99)},
		)
		_, _ = w.Compile(defaultTemplate)
	}
}
