package sqlgen

import (
	"testing"
)

func TestWhereAnd(t *testing.T) {
	var s, e string

	and := NewAnd(
		&ColumnValue{Column: Column{Name: "id"}, Operator: ">", Value: NewValue(&Raw{Value: "8"})},
		&ColumnValue{Column: Column{Name: "id"}, Operator: "<", Value: NewValue(&Raw{Value: "99"})},
		&ColumnValue{Column: Column{Name: "name"}, Operator: "=", Value: NewValue("John")},
	)

	s = and.Compile(defaultTemplate)
	e = `("id" > 8 AND "id" < 99 AND "name" = 'John')`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}
}

func TestWhereOr(t *testing.T) {
	var s, e string

	or := NewOr(
		&ColumnValue{Column: Column{Name: "id"}, Operator: "=", Value: NewValue(&Raw{Value: "8"})},
		&ColumnValue{Column: Column{Name: "id"}, Operator: "=", Value: NewValue(&Raw{Value: "99"})},
	)

	s = or.Compile(defaultTemplate)
	e = `("id" = 8 OR "id" = 99)`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}
}

func TestWhereAndOr(t *testing.T) {
	var s, e string

	and := NewAnd(
		&ColumnValue{Column: Column{Name: "id"}, Operator: ">", Value: NewValue(&Raw{Value: "8"})},
		&ColumnValue{Column: Column{Name: "id"}, Operator: "<", Value: NewValue(&Raw{Value: "99"})},
		&ColumnValue{Column: Column{Name: "name"}, Operator: "=", Value: NewValue("John")},
		NewOr(
			&ColumnValue{Column: Column{Name: "last_name"}, Operator: "=", Value: NewValue("Smith")},
			&ColumnValue{Column: Column{Name: "last_name"}, Operator: "=", Value: NewValue("Reyes")},
		),
	)

	s = and.Compile(defaultTemplate)
	e = `("id" > 8 AND "id" < 99 AND "name" = 'John' AND ("last_name" = 'Smith' OR "last_name" = 'Reyes'))`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}
}

func TestWhereAndRawOrAnd(t *testing.T) {
	var s, e string

	where := NewWhere(
		NewAnd(
			&ColumnValue{Column: Column{Name: "id"}, Operator: ">", Value: NewValue(&Raw{Value: "8"})},
			&ColumnValue{Column: Column{Name: "id"}, Operator: "<", Value: NewValue(&Raw{Value: "99"})},
		),
		&ColumnValue{Column: Column{Name: "name"}, Operator: "=", Value: NewValue("John")},
		&Raw{Value: "city_id = 728"},
		NewOr(
			&ColumnValue{Column: Column{Name: "last_name"}, Operator: "=", Value: NewValue("Smith")},
			&ColumnValue{Column: Column{Name: "last_name"}, Operator: "=", Value: NewValue("Reyes")},
		),
		NewAnd(
			&ColumnValue{Column: Column{Name: "age"}, Operator: ">", Value: NewValue(&Raw{Value: "18"})},
			&ColumnValue{Column: Column{Name: "age"}, Operator: "<", Value: NewValue(&Raw{Value: "41"})},
		),
	)

	s = trim(where.Compile(defaultTemplate))
	e = `WHERE (("id" > 8 AND "id" < 99) AND "name" = 'John' AND city_id = 728 AND ("last_name" = 'Smith' OR "last_name" = 'Reyes') AND ("age" > 18 AND "age" < 41))`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}
}

func BenchmarkWhere(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = NewWhere(
			&ColumnValue{Column: Column{Name: "baz"}, Operator: "=", Value: NewValue(99)},
		)
	}
}

func BenchmarkCompileWhere(b *testing.B) {
	w := NewWhere(
		&ColumnValue{Column: Column{Name: "baz"}, Operator: "=", Value: NewValue(99)},
	)
	for i := 0; i < b.N; i++ {
		w.Compile(defaultTemplate)
	}
}

func BenchmarkCompileWhereNoCache(b *testing.B) {
	for i := 0; i < b.N; i++ {
		w := NewWhere(
			&ColumnValue{Column: Column{Name: "baz"}, Operator: "=", Value: NewValue(99)},
		)
		w.Compile(defaultTemplate)
	}
}
