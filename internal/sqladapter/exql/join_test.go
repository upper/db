package exql

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOnAndRawOrAnd(t *testing.T) {
	on := OnConditions(
		JoinWithAnd(
			&ColumnValue{Column: &Column{Name: "id"}, Operator: ">", Value: NewValue(&Raw{Value: "8"})},
			&ColumnValue{Column: &Column{Name: "id"}, Operator: "<", Value: NewValue(&Raw{Value: "99"})},
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

	s := mustTrim(on.Compile(defaultTemplate))
	assert.Equal(t, `ON (("id" > 8 AND "id" < 99) AND "name" = 'John' AND city_id = 728 AND ("last_name" = 'Smith' OR "last_name" = 'Reyes') AND ("age" > 18 AND "age" < 41))`, s)
}

func TestUsing(t *testing.T) {
	using := UsingColumns(
		&Column{Name: "country"},
		&Column{Name: "state"},
	)

	s := mustTrim(using.Compile(defaultTemplate))
	assert.Equal(t, `USING ("country", "state")`, s)
}

func TestJoinOn(t *testing.T) {
	join := JoinConditions(
		&Join{
			Table: TableWithName("countries c"),
			On: OnConditions(
				&ColumnValue{
					Column:   &Column{Name: "p.country_id"},
					Operator: "=",
					Value:    NewValue(&Column{Name: "a.id"}),
				},
				&ColumnValue{
					Column:   &Column{Name: "p.country_code"},
					Operator: "=",
					Value:    NewValue(&Column{Name: "a.code"}),
				},
			),
		},
	)

	s := mustTrim(join.Compile(defaultTemplate))
	assert.Equal(t, `JOIN "countries" AS "c" ON ("p"."country_id" = "a"."id" AND "p"."country_code" = "a"."code")`, s)
}

func TestInnerJoinOn(t *testing.T) {
	join := JoinConditions(&Join{
		Type:  "INNER",
		Table: TableWithName("countries c"),
		On: OnConditions(
			&ColumnValue{
				Column:   &Column{Name: "p.country_id"},
				Operator: "=",
				Value:    NewValue(ColumnWithName("a.id")),
			},
			&ColumnValue{
				Column:   &Column{Name: "p.country_code"},
				Operator: "=",
				Value:    NewValue(ColumnWithName("a.code")),
			},
		),
	})

	s := mustTrim(join.Compile(defaultTemplate))
	assert.Equal(t, `INNER JOIN "countries" AS "c" ON ("p"."country_id" = "a"."id" AND "p"."country_code" = "a"."code")`, s)
}

func TestLeftJoinUsing(t *testing.T) {
	join := JoinConditions(&Join{
		Type:  "LEFT",
		Table: TableWithName("countries"),
		Using: UsingColumns(ColumnWithName("name")),
	})

	s := mustTrim(join.Compile(defaultTemplate))
	assert.Equal(t, `LEFT JOIN "countries" USING ("name")`, s)
}

func TestNaturalJoinOn(t *testing.T) {
	join := JoinConditions(&Join{
		Table: TableWithName("countries"),
	})

	s := mustTrim(join.Compile(defaultTemplate))
	assert.Equal(t, `NATURAL JOIN "countries"`, s)
}

func TestNaturalInnerJoinOn(t *testing.T) {
	join := JoinConditions(&Join{
		Type:  "INNER",
		Table: TableWithName("countries"),
	})

	s := mustTrim(join.Compile(defaultTemplate))
	assert.Equal(t, `NATURAL INNER JOIN "countries"`, s)
}

func TestCrossJoin(t *testing.T) {
	join := JoinConditions(&Join{
		Type:  "CROSS",
		Table: TableWithName("countries"),
	})

	s := mustTrim(join.Compile(defaultTemplate))
	assert.Equal(t, `CROSS JOIN "countries"`, s)
}

func TestMultipleJoins(t *testing.T) {
	join := JoinConditions(&Join{
		Type:  "LEFT",
		Table: TableWithName("countries"),
	}, &Join{
		Table: TableWithName("cities"),
	})

	s := mustTrim(join.Compile(defaultTemplate))
	assert.Equal(t, `NATURAL LEFT JOIN "countries" NATURAL JOIN "cities"`, s)
}

func BenchmarkJoin(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = JoinConditions(&Join{
			Table: TableWithName("countries c"),
			On: OnConditions(
				&ColumnValue{
					Column:   &Column{Name: "p.country_id"},
					Operator: "=",
					Value:    NewValue(&Column{Name: "a.id"}),
				},
				&ColumnValue{
					Column:   &Column{Name: "p.country_code"},
					Operator: "=",
					Value:    NewValue(&Column{Name: "a.code"}),
				},
			),
		})
	}
}

func BenchmarkCompileJoin(b *testing.B) {
	j := JoinConditions(&Join{
		Table: TableWithName("countries c"),
		On: OnConditions(
			&ColumnValue{
				Column:   &Column{Name: "p.country_id"},
				Operator: "=",
				Value:    NewValue(&Column{Name: "a.id"}),
			},
			&ColumnValue{
				Column:   &Column{Name: "p.country_code"},
				Operator: "=",
				Value:    NewValue(&Column{Name: "a.code"}),
			},
		),
	})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = j.Compile(defaultTemplate)
	}
}

func BenchmarkCompileJoinNoCache(b *testing.B) {
	for i := 0; i < b.N; i++ {
		j := JoinConditions(&Join{
			Table: TableWithName("countries c"),
			On: OnConditions(
				&ColumnValue{
					Column:   &Column{Name: "p.country_id"},
					Operator: "=",
					Value:    NewValue(&Column{Name: "a.id"}),
				},
				&ColumnValue{
					Column:   &Column{Name: "p.country_code"},
					Operator: "=",
					Value:    NewValue(&Column{Name: "a.code"}),
				},
			),
		})
		_, _ = j.Compile(defaultTemplate)
	}
}

func BenchmarkCompileJoinNoCache2(b *testing.B) {
	for i := 0; i < b.N; i++ {
		j := JoinConditions(&Join{
			Table: TableWithName(fmt.Sprintf("countries c%d", i)),
			On: OnConditions(
				&ColumnValue{
					Column:   &Column{Name: "p.country_id"},
					Operator: "=",
					Value:    NewValue(&Column{Name: "a.id"}),
				},
				&ColumnValue{
					Column:   &Column{Name: "p.country_code"},
					Operator: "=",
					Value:    NewValue(&Column{Name: "a.code"}),
				},
			),
		})
		_, _ = j.Compile(defaultTemplate)
	}
}
