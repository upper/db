package sqlgen

import (
	"testing"
)

func TestWhereAnd(t *testing.T) {
	var s, e string
	var and And

	and = And{
		ColumnValue{Column{"id"}, ">", Value{Raw{"8"}}},
		ColumnValue{Column{"id"}, "<", Value{Raw{"99"}}},
		ColumnValue{Column{"name"}, "=", Value{"John"}},
	}

	s = and.Compile(defaultTemplate)
	e = `("id" > 8 AND "id" < 99 AND "name" = 'John')`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}
}

func TestWhereOr(t *testing.T) {
	var s, e string
	var or Or

	or = Or{
		ColumnValue{Column{"id"}, "=", Value{Raw{"8"}}},
		ColumnValue{Column{"id"}, "=", Value{Raw{"99"}}},
	}

	s = or.Compile(defaultTemplate)
	e = `("id" = 8 OR "id" = 99)`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}
}

func TestWhereAndOr(t *testing.T) {
	var s, e string
	var and And

	and = And{
		ColumnValue{Column{"id"}, ">", Value{Raw{"8"}}},
		ColumnValue{Column{"id"}, "<", Value{Raw{"99"}}},
		ColumnValue{Column{"name"}, "=", Value{"John"}},
		Or{
			ColumnValue{Column{"last_name"}, "=", Value{"Smith"}},
			ColumnValue{Column{"last_name"}, "=", Value{"Reyes"}},
		},
	}

	s = and.Compile(defaultTemplate)
	e = `("id" > 8 AND "id" < 99 AND "name" = 'John' AND ("last_name" = 'Smith' OR "last_name" = 'Reyes'))`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}
}

func TestWhereAndRawOrAnd(t *testing.T) {
	var s, e string
	var where Where

	where = Where{
		And{
			ColumnValue{Column{"id"}, ">", Value{Raw{"8"}}},
			ColumnValue{Column{"id"}, "<", Value{Raw{"99"}}},
		},
		ColumnValue{Column{"name"}, "=", Value{"John"}},
		Raw{"city_id = 728"},
		Or{
			ColumnValue{Column{"last_name"}, "=", Value{"Smith"}},
			ColumnValue{Column{"last_name"}, "=", Value{"Reyes"}},
		},
		And{
			ColumnValue{Column{"age"}, ">", Value{Raw{"18"}}},
			ColumnValue{Column{"age"}, "<", Value{Raw{"41"}}},
		},
	}

	s = trim(where.Compile(defaultTemplate))
	e = `WHERE (("id" > 8 AND "id" < 99) AND "name" = 'John' AND city_id = 728 AND ("last_name" = 'Smith' OR "last_name" = 'Reyes') AND ("age" > 18 AND "age" < 41))`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}
}
