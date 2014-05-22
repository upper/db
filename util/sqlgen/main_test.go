package sqlgen

import (
	"testing"
)

func TestColumnString(t *testing.T) {
	var s, e string

	column := Column{"role.name"}

	s = column.String()
	e = `"role"."name"`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}
}

func TestColumns(t *testing.T) {
	var s, e string

	columns := Columns{
		[]Column{
			{"id"},
			{"customer"},
			{"service_id"},
			{"role.name"},
			{"role.id"},
		},
	}

	s = columns.String()
	e = `"id", "customer", "service_id", "role"."name", "role"."id"`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}

}

func TestValue(t *testing.T) {
	var s, e string
	var val Value

	val = Value{1}

	s = val.String()
	e = `"1"`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}

	val = Value{Raw{"NOW()"}}

	s = val.String()
	e = `NOW()`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}

}

func TestColumnValue(t *testing.T) {
	var s, e string
	var cv ColumnValue

	cv = ColumnValue{Column{"id"}, "=", Value{1}}

	s = cv.String()
	e = `"id" = "1"`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}

	cv = ColumnValue{Column{"date"}, "=", Value{Raw{"NOW()"}}}

	s = cv.String()
	e = `"date" = NOW()`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}
}

func TestColumnValues(t *testing.T) {
	var s, e string
	var cvs ColumnValues

	cvs = ColumnValues{
		[]ColumnValue{
			{Column{"id"}, ">", Value{8}},
			{Column{"other.id"}, "<", Value{Raw{"100"}}},
			{Column{"name"}, "=", Value{"Haruki Murakami"}},
			{Column{"created"}, ">=", Value{Raw{"NOW()"}}},
			{Column{"modified"}, "<=", Value{Raw{"NOW()"}}},
		},
	}

	s = cvs.String()
	e = `"id" > "8", "other"."id" < 100, "name" = "Haruki Murakami", "created" >= NOW(), "modified" <= NOW()`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}

}
