package sqlgen

import (
	"testing"
)

func TestColumnValue(t *testing.T) {
	var s, e string
	var cv ColumnValue

	cv = ColumnValue{Column{"id"}, "=", Value{1}}

	s = cv.Compile(defaultTemplate)
	e = `"id" = '1'`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}

	cv = ColumnValue{Column{"date"}, "=", Value{Raw{"NOW()"}}}

	s = cv.Compile(defaultTemplate)
	e = `"date" = NOW()`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}
}

func TestColumnValues(t *testing.T) {
	var s, e string
	var cvs ColumnValues

	cvs = ColumnValues{
		{Column{"id"}, ">", Value{8}},
		{Column{"other.id"}, "<", Value{Raw{"100"}}},
		{Column{"name"}, "=", Value{"Haruki Murakami"}},
		{Column{"created"}, ">=", Value{Raw{"NOW()"}}},
		{Column{"modified"}, "<=", Value{Raw{"NOW()"}}},
	}

	s = cvs.Compile(defaultTemplate)
	e = `"id" > '8', "other"."id" < 100, "name" = 'Haruki Murakami', "created" >= NOW(), "modified" <= NOW()`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}

}
