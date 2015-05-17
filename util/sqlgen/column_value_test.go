package sqlgen

import (
	"fmt"
	"testing"
)

func TestColumnValueHash(t *testing.T) {
	var s, e string

	c := ColumnValue{Column: Column{Name: "id"}, Operator: "=", Value: Value{1}}

	s = c.Hash()
	e = fmt.Sprintf(`sqlgen.ColumnValue{Name:%q, Operator:%q, Value:%q}`, c.Column.Hash(), c.Operator, c.Value.Hash())

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}
}

func TestColumnValuesHash(t *testing.T) {
	var s, e string

	c := NewColumnValues(
		ColumnValue{Column: Column{Name: "id"}, Operator: "=", Value: Value{1}},
		ColumnValue{Column: Column{Name: "id"}, Operator: "=", Value: Value{2}},
	)

	s = c.Hash()

	e = fmt.Sprintf(`sqlgen.ColumnValues{ColumnValues:{%s, %s}}`, c.ColumnValues[0].Hash(), c.ColumnValues[1].Hash())

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}
}

func TestColumnValue(t *testing.T) {
	var s, e string
	var cv ColumnValue

	cv = ColumnValue{Column: Column{Name: "id"}, Operator: "=", Value: Value{1}}

	s = cv.Compile(defaultTemplate)
	e = `"id" = '1'`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}

	cv = ColumnValue{Column: Column{Name: "date"}, Operator: "=", Value: Value{Raw{Value: "NOW()"}}}

	s = cv.Compile(defaultTemplate)
	e = `"date" = NOW()`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}
}

func TestColumnValues(t *testing.T) {
	var s, e string

	cvs := NewColumnValues(
		ColumnValue{Column: Column{Name: "id"}, Operator: ">", Value: Value{8}},
		ColumnValue{Column: Column{Name: "other.id"}, Operator: "<", Value: Value{Raw{Value: "100"}}},
		ColumnValue{Column: Column{Name: "name"}, Operator: "=", Value: Value{"Haruki Murakami"}},
		ColumnValue{Column: Column{Name: "created"}, Operator: ">=", Value: Value{Raw{Value: "NOW()"}}},
		ColumnValue{Column: Column{Name: "modified"}, Operator: "<=", Value: Value{Raw{Value: "NOW()"}}},
	)

	s = cvs.Compile(defaultTemplate)
	e = `"id" > '8', "other"."id" < 100, "name" = 'Haruki Murakami', "created" >= NOW(), "modified" <= NOW()`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}
}

func BenchmarkColumnValueHash(b *testing.B) {
	cv := ColumnValue{Column: Column{Name: "id"}, Operator: "=", Value: Value{1}}
	for i := 0; i < b.N; i++ {
		cv.Hash()
	}
}

func BenchmarkColumnValueCompile(b *testing.B) {
	cv := ColumnValue{Column: Column{Name: "id"}, Operator: "=", Value: Value{1}}
	for i := 0; i < b.N; i++ {
		cv.Compile(defaultTemplate)
	}
}

func BenchmarkColumnValuesHash(b *testing.B) {
	cvs := NewColumnValues(
		ColumnValue{Column: Column{Name: "id"}, Operator: ">", Value: Value{8}},
		ColumnValue{Column: Column{Name: "other.id"}, Operator: "<", Value: Value{Raw{Value: "100"}}},
		ColumnValue{Column: Column{Name: "name"}, Operator: "=", Value: Value{"Haruki Murakami"}},
		ColumnValue{Column: Column{Name: "created"}, Operator: ">=", Value: Value{Raw{Value: "NOW()"}}},
		ColumnValue{Column: Column{Name: "modified"}, Operator: "<=", Value: Value{Raw{Value: "NOW()"}}},
	)
	for i := 0; i < b.N; i++ {
		cvs.Hash()
	}
}

func BenchmarkColumnValuesCompile(b *testing.B) {
	cvs := NewColumnValues(
		ColumnValue{Column: Column{Name: "id"}, Operator: ">", Value: Value{8}},
		ColumnValue{Column: Column{Name: "other.id"}, Operator: "<", Value: Value{Raw{Value: "100"}}},
		ColumnValue{Column: Column{Name: "name"}, Operator: "=", Value: Value{"Haruki Murakami"}},
		ColumnValue{Column: Column{Name: "created"}, Operator: ">=", Value: Value{Raw{Value: "NOW()"}}},
		ColumnValue{Column: Column{Name: "modified"}, Operator: "<=", Value: Value{Raw{Value: "NOW()"}}},
	)
	for i := 0; i < b.N; i++ {
		cvs.Compile(defaultTemplate)
	}
}
