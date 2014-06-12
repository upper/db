package sqlgen

import (
	"testing"
)

func TestValue(t *testing.T) {
	var s, e string
	var val Value

	val = Value{1}

	s = val.Compile(defaultTemplate)
	e = `'1'`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}

	val = Value{Raw{"NOW()"}}

	s = val.Compile(defaultTemplate)
	e = `NOW()`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}
}

func TestValues(t *testing.T) {
	var s, e string
	var val Values

	val = Values{
		Value{Raw{"1"}},
		Value{Raw{"2"}},
		Value{"3"},
	}

	s = val.Compile(defaultTemplate)
	e = `1, 2, '3'`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}
}
