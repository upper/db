package sqlgen

import (
	"testing"
)

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
