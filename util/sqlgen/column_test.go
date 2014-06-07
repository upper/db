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
