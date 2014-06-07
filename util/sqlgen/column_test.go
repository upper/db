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
