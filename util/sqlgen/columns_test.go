package sqlgen

import (
	"testing"
)

func TestColumns(t *testing.T) {
	var s, e string

	columns := Columns{
		{Name: "id"},
		{Name: "customer"},
		{Name: "service_id"},
		{Name: "role.name"},
		{Name: "role.id"},
	}

	s = columns.Compile(defaultTemplate)
	e = `"id", "customer", "service_id", "role"."name", "role"."id"`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}

}
