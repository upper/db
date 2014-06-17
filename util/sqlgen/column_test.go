package sqlgen

import (
	"testing"
)

func TestColumnString(t *testing.T) {
	var s, e string

	column := Column{"role.name"}

	s = column.Compile(defaultTemplate)
	e = `"role"."name"`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}
}

func TestColumnAs(t *testing.T) {
	var s, e string

	column := Column{"role.name as foo"}

	s = column.Compile(defaultTemplate)
	e = `"role"."name" AS "foo"`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}
}

func TestColumnImplicitAs(t *testing.T) {
	var s, e string

	column := Column{"role.name foo"}

	s = column.Compile(defaultTemplate)
	e = `"role"."name" AS "foo"`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}
}

func TestColumnRaw(t *testing.T) {
	var s, e string

	column := Column{Raw{"role.name As foo"}}

	s = column.Compile(defaultTemplate)
	e = `role.name As foo`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}
}
