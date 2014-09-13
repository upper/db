package sqlgen

import (
	"testing"
)

func TestTableSimple(t *testing.T) {
	var s, e string
	var table Table

	table = Table{"artist"}

	s = trim(table.Compile(defaultTemplate))
	e = `"artist"`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}
}

func TestTableCompound(t *testing.T) {
	var s, e string
	var table Table

	table = Table{"artist.foo"}

	s = trim(table.Compile(defaultTemplate))
	e = `"artist"."foo"`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}
}

func TestTableCompoundAlias(t *testing.T) {
	var s, e string
	var table Table

	table = Table{"artist.foo AS baz"}

	s = trim(table.Compile(defaultTemplate))
	e = `"artist"."foo" AS "baz"`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}
}

func TestTableImplicitAlias(t *testing.T) {
	var s, e string
	var table Table

	table = Table{"artist.foo baz"}

	s = trim(table.Compile(defaultTemplate))
	e = `"artist"."foo" AS "baz"`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}
}

func TestTableMultiple(t *testing.T) {
	var s, e string
	var table Table

	table = Table{"artist.foo, artist.bar, artist.baz"}

	s = trim(table.Compile(defaultTemplate))
	e = `"artist"."foo", "artist"."bar", "artist"."baz"`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}
}

func TestTableMultipleAlias(t *testing.T) {
	var s, e string
	var table Table

	table = Table{"artist.foo AS foo, artist.bar as bar, artist.baz As baz"}

	s = trim(table.Compile(defaultTemplate))
	e = `"artist"."foo" AS "foo", "artist"."bar" AS "bar", "artist"."baz" AS "baz"`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}
}

func TestTableMinimal(t *testing.T) {
	var s, e string
	var table Table

	table = Table{"a"}

	s = trim(table.Compile(defaultTemplate))
	e = `"a"`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}
}

func TestTableEmpty(t *testing.T) {
	var s, e string
	var table Table

	table = Table{""}

	s = trim(table.Compile(defaultTemplate))
	e = ``

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}
}
