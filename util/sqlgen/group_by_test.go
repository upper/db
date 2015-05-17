package sqlgen

import (
	"testing"
)

func TestGroupBy(t *testing.T) {
	var s, e string

	columns := NewGroupBy(
		Column{Name: "id"},
		Column{Name: "customer"},
		Column{Name: "service_id"},
		Column{Name: "role.name"},
		Column{Name: "role.id"},
	)

	s = columns.Compile(defaultTemplate)
	e = `GROUP BY "id", "customer", "service_id", "role"."name", "role"."id"`

	if trim(s) != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}
}

func BenchmarkNewGroupBy(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = NewGroupBy(
			Column{Name: "a"},
			Column{Name: "b"},
			Column{Name: "c"},
		)
	}
}

func BenchmarkGroupByHash(b *testing.B) {
	c := NewGroupBy(
		Column{Name: "id"},
		Column{Name: "customer"},
		Column{Name: "service_id"},
		Column{Name: "role.name"},
		Column{Name: "role.id"},
	)
	for i := 0; i < b.N; i++ {
		c.Hash()
	}
}

func BenchmarkGroupByCompile(b *testing.B) {
	c := NewGroupBy(
		Column{Name: "id"},
		Column{Name: "customer"},
		Column{Name: "service_id"},
		Column{Name: "role.name"},
		Column{Name: "role.id"},
	)
	for i := 0; i < b.N; i++ {
		c.Compile(defaultTemplate)
	}
}

func BenchmarkGroupByCompileNoCache(b *testing.B) {
	for i := 0; i < b.N; i++ {
		c := NewGroupBy(
			Column{Name: "id"},
			Column{Name: "customer"},
			Column{Name: "service_id"},
			Column{Name: "role.name"},
			Column{Name: "role.id"},
		)
		c.Compile(defaultTemplate)
	}
}
