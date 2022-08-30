package exql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGroupBy(t *testing.T) {
	columns := GroupByColumns(
		&Column{Name: "id"},
		&Column{Name: "customer"},
		&Column{Name: "service_id"},
		&Column{Name: "role.name"},
		&Column{Name: "role.id"},
	)

	s := mustTrim(columns.Compile(defaultTemplate))
	assert.Equal(t, `GROUP BY "id", "customer", "service_id", "role"."name", "role"."id"`, s)
}

func BenchmarkGroupByColumns(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = GroupByColumns(
			&Column{Name: "a"},
			&Column{Name: "b"},
			&Column{Name: "c"},
		)
	}
}

func BenchmarkGroupByHash(b *testing.B) {
	c := GroupByColumns(
		&Column{Name: "id"},
		&Column{Name: "customer"},
		&Column{Name: "service_id"},
		&Column{Name: "role.name"},
		&Column{Name: "role.id"},
	)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Hash()
	}
}

func BenchmarkGroupByCompile(b *testing.B) {
	c := GroupByColumns(
		&Column{Name: "id"},
		&Column{Name: "customer"},
		&Column{Name: "service_id"},
		&Column{Name: "role.name"},
		&Column{Name: "role.id"},
	)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = c.Compile(defaultTemplate)
	}
}

func BenchmarkGroupByCompileNoCache(b *testing.B) {
	for i := 0; i < b.N; i++ {
		c := GroupByColumns(
			&Column{Name: "id"},
			&Column{Name: "customer"},
			&Column{Name: "service_id"},
			&Column{Name: "role.name"},
			&Column{Name: "role.id"},
		)
		_, _ = c.Compile(defaultTemplate)
	}
}
