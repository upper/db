package exql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestColumns(t *testing.T) {
	columns := JoinColumns(
		&Column{Name: "id"},
		&Column{Name: "customer"},
		&Column{Name: "service_id"},
		&Column{Name: "role.name"},
		&Column{Name: "role.id"},
	)

	s, err := columns.Compile(defaultTemplate)
	assert.NoError(t, err)
	assert.Equal(t, `"id", "customer", "service_id", "role"."name", "role"."id"`, s)
}

func BenchmarkJoinColumns(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = JoinColumns(
			&Column{Name: "a"},
			&Column{Name: "b"},
			&Column{Name: "c"},
		)
	}
}

func BenchmarkColumnsHash(b *testing.B) {
	c := JoinColumns(
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

func BenchmarkColumnsCompile(b *testing.B) {
	c := JoinColumns(
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

func BenchmarkColumnsCompileNoCache(b *testing.B) {
	for i := 0; i < b.N; i++ {
		c := JoinColumns(
			&Column{Name: "id"},
			&Column{Name: "customer"},
			&Column{Name: "service_id"},
			&Column{Name: "role.name"},
			&Column{Name: "role.id"},
		)
		_, _ = c.Compile(defaultTemplate)
	}
}
