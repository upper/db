package exql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestColumnString(t *testing.T) {
	column := Column{Name: "role.name"}
	s, err := column.Compile(defaultTemplate)
	assert.NoError(t, err)
	assert.Equal(t, `"role"."name"`, s)
}

func TestColumnAs(t *testing.T) {
	column := Column{Name: "role.name as foo"}
	s, err := column.Compile(defaultTemplate)
	assert.NoError(t, err)
	assert.Equal(t, `"role"."name" AS "foo"`, s)
}

func TestColumnImplicitAs(t *testing.T) {
	column := Column{Name: "role.name foo"}
	s, err := column.Compile(defaultTemplate)
	assert.NoError(t, err)
	assert.Equal(t, `"role"."name" AS "foo"`, s)
}

func TestColumnRaw(t *testing.T) {
	column := Column{Name: &Raw{Value: "role.name As foo"}}
	s, err := column.Compile(defaultTemplate)
	assert.NoError(t, err)
	assert.Equal(t, `role.name As foo`, s)
}

func BenchmarkColumnWithName(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = ColumnWithName("a")
	}
}

func BenchmarkColumnHash(b *testing.B) {
	c := Column{Name: "name"}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Hash()
	}
}

func BenchmarkColumnCompile(b *testing.B) {
	c := Column{Name: "name"}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = c.Compile(defaultTemplate)
	}
}

func BenchmarkColumnCompileNoCache(b *testing.B) {
	for i := 0; i < b.N; i++ {
		c := Column{Name: "name"}
		_, _ = c.Compile(defaultTemplate)
	}
}

func BenchmarkColumnWithDotCompile(b *testing.B) {
	c := Column{Name: "role.name"}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = c.Compile(defaultTemplate)
	}
}

func BenchmarkColumnWithImplicitAsKeywordCompile(b *testing.B) {
	c := Column{Name: "role.name foo"}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = c.Compile(defaultTemplate)
	}
}

func BenchmarkColumnWithAsKeywordCompile(b *testing.B) {
	c := Column{Name: "role.name AS foo"}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = c.Compile(defaultTemplate)
	}
}
