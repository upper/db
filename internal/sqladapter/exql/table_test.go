package exql

import (
	"github.com/stretchr/testify/assert"

	"testing"
)

func TestTableSimple(t *testing.T) {
	table := TableWithName("artist")
	assert.Equal(t, `"artist"`, mustTrim(table.Compile(defaultTemplate)))
}

func TestTableCompound(t *testing.T) {
	table := TableWithName("artist.foo")
	assert.Equal(t, `"artist"."foo"`, mustTrim(table.Compile(defaultTemplate)))
}

func TestTableCompoundAlias(t *testing.T) {
	table := TableWithName("artist.foo AS baz")

	assert.Equal(t, `"artist"."foo" AS "baz"`, mustTrim(table.Compile(defaultTemplate)))
}

func TestTableImplicitAlias(t *testing.T) {
	table := TableWithName("artist.foo baz")

	assert.Equal(t, `"artist"."foo" AS "baz"`, mustTrim(table.Compile(defaultTemplate)))
}

func TestTableMultiple(t *testing.T) {
	table := TableWithName("artist.foo, artist.bar, artist.baz")

	assert.Equal(t, `"artist"."foo", "artist"."bar", "artist"."baz"`, mustTrim(table.Compile(defaultTemplate)))
}

func TestTableMultipleAlias(t *testing.T) {
	table := TableWithName("artist.foo AS foo, artist.bar as bar, artist.baz As baz")

	assert.Equal(t, `"artist"."foo" AS "foo", "artist"."bar" AS "bar", "artist"."baz" AS "baz"`, mustTrim(table.Compile(defaultTemplate)))
}

func TestTableMinimal(t *testing.T) {
	table := TableWithName("a")

	assert.Equal(t, `"a"`, mustTrim(table.Compile(defaultTemplate)))
}

func TestTableEmpty(t *testing.T) {
	table := TableWithName("")

	assert.Equal(t, "", mustTrim(table.Compile(defaultTemplate)))
}

func BenchmarkTableWithName(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = TableWithName("foo")
	}
}

func BenchmarkTableHash(b *testing.B) {
	t := TableWithName("name")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		t.Hash()
	}
}

func BenchmarkTableCompile(b *testing.B) {
	t := TableWithName("name")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = t.Compile(defaultTemplate)
	}
}

func BenchmarkTableCompileNoCache(b *testing.B) {
	for i := 0; i < b.N; i++ {
		t := TableWithName("name")
		_, _ = t.Compile(defaultTemplate)
	}
}
