package exql

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDatabaseCompile(t *testing.T) {
	column := Database{Name: "name"}
	s, err := column.Compile(defaultTemplate)
	assert.NoError(t, err)
	assert.Equal(t, `"name"`, s)
}

func BenchmarkDatabaseHash(b *testing.B) {
	c := Database{Name: "name"}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Hash()
	}
}

func BenchmarkDatabaseCompile(b *testing.B) {
	c := Database{Name: "name"}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = c.Compile(defaultTemplate)
	}
}

func BenchmarkDatabaseCompileNoCache(b *testing.B) {
	for i := 0; i < b.N; i++ {
		c := Database{Name: "name"}
		_, _ = c.Compile(defaultTemplate)
	}
}

func BenchmarkDatabaseCompileNoCache2(b *testing.B) {
	for i := 0; i < b.N; i++ {
		c := Database{Name: strconv.Itoa(i)}
		_, _ = c.Compile(defaultTemplate)
	}
}
