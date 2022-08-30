package exql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRawString(t *testing.T) {
	raw := &Raw{Value: "foo"}
	s, err := raw.Compile(defaultTemplate)
	assert.NoError(t, err)
	assert.Equal(t, `foo`, s)
}

func TestRawCompile(t *testing.T) {
	raw := &Raw{Value: "foo"}
	s, err := raw.Compile(defaultTemplate)
	assert.NoError(t, err)
	assert.Equal(t, `foo`, s)
}

func BenchmarkRawCreate(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = Raw{Value: "foo"}
	}
}

func BenchmarkRawString(b *testing.B) {
	raw := &Raw{Value: "foo"}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = raw.String()
	}
}

func BenchmarkRawCompile(b *testing.B) {
	raw := &Raw{Value: "foo"}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = raw.Compile(defaultTemplate)
	}
}

func BenchmarkRawHash(b *testing.B) {
	raw := &Raw{Value: "foo"}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		raw.Hash()
	}
}
