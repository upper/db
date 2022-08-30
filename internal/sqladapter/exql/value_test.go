package exql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValue(t *testing.T) {
	val := NewValue(1)

	s, err := val.Compile(defaultTemplate)
	assert.NoError(t, err)
	assert.Equal(t, `'1'`, s)

	val = NewValue(&Raw{Value: "NOW()"})

	s, err = val.Compile(defaultTemplate)
	assert.NoError(t, err)
	assert.Equal(t, `NOW()`, s)
}

func TestSameRawValue(t *testing.T) {
	{
		val := NewValue(&Raw{Value: `"1"`})

		s, err := val.Compile(defaultTemplate)
		assert.NoError(t, err)
		assert.Equal(t, `"1"`, s)
	}
	{
		val := NewValue(&Raw{Value: `'1'`})

		s, err := val.Compile(defaultTemplate)
		assert.NoError(t, err)
		assert.Equal(t, `'1'`, s)
	}
	{
		val := NewValue(&Raw{Value: `1`})

		s, err := val.Compile(defaultTemplate)
		assert.NoError(t, err)
		assert.Equal(t, `1`, s)
	}
	{
		val := NewValue("1")

		s, err := val.Compile(defaultTemplate)
		assert.NoError(t, err)
		assert.Equal(t, `'1'`, s)
	}
	{
		val := NewValue(1)

		s, err := val.Compile(defaultTemplate)
		assert.NoError(t, err)
		assert.Equal(t, `'1'`, s)
	}
}

func TestValues(t *testing.T) {
	val := NewValueGroup(
		&Value{V: &Raw{Value: "1"}},
		&Value{V: &Raw{Value: "2"}},
		&Value{V: "3"},
	)

	s, err := val.Compile(defaultTemplate)
	assert.NoError(t, err)

	assert.Equal(t, `(1, 2, '3')`, s)
}

func BenchmarkValue(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = NewValue("a")
	}
}

func BenchmarkValueHash(b *testing.B) {
	v := NewValue("a")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = v.Hash()
	}
}

func BenchmarkValueCompile(b *testing.B) {
	v := NewValue("a")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = v.Compile(defaultTemplate)
	}
}

func BenchmarkValueCompileNoCache(b *testing.B) {
	for i := 0; i < b.N; i++ {
		v := NewValue("a")
		_, _ = v.Compile(defaultTemplate)
	}
}

func BenchmarkValues(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = NewValueGroup(NewValue("a"), NewValue("b"))
	}
}

func BenchmarkValuesHash(b *testing.B) {
	vs := NewValueGroup(NewValue("a"), NewValue("b"))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = vs.Hash()
	}
}

func BenchmarkValuesCompile(b *testing.B) {
	vs := NewValueGroup(NewValue("a"), NewValue("b"))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = vs.Compile(defaultTemplate)
	}
}

func BenchmarkValuesCompileNoCache(b *testing.B) {
	for i := 0; i < b.N; i++ {
		vs := NewValueGroup(NewValue("a"), NewValue("b"))
		_, _ = vs.Compile(defaultTemplate)
	}
}
