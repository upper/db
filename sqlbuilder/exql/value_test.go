package exql

import (
	"testing"
)

func TestValue(t *testing.T) {
	var s, e string
	var val *Value

	val = NewValue(1)

	s = val.Compile(defaultTemplate)
	e = `'1'`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}

	val = NewValue(&Raw{Value: "NOW()"})

	s = val.Compile(defaultTemplate)
	e = `NOW()`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}
}

func TestValues(t *testing.T) {
	var s, e string

	val := NewValueGroup(
		&Value{V: &Raw{Value: "1"}},
		&Value{V: &Raw{Value: "2"}},
		&Value{V: "3"},
	)

	s = val.Compile(defaultTemplate)
	e = `(1, 2, '3')`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}
}

func BenchmarkValue(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = NewValue("a")
	}
}

func BenchmarkValueHash(b *testing.B) {
	v := NewValue("a")
	for i := 0; i < b.N; i++ {
		_ = v.Hash()
	}
}

func BenchmarkValueCompile(b *testing.B) {
	v := NewValue("a")
	for i := 0; i < b.N; i++ {
		_ = v.Compile(defaultTemplate)
	}
}

func BenchmarkValueCompileNoCache(b *testing.B) {
	for i := 0; i < b.N; i++ {
		v := NewValue("a")
		_ = v.Compile(defaultTemplate)
	}
}

func BenchmarkValues(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = NewValueGroup(NewValue("a"), NewValue("b"))
	}
}

func BenchmarkValuesHash(b *testing.B) {
	vs := NewValueGroup(NewValue("a"), NewValue("b"))
	for i := 0; i < b.N; i++ {
		_ = vs.Hash()
	}
}

func BenchmarkValuesCompile(b *testing.B) {
	vs := NewValueGroup(NewValue("a"), NewValue("b"))
	for i := 0; i < b.N; i++ {
		_ = vs.Compile(defaultTemplate)
	}
}

func BenchmarkValuesCompileNoCache(b *testing.B) {
	for i := 0; i < b.N; i++ {
		vs := NewValueGroup(NewValue("a"), NewValue("b"))
		_ = vs.Compile(defaultTemplate)
	}
}
