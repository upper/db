package sqlgen

import (
	"fmt"
	"testing"
)

func TestDatabaseHash(t *testing.T) {
	var s, e string

	column := Database{Name: "users"}

	s = column.Hash()
	e = fmt.Sprintf(`Database{Name:"%s"}`, column.Name)

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}
}

func TestDatabaseCompile(t *testing.T) {
	var s, e string

	column := Database{Name: "name"}

	s = column.Compile(defaultTemplate)
	e = `"name"`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}
}

func BenchmarkDatabaseHash(b *testing.B) {
	c := Database{Name: "name"}
	for i := 0; i < b.N; i++ {
		c.Hash()
	}
}

func BenchmarkDatabaseCompile(b *testing.B) {
	c := Database{Name: "name"}
	for i := 0; i < b.N; i++ {
		c.Compile(defaultTemplate)
	}
}

func BenchmarkDatabaseCompileNoCache(b *testing.B) {
	for i := 0; i < b.N; i++ {
		c := Database{Name: "name"}
		c.Compile(defaultTemplate)
	}
}
