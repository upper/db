package sqlgen

import (
	"bytes"
	"regexp"
	"strings"
	"testing"
)

func TestUtilIsSpace(t *testing.T) {
	if isSpace(' ') == false {
		t.Fail()
	}
	if isSpace('\n') == false {
		t.Fail()
	}
	if isSpace('\t') == false {
		t.Fail()
	}
	if isSpace('\r') == false {
		t.Fail()
	}
	if isSpace('x') == true {
		t.Fail()
	}
}

func TestUtilTrimByte(t *testing.T) {
	var trimmed []byte

	trimmed = trimByte([]byte("  \t\nHello World!     \n"))
	if string(trimmed) != "Hello World!" {
		t.Fatalf("Got: %s\n", string(trimmed))
	}

	trimmed = trimByte([]byte("Nope"))
	if string(trimmed) != "Nope" {
		t.Fatalf("Got: %s\n", string(trimmed))
	}

	trimmed = trimByte([]byte(""))
	if string(trimmed) != "" {
		t.Fatalf("Got: %s\n", string(trimmed))
	}

	trimmed = trimByte(nil)
	if string(trimmed) != "" {
		t.Fatalf("Got: %s\n", string(trimmed))
	}
}

func TestUtilSeparateByComma(t *testing.T) {
	chunks := separateByComma("Hello,,World!,Enjoy")

	if len(chunks) != 4 {
		t.Fatal()
	}

	if chunks[0] != "Hello" {
		t.Fatal()
	}
	if chunks[1] != "" {
		t.Fatal()
	}
	if chunks[2] != "World!" {
		t.Fatal()
	}
	if chunks[3] != "Enjoy" {
		t.Fatal()
	}
}

func TestUtilSeparateBySpace(t *testing.T) {
	chunks := separateBySpace("       Hello        World!        Enjoy")

	if len(chunks) != 3 {
		t.Fatal()
	}

	if chunks[0] != "Hello" {
		t.Fatal()
	}
	if chunks[1] != "World!" {
		t.Fatal()
	}
	if chunks[2] != "Enjoy" {
		t.Fatal()
	}
}

func TestUtilSeparateByAS(t *testing.T) {
	var chunks []string

	var tests = []string{
		`table.Name AS myTableAlias`,
		`table.Name     AS         myTableAlias`,
		"table.Name\tAS\r\nmyTableAlias",
	}

	for _, test := range tests {
		chunks = separateByAS(test)

		if len(chunks) != 2 {
			t.Fatalf(`Expecting 2 results.`)
		}

		if chunks[0] != "table.Name" {
			t.Fatal(`Expecting first result to be "table.Name".`)
		}
		if chunks[1] != "myTableAlias" {
			t.Fatal(`Expecting second result to be myTableAlias.`)
		}
	}

	// Single character.
	chunks = separateByAS("a")

	if len(chunks) != 1 {
		t.Fatalf(`Expecting 1 results.`)
	}

	if chunks[0] != "a" {
		t.Fatal(`Expecting first result to be "a".`)
	}

	// Empty name
	chunks = separateByAS("")

	if len(chunks) != 1 {
		t.Fatalf(`Expecting 1 results.`)
	}

	if chunks[0] != "" {
		t.Fatal(`Expecting first result to be "".`)
	}

	// Single name
	chunks = separateByAS("  A Single Table ")

	if len(chunks) != 1 {
		t.Fatalf(`Expecting 1 results.`)
	}

	if chunks[0] != "A Single Table" {
		t.Fatal(`Expecting first result to be "ASingleTable".`)
	}

	// Minimal expression.
	chunks = separateByAS("a AS b")

	if len(chunks) != 2 {
		t.Fatalf(`Expecting 2 results.`)
	}

	if chunks[0] != "a" {
		t.Fatal(`Expecting first result to be "a".`)
	}

	if chunks[1] != "b" {
		t.Fatal(`Expecting first result to be "b".`)
	}

	// Minimal expression with spaces.
	chunks = separateByAS("   a    AS    b ")

	if len(chunks) != 2 {
		t.Fatalf(`Expecting 2 results.`)
	}

	if chunks[0] != "a" {
		t.Fatal(`Expecting first result to be "a".`)
	}

	if chunks[1] != "b" {
		t.Fatal(`Expecting first result to be "b".`)
	}

	// Minimal expression + 1 with spaces.
	chunks = separateByAS("   a    AS    bb ")

	if len(chunks) != 2 {
		t.Fatalf(`Expecting 2 results.`)
	}

	if chunks[0] != "a" {
		t.Fatal(`Expecting first result to be "a".`)
	}

	if chunks[1] != "bb" {
		t.Fatal(`Expecting first result to be "bb".`)
	}
}

func BenchmarkUtilIsSpace(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = isSpace(' ')
	}
}

func BenchmarkUtilTrimByte(b *testing.B) {
	s := []byte("               Hello world!             ")
	for i := 0; i < b.N; i++ {
		_ = trimByte(s)
	}
}

func BenchmarkUtilTrimString(b *testing.B) {
	s := "               Hello world!             "
	for i := 0; i < b.N; i++ {
		_ = trimString(s)
	}
}

func BenchmarkUtilStdBytesTrimSpace(b *testing.B) {
	s := []byte("               Hello world!             ")
	for i := 0; i < b.N; i++ {
		_ = bytes.TrimSpace(s)
	}
}

func BenchmarkUtilStdStringsTrimSpace(b *testing.B) {
	s := "               Hello world!             "
	for i := 0; i < b.N; i++ {
		_ = strings.TrimSpace(s)
	}
}

func BenchmarkUtilSeparateByComma(b *testing.B) {
	s := "Hello,,World!,Enjoy"
	for i := 0; i < b.N; i++ {
		_ = separateByComma(s)
	}
}

func BenchmarkUtilSeparateBySpace(b *testing.B) {
	s := " Hello  World! Enjoy"
	for i := 0; i < b.N; i++ {
		_ = separateBySpace(s)
	}
}

func BenchmarkUtilSeparateByAS(b *testing.B) {
	s := "table.Name AS myTableAlias"
	for i := 0; i < b.N; i++ {
		_ = separateByAS(s)
	}
}

func BenchmarkUtilSeparateByCommaRegExp(b *testing.B) {
	sep := regexp.MustCompile(`\s*?,\s*?`)
	s := "Hello,,World!,Enjoy"
	for i := 0; i < b.N; i++ {
		_ = sep.Split(s, -1)
	}
}

func BenchmarkUtilSeparateBySpaceRegExp(b *testing.B) {
	sep := regexp.MustCompile(`\s+`)
	s := " Hello  World! Enjoy"
	for i := 0; i < b.N; i++ {
		_ = sep.Split(s, -1)
	}
}

func BenchmarkUtilSeparateByASRegExp(b *testing.B) {
	sep := regexp.MustCompile(`(?i:\s+AS\s+)`)
	s := "table.Name AS myTableAlias"
	for i := 0; i < b.N; i++ {
		_ = sep.Split(s, -1)
	}
}
