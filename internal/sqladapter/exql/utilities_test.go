package exql

import (
	"bytes"
	"regexp"
	"strings"
	"testing"
	"unicode"

	"github.com/stretchr/testify/assert"
)

const (
	blankSymbol         = ' '
	stringWithCommas    = "Hello,,World!,Enjoy"
	stringWithSpaces    = " Hello  World! Enjoy"
	stringWithASKeyword = "table.Name AS myTableAlias"
)

var (
	bytesWithLeadingBlanks  = []byte("               Hello world!             ")
	stringWithLeadingBlanks = string(bytesWithLeadingBlanks)
)

var (
	reInvisible = regexp.MustCompile(`[\t\n\r]`)
	reSpace     = regexp.MustCompile(`\s+`)
)

func mustTrim(a string, err error) string {
	if err != nil {
		panic(err.Error())
	}
	a = reInvisible.ReplaceAllString(strings.TrimSpace(a), " ")
	a = reSpace.ReplaceAllString(strings.TrimSpace(a), " ")
	return a
}

func TestUtilIsBlankSymbol(t *testing.T) {
	assert.True(t, isBlankSymbol(' '))
	assert.True(t, isBlankSymbol('\n'))
	assert.True(t, isBlankSymbol('\t'))
	assert.True(t, isBlankSymbol('\r'))
	assert.False(t, isBlankSymbol('x'))
}

func TestUtilTrimBytes(t *testing.T) {
	var trimmed []byte

	trimmed = trimBytes([]byte("  \t\nHello World!     \n"))
	assert.Equal(t, "Hello World!", string(trimmed))

	trimmed = trimBytes([]byte("Nope"))
	assert.Equal(t, "Nope", string(trimmed))

	trimmed = trimBytes([]byte(""))
	assert.Equal(t, "", string(trimmed))

	trimmed = trimBytes([]byte(" "))
	assert.Equal(t, "", string(trimmed))

	trimmed = trimBytes(nil)
	assert.Equal(t, "", string(trimmed))
}

func TestUtilSeparateByComma(t *testing.T) {
	chunks := separateByComma("Hello,,World!,Enjoy")
	assert.Equal(t, 4, len(chunks))

	assert.Equal(t, "Hello", chunks[0])
	assert.Equal(t, "", chunks[1])
	assert.Equal(t, "World!", chunks[2])
	assert.Equal(t, "Enjoy", chunks[3])
}

func TestUtilSeparateBySpace(t *testing.T) {
	chunks := separateBySpace("       Hello        World!        Enjoy")
	assert.Equal(t, 3, len(chunks))

	assert.Equal(t, "Hello", chunks[0])
	assert.Equal(t, "World!", chunks[1])
	assert.Equal(t, "Enjoy", chunks[2])
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
		assert.Len(t, chunks, 2)

		assert.Equal(t, "table.Name", chunks[0])
		assert.Equal(t, "myTableAlias", chunks[1])
	}

	// Single character.
	chunks = separateByAS("a")
	assert.Len(t, chunks, 1)
	assert.Equal(t, "a", chunks[0])

	// Empty name
	chunks = separateByAS("")
	assert.Len(t, chunks, 1)
	assert.Equal(t, "", chunks[0])

	// Single name
	chunks = separateByAS("  A Single Table ")
	assert.Len(t, chunks, 1)
	assert.Equal(t, "A Single Table", chunks[0])

	// Minimal expression.
	chunks = separateByAS("a AS b")
	assert.Len(t, chunks, 2)
	assert.Equal(t, "a", chunks[0])
	assert.Equal(t, "b", chunks[1])

	// Minimal expression with spaces.
	chunks = separateByAS("   a    AS    b ")
	assert.Len(t, chunks, 2)
	assert.Equal(t, "a", chunks[0])
	assert.Equal(t, "b", chunks[1])

	// Minimal expression + 1 with spaces.
	chunks = separateByAS("   a    AS    bb ")
	assert.Len(t, chunks, 2)
	assert.Equal(t, "a", chunks[0])
	assert.Equal(t, "bb", chunks[1])
}

func BenchmarkUtilIsBlankSymbol(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = isBlankSymbol(blankSymbol)
	}
}

func BenchmarkUtilStdlibIsBlankSymbol(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = unicode.IsSpace(blankSymbol)
	}
}

func BenchmarkUtilTrimBytes(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = trimBytes(bytesWithLeadingBlanks)
	}
}
func BenchmarkUtilStdlibBytesTrimSpace(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = bytes.TrimSpace(bytesWithLeadingBlanks)
	}
}

func BenchmarkUtilTrimString(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = trimString(stringWithLeadingBlanks)
	}
}

func BenchmarkUtilStdlibStringsTrimSpace(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = strings.TrimSpace(stringWithLeadingBlanks)
	}
}

func BenchmarkUtilSeparateByComma(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = separateByComma(stringWithCommas)
	}
}

func BenchmarkUtilRegExpSeparateByComma(b *testing.B) {
	sep := regexp.MustCompile(`\s*?,\s*?`)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = sep.Split(stringWithCommas, -1)
	}
}

func BenchmarkUtilSeparateBySpace(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = separateBySpace(stringWithSpaces)
	}
}

func BenchmarkUtilRegExpSeparateBySpace(b *testing.B) {
	sep := regexp.MustCompile(`\s+`)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = sep.Split(stringWithSpaces, -1)
	}
}

func BenchmarkUtilSeparateByAS(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = separateByAS(stringWithASKeyword)
	}
}

func BenchmarkUtilRegExpSeparateByAS(b *testing.B) {
	sep := regexp.MustCompile(`(?i:\s+AS\s+)`)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = sep.Split(stringWithASKeyword, -1)
	}
}
