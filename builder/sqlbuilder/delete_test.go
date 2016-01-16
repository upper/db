package sqlbuilder

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDelete(t *testing.T) {
	bt := NewBuilderWithTemplate(&testTemplate)
	assert := assert.New(t)

	assert.Equal(
		`DELETE FROM "artist" WHERE (name = $1)`,
		bt.DeleteFrom("artist").Where("name = ?", "Chavela Vargas").String(),
	)

	assert.Equal(
		`DELETE FROM "artist" WHERE (id > 5)`,
		bt.DeleteFrom("artist").Where("id > 5").String(),
	)
}

func BenchmarkDelete1(b *testing.B) {
	bt := NewBuilderWithTemplate(&testTemplate)
	for n := 0; n < b.N; n++ {
		bt.DeleteFrom("artist").Where("name = ?", "Chavela Vargas").Limit(1).String()
	}
}

func BenchmarkDelete2(b *testing.B) {
	bt := NewBuilderWithTemplate(&testTemplate)
	for n := 0; n < b.N; n++ {
		bt.DeleteFrom("artist").Where("id > 5").String()
	}
}
