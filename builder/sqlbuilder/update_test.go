package sqlbuilder

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"upper.io/db.v2/builder"
)

func TestUpdateSpecific(t *testing.T) {
	b := NewBuilderWithTemplate(&testTemplate)
	assert := assert.New(t)

	assert.Equal(
		`UPDATE "artist" SET "name" = $1`,
		b.Update("artist").Set("name", "Artist").String(),
	)

	assert.Equal(
		`UPDATE "artist" SET "name" = $1 WHERE ("id" < $2)`,
		b.Update("artist").Set("name = ?", "Artist").Where("id <", 5).String(),
	)

	assert.Equal(
		`UPDATE "artist" SET "name" = $1 WHERE ("id" < $2)`,
		b.Update("artist").Set(map[string]string{"name": "Artist"}).Where(builder.M{"id <": 5}).String(),
	)

	assert.Equal(
		`UPDATE "artist" SET "name" = $1 WHERE ("id" < $2)`,
		b.Update("artist").Set(struct {
			Nombre string `db:"name"`
		}{"Artist"}).Where(builder.M{"id <": 5}).String(),
	)

	assert.Equal(
		`UPDATE "artist" SET "name" = $1, "last_name" = $2 WHERE ("id" < $3)`,
		b.Update("artist").Set(struct {
			Nombre string `db:"name"`
		}{"Artist"}).Set(map[string]string{"last_name": "Foo"}).Where(builder.M{"id <": 5}).String(),
	)

	assert.Equal(
		`UPDATE "artist" SET "name" = $1 || ' ' || $2 || id, "id" = id + $3 WHERE (id > $4)`,
		b.Update("artist").Set(
			"name = ? || ' ' || ? || id", "Artist", "#",
			"id = id + ?", 10,
		).Where("id > ?", 0).String(),
	)
}

func BenchmarkUpdate1(b *testing.B) {
	bt := NewBuilderWithTemplate(&testTemplate)
	for n := 0; n < b.N; n++ {
		bt.Update("artist").Set("name", "Artist").String()
	}
}

func BenchmarkUpdate2(b *testing.B) {
	bt := NewBuilderWithTemplate(&testTemplate)
	for n := 0; n < b.N; n++ {
		bt.Update("artist").Set("name = ?", "Artist").Where("id <", 5).String()
	}
}

func BenchmarkUpdate3(b *testing.B) {
	bt := NewBuilderWithTemplate(&testTemplate)
	for n := 0; n < b.N; n++ {
		bt.Update("artist").Set(struct {
			Nombre string `db:"name"`
		}{"Artist"}).Set(map[string]string{"last_name": "Foo"}).Where(builder.M{"id <": 5}).String()
	}
}

func BenchmarkUpdate4(b *testing.B) {
	bt := NewBuilderWithTemplate(&testTemplate)
	for n := 0; n < b.N; n++ {
		bt.Update("artist").Set(map[string]string{"name": "Artist"}).Where(builder.M{"id <": 5}).String()
	}
}

func BenchmarkUpdate5(b *testing.B) {
	bt := NewBuilderWithTemplate(&testTemplate)
	for n := 0; n < b.N; n++ {
		bt.Update("artist").Set(
			"name = ? || ' ' || ? || id", "Artist", "#",
			"id = id + ?", 10,
		).Where("id > ?", 0).String()
	}
}
