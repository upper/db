package sqlbuilder

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInsertSpecific(t *testing.T) {
	b := NewBuilderWithTemplate(&testTemplate)
	assert := assert.New(t)

	assert.Equal(
		`INSERT INTO "artist" VALUES ($1, $2), ($3, $4), ($5, $6)`,
		b.InsertInto("artist").
			Values(10, "Ryuichi Sakamoto").
			Values(11, "Alondra de la Parra").
			Values(12, "Haruki Murakami").
			String(),
	)

	assert.Equal(
		`INSERT INTO "artist" ("id", "name") VALUES ($1, $2)`,
		b.InsertInto("artist").Values(map[string]string{"id": "12", "name": "Chavela Vargas"}).String(),
	)

	assert.Equal(
		`INSERT INTO "artist" ("id", "name") VALUES ($1, $2) RETURNING "id"`,
		b.InsertInto("artist").Values(map[string]string{"id": "12", "name": "Chavela Vargas"}).Returning("id").String(),
	)

	assert.Equal(
		`INSERT INTO "artist" ("id", "name") VALUES ($1, $2)`,
		b.InsertInto("artist").Values(map[string]interface{}{"name": "Chavela Vargas", "id": 12}).String(),
	)

	assert.Equal(
		`INSERT INTO "artist" ("id", "name") VALUES ($1, $2)`,
		b.InsertInto("artist").Values(struct {
			ID   int    `db:"id"`
			Name string `db:"name"`
		}{12, "Chavela Vargas"}).String(),
	)

	assert.Equal(
		`INSERT INTO "artist" ("name", "id") VALUES ($1, $2)`,
		b.InsertInto("artist").Columns("name", "id").Values("Chavela Vargas", 12).String(),
	)
}

func BenchmarkInsert1(b *testing.B) {
	bt := NewBuilderWithTemplate(&testTemplate)
	for n := 0; n < b.N; n++ {
		bt.InsertInto("artist").Values(10, "Ryuichi Sakamoto").Values(11, "Alondra de la Parra").Values(12, "Haruki Murakami").String()
	}
}

func BenchmarkInsert2(b *testing.B) {
	bt := NewBuilderWithTemplate(&testTemplate)
	for n := 0; n < b.N; n++ {
		bt.InsertInto("artist").Values(map[string]string{"id": "12", "name": "Chavela Vargas"}).String()
	}
}

func BenchmarkInsert3(b *testing.B) {
	bt := NewBuilderWithTemplate(&testTemplate)
	for n := 0; n < b.N; n++ {
		bt.InsertInto("artist").Values(map[string]string{"id": "12", "name": "Chavela Vargas"}).Returning("id").String()
	}
}

func BenchmarkInsert4(b *testing.B) {
	bt := NewBuilderWithTemplate(&testTemplate)
	for n := 0; n < b.N; n++ {
		bt.InsertInto("artist").Values(map[string]interface{}{"name": "Chavela Vargas", "id": 12}).String()
	}
}

func BenchmarkInsert5(b *testing.B) {
	bt := NewBuilderWithTemplate(&testTemplate)
	for n := 0; n < b.N; n++ {
		bt.InsertInto("artist").Values(struct {
			ID   int    `db:"id"`
			Name string `db:"name"`
		}{12, "Chavela Vargas"}).String()
	}
}
