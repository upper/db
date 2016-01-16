package sqlbuilder

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"upper.io/db.v2/builder"
)

func TestSelectSpecific(t *testing.T) {

	b := NewBuilderWithTemplate(&testTemplate)
	assert := assert.New(t)

	assert.Equal(
		`SELECT * FROM "artist"`,
		b.SelectAllFrom("artist").String(),
	)

	assert.Equal(
		`SELECT * FROM "artist"`,
		b.Select().From("artist").String(),
	)

	assert.Equal(
		`SELECT * FROM "artist" ORDER BY "name" DESC`,
		b.Select().From("artist").OrderBy("name DESC").String(),
	)

	assert.Equal(
		`SELECT * FROM "artist" ORDER BY "name" DESC`,
		b.Select().From("artist").OrderBy("-name").String(),
	)

	assert.Equal(
		`SELECT * FROM "artist" ORDER BY "name" ASC`,
		b.Select().From("artist").OrderBy("name").String(),
	)

	assert.Equal(
		`SELECT * FROM "artist" ORDER BY "name" ASC`,
		b.Select().From("artist").OrderBy("name ASC").String(),
	)

	assert.Equal(
		`SELECT * FROM "artist" LIMIT -1 OFFSET 5`,
		b.Select().From("artist").Limit(-1).Offset(5).String(),
	)

	assert.Equal(
		`SELECT "id" FROM "artist"`,
		b.Select("id").From("artist").String(),
	)

	assert.Equal(
		`SELECT "id", "name" FROM "artist"`,
		b.Select("id", "name").From("artist").String(),
	)

	assert.Equal(
		`SELECT * FROM "artist" WHERE ("name" = $1)`,
		b.SelectAllFrom("artist").Where("name", "Haruki").String(),
	)

	assert.Equal(
		`SELECT * FROM "artist" WHERE (name LIKE $1)`,
		b.SelectAllFrom("artist").Where("name LIKE ?", `%F%`).String(),
	)

	assert.Equal(
		`SELECT "id" FROM "artist" WHERE (name LIKE $1 OR name LIKE $2)`,
		b.Select("id").From("artist").Where(`name LIKE ? OR name LIKE ?`, `%Miya%`, `F%`).String(),
	)

	assert.Equal(
		`SELECT * FROM "artist" WHERE ("id" > $1)`,
		b.SelectAllFrom("artist").Where("id >", 2).String(),
	)

	assert.Equal(
		`SELECT * FROM "artist" WHERE (id <= 2 AND name != $1)`,
		b.SelectAllFrom("artist").Where("id <= 2 AND name != ?", "A").String(),
	)

	assert.Equal(
		`SELECT * FROM "artist" WHERE ("id" IN ($1, $2, $3, $4))`,
		b.SelectAllFrom("artist").Where("id IN", []int{1, 9, 8, 7}).String(),
	)

	assert.Equal(
		`SELECT * FROM "artist" WHERE ("id" IN ($1, $2, $3, $4))`,
		b.SelectAllFrom("artist").Where(`"id" IN ?`, []int{1, 9, 8, 7}).String(),
	)

	assert.Equal(
		`SELECT * FROM "artist" WHERE ("id" IN ($1, $2) OR "name" IN ($3, $4))`,
		b.SelectAllFrom("artist").Where(`"id" IN ? OR "name" IN ?`, []int{1, 9}, []string{"John", "Smith"}).String(),
	)

	assert.Equal(
		`SELECT * FROM "artist" WHERE (name IS NOT NULL)`,
		b.SelectAllFrom("artist").Where("name IS NOT NULL").String(),
	)

	assert.Equal(
		`SELECT * FROM "artist" AS "a", "publication" AS "p" WHERE (p.author_id = a.id) LIMIT 1`,
		b.Select().From("artist a", "publication as p").Where("p.author_id = a.id").Limit(1).String(),
	)

	assert.Equal(
		`SELECT "id" FROM "artist" NATURAL JOIN "publication"`,
		b.Select("id").From("artist").Join("publication").String(),
	)

	assert.Equal(
		`SELECT * FROM "artist" AS "a" JOIN "publication" AS "p" ON (p.author_id = a.id) LIMIT 1`,
		b.SelectAllFrom("artist a").Join("publication p").On("p.author_id = a.id").Limit(1).String(),
	)

	assert.Equal(
		`SELECT * FROM "artist" AS "a" JOIN "publication" AS "p" ON (p.author_id = a.id) WHERE ("a"."id" = $1) LIMIT 1`,
		b.SelectAllFrom("artist a").Join("publication p").On("p.author_id = a.id").Where("a.id", 2).Limit(1).String(),
	)

	assert.Equal(
		`SELECT * FROM "artist" JOIN "publication" AS "p" ON (p.author_id = a.id) WHERE (a.id = 2) LIMIT 1`,
		b.SelectAllFrom("artist").Join("publication p").On("p.author_id = a.id").Where("a.id = 2").Limit(1).String(),
	)

	assert.Equal(
		`SELECT * FROM "artist" AS "a" JOIN "publication" AS "p" ON (p.title LIKE $1 OR p.title LIKE $2) WHERE (a.id = $3) LIMIT 1`,
		b.SelectAllFrom("artist a").Join("publication p").On("p.title LIKE ? OR p.title LIKE ?", "%Totoro%", "%Robot%").Where("a.id = ?", 2).Limit(1).String(),
	)

	assert.Equal(
		`SELECT * FROM "artist" AS "a" LEFT JOIN "publication" AS "p1" ON (p1.id = a.id) RIGHT JOIN "publication" AS "p2" ON (p2.id = a.id)`,
		b.SelectAllFrom("artist a").
			LeftJoin("publication p1").On("p1.id = a.id").
			RightJoin("publication p2").On("p2.id = a.id").
			String(),
	)

	assert.Equal(
		`SELECT * FROM "artist" CROSS JOIN "publication"`,
		b.SelectAllFrom("artist").CrossJoin("publication").String(),
	)

	assert.Equal(
		`SELECT * FROM "artist" JOIN "publication" USING ("id")`,
		b.SelectAllFrom("artist").Join("publication").Using("id").String(),
	)

	assert.Equal(
		`SELECT DATE()`,
		b.Select(builder.Raw("DATE()")).String(),
	)
}

func BenchmarkSelect1(b *testing.B) {
	bt := NewBuilderWithTemplate(&testTemplate)
	for n := 0; n < b.N; n++ {
		bt.Select().From("artist").OrderBy("name DESC").String()
	}
}

func BenchmarkSelect2(b *testing.B) {
	bt := NewBuilderWithTemplate(&testTemplate)
	for n := 0; n < b.N; n++ {
		bt.Select("id").From("artist").Where(`name LIKE ? OR name LIKE ?`, `%Miya%`, `F%`).String()
	}
}

func BenchmarkSelect3(b *testing.B) {
	bt := NewBuilderWithTemplate(&testTemplate)
	for n := 0; n < b.N; n++ {
		bt.Select().From("artist a", "publication as p").Where("p.author_id = a.id").Limit(1).String()
	}
}

func BenchmarkSelect4(b *testing.B) {
	bt := NewBuilderWithTemplate(&testTemplate)
	for n := 0; n < b.N; n++ {
		bt.SelectAllFrom("artist").Join("publication p").On("p.author_id = a.id").Where("a.id = 2").Limit(1).String()
	}
}

func BenchmarkSelect5(b *testing.B) {
	bt := NewBuilderWithTemplate(&testTemplate)
	for n := 0; n < b.N; n++ {
		bt.SelectAllFrom("artist a").
			LeftJoin("publication p1").On("p1.id = a.id").
			RightJoin("publication p2").On("p2.id = a.id").
			String()
	}
}
