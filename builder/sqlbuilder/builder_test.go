package sqlbuilder

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"upper.io/db.v2/builder"
	"upper.io/db.v2/builder/internal/util"
)

func TestSelect(t *testing.T) {

	b := &sqlBuilder{t: util.NewTemplateWithUtils(&testTemplate)}
	assert := assert.New(t)

	assert.Equal(
		`SELECT DATE()`,
		b.Select(builder.Func("DATE")).String(),
	)

	assert.Equal(
		`SELECT * FROM "artist"`,
		b.SelectAllFrom("artist").String(),
	)

	assert.Equal(
		`SELECT DISTINCT(name) FROM "artist"`,
		b.Select(builder.Func("DISTINCT", "name")).From("artist").String(),
	)

	assert.Equal(
		`SELECT * FROM "artist" WHERE ("id" NOT IN ($1, $2))`,
		b.Select().From("artist").Where(builder.M{"id NOT IN": []int{0, -1}}).String(),
	)

	assert.Equal(
		`SELECT * FROM "artist" WHERE ("id" NOT IN ($1))`,
		b.Select().From("artist").Where(builder.M{"id NOT IN": []int{-1}}).String(),
	)

	assert.Equal(
		`SELECT * FROM "artist" WHERE ("id" IN ($1, $2))`,
		b.Select().From("artist").Where(builder.M{"id IN": []int{0, -1}}).String(),
	)

	assert.Equal(
		`SELECT * FROM "artist" WHERE (("id" = $1 OR "id" = $2 OR "id" = $3))`,
		b.Select().From("artist").Where(
			builder.Or(
				builder.M{"id": 1},
				builder.M{"id": 2},
				builder.M{"id": 3},
			),
		).String(),
	)

	assert.Equal(
		`SELECT * FROM "artist" WHERE ((("id" = $1 OR "id" = $2 OR "id" IS NULL) OR ("name" = $3 OR "name" = $4)))`,
		b.Select().From("artist").Where(
			builder.Or(
				builder.Or(
					builder.M{"id": 1},
					builder.M{"id": 2},
					builder.M{"id IS": nil},
				),
				builder.Or(
					builder.M{"name": "John"},
					builder.M{"name": "Peter"},
				),
			),
		).String(),
	)

	assert.Equal(
		`SELECT * FROM "artist" WHERE ((("id" = $1 OR "id" = $2 OR "id" = $3 OR "id" = $4) AND ("name" = $5 AND "last_name" = $6) AND "age" > $7))`,
		b.Select().From("artist").Where(
			builder.And(
				builder.Or(
					builder.M{"id": 1},
					builder.M{"id": 2},
					builder.M{"id": 3},
				).Or(
					builder.M{"id": 4},
				),
				builder.Or(),
				builder.And(
					builder.M{"name": "John"},
					builder.M{"last_name": "Smith"},
				),
				builder.And(),
			).And(
				builder.M{"age >": "20"},
			),
		).String(),
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

func TestInsert(t *testing.T) {
	b := &sqlBuilder{t: util.NewTemplateWithUtils(&testTemplate)}
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

func TestUpdate(t *testing.T) {
	b := &sqlBuilder{t: util.NewTemplateWithUtils(&testTemplate)}
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
