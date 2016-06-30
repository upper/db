//go:generate bash -c "sed s/ADAPTER/postgresql/g ../internal/sqladapter/testing/adapter_benchmark.go.tpl > generated_benchmark_test.go"
package postgresql

const (
	truncateArtist                             = `TRUNCATE TABLE "artist" RESTART IDENTITY`
	insertHayaoMiyazaki                        = `INSERT INTO "artist" ("name") VALUES('Hayao Miyazaki') RETURNING "id"`
	insertIntoArtistWithPlaceholderReturningID = `INSERT INTO "artist" ("name") VALUES($1) RETURNING "id"`
	selectFromArtistWhereName                  = `SELECT * FROM "artist" WHERE "name" = $1`
	updateArtistWhereName                      = `UPDATE "artist" SET "name" = $1 WHERE "name" = $2`
	deleteArtistWhereName                      = `DELETE FROM "artist" WHERE "name" = $1`
)
