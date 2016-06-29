//go:generate bash -c "sed s/ADAPTER/sqlite/g ../internal/sqladapter/testing/adapter_benchmark.go.tpl > generated_benchmark_test.go"
package sqlite

const (
	truncateArtist                             = `DELETE FROM "artist"`
	insertHayaoMiyazaki                        = `INSERT INTO "artist" ("name") VALUES('Hayao Miyazaki')`
	insertIntoArtistWithPlaceholderReturningID = `INSERT INTO "artist" ("name") VALUES(?)`
	selectFromArtistWhereName                  = `SELECT * FROM "artist" WHERE "name" = ?`
	updateArtistWhereName                      = `UPDATE "artist" SET "name" = ? WHERE "name" = ?`
	deleteArtistWhereName                      = `DELETE FROM "artist" WHERE "name" = ?`
)
