// Copyright (c) 2012-2014 José Carlos Nieto, https://menteslibres.net/xiam
//
// Permission is hereby granted, free of charge, to any person obtaining
// a copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to
// permit persons to whom the Software is furnished to do so, subject to
// the following conditions:
//
// The above copyright notice and this permission notice shall be
// included in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
// LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
// OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
// WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

package sqlite

// In order to execute these tests you must initialize the database first:
//
// cd _dumps
// make
// cd ..
// go test

import (
	"database/sql"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"menteslibres.net/gosexy/to"
	"upper.io/db"
	"upper.io/db/util/sqlutil"
)

const (
	database = `_dumps/gotest.sqlite3.db`
)

var settings = db.Settings{
	Database: database,
}

// Structure for testing conversions and datatypes.
type testValuesStruct struct {
	Uint   uint   `field:"_uint"`
	Uint8  uint8  `field:"_uint8"`
	Uint16 uint16 `field:"_uint16"`
	Uint32 uint32 `field:"_uint32"`
	Uint64 uint64 `field:"_uint64"`

	Int   int   `field:"_int"`
	Int8  int8  `field:"_int8"`
	Int16 int16 `field:"_int16"`
	Int32 int32 `field:"_int32"`
	Int64 int64 `field:"_int64"`

	Float32 float32 `field:"_float32"`
	Float64 float64 `field:"_float64"`

	Bool   bool   `field:"_bool"`
	String string `field:"_string"`

	Date time.Time     `field:"_date"`
	Time time.Duration `field:"_time"`
}

// Declaring some values to insert, we expect the same values to be returned.
var testValues = testValuesStruct{
	1, 1, 1, 1, 1,
	-1, -1, -1, -1, -1,
	1.337, 1.337,
	true,
	"Hello world!",
	time.Date(2012, 7, 28, 1, 2, 3, 0, time.Local),
	time.Second * time.Duration(7331),
}

// Loggin some information to stdout (like the SQL query and its
// arguments), useful for development.
func TestEnableDebug(t *testing.T) {
	os.Setenv(db.EnvEnableDebug, "TRUE")
}

// Attempts to open an empty datasource.
func TestOpenFailed(t *testing.T) {
	var err error

	// Attempt to open an empty database.
	if _, err = db.Open(Adapter, db.Settings{}); err == nil {
		// Must fail.
		t.Fatalf("Expecting an error.")
	}
}

// Attempts to get all collections and truncate each one of them.
func TestTruncate(t *testing.T) {
	var err error
	var sess db.Database
	var collections []string
	var col db.Collection

	// Opening database.
	if sess, err = db.Open(Adapter, settings); err != nil {
		t.Fatal(err)
	}

	// We should close the database when it's no longer in use.
	defer sess.Close()

	// Getting a list of all collections in this database.
	if collections, err = sess.Collections(); err != nil {
		t.Fatal(err)
	}

	if len(collections) == 0 {
		t.Fatalf("Expecting some collections.")
	}

	// Walking over collections.
	for _, name := range collections {

		// Getting a collection.
		if col, err = sess.Collection(name); err != nil {
			t.Fatal(err)
		}

		// Table must exists before we can use it.
		if col.Exists() == true {
			// Truncating the table.
			if err = col.Truncate(); err != nil {
				t.Fatal(err)
			}
		}
	}
}

// Attempts to append some data into the "artist" table.
func TestAppend(t *testing.T) {
	var err error
	var id interface{}
	var sess db.Database
	var artist db.Collection
	var total uint64

	if sess, err = db.Open(Adapter, settings); err != nil {
		t.Fatal(err)
	}

	defer sess.Close()

	if artist, err = sess.Collection("artist"); err != nil {
		t.Fatal(err)
	}

	// Attempt to append a map.
	item_m := map[string]string{
		"name": "Ozzie",
	}

	if id, err = artist.Append(item_m); err != nil {
		t.Fatal(err)
	}

	if to.Int64(id) == 0 {
		t.Fatalf("Expecting an ID.")
	}

	// Attempt to append a struct.
	item_s := struct {
		Name string `db:"name"`
	}{
		"Flea",
	}

	if id, err = artist.Append(item_s); err != nil {
		t.Fatal(err)
	}

	if to.Int64(id) == 0 {
		t.Fatalf("Expecting an ID.")
	}

	// Append to append a tagged struct.
	item_t := struct {
		ArtistName string `db:"name"`
	}{
		"Slash",
	}

	if id, err = artist.Append(item_t); err != nil {
		t.Fatal(err)
	}

	if to.Int64(id) == 0 {
		t.Fatalf("Expecting an ID.")
	}

	// Counting elements, must be exactly 3 elements.
	if total, err = artist.Find().Count(); err != nil {
		t.Fatal(err)
	}

	if total != 3 {
		t.Fatalf("Expecting exactly 3 rows.")
	}

}

// Attempts to count all rows in our newly defined set.
func TestResultCount(t *testing.T) {
	var err error
	var res db.Result
	var sess db.Database
	var artist db.Collection
	var total uint64

	if sess, err = db.Open(Adapter, settings); err != nil {
		t.Fatal(err)
	}

	defer sess.Close()

	// We should close the database when it's no longer in use.
	if artist, err = sess.Collection("artist"); err != nil {
		t.Fatal(err)
	}

	// Defining a set with no conditions.
	res = artist.Find()

	// Counting all the matching rows.
	if total, err = res.Count(); err != nil {
		t.Fatal(err)
	}

	if total == 0 {
		t.Fatalf("Counter should not be zero, we've just added some rows!")
	}
}

// Attempts to fetch results one by one.
func TestResultFetch(t *testing.T) {
	var err error
	var res db.Result
	var sess db.Database
	var artist db.Collection

	if sess, err = db.Open(Adapter, settings); err != nil {
		t.Fatal(err)
	}

	defer sess.Close()

	if artist, err = sess.Collection("artist"); err != nil {
		t.Fatal(err)
	}

	// Dumping into a map.
	row_m := map[string]interface{}{}

	res = artist.Find()

	for {
		err = res.Next(&row_m)

		if err == db.ErrNoMoreRows {
			break
		}

		if err == nil {
			if to.Int64(row_m["id"]) == 0 {
				t.Fatalf("Expecting a not null ID.")
			}
			if to.String(row_m["name"]) == "" {
				t.Fatalf("Expecting a name.")
			}
		} else {
			t.Fatal(err)
		}
	}

	res.Close()

	// Dumping into an struct with no tags.
	row_s := struct {
		Id   uint64
		Name string
	}{}

	res = artist.Find()

	for {
		err = res.Next(&row_s)

		if err == db.ErrNoMoreRows {
			break
		}

		if err == nil {
			if row_s.Id == 0 {
				t.Fatalf("Expecting a not null ID.")
			}
			if row_s.Name == "" {
				t.Fatalf("Expecting a name.")
			}
		} else {
			t.Fatal(err)
		}
	}

	res.Close()

	// Dumping into a tagged struct.
	row_t := struct {
		Value1 uint64 `field:"id"`
		Value2 string `field:"name"`
	}{}

	res = artist.Find()

	for {
		err = res.Next(&row_t)

		if err == db.ErrNoMoreRows {
			break
		}

		if err == nil {
			if row_t.Value1 == 0 {
				t.Fatalf("Expecting a not null ID.")
			}
			if row_t.Value2 == "" {
				t.Fatalf("Expecting a name.")
			}
		} else {
			t.Fatal(err)
		}
	}

	res.Close()

	// Dumping into an slice of maps.
	all_rows_m := []map[string]interface{}{}

	res = artist.Find()
	if err = res.All(&all_rows_m); err != nil {
		t.Fatal(err)
	}

	if len(all_rows_m) != 3 {
		t.Fatalf("Expecting 3 items.")
	}

	for _, single_row_m := range all_rows_m {
		if to.Int64(single_row_m["id"]) == 0 {
			t.Fatalf("Expecting a not null ID.")
		}
	}

	// Dumping into an slice of structs.

	all_rows_s := []struct {
		Id   uint64
		Name string
	}{}

	res = artist.Find()
	if err = res.All(&all_rows_s); err != nil {
		t.Fatal(err)
	}

	if len(all_rows_s) != 3 {
		t.Fatalf("Expecting 3 items.")
	}

	for _, single_row_s := range all_rows_s {
		if single_row_s.Id == 0 {
			t.Fatalf("Expecting a not null ID.")
		}
	}

	// Dumping into an slice of tagged structs.
	all_rows_t := []struct {
		Value1 uint64 `field:"id"`
		Value2 string `field:"name"`
	}{}

	res = artist.Find()

	if err = res.All(&all_rows_t); err != nil {
		t.Fatal(err)
	}

	if len(all_rows_t) != 3 {
		t.Fatalf("Expecting 3 items.")
	}

	for _, single_row_t := range all_rows_t {
		if single_row_t.Value1 == 0 {
			t.Fatalf("Expecting a not null ID.")
		}
	}
}

// Attempts to modify previously added rows.
func TestUpdate(t *testing.T) {
	var err error
	var sess db.Database
	var artist db.Collection

	if sess, err = db.Open(Adapter, settings); err != nil {
		t.Fatal(err)
	}

	defer sess.Close()

	if artist, err = sess.Collection("artist"); err != nil {
		t.Fatal(err)
	}

	// Defining destination struct
	value := struct {
		Id   uint64
		Name string
	}{}

	// Getting the first artist.
	res := artist.Find(db.Cond{"id !=": 0}).Limit(1)

	if err = res.One(&value); err != nil {
		t.Fatal(err)
	}

	// Updating set with a map
	row_m := map[string]interface{}{
		"name": strings.ToUpper(value.Name),
	}

	if err = res.Update(row_m); err != nil {
		t.Fatal(err)
	}

	// Pulling it again.
	if err = res.One(&value); err != nil {
		t.Fatal(err)
	}

	// Verifying.
	if value.Name != row_m["name"] {
		t.Fatalf("Expecting a modification.")
	}

	// Updating set with a struct
	row_s := struct {
		Name string
	}{strings.ToLower(value.Name)}

	if err = res.Update(row_s); err != nil {
		t.Fatal(err)
	}

	// Pulling it again.
	if err = res.One(&value); err != nil {
		t.Fatal(err)
	}

	// Verifying
	if value.Name != row_s.Name {
		t.Fatalf("Expecting a modification.")
	}

	// Updating set with a tagged struct
	row_t := struct {
		Value1 string `db:"name"`
	}{strings.Replace(value.Name, "z", "Z", -1)}

	if err = res.Update(row_t); err != nil {
		t.Fatal(err)
	}

	// Pulling it again.
	if err = res.One(&value); err != nil {
		t.Fatal(err)
	}

	// Verifying
	if value.Name != row_t.Value1 {
		t.Fatalf("Expecting a modification.")
	}
}

// Attempts to use functions within database queries.
func TestFunction(t *testing.T) {
	var err error
	var res db.Result
	var sess db.Database
	var artist db.Collection
	var total uint64

	if sess, err = db.Open(Adapter, settings); err != nil {
		t.Fatal(err)
	}

	defer sess.Close()

	if artist, err = sess.Collection("artist"); err != nil {
		t.Fatal(err)
	}

	row_s := struct {
		Id   uint64
		Name string
	}{}

	res = artist.Find(db.Cond{"id NOT IN": []int{0, -1}})

	if err = res.One(&row_s); err != nil {
		t.Fatal(err)
	}

	if total, err = res.Count(); err != nil {
		t.Fatal(err)
	}

	if total != 3 {
		t.Fatalf("Expecting 3 items.")
	}

	res = artist.Find(db.Cond{"id": db.Func{"NOT IN", []int{0, -1}}})

	if err = res.One(&row_s); err != nil {
		t.Fatal(err)
	}

	if total, err = res.Count(); err != nil {
		t.Fatal(err)
	}

	if total != 3 {
		t.Fatalf("Expecting 3 items.")
	}

	res.Close()
}

// Attempts to delete previously added rows.
func TestRemove(t *testing.T) {
	var err error
	var res db.Result
	var sess db.Database
	var artist db.Collection

	if sess, err = db.Open(Adapter, settings); err != nil {
		t.Fatal(err)
	}

	defer sess.Close()

	if artist, err = sess.Collection("artist"); err != nil {
		t.Fatal(err)
	}

	// Getting the artist with id = 1
	res = artist.Find(db.Cond{"id": 1})

	// Trying to remove the row.
	if err = res.Remove(); err != nil {
		t.Fatal(err)
	}
}

// Attempts to use SQL raw statements.
func TestRawRelations(t *testing.T) {
	var sess db.Database
	var err error

	var artist db.Collection
	var publication db.Collection
	var review db.Collection

	type artist_t struct {
		Id   int64  `db:"id,omitempty"`
		Name string `db:"name"`
	}

	type publication_t struct {
		Id       int64  `db:"id,omitempty"`
		Title    string `db:"title"`
		AuthorId int64  `db:"author_id"`
	}

	type review_t struct {
		Id            int64     `db:"id,omitempty"`
		PublicationId int64     `db:"publication_id"`
		Name          string    `db:"name"`
		Comments      string    `db:"comments"`
		Created       time.Time `db:"created"`
	}

	if sess, err = db.Open(Adapter, settings); err != nil {
		t.Fatal(err)
	}

	defer sess.Close()

	// Artist collection.
	if artist, err = sess.Collection("artist"); err != nil {
		t.Fatal(err)
	}

	if err = artist.Truncate(); err != nil {
		t.Fatal(err)
	}

	// Publication collection.
	if publication, err = sess.Collection("publication"); err != nil {
		t.Fatal(err)
	}

	if err = publication.Truncate(); err != nil {
		t.Fatal(err)
	}

	// Review collection.
	if review, err = sess.Collection("review"); err != nil {
		t.Fatal(err)
	}

	if err = review.Truncate(); err != nil {
		t.Fatal(err)
	}

	// Adding some artists.
	var miyazakiId interface{}
	miyazaki := artist_t{Name: `Hayao Miyazaki`}
	if miyazakiId, err = artist.Append(miyazaki); err != nil {
		t.Fatal(err)
	}
	miyazaki.Id = miyazakiId.(int64)

	var asimovId interface{}
	asimov := artist_t{Name: `Isaac Asimov`}
	if asimovId, err = artist.Append(asimov); err != nil {
		t.Fatal(err)
	}

	var marquezId interface{}
	marquez := artist_t{Name: `Gabriel García Márquez`}
	if marquezId, err = artist.Append(marquez); err != nil {
		t.Fatal(err)
	}

	// Adding some publications.
	publication.Append(publication_t{
		Title:    `Tonari no Totoro`,
		AuthorId: miyazakiId.(int64),
	})

	publication.Append(publication_t{
		Title:    `Howl's Moving Castle`,
		AuthorId: miyazakiId.(int64),
	})

	publication.Append(publication_t{
		Title:    `Ponyo`,
		AuthorId: miyazakiId.(int64),
	})

	publication.Append(publication_t{
		Title:    `Memoria de mis Putas Tristes`,
		AuthorId: marquezId.(int64),
	})

	publication.Append(publication_t{
		Title:    `El Coronel no tiene quien le escriba`,
		AuthorId: marquezId.(int64),
	})

	publication.Append(publication_t{
		Title:    `El Amor en los tiempos del Cólera`,
		AuthorId: marquezId.(int64),
	})

	publication.Append(publication_t{
		Title:    `I, Robot`,
		AuthorId: asimovId.(int64),
	})

	var foundationId interface{}
	foundationId, err = publication.Append(publication_t{
		Title:    `Foundation`,
		AuthorId: asimovId.(int64),
	})
	if err != nil {
		t.Fatal(err)
	}

	publication.Append(publication_t{
		Title:    `The Robots of Dawn`,
		AuthorId: asimovId.(int64),
	})

	// Adding reviews for foundation.
	review.Append(review_t{
		PublicationId: foundationId.(int64),
		Name:          "John Doe",
		Comments:      "I love The Foundation series.",
		Created:       time.Now(),
	})

	review.Append(review_t{
		PublicationId: foundationId.(int64),
		Name:          "Edr Pls",
		Comments:      "The Foundation series made me fall in love with Isaac Asimov.",
		Created:       time.Now(),
	})

	// Exec'ing a raw query.
	var artistPublication db.Collection
	if artistPublication, err = sess.Collection(`artist AS a`, `publication AS p`); err != nil {
		t.Fatal(err)
	}

	res := artistPublication.Find(
		db.Raw{`a.id = p.author_id`},
	).Select(
		"p.id",
		"p.title as publication_title",
		"a.name AS artist_name",
	)

	type artistPublication_t struct {
		Id               int64  `db:"id"`
		PublicationTitle string `db:"publication_title"`
		ArtistName       string `db:"artist_name"`
	}

	all := []artistPublication_t{}

	if err = res.All(&all); err != nil {
		t.Fatal(err)
	}

	if len(all) != 9 {
		t.Fatalf("Expecting some rows.")
	}
}

func TestRawQuery(t *testing.T) {
	var sess db.Database
	var rows *sql.Rows
	var err error
	var drv *sql.DB

	type publication_t struct {
		Id       int64  `db:"id,omitempty"`
		Title    string `db:"title"`
		AuthorId int64  `db:"author_id"`
	}

	if sess, err = db.Open(Adapter, settings); err != nil {
		t.Fatal(err)
	}

	defer sess.Close()

	drv = sess.Driver().(*sql.DB)

	rows, err = drv.Query(`
		SELECT
			p.id,
			p.title AS publication_title,
			a.name AS artist_name
		FROM
			artist AS a,
			publication AS p
		WHERE
			a.id = p.author_id
	`)

	if err != nil {
		t.Fatal(err)
	}

	var all []publication_t

	if err = sqlutil.FetchRows(rows, &all); err != nil {
		t.Fatal(err)
	}

	if len(all) != 9 {
		t.Fatalf("Expecting some rows.")
	}
}

// Attempts to test database transactions.
func TestTransactionsAndRollback(t *testing.T) {
	var sess db.Database
	var err error

	type artist_t struct {
		Id   int64  `db:"id,omitempty"`
		Name string `db:"name"`
	}

	if sess, err = db.Open(Adapter, settings); err != nil {
		t.Fatal(err)
	}

	defer sess.Close()

	// Simple transaction that should not fail.
	var tx db.Tx
	if tx, err = sess.Transaction(); err != nil {
		t.Fatal(err)
	}

	var artist db.Collection
	if artist, err = tx.Collection("artist"); err != nil {
		t.Fatal(err)
	}

	if err = artist.Truncate(); err != nil {
		t.Fatal(err)
	}

	// Simple transaction
	if _, err = artist.Append(artist_t{1, "First"}); err != nil {
		t.Fatal(err)
	}

	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// Attempt to use the same transaction should fail.
	if _, err = tx.Collection("artist"); err == nil {
		t.Fatalf("Illegal, transaction has already been commited.")
	}

	// Use another transaction.
	if tx, err = sess.Transaction(); err != nil {
		t.Fatal(err)
	}

	if artist, err = tx.Collection("artist"); err != nil {
		t.Fatal(err)
	}

	// Won't fail.
	if _, err = artist.Append(artist_t{2, "Second"}); err != nil {
		t.Fatal(err)
	}

	// Won't fail.
	if _, err = artist.Append(artist_t{3, "Third"}); err != nil {
		t.Fatal(err)
	}

	// Will fail.
	if _, err = artist.Append(artist_t{1, "Duplicated"}); err == nil {
		t.Fatal("Should have failed, as we have already inserted ID 1.")
	}

	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	if err = tx.Commit(); err == nil {
		t.Fatalf("Should have failed, as we've already rolled back.")
	}

	// Let's verify we still have one element.
	if artist, err = sess.Collection("artist"); err != nil {
		t.Fatal(err)
	}

	var count uint64
	if count, err = artist.Find().Count(); err != nil {
		t.Fatal(err)
	}

	if count != 1 {
		t.Fatalf("Expecting only one element.")
	}

	// Attempt to add some rows.
	if tx, err = sess.Transaction(); err != nil {
		t.Fatal(err)
	}

	if artist, err = tx.Collection("artist"); err != nil {
		t.Fatal(err)
	}

	// Won't fail.
	if _, err = artist.Append(artist_t{2, "Second"}); err != nil {
		t.Fatal(err)
	}

	// Won't fail.
	if _, err = artist.Append(artist_t{3, "Third"}); err != nil {
		t.Fatal(err)
	}

	// Then rollback for no reason.
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	if err = tx.Commit(); err == nil {
		t.Fatalf("Should have failed, as we've already rolled back.")
	}

	// Let's verify we still have one element.
	if artist, err = sess.Collection("artist"); err != nil {
		t.Fatal(err)
	}

	if count, err = artist.Find().Count(); err != nil {
		t.Fatal(err)
	}

	if count != 1 {
		t.Fatalf("Expecting only one element.")
	}

	// Attempt to add some rows.
	if tx, err = sess.Transaction(); err != nil {
		t.Fatal(err)
	}

	if artist, err = tx.Collection("artist"); err != nil {
		t.Fatal(err)
	}

	// Won't fail.
	if _, err = artist.Append(artist_t{2, "Second"}); err != nil {
		t.Fatal(err)
	}

	// Won't fail.
	if _, err = artist.Append(artist_t{3, "Third"}); err != nil {
		t.Fatal(err)
	}

	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}

	if err = tx.Rollback(); err == nil {
		t.Fatalf("Should have failed, as we've already commited.")
	}

	// Let's verify we have 3 rows.
	if artist, err = sess.Collection("artist"); err != nil {
		t.Fatal(err)
	}

	if count, err = artist.Find().Count(); err != nil {
		t.Fatal(err)
	}

	if count != 3 {
		t.Fatalf("Expecting only one element.")
	}

}

// Attempts to add many different datatypes to a single row in a collection,
// then it tries to get the stored datatypes and check if the stored and the
// original values match.
func TestDataTypes(t *testing.T) {
	var res db.Result
	var sess db.Database
	var dataTypes db.Collection
	var err error
	var id interface{}
	var exists uint64

	if sess, err = db.Open(Adapter, settings); err != nil {
		t.Fatal(err)
	}

	defer sess.Close()

	// Getting a pointer to the "data_types" collection.
	if dataTypes, err = sess.Collection("data_types"); err != nil {
		t.Fatal(err)
	}

	// Removing all data.
	if err = dataTypes.Truncate(); err != nil {
		t.Fatal(err)
	}

	// Appending our test subject.
	if id, err = dataTypes.Append(testValues); err != nil {
		t.Fatal(err)
	}

	// Defining our set.
	res = dataTypes.Find(db.Cond{"id": id})

	if exists, err = res.Count(); err != nil {
		t.Fatal(err)
	}

	if exists == 0 {
		t.Fatalf("Expecting an item.")
	}

	// Trying to dump the subject into an empty structure of the same type.
	var item testValuesStruct

	res.One(&item)

	// The original value and the test subject must match.
	if reflect.DeepEqual(item, testValues) == false {
		t.Fatalf("Struct is different.")
	}
}

// We are going to benchmark the engine, so this is no longed needed.
func TestDisableDebug(t *testing.T) {
	os.Setenv(db.EnvEnableDebug, "")
}

// Benchmarking raw database/sql.
func BenchmarkAppendRawSQL(b *testing.B) {
	var err error
	var sess db.Database

	if sess, err = db.Open(Adapter, settings); err != nil {
		b.Fatal(err)
	}

	defer sess.Close()

	driver := sess.Driver().(*sql.DB)

	if _, err = driver.Exec(`DELETE FROM "artist"`); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err = driver.Exec(`INSERT INTO "artist" ("name") VALUES('Hayao Miyazaki')`); err != nil {
			b.Fatal(err)
		}
	}
}

// Benchmarking Append().
//
// Contributed by wei2912
// See: https://github.com/gosexy/db/issues/20#issuecomment-20097801
func BenchmarkAppendUpper(b *testing.B) {
	sess, err := db.Open(Adapter, settings)

	if err != nil {
		b.Fatal(err)
	}

	defer sess.Close()

	artist, err := sess.Collection("artist")
	artist.Truncate()

	item := struct {
		Name string `db:"name"`
	}{"Hayao Miyazaki"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err = artist.Append(item); err != nil {
			b.Fatal(err)
		}
	}
}

// Benchmarking raw database/sql.
func BenchmarkAppendTxRawSQL(b *testing.B) {
	var err error
	var sess db.Database
	var tx *sql.Tx

	if sess, err = db.Open(Adapter, settings); err != nil {
		b.Fatal(err)
	}

	defer sess.Close()

	driver := sess.Driver().(*sql.DB)

	if tx, err = driver.Begin(); err != nil {
		b.Fatal(err)
	}

	if _, err = tx.Exec(`DELETE FROM "artist"`); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err = tx.Exec(`INSERT INTO "artist" ("name") VALUES('Hayao Miyazaki')`); err != nil {
			b.Fatal(err)
		}
	}

	if err = tx.Commit(); err != nil {
		b.Fatal(err)
	}
}

// Benchmarking Append() with transactions.
func BenchmarkAppendTxUpper(b *testing.B) {
	var sess db.Database
	var err error

	if sess, err = db.Open(Adapter, settings); err != nil {
		b.Fatal(err)
	}

	defer sess.Close()

	var tx db.Tx
	if tx, err = sess.Transaction(); err != nil {
		b.Fatal(err)
	}

	var artist db.Collection
	if artist, err = tx.Collection("artist"); err != nil {
		b.Fatal(err)
	}

	if err = artist.Truncate(); err != nil {
		b.Fatal(err)
	}

	item := struct {
		Name string `db:"name"`
	}{"Hayao Miyazaki"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err = artist.Append(item); err != nil {
			b.Fatal(err)
		}
	}

	if err = tx.Commit(); err != nil {
		b.Fatal(err)
	}
}

// Benchmarking Append() with map.
func BenchmarkAppendTxUpperMap(b *testing.B) {
	var sess db.Database
	var err error

	if sess, err = db.Open(Adapter, settings); err != nil {
		b.Fatal(err)
	}

	defer sess.Close()

	var tx db.Tx
	if tx, err = sess.Transaction(); err != nil {
		b.Fatal(err)
	}

	var artist db.Collection
	if artist, err = tx.Collection("artist"); err != nil {
		b.Fatal(err)
	}

	if err = artist.Truncate(); err != nil {
		b.Fatal(err)
	}

	item := map[string]string{"name": "Hayao Miyazaki"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err = artist.Append(item); err != nil {
			b.Fatal(err)
		}
	}

	if err = tx.Commit(); err != nil {
		b.Fatal(err)
	}
}
