/*
  Copyright (c) 2012-2013 José Carlos Nieto, https://menteslibres.net/xiam

  Permission is hereby granted, free of charge, to any person obtaining
  a copy of this software and associated documentation files (the
  "Software"), to deal in the Software without restriction, including
  without limitation the rights to use, copy, modify, merge, publish,
  distribute, sublicense, and/or sell copies of the Software, and to
  permit persons to whom the Software is furnished to do so, subject to
  the following conditions:

  The above copyright notice and this permission notice shall be
  included in all copies or substantial portions of the Software.

  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
  NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
  LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
  OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
  WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

/*
	Tests for the sqlite wrapper.

	Execute the Makefile in ./_dumps/ to create the expected database structure.

	cd _dumps
	make
	cd ..
	go test
*/
package sqlite

import (
	"database/sql"
	"menteslibres.net/gosexy/to"
	"reflect"
	"strings"
	"testing"
	"time"
	"upper.io/db"
)

// Wrapper.
const wrapperName = "sqlite"

// Wrapper settings.
const databaseFilename = "./_dumps/gotest.sqlite3.db"

// Global settings for tests.
var settings = db.Settings{
	Database: databaseFilename,
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

// Enabling outputting some information to stdout (like the SQL query and its
// arguments), useful for development.
func TestEnableDebug(t *testing.T) {
	Debug = true
}

// Trying to open an empty datasource, it must fail.
func TestOpenFailed(t *testing.T) {
	_, err := db.Open(wrapperName, db.Settings{})

	if err == nil {
		t.Errorf("Expecting an error.")
	}
}

// Truncates all collections.
func TestTruncate(t *testing.T) {

	var err error

	// Opening database.
	sess, err := db.Open(wrapperName, settings)

	if err != nil {
		t.Fatalf(err.Error())
	}

	// We should close the database when it's no longer in use.
	defer sess.Close()

	// Getting a list of all collections in this database.
	collections, err := sess.Collections()

	if err != nil {
		t.Fatalf(err.Error())
	}

	for _, name := range collections {

		// Pointing the collection.
		col, err := sess.Collection(name)
		if err != nil {
			t.Fatalf(err.Error())
		}

		// Since this is a SQL collection (table), the structure must exists before
		// we can use it.
		exists := col.Exists()

		if exists == true {
			// Truncating the structure, if exists.
			err = col.Truncate()

			if err != nil {
				t.Fatalf(err.Error())
			}
		}

	}
}

// This test appends some data into the "artist" table.
func TestAppend(t *testing.T) {

	var err error
	var id interface{}

	// Opening database.
	sess, err := db.Open(wrapperName, settings)

	if err != nil {
		t.Fatalf(err.Error())
	}

	// We should close the database when it's no longer in use.
	defer sess.Close()

	// Getting a pointer to the "artist" collection.
	artist, err := sess.Collection("artist")

	if err != nil {
		t.Fatalf(err.Error())
	}

	// Appending a map.
	id, err = artist.Append(map[string]string{
		"name": "Ozzie",
	})

	if to.Int64(id) == 0 {
		t.Fatalf("Expecting an ID.")
	}

	// Appending a struct.
	id, err = artist.Append(struct {
		Name string `field:name`
	}{
		"Flea",
	})

	if to.Int64(id) == 0 {
		t.Fatalf("Expecting an ID.")
	}

	// Appending a struct (using tags to specify the field name).
	id, err = artist.Append(struct {
		ArtistName string `field:"name"`
	}{
		"Slash",
	})

	if to.Int64(id) == 0 {
		t.Fatalf("Expecting an ID.")
	}

}

// This test tries to use an empty filter and count how many elements were
// added into the artist collection.
func TestResultCount(t *testing.T) {

	var err error
	var res db.Result

	// Opening database.
	sess, err := db.Open(wrapperName, settings)

	if err != nil {
		t.Fatalf(err.Error())
	}

	defer sess.Close()

	// We should close the database when it's no longer in use.
	artist, _ := sess.Collection("artist")

	res = artist.Find()

	// Counting all the matching rows.
	total, err := res.Count()

	if err != nil {
		t.Fatalf(err.Error())
	}

	if total == 0 {
		t.Fatalf("Should not be empty, we've just added some rows!")
	}

}

// This test uses and result and tries to fetch items one by one.
func TestResultFetch(t *testing.T) {

	var err error
	var res db.Result

	// Opening database.
	sess, err := db.Open(wrapperName, settings)

	if err != nil {
		t.Fatalf(err.Error())
	}

	// We should close the database when it's no longer in use.
	defer sess.Close()

	artist, err := sess.Collection("artist")

	if err != nil {
		t.Fatalf(err.Error())
	}

	// Testing map
	res = artist.Find()

	row_m := map[string]interface{}{}

	for {
		err = res.Next(&row_m)

		if err == db.ErrNoMoreRows {
			// No more row_ms left.
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
			t.Fatalf(err.Error())
		}
	}

	res.Close()

	// Testing struct
	row_s := struct {
		Id   uint64
		Name string
	}{}

	res = artist.Find()

	for {
		err = res.Next(&row_s)

		if err == db.ErrNoMoreRows {
			// No more row_s' left.
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
			t.Fatalf(err.Error())
		}
	}

	res.Close()

	// Testing tagged struct
	row_t := struct {
		Value1 uint64 `field:"id"`
		Value2 string `field:"name"`
	}{}

	res = artist.Find()

	for {
		err = res.Next(&row_t)

		if err == db.ErrNoMoreRows {
			// No more row_t's left.
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
			t.Fatalf(err.Error())
		}
	}

	res.Close()

	// Testing Result.All() with a slice of maps.
	res = artist.Find()

	all_rows_m := []map[string]interface{}{}
	err = res.All(&all_rows_m)

	if err != nil {
		t.Fatalf(err.Error())
	}

	for _, single_row_m := range all_rows_m {
		if to.Int64(single_row_m["id"]) == 0 {
			t.Fatalf("Expecting a not null ID.")
		}
	}

	// Testing Result.All() with a slice of structs.
	res = artist.Find()

	all_rows_s := []struct {
		Id   uint64
		Name string
	}{}
	err = res.All(&all_rows_s)

	if err != nil {
		t.Fatalf(err.Error())
	}

	for _, single_row_s := range all_rows_s {
		if single_row_s.Id == 0 {
			t.Fatalf("Expecting a not null ID.")
		}
	}

	// Testing Result.All() with a slice of tagged structs.
	res = artist.Find()

	all_rows_t := []struct {
		Value1 uint64 `field:"id"`
		Value2 string `field:"name"`
	}{}
	err = res.All(&all_rows_t)

	if err != nil {
		t.Fatalf(err.Error())
	}

	for _, single_row_t := range all_rows_t {
		if single_row_t.Value1 == 0 {
			t.Fatalf("Expecting a not null ID.")
		}
	}
}

// This test tries to update some previously added rows.
func TestUpdate(t *testing.T) {
	var err error

	// Opening database.
	sess, err := db.Open(wrapperName, settings)

	if err != nil {
		t.Fatalf(err.Error())
	}

	// We should close the database when it's no longer in use.
	defer sess.Close()

	// Getting a pointer to the "artist" collection.
	artist, err := sess.Collection("artist")

	if err != nil {
		t.Fatalf(err.Error())
	}

	// Value
	value := struct {
		Id   uint64
		Name string
	}{}

	// Getting the first artist.
	res := artist.Find(db.Cond{"id !=": 0}).Limit(1)

	err = res.One(&value)

	if err != nil {
		t.Fatalf(err.Error())
	}

	// Updating with a map
	row_m := map[string]interface{}{
		"name": strings.ToUpper(value.Name),
	}

	err = res.Update(row_m)

	if err != nil {
		t.Fatalf(err.Error())
	}

	err = res.One(&value)

	if err != nil {
		t.Fatalf(err.Error())
	}

	if value.Name != row_m["name"] {
		t.Fatalf("Expecting a modification.")
	}

	// Updating with a struct
	row_s := struct {
		Name string
	}{strings.ToLower(value.Name)}

	err = res.Update(row_s)

	if err != nil {
		t.Fatalf(err.Error())
	}

	err = res.One(&value)

	if err != nil {
		t.Fatalf(err.Error())
	}

	if value.Name != row_s.Name {
		t.Fatalf("Expecting a modification.")
	}

	// Updating with a tagged struct
	row_t := struct {
		Value1 string `field:"name"`
	}{strings.Replace(value.Name, "z", "Z", -1)}

	err = res.Update(row_t)

	if err != nil {
		t.Fatalf(err.Error())
	}

	err = res.One(&value)

	if err != nil {
		t.Fatalf(err.Error())
	}

	if value.Name != row_t.Value1 {
		t.Fatalf("Expecting a modification.")
	}

}

// This test tries to remove some previously added rows.
func TestRemove(t *testing.T) {

	var err error

	// Opening database.
	sess, err := db.Open(wrapperName, settings)

	if err != nil {
		t.Fatalf(err.Error())
	}

	// We should close the database when it's no longer in use.
	defer sess.Close()

	// Getting a pointer to the "artist" collection.
	artist, err := sess.Collection("artist")

	if err != nil {
		t.Fatalf(err.Error())
	}

	// Getting the artist with id = 1
	res := artist.Find(db.Cond{"id": 1})

	// Trying to remove the row.
	err = res.Remove()

	if err != nil {
		t.Fatalf(err.Error())
	}
}

// This test tries to add many different datatypes to a single row in a
// collection, then it tries to get the stored datatypes and check if the
// stored and the original values match.
func TestDataTypes(t *testing.T) {
	var res db.Result

	// Opening database.
	sess, err := db.Open(wrapperName, settings)

	if err != nil {
		t.Fatalf(err.Error())
	}

	// We should close the database when it's no longer in use.
	defer sess.Close()

	// Getting a pointer to the "data_types" collection.
	dataTypes, err := sess.Collection("data_types")
	dataTypes.Truncate()

	// Appending our test subject.
	id, err := dataTypes.Append(testValues)

	if err != nil {
		t.Fatalf(err.Error())
	}

	// Trying to get the same subject we added.
	res = dataTypes.Find(db.Cond{"id": id})

	exists, err := res.Count()

	if err != nil {
		t.Fatalf(err.Error())
	}

	if exists == 0 {
		t.Errorf("Expecting an item.")
	}

	// Trying to dump the subject into an empty structure of the same type.
	var item testValuesStruct
	res.One(&item)

	// The original value and the test subject must match.
	if reflect.DeepEqual(item, testValues) == false {
		t.Errorf("Struct is different.")
	}
}

// We are going to benchmark the engine, so this is no longed needed.
func TestDisableDebug(t *testing.T) {
	Debug = false
}

// Benchmarking raw database/sql.
func BenchmarkAppendRaw(b *testing.B) {
	sess, err := db.Open(wrapperName, settings)

	if err != nil {
		b.Fatalf(err.Error())
	}

	defer sess.Close()

	artist, err := sess.Collection("artist")
	artist.Truncate()

	driver := sess.Driver().(*sql.DB)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := driver.Exec(`INSERT INTO artist (name) VALUES("Hayao Miyazaki")`)
		if err != nil {
			b.Fatalf(err.Error())
		}
	}
}

// Benchmarking Append().
//
// Contributed by wei2912
// See: https://github.com/gosexy/db/issues/20#issuecomment-20097801
func BenchmarkAppendDbItem(b *testing.B) {
	sess, err := db.Open(wrapperName, settings)

	if err != nil {
		b.Fatalf(err.Error())
	}

	defer sess.Close()

	artist, err := sess.Collection("artist")
	artist.Truncate()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err = artist.Append(map[string]string{"name": "Leonardo DaVinci"})
		if err != nil {
			b.Fatalf(err.Error())
		}
	}
}

// Benchmarking Append() with transactions.
//
// Contributed by wei2912
// See: https://github.com/gosexy/db/issues/20#issuecomment-20167939
// Applying the BEGIN and END transaction optimizations.
func BenchmarkAppendDbItem_Transaction(b *testing.B) {
	sess, err := db.Open(wrapperName, settings)

	if err != nil {
		b.Fatalf(err.Error())
	}

	defer sess.Close()

	artist, err := sess.Collection("artist")
	artist.Truncate()

	err = sess.Begin()
	if err != nil {
		b.Fatalf(err.Error())
	}

	for i := 0; i < b.N; i++ {
		_, err = artist.Append(map[string]string{"name": "Isaac Asimov"})
		if err != nil {
			b.Fatalf(err.Error())
		}
	}

	err = sess.End()
	if err != nil {
		b.Fatalf(err.Error())
	}
}

// Benchmarking Append with a struct.
func BenchmarkAppendStruct(b *testing.B) {
	sess, err := db.Open(wrapperName, settings)

	if err != nil {
		b.Fatalf(err.Error())
	}

	defer sess.Close()

	artist, err := sess.Collection("artist")
	artist.Truncate()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err = artist.Append(struct{ Name string }{"John Lennon"})
		if err != nil {
			b.Fatalf(err.Error())
		}
	}
}
