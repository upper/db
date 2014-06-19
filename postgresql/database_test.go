/*
  Copyright (c) 2012-2014 José Carlos Nieto, https://menteslibres.net/xiam

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
	Tests for the postgresql wrapper.

	Execute the Makefile in ./_dumps/ to create the expected database structure.

	cd _dumps
	make
	cd ..
	go test
*/

package postgresql

import (
	"database/sql"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"menteslibres.net/gosexy/to"
	"upper.io/db"
)

// Wrapper.
const wrapperName = "postgresql"

// Wrapper settings.
const (
	host     = "testserver.local"
	dbname   = "upperio_tests"
	username = "upperio"
	password = "upperio"
)

// Global settings for tests.
var settings = db.Settings{
	Database: dbname,
	Host:     host,
	User:     username,
	Password: password,
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
	os.Setenv(db.EnvEnableDebug, "TRUE")
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
	sess, err := db.Open(wrapperName, settings)
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close()

	// Getting a list of all collections in this database.
	collections, err := sess.Collections()
	if err != nil {
		t.Fatal(err)
	}

	for _, name := range collections {

		// Pointing the collection.
		col, err := sess.Collection(name)
		if err != nil {
			t.Fatal(err)
		}

		// Since this is a SQL collection (table), the structure must exists before
		// we can use it.
		exists := col.Exists()

		if exists == true {
			// Truncating the structure, if exists.
			err = col.Truncate()
			if err != nil {
				t.Fatal(err)
			}
		}

	}
}

func TestSetCursorError(t *testing.T) {
	sess, err := db.Open(wrapperName, settings)
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close()

	artist, err := sess.Collection("artist")
	if err != nil {
		t.Fatal(err)
	}

	// trigger Postgres error. "" is not an int.
	res := artist.Find(db.Cond{"id": ""})

	var row map[string]interface{}
	err = res.One(&row)
	if err == db.ErrNoMoreRows || err == nil {
		t.Fatalf("err = %#v, want PQ error", err)
	}
}

// This test appends some data into the "artist" table.
func TestAppend(t *testing.T) {
	var id interface{}

	// Opening database.
	sess, err := db.Open(wrapperName, settings)
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close()

	// Getting a pointer to the "artist" collection.
	artist, err := sess.Collection("artist")
	if err != nil {
		t.Fatal(err)
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
		Name string `field:"name"`
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
	var res db.Result

	// Opening database.
	sess, err := db.Open(wrapperName, settings)
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close()

	// We should close the database when it's no longer in use.
	artist, _ := sess.Collection("artist")

	res = artist.Find()

	// Counting all the matching rows.
	total, err := res.Count()
	if err != nil {
		t.Fatal(err)
	}

	if total == 0 {
		t.Fatalf("Should not be empty, we've just added some rows!")
	}

}

// This test uses and result and tries to fetch items one by one.
func TestResultFetch(t *testing.T) {
	var res db.Result

	// Opening database.
	sess, err := db.Open(wrapperName, settings)
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close()

	artist, err := sess.Collection("artist")
	if err != nil {
		t.Fatal(err)
	}

	// Testing map
	res = artist.Find()

	rowM := map[string]interface{}{}

	for {
		err = res.Next(&rowM)

		if err == db.ErrNoMoreRows {
			// No more rowMs left.
			break
		}

		if err == nil {
			if to.Int64(rowM["id"]) == 0 {
				t.Fatalf("Expecting a not null ID.")
			}
			if to.String(rowM["name"]) == "" {
				t.Fatalf("Expecting a name.")
			}
		} else {
			t.Fatal(err)
		}
	}

	res.Close()

	// Testing struct
	rowS := struct {
		ID   uint64
		Name string
	}{}

	res = artist.Find()

	for {
		err = res.Next(&rowS)

		if err == db.ErrNoMoreRows {
			// No more rowS' left.
			break
		}

		if err == nil {
			if rowS.ID == 0 {
				t.Fatalf("Expecting a not null ID.")
			}
			if rowS.Name == "" {
				t.Fatalf("Expecting a name.")
			}
		} else {
			t.Fatal(err)
		}
	}

	res.Close()

	// Testing tagged struct
	rowT := struct {
		Value1 uint64 `field:"id"`
		Value2 string `field:"name"`
	}{}

	res = artist.Find()

	for {
		err = res.Next(&rowT)

		if err == db.ErrNoMoreRows {
			// No more rowT's left.
			break
		}

		if err == nil {
			if rowT.Value1 == 0 {
				t.Fatalf("Expecting a not null ID.")
			}
			if rowT.Value2 == "" {
				t.Fatalf("Expecting a name.")
			}
		} else {
			t.Fatal(err)
		}
	}

	res.Close()

	// Testing Result.All() with a slice of maps.
	res = artist.Find()

	allRowsM := []map[string]interface{}{}
	err = res.All(&allRowsM)
	if err != nil {
		t.Fatal(err)
	}

	for _, singleRowM := range allRowsM {
		if to.Int64(singleRowM["id"]) == 0 {
			t.Fatalf("Expecting a not null ID.")
		}
	}

	// Testing Result.All() with a slice of structs.
	res = artist.Find()

	allRowsS := []struct {
		ID   uint64
		Name string
	}{}
	err = res.All(&allRowsS)
	if err != nil {
		t.Fatal(err)
	}

	for _, singleRowS := range allRowsS {
		if singleRowS.ID == 0 {
			t.Fatalf("Expecting a not null ID.")
		}
	}

	// Testing Result.All() with a slice of tagged structs.
	res = artist.Find()

	allRowsT := []struct {
		Value1 uint64 `field:"id"`
		Value2 string `field:"name"`
	}{}
	err = res.All(&allRowsT)
	if err != nil {
		t.Fatal(err)
	}

	for _, singleRowT := range allRowsT {
		if singleRowT.Value1 == 0 {
			t.Fatalf("Expecting a not null ID.")
		}
	}
}

// This test tries to update some previously added rows.
func TestUpdate(t *testing.T) {
	// Opening database.
	sess, err := db.Open(wrapperName, settings)
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close()

	// Getting a pointer to the "artist" collection.
	artist, err := sess.Collection("artist")
	if err != nil {
		t.Fatal(err)
	}

	// Value
	value := struct {
		ID   uint64
		Name string
	}{}

	// Getting the first artist.
	res := artist.Find(db.Cond{"id !=": 0}).Limit(1)

	// Updating with a map
	rowM := map[string]interface{}{
		"name": strings.ToUpper(value.Name),
	}

	err = res.Update(rowM)
	if err != nil {
		t.Fatal(err)
	}

	err = res.One(&value)
	if err != nil {
		t.Fatal(err)
	}

	if value.Name != rowM["name"] {
		t.Fatalf("Expecting a modification.")
	}

	// Updating with a struct
	rowS := struct {
		Name string
	}{strings.ToLower(value.Name)}

	err = res.Update(rowS)
	if err != nil {
		t.Fatal(err)
	}

	err = res.One(&value)
	if err != nil {
		t.Fatal(err)
	}

	if value.Name != rowS.Name {
		t.Fatalf("Expecting a modification.")
	}

	// Updating with a tagged struct
	rowT := struct {
		Value1 string `field:"name"`
	}{strings.Replace(value.Name, "z", "Z", -1)}

	err = res.Update(rowT)
	if err != nil {
		t.Fatal(err)
	}

	err = res.One(&value)
	if err != nil {
		t.Fatal(err)
	}

	if value.Name != rowT.Value1 {
		t.Fatalf("Expecting a modification.")
	}

}

// Test database functions
func TestFunction(t *testing.T) {
	var err error
	var res db.Result

	// Opening database.
	sess, err := db.Open(wrapperName, settings)
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close()

	// Getting a pointer to the "artist" collection.
	artist, err := sess.Collection("artist")
	if err != nil {
		t.Fatal(err)
	}

	rowS := struct {
		ID   uint64
		Name string
	}{}

	res = artist.Find(db.Cond{"id NOT IN": []int{0, -1}})

	if err = res.One(&rowS); err != nil {
		t.Fatalf("One: %q", err)
	}

	res = artist.Find(db.Cond{"id": db.Func{"NOT IN", []int{0, -1}}})

	if err = res.One(&rowS); err != nil {
		t.Fatalf("One: %q", err)
	}

	res.Close()
}

// This test tries to remove some previously added rows.
func TestRemove(t *testing.T) {
	sess, err := db.Open(wrapperName, settings)
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close()

	// Getting a pointer to the "artist" collection.
	artist, err := sess.Collection("artist")
	if err != nil {
		t.Fatal(err)
	}

	// Getting the artist with id = 1
	res := artist.Find(db.Cond{"id": 1})

	// Trying to remove the row.
	err = res.Remove()
	if err != nil {
		t.Fatal(err)
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
		t.Fatal(err)
	}
	defer sess.Close()

	// Getting a pointer to the "data_types" collection.
	dataTypes, err := sess.Collection("data_types")
	dataTypes.Truncate()

	// Appending our test subject.
	id, err := dataTypes.Append(testValues)
	if err != nil {
		t.Fatal(err)
	}

	// Trying to get the same subject we added.
	res = dataTypes.Find(db.Cond{"id": id})

	exists, err := res.Count()
	if err != nil {
		t.Fatal(err)
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
	os.Setenv(db.EnvEnableDebug, "")
}

// Benchmarking raw database/sql.
func BenchmarkAppendRaw(b *testing.B) {
	sess, err := db.Open(wrapperName, settings)
	if err != nil {
		b.Fatal(err)
	}
	defer sess.Close()

	artist, err := sess.Collection("artist")
	artist.Truncate()

	driver := sess.Driver().(*sql.DB)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := driver.Exec(`INSERT INTO artist (name) VALUES('Hayao Miyazaki')`)
		if err != nil {
			b.Fatal(err)
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
		b.Fatal(err)
	}
	defer sess.Close()

	artist, err := sess.Collection("artist")
	artist.Truncate()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err = artist.Append(map[string]string{"name": "Leonardo DaVinci"})
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Benchmarking Append() with transactions.
//
// Contributed by wei2912
// See: https://github.com/gosexy/db/issues/20#issuecomment-20167939
// Applying the BEGIN and END transaction optimizations.
func BenchmarkAppendDbItemWithTransaction(b *testing.B) {
	sess, err := db.Open(wrapperName, settings)
	if err != nil {
		b.Fatal(err)
	}
	defer sess.Close()

	artist, err := sess.Collection("artist")
	artist.Truncate()

	err = sess.Begin()
	if err != nil {
		b.Fatal(err)
	}

	for i := 0; i < b.N; i++ {
		_, err = artist.Append(map[string]string{"name": "Isaac Asimov"})
		if err != nil {
			b.Fatal(err)
		}
	}

	err = sess.End()
	if err != nil {
		b.Fatal(err)
	}
}

// Benchmarking Append with a struct.
func BenchmarkAppendStruct(b *testing.B) {
	sess, err := db.Open(wrapperName, settings)
	if err != nil {
		b.Fatal(err)
	}
	defer sess.Close()

	artist, err := sess.Collection("artist")
	artist.Truncate()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err = artist.Append(struct{ Name string }{"John Lennon"})
		if err != nil {
			b.Fatal(err)
		}
	}
}
