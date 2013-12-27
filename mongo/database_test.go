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
	Tests for the mongo wrapper.
*/
package mongo

import (
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"menteslibres.net/gosexy/to"
	"reflect"
	"strings"
	"testing"
	"time"
	"upper.io/db"
)

// Wrapper.
const wrapperName = "mongo"

// Wrapper settings.
const host = "127.0.0.1"
const dbname = "upperio_tests"

// Global settings for tests.
var settings = db.Settings{
	Host:     host,
	Database: dbname,
	User:     "upperio",
	Password: "upperio",
}

// Structure for testing conversions and datatypes.
type testValuesStruct struct {
	Uint   uint   `bson:"_uint"`
	Uint8  uint8  `bson:"_uint8"`
	Uint16 uint16 `bson:"_uint16"`
	Uint32 uint32 `bson:"_uint32"`
	Uint64 uint64 `bson:"_uint64"`

	Int   int   `bson:"_int"`
	Int8  int8  `bson:"_int8"`
	Int16 int16 `bson:"_int16"`
	Int32 int32 `bson:"_int32"`
	Int64 int64 `bson:"_int64"`

	Float32 float32 `bson:"_float32"`
	Float64 float64 `bson:"_float64"`

	Bool   bool   `bson:"_bool"`
	String string `bson:"_string"`

	Date time.Time     `bson:"_date"`
	Time time.Duration `bson:"_time"`
}

// Declaring some values to insert, we expect the same values to be returned.
var testValues = testValuesStruct{
	1, 1, 1, 1, 1,
	-1, -1, -1, -1, -1,
	1.337, 1.337,
	true,
	"Hello world!",
	time.Unix(1234567890, 0),
	time.Second * time.Duration(7331),
}

// Enabling outputting some information to stdout, useful for development.
func TestEnableDebug(t *testing.T) {
	Debug = true
}

// Trying to open an empty datasource, it must succeed (mongo).
func TestOpenFailed(t *testing.T) {
	_, err := db.Open(wrapperName, db.Settings{})

	if err != nil {
		t.Errorf(err.Error())
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

		// The collection may ot may not exists.
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
		// We can use the collection even if it does not exists.
		if err != db.ErrCollectionDoesNotExists {
			t.Fatalf(err.Error())
		}
	}

	// Appending a map.
	id, err = artist.Append(map[string]string{
		"name": "Ozzie",
	})

	if id == nil {
		t.Fatalf("Expecting an ID.")
	}

	if _, ok := id.(bson.ObjectId); ok != true {
		t.Fatalf("Expecting a bson.ObjectId.")
	}

	if id.(bson.ObjectId).Valid() != true {
		t.Fatalf("Expecting a valid bson.ObjectId.")
	}

	// Appending a struct.
	id, err = artist.Append(struct {
		Name string
	}{
		"Flea",
	})

	if id == nil {
		t.Fatalf("Expecting an ID.")
	}

	if _, ok := id.(bson.ObjectId); ok != true {
		t.Fatalf("Expecting a bson.ObjectId.")
	}

	if id.(bson.ObjectId).Valid() != true {
		t.Fatalf("Expecting a valid bson.ObjectId.")
	}

	// Appending a struct (using tags to specify the field name).
	id, err = artist.Append(struct {
		ArtistName string `bson:"name"`
	}{
		"Slash",
	})

	if id == nil {
		t.Fatalf("Expecting an ID.")
	}

	if _, ok := id.(bson.ObjectId); ok != true {
		t.Fatalf("Expecting a bson.ObjectId.")
	}

	if id.(bson.ObjectId).Valid() != true {
		t.Fatalf("Expecting a valid bson.ObjectId.")
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
			if row_m["_id"] == nil {
				t.Fatalf("Expecting an ID.")
			}
			if _, ok := row_m["_id"].(bson.ObjectId); ok != true {
				t.Fatalf("Expecting a bson.ObjectId.")
			}

			if row_m["_id"].(bson.ObjectId).Valid() != true {
				t.Fatalf("Expecting a valid bson.ObjectId.")
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
		Id   bson.ObjectId `bson:"_id"`
		Name string        `bson:"name"`
	}{}

	res = artist.Find()

	for {
		err = res.Next(&row_s)

		if err == db.ErrNoMoreRows {
			// No more row_s' left.
			break
		}

		if err == nil {
			if row_s.Id.Valid() == false {
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
		Value1 bson.ObjectId `bson:"_id"`
		Value2 string        `bson:"name"`
	}{}

	res = artist.Find()

	for {
		err = res.Next(&row_t)

		if err == db.ErrNoMoreRows {
			// No more row_t's left.
			break
		}

		if err == nil {
			if row_t.Value1.Valid() == false {
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
		if single_row_m["_id"] == nil {
			t.Fatalf("Expecting a not null ID.")
		}
	}

	// Testing Result.All() with a slice of structs.
	res = artist.Find()

	all_rows_s := []struct {
		Id   bson.ObjectId `bson:"_id"`
		Name string
	}{}
	err = res.All(&all_rows_s)

	if err != nil {
		t.Fatalf(err.Error())
	}

	for _, single_row_s := range all_rows_s {
		if single_row_s.Id.Valid() == false {
			t.Fatalf("Expecting a not null ID.")
		}
	}

	// Testing Result.All() with a slice of tagged structs.
	res = artist.Find()

	all_rows_t := []struct {
		Value1 bson.ObjectId `bson:"_id"`
		Value2 string        `bson:"name"`
	}{}
	err = res.All(&all_rows_t)

	if err != nil {
		t.Fatalf(err.Error())
	}

	for _, single_row_t := range all_rows_t {
		if single_row_t.Value1.Valid() == false {
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
		Id   bson.ObjectId `bson:"_id"`
		Name string
	}{}

	// Getting the first artist.
	res := artist.Find(db.Cond{"_id $ne": nil}).Limit(1)

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
		Value1 string `bson:"name"`
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

	// Getting the first artist.
	res := artist.Find(db.Cond{"_id $ne": nil}).Limit(1)

	var first struct {
		Id bson.ObjectId `bson:"_id"`
	}

	err = res.One(&first)

	if err != nil {
		t.Fatalf(err.Error())
	}

	res = artist.Find(db.Cond{"_id": first.Id})

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
	res = dataTypes.Find(db.Cond{"_id": id})

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

// Benchmarking raw mgo queries.
func BenchmarkAppendRaw(b *testing.B) {
	sess, err := db.Open(wrapperName, settings)

	if err != nil {
		b.Fatalf(err.Error())
	}

	defer sess.Close()

	artist, err := sess.Collection("artist")
	artist.Truncate()

	driver := sess.Driver().(*mgo.Session)

	mgodb := driver.DB(dbname)
	col := mgodb.C("artist")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := col.Insert(map[string]string{"name": "Hayao Miyazaki"})
		if err != nil {
			b.Fatalf(err.Error())
		}
	}
}

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
