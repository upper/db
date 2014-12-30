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

// Tests for the mongodb adapter.
package mongo

import (
	"errors"
	"flag"
	"math/rand"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"menteslibres.net/gosexy/to"
	"upper.io/db"
)

// Wrapper settings.
const (
	database = "upperio_tests"
	username = "upperio"
	password = "upperio"
)

// Global settings for tests.
var settings = ConnectionURL{
	Database: database,
	User:     username,
	Password: password,
}

var host = flag.String("host", "testserver.local", "Testing server address.")

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

	Date  time.Time     `bson:"_date"`
	DateN *time.Time    `bson:"_nildate"`
	DateP *time.Time    `bson:"_ptrdate"`
	Time  time.Duration `bson:"_time"`
}

type artistWithObjectIdKey struct {
	id   bson.ObjectId
	Name string `db:"name"`
}

func (artist *artistWithObjectIdKey) SetID(id bson.ObjectId) error {
	artist.id = id
	return nil
}

type itemWithKey struct {
	ID      bson.ObjectId `bson:"-"`
	SomeVal string        `bson:"some_val"`
}

func (item itemWithKey) Constraint() db.Cond {
	cond := db.Cond{
		"_id": item.ID,
	}
	return cond
}

func (item *itemWithKey) SetID(keys map[string]interface{}) error {
	if len(keys) == 1 {
		item.ID = keys["_id"].(bson.ObjectId)
		return nil
	}
	return errors.New(`Expecting exactly two keys.`)
}

var testValues testValuesStruct

func init() {
	t := time.Date(2012, 7, 28, 1, 2, 3, 0, time.Local)

	testValues = testValuesStruct{
		1, 1, 1, 1, 1,
		-1, -1, -1, -1, -1,
		1.337, 1.337,
		true,
		"Hello world!",
		t,
		nil,
		&t,
		time.Second * time.Duration(7331),
	}

	flag.Parse()
	settings.Address = db.ParseAddress(*host)
}

// Enabling outputting some information to stdout, useful for development.
func TestEnableDebug(t *testing.T) {
	os.Setenv(db.EnvEnableDebug, "TRUE")
}

// Trying to open an empty datasource, it must succeed (mongo).
/*
func TestOpenFailed(t *testing.T) {
	_, err := db.Open(Adapter, db.Settings{})

	if err != nil {
		t.Errorf(err.Error())
	}
}
*/

// Attempts to open an empty datasource.
func TestOpenWithWrongData(t *testing.T) {
	var err error
	var rightSettings, wrongSettings db.Settings

	// Attempt to open with safe settings.
	rightSettings = db.Settings{
		Database: database,
		Host:     *host,
		User:     username,
		Password: password,
	}

	// Attempt to open an empty database.
	if _, err = db.Open(Adapter, rightSettings); err != nil {
		// Must fail.
		t.Fatal(err)
	}

	// Attempt to open with wrong password.
	wrongSettings = db.Settings{
		Database: database,
		Host:     *host,
		User:     username,
		Password: "fail",
	}

	if _, err = db.Open(Adapter, wrongSettings); err == nil {
		t.Fatalf("Expecting an error.")
	}

	// Attempt to open with wrong database.
	wrongSettings = db.Settings{
		Database: "fail",
		Host:     *host,
		User:     username,
		Password: password,
	}

	if _, err = db.Open(Adapter, wrongSettings); err == nil {
		t.Fatalf("Expecting an error.")
	}

	// Attempt to open with wrong username.
	wrongSettings = db.Settings{
		Database: database,
		Host:     *host,
		User:     "fail",
		Password: password,
	}

	if _, err = db.Open(Adapter, wrongSettings); err == nil {
		t.Fatalf("Expecting an error.")
	}
}

// Old settings must be compatible.
func TestOldSettings(t *testing.T) {
	var err error
	var sess db.Database

	oldSettings := db.Settings{
		Database: database,
		User:     username,
		Password: password,
		Host:     settings.Address.String(),
	}

	// Opening database.
	if sess, err = db.Open(Adapter, oldSettings); err != nil {
		t.Fatal(err)
	}

	// Closing database.
	sess.Close()
}

// Test USE
func TestUse(t *testing.T) {
	var err error
	var sess db.Database

	// Opening database, no error expected.
	if sess, err = db.Open(Adapter, settings); err != nil {
		t.Fatal(err)
	}

	// Connecting to another database, error expected.
	if err = sess.Use("."); err == nil {
		t.Fatal("This is not a database")
	}

	// Closing connection.
	sess.Close()
}

// Truncates all collections.
func TestTruncate(t *testing.T) {

	var err error

	// Opening database.
	sess, err := db.Open(Adapter, settings)

	if err != nil {
		t.Fatal(err)
	}

	// We should close the database when it's no longer in use.
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

		// The collection may ot may not exists.
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

// This test appends some data into the "artist" table.
func TestAppend(t *testing.T) {

	var err error
	var id interface{}

	// Opening database.
	sess, err := db.Open(Adapter, settings)

	if err != nil {
		t.Fatal(err)
	}

	// We should close the database when it's no longer in use.
	defer sess.Close()

	// Getting a pointer to the "artist" collection.
	artist, err := sess.Collection("artist")

	if err != nil {
		// We can use the collection even if it does not exists.
		if err != db.ErrCollectionDoesNotExist {
			t.Fatal(err)
		}
	}

	// Appending a map.
	id, err = artist.Append(map[string]string{
		"name": "Ozzie",
	})

	if err != nil {
		t.Fatalf("Append(): %s", err.Error())
	}

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

	// Attempt to append and update a private key
	itemStruct3 := artistWithObjectIdKey{
		Name: "Janus",
	}

	if _, err = artist.Append(&itemStruct3); err != nil {
		t.Fatal(err)
	}

	if itemStruct3.id.Valid() == false {
		t.Fatalf("Expecting an ID.")
	}

	var total uint64

	// Counting elements, must be exactly 4 elements.
	if total, err = artist.Find().Count(); err != nil {
		t.Fatal(err)
	}

	if total != 4 {
		t.Fatalf("Expecting exactly 4 rows.")
	}

}

// This test tries to use an empty filter and count how many elements were
// added into the artist collection.
func TestResultCount(t *testing.T) {

	var err error
	var res db.Result

	// Opening database.
	sess, err := db.Open(Adapter, settings)

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

func TestGroup(t *testing.T) {

	var err error
	var sess db.Database
	var stats db.Collection

	if sess, err = db.Open(Adapter, settings); err != nil {
		t.Fatal(err)
	}

	type stats_t struct {
		Numeric int `db:"numeric" bson:"numeric"`
		Value   int `db:"value" bson:"value"`
	}

	defer sess.Close()

	if stats, err = sess.Collection("stats_test"); err != nil {
		if err != db.ErrCollectionDoesNotExist {
			t.Fatal(err)
		}
	}

	// Truncating table.
	if err == nil {
		if err = stats.Truncate(); err != nil {
			t.Fatal(err)
		}
	}

	// Adding row append.
	for i := 0; i < 1000; i++ {
		numeric, value := rand.Intn(10), rand.Intn(100)
		if _, err = stats.Append(stats_t{numeric, value}); err != nil {
			t.Fatal(err)
		}
	}

	// db.stats_test.group({key: {numeric: true}, initial: {sum: 0}, reduce: function(doc, prev) { prev.sum += 1}});

	// Testing GROUP BY
	res := stats.Find().Group(bson.M{
		"key":     bson.M{"numeric": true},
		"initial": bson.M{"sum": 0},
		"reduce":  `function(doc, prev) { prev.sum += 1}`,
	})

	var results []map[string]interface{}

	err = res.All(&results)

	// Currently not supported.
	if err != db.ErrUnsupported {
		t.Fatal(err)
	}

	//if len(results) != 10 {
	//	t.Fatalf(`Expecting exactly 10 results, this could fail, but it's very unlikely to happen.`)
	//}

}

// This test uses and result and tries to fetch items one by one.
func TestResultFetch(t *testing.T) {

	var err error
	var res db.Result

	// Opening database.
	sess, err := db.Open(Adapter, settings)

	if err != nil {
		t.Fatal(err)
	}

	// We should close the database when it's no longer in use.
	defer sess.Close()

	artist, err := sess.Collection("artist")

	if err != nil {
		t.Fatal(err)
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
			t.Fatal(err)
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
			t.Fatal(err)
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
			t.Fatal(err)
		}
	}

	res.Close()

	// Testing Result.All() with a slice of maps.
	res = artist.Find()

	all_rows_m := []map[string]interface{}{}
	err = res.All(&all_rows_m)

	if err != nil {
		t.Fatal(err)
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
		t.Fatal(err)
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
		t.Fatal(err)
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
	sess, err := db.Open(Adapter, settings)

	if err != nil {
		t.Fatal(err)
	}

	// We should close the database when it's no longer in use.
	defer sess.Close()

	// Getting a pointer to the "artist" collection.
	artist, err := sess.Collection("artist")

	if err != nil {
		t.Fatal(err)
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
		t.Fatal(err)
	}

	// Updating with a map
	row_m := map[string]interface{}{
		"name": strings.ToUpper(value.Name),
	}

	err = res.Update(row_m)

	if err != nil {
		t.Fatal(err)
	}

	err = res.One(&value)

	if err != nil {
		t.Fatal(err)
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
		t.Fatal(err)
	}

	err = res.One(&value)

	if err != nil {
		t.Fatal(err)
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
		t.Fatal(err)
	}

	err = res.One(&value)

	if err != nil {
		t.Fatal(err)
	}

	if value.Name != row_t.Value1 {
		t.Fatalf("Expecting a modification.")
	}

}

// Test database functions
func TestFunction(t *testing.T) {
	var err error
	var res db.Result

	// Opening database.
	sess, err := db.Open(Adapter, settings)

	if err != nil {
		t.Fatal(err)
	}

	// We should close the database when it's no longer in use.
	defer sess.Close()

	// Getting a pointer to the "artist" collection.
	artist, err := sess.Collection("artist")

	if err != nil {
		t.Fatal(err)
	}

	row_s := struct {
		Id   uint64
		Name string
	}{}

	res = artist.Find(db.Cond{"_id $nin": []int{0, -1}})

	if err = res.One(&row_s); err != nil {
		t.Fatalf("One: %q", err)
	}

	res = artist.Find(db.Cond{"_id": db.Func{"$nin", []int{0, -1}}})

	if err = res.One(&row_s); err != nil {
		t.Fatalf("One: %q", err)
	}

	res.Close()
}

// This test tries to remove some previously added rows.
func TestRemove(t *testing.T) {

	var err error

	// Opening database.
	sess, err := db.Open(Adapter, settings)

	if err != nil {
		t.Fatal(err)
	}

	// We should close the database when it's no longer in use.
	defer sess.Close()

	// Getting a pointer to the "artist" collection.
	artist, err := sess.Collection("artist")

	if err != nil {
		t.Fatal(err)
	}

	// Getting the first artist.
	res := artist.Find(db.Cond{"_id $ne": nil}).Limit(1)

	var first struct {
		Id bson.ObjectId `bson:"_id"`
	}

	err = res.One(&first)

	if err != nil {
		t.Fatal(err)
	}

	res = artist.Find(db.Cond{"_id": first.Id})

	// Trying to remove the row.
	err = res.Remove()

	if err != nil {
		t.Fatal(err)
	}
}

// MongoDB: Does not support schemas so it can't has composite keys. We're
// testing db.Constrainer and db.IDSetter interface.
func TestSetterAndConstrainer(t *testing.T) {
	var err error
	var id interface{}
	var sess db.Database
	var compositeKeys db.Collection

	if sess, err = db.Open(Adapter, settings); err != nil {
		t.Fatal(err)
	}

	defer sess.Close()

	if compositeKeys, err = sess.Collection("composite_keys"); err != nil {
		if err != db.ErrCollectionDoesNotExist {
			t.Fatal(err)
		}
	}

	//n := rand.Intn(100000)

	item := itemWithKey{
		// 		"ABCDEF",
		// 		strconv.Itoa(n),
		SomeVal: "Some value",
	}

	if id, err = compositeKeys.Append(&item); err != nil {
		t.Fatal(err)
	}

	//	ids := id.([]interface{})

	// 	if ids[0].(string) != item.Code {
	// 		t.Fatal(`Keys must match.`)
	// 	}
	//
	// 	if ids[1].(string) != item.UserID {
	// 		t.Fatal(`Keys must match.`)
	// 	}

	// Using constraint interface.
	res := compositeKeys.Find(itemWithKey{ID: id.(bson.ObjectId)})

	var item2 itemWithKey

	if item2.SomeVal == item.SomeVal {
		t.Fatal(`Values must be different before query.`)
	}

	if err := res.One(&item2); err != nil {
		t.Fatal(err)
	}

	if item2.SomeVal != item.SomeVal {
		t.Fatal(`Values must be equal after query.`)
	}

}

// This test tries to add many different datatypes to a single row in a
// collection, then it tries to get the stored datatypes and check if the
// stored and the original values match.
func TestDataTypes(t *testing.T) {
	var res db.Result

	// Opening database.
	sess, err := db.Open(Adapter, settings)

	if err != nil {
		t.Fatal(err)
	}

	// We should close the database when it's no longer in use.
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
	res = dataTypes.Find(db.Cond{"_id": id})

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

// Benchmarking raw mgo queries.
func BenchmarkAppendRaw(b *testing.B) {
	sess, err := db.Open(Adapter, settings)

	if err != nil {
		b.Fatal(err)
	}

	defer sess.Close()

	artist, err := sess.Collection("artist")
	artist.Truncate()

	driver := sess.Driver().(*mgo.Session)

	mgodb := driver.DB(database)
	col := mgodb.C("artist")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := col.Insert(map[string]string{"name": "Hayao Miyazaki"})
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAppendDbItem(b *testing.B) {
	sess, err := db.Open(Adapter, settings)

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

func BenchmarkAppendStruct(b *testing.B) {
	sess, err := db.Open(Adapter, settings)

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
