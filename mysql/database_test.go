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

package mysql

// In order to execute these tests you must initialize the database first:
//
// cd _dumps
// make
// cd ..
// go test

import (
	"database/sql"
	"errors"
	"flag"
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"menteslibres.net/gosexy/to"
	"upper.io/db"
	"upper.io/db/util/sqlutil"
)

const (
	database = "upperio_tests"
	username = "upperio"
	password = "upperio"
)

var settings = ConnectionURL{
	Database: database,
	User:     username,
	Password: password,
}

var host = flag.String("host", "testserver.local", "Testing server address.")

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

	Date  time.Time     `field:"_date"`
	DateN *time.Time    `field:"_nildate"`
	DateP *time.Time    `field:"_ptrdate"`
	Time  time.Duration `field:"_time"`
}

type artistWithInt64Key struct {
	id   int64
	Name string `db:"name"`
}

func (artist *artistWithInt64Key) SetID(id int64) error {
	artist.id = id
	return nil
}

type itemWithKey struct {
	Code    string `db:"code"`
	UserID  string `db:"user_id"`
	SomeVal string `db:"some_val"`
}

func (item itemWithKey) Constraint() db.Cond {
	cond := db.Cond{
		"code":    item.Code,
		"user_id": item.UserID,
	}
	return cond
}

func (item *itemWithKey) SetID(keys map[string]interface{}) error {
	if len(keys) == 2 {
		item.Code = keys["code"].(string)
		item.UserID = keys["user_id"].(string)
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
		Host:     *host,
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
	if err = sess.Use("Another database"); err == nil {
		t.Fatal("This database does not exists!")
	}

	// Closing connection.
	sess.Close()
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
	itemMap := map[string]string{
		"name": "Ozzie",
	}

	if id, err = artist.Append(itemMap); err != nil {
		t.Fatal(err)
	}

	if to.Int64(id) == 0 {
		t.Fatalf("Expecting an ID.")
	}

	// Attempt to append a struct.
	itemStruct := struct {
		Name string `db:"name"`
	}{
		"Flea",
	}

	if id, err = artist.Append(itemStruct); err != nil {
		t.Fatal(err)
	}

	if to.Int64(id) == 0 {
		t.Fatalf("Expecting an ID.")
	}

	// Attempt to append a tagged struct.
	itemStruct2 := struct {
		ArtistName string `db:"name"`
	}{
		"Slash",
	}

	if id, err = artist.Append(itemStruct2); err != nil {
		t.Fatal(err)
	}

	if to.Int64(id) == 0 {
		t.Fatalf("Expecting an ID.")
	}

	// Attempt to append and update a private key
	itemStruct3 := artistWithInt64Key{
		Name: "Janus",
	}

	if _, err = artist.Append(&itemStruct3); err != nil {
		t.Fatal(err)
	}

	if itemStruct3.id == 0 {
		t.Fatalf("Expecting an ID.")
	}

	// Counting elements, must be exactly 4 elements.
	if total, err = artist.Find().Count(); err != nil {
		t.Fatal(err)
	}

	if total != 4 {
		t.Fatalf("Expecting exactly 4 rows.")
	}

}

// Attempts to test nullable fields.
func TestNullableFields(t *testing.T) {
	var err error
	var sess db.Database
	var col db.Collection
	var id interface{}

	if sess, err = db.Open(Adapter, settings); err != nil {
		t.Fatal(err)
	}

	defer sess.Close()

	type testType struct {
		ID              int64           `db:"id,omitempty"`
		NullStringTest  sql.NullString  `db:"_string"`
		NullInt64Test   sql.NullInt64   `db:"_int64"`
		NullFloat64Test sql.NullFloat64 `db:"_float64"`
		NullBoolTest    sql.NullBool    `db:"_bool"`
	}

	var test testType

	if col, err = sess.Collection(`data_types`); err != nil {
		t.Fatal(err)
	}

	if err = col.Truncate(); err != nil {
		t.Fatal(err)
	}

	// Testing insertion of invalid nulls.
	test = testType{
		NullStringTest:  sql.NullString{"", false},
		NullInt64Test:   sql.NullInt64{0, false},
		NullFloat64Test: sql.NullFloat64{0.0, false},
		NullBoolTest:    sql.NullBool{false, false},
	}
	if id, err = col.Append(testType{}); err != nil {
		t.Fatal(err)
	}

	// Testing fetching of invalid nulls.
	if err = col.Find(db.Cond{"id": id}).One(&test); err != nil {
		t.Fatal(err)
	}

	if test.NullInt64Test.Valid {
		t.Fatalf(`Expecting invalid null.`)
	}
	if test.NullFloat64Test.Valid {
		t.Fatalf(`Expecting invalid null.`)
	}
	if test.NullBoolTest.Valid {
		t.Fatalf(`Expecting invalid null.`)
	}
	if test.NullStringTest.Valid {
		t.Fatalf(`Expecting invalid null.`)
	}

	// Testing insertion of valid nulls.
	test = testType{
		NullStringTest:  sql.NullString{"", true},
		NullInt64Test:   sql.NullInt64{0, true},
		NullFloat64Test: sql.NullFloat64{0.0, true},
		NullBoolTest:    sql.NullBool{false, true},
	}
	if id, err = col.Append(test); err != nil {
		t.Fatal(err)
	}

	// Testing fetching of valid nulls.
	if err = col.Find(db.Cond{"id": id}).One(&test); err != nil {
		t.Fatal(err)
	}

	if test.NullInt64Test.Valid == false {
		t.Fatalf(`Expecting valid value.`)
	}
	if test.NullFloat64Test.Valid == false {
		t.Fatalf(`Expecting valid value.`)
	}
	if test.NullBoolTest.Valid == false {
		t.Fatalf(`Expecting valid value.`)
	}
	if test.NullStringTest.Valid == false {
		t.Fatalf(`Expecting valid value.`)
	}
}

func TestGroup(t *testing.T) {

	var err error
	var sess db.Database
	var stats db.Collection

	if sess, err = db.Open(Adapter, settings); err != nil {
		t.Fatal(err)
	}

	type statsType struct {
		Numeric int `db:"numeric"`
		Value   int `db:"value"`
	}

	defer sess.Close()

	if stats, err = sess.Collection("stats_test"); err != nil {
		t.Fatal(err)
	}

	// Truncating table.
	if err = stats.Truncate(); err != nil {
		t.Fatal(err)
	}

	// Adding row append.
	for i := 0; i < 1000; i++ {
		numeric, value := rand.Intn(10), rand.Intn(100)
		if _, err = stats.Append(statsType{numeric, value}); err != nil {
			t.Fatal(err)
		}
	}

	// db.Func{"COUNT", 1},
	// db.Func{"SUM", `value`},

	// Testing GROUP BY
	res := stats.Find().Select(
		`numeric`,
		db.Raw{`COUNT(1) AS counter`},
		db.Raw{`SUM(value) AS total`},
	).Group(`numeric`)

	var results []map[string]interface{}

	if err = res.All(&results); err != nil {
		t.Fatal(err)
	}

	if len(results) != 10 {
		t.Fatalf(`Expecting exactly 10 results, this could fail, but it's very unlikely to happen.`)
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
	rowMap := map[string]interface{}{}

	res = artist.Find()

	for {
		err = res.Next(&rowMap)

		if err == db.ErrNoMoreRows {
			break
		}

		if err == nil {
			if to.Int64(rowMap["id"]) == 0 {
				t.Fatalf("Expecting a not null ID.")
			}
			if to.String(rowMap["name"]) == "" {
				t.Fatalf("Expecting a name.")
			}
		} else {
			t.Fatal(err)
		}
	}

	res.Close()

	// Dumping into an struct with no tags.
	rowStruct := struct {
		ID   uint64
		Name string
	}{}

	res = artist.Find()

	for {
		err = res.Next(&rowStruct)

		if err == db.ErrNoMoreRows {
			break
		}

		if err == nil {
			if rowStruct.ID == 0 {
				t.Fatalf("Expecting a not null ID.")
			}
			if rowStruct.Name == "" {
				t.Fatalf("Expecting a name.")
			}
		} else {
			t.Fatal(err)
		}
	}

	res.Close()

	// Dumping into a tagged struct.
	rowStruct2 := struct {
		Value1 uint64 `field:"id"`
		Value2 string `field:"name"`
	}{}

	res = artist.Find()

	for {
		err = res.Next(&rowStruct2)

		if err == db.ErrNoMoreRows {
			break
		}

		if err == nil {
			if rowStruct2.Value1 == 0 {
				t.Fatalf("Expecting a not null ID.")
			}
			if rowStruct2.Value2 == "" {
				t.Fatalf("Expecting a name.")
			}
		} else {
			t.Fatal(err)
		}
	}

	res.Close()

	// Dumping into an slice of maps.
	allRowsMap := []map[string]interface{}{}

	res = artist.Find()
	if err = res.All(&allRowsMap); err != nil {
		t.Fatal(err)
	}

	if len(allRowsMap) != 4 {
		t.Fatalf("Expecting 4 items.")
	}

	for _, singleRowMap := range allRowsMap {
		if to.Int64(singleRowMap["id"]) == 0 {
			t.Fatalf("Expecting a not null ID.")
		}
	}

	// Dumping into an slice of structs.

	allRowsStruct := []struct {
		ID   uint64
		Name string
	}{}

	res = artist.Find()
	if err = res.All(&allRowsStruct); err != nil {
		t.Fatal(err)
	}

	if len(allRowsStruct) != 4 {
		t.Fatalf("Expecting 4 items.")
	}

	for _, singleRowStruct := range allRowsStruct {
		if singleRowStruct.ID == 0 {
			t.Fatalf("Expecting a not null ID.")
		}
	}

	// Dumping into an slice of tagged structs.
	allRowsStruct2 := []struct {
		Value1 uint64 `field:"id"`
		Value2 string `field:"name"`
	}{}

	res = artist.Find()

	if err = res.All(&allRowsStruct2); err != nil {
		t.Fatal(err)
	}

	if len(allRowsStruct2) != 4 {
		t.Fatalf("Expecting 4 items.")
	}

	for _, singleRowStruct2 := range allRowsStruct2 {
		if singleRowStruct2.Value1 == 0 {
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
		ID   uint64
		Name string
	}{}

	// Getting the first artist.
	res := artist.Find(db.Cond{"id !=": 0}).Limit(1)

	if err = res.One(&value); err != nil {
		t.Fatal(err)
	}

	// Updating set with a map
	rowMap := map[string]interface{}{
		"name": strings.ToUpper(value.Name),
	}

	if err = res.Update(rowMap); err != nil {
		t.Fatal(err)
	}

	// Pulling it again.
	if err = res.One(&value); err != nil {
		t.Fatal(err)
	}

	// Verifying.
	if value.Name != rowMap["name"] {
		t.Fatalf("Expecting a modification.")
	}

	// Updating set with a struct
	rowStruct := struct {
		Name string
	}{strings.ToLower(value.Name)}

	if err = res.Update(rowStruct); err != nil {
		t.Fatal(err)
	}

	// Pulling it again.
	if err = res.One(&value); err != nil {
		t.Fatal(err)
	}

	// Verifying
	if value.Name != rowStruct.Name {
		t.Fatalf("Expecting a modification.")
	}

	// Updating set with a tagged struct
	rowStruct2 := struct {
		Value1 string `db:"name"`
	}{strings.Replace(value.Name, "z", "Z", -1)}

	if err = res.Update(rowStruct2); err != nil {
		t.Fatal(err)
	}

	// Pulling it again.
	if err = res.One(&value); err != nil {
		t.Fatal(err)
	}

	// Verifying
	if value.Name != rowStruct2.Value1 {
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
	var rowMap map[string]interface{}

	if sess, err = db.Open(Adapter, settings); err != nil {
		t.Fatal(err)
	}

	defer sess.Close()

	if artist, err = sess.Collection("artist"); err != nil {
		t.Fatal(err)
	}

	rowStruct := struct {
		ID   uint64
		Name string
	}{}

	res = artist.Find(db.Cond{"id NOT IN": []int{0, -1}})

	if err = res.One(&rowStruct); err != nil {
		t.Fatal(err)
	}

	if total, err = res.Count(); err != nil {
		t.Fatal(err)
	}

	if total != 4 {
		t.Fatalf("Expecting 4 items.")
	}

	// Testing conditions
	res = artist.Find(db.Cond{"id": db.Func{"NOT IN", []int{0, -1}}})

	if err = res.One(&rowStruct); err != nil {
		t.Fatal(err)
	}

	if total, err = res.Count(); err != nil {
		t.Fatal(err)
	}

	if total != 4 {
		t.Fatalf("Expecting 4 items.")
	}

	// Testing DISTINCT (function)
	res = artist.Find().Select(
		db.Func{`DISTINCT`, `name`},
	)

	if err = res.One(&rowMap); err != nil {
		t.Fatal(err)
	}

	if total, err = res.Count(); err != nil {
		t.Fatal(err)
	}

	if total != 4 {
		t.Fatalf("Expecting 4 items.")
	}

	// Testing DISTINCT (raw)
	res = artist.Find().Select(
		db.Raw{`DISTINCT(name)`},
	)

	if err = res.One(&rowMap); err != nil {
		t.Fatal(err)
	}

	if total, err = res.Count(); err != nil {
		t.Fatal(err)
	}

	if total != 4 {
		t.Fatalf("Expecting 4 items.")
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

	type artistType struct {
		ID   int64  `db:"id,omitempty"`
		Name string `db:"name"`
	}

	type publicationType struct {
		ID       int64  `db:"id,omitempty"`
		Title    string `db:"title"`
		AuthorID int64  `db:"author_id"`
	}

	type reviewType struct {
		ID            int64     `db:"id,omitempty"`
		PublicationID int64     `db:"publication_id"`
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
	var miyazakiID interface{}
	miyazaki := artistType{Name: `Hayao Miyazaki`}
	if miyazakiID, err = artist.Append(miyazaki); err != nil {
		t.Fatal(err)
	}
	miyazaki.ID = miyazakiID.(int64)

	var asimovID interface{}
	asimov := artistType{Name: `Isaac Asimov`}
	if asimovID, err = artist.Append(asimov); err != nil {
		t.Fatal(err)
	}

	var marquezID interface{}
	marquez := artistType{Name: `Gabriel García Márquez`}
	if marquezID, err = artist.Append(marquez); err != nil {
		t.Fatal(err)
	}

	// Adding some publications.
	publication.Append(publicationType{
		Title:    `Tonari no Totoro`,
		AuthorID: miyazakiID.(int64),
	})

	publication.Append(publicationType{
		Title:    `Howl's Moving Castle`,
		AuthorID: miyazakiID.(int64),
	})

	publication.Append(publicationType{
		Title:    `Ponyo`,
		AuthorID: miyazakiID.(int64),
	})

	publication.Append(publicationType{
		Title:    `Memoria de mis Putas Tristes`,
		AuthorID: marquezID.(int64),
	})

	publication.Append(publicationType{
		Title:    `El Coronel no tiene quien le escriba`,
		AuthorID: marquezID.(int64),
	})

	publication.Append(publicationType{
		Title:    `El Amor en los tiempos del Cólera`,
		AuthorID: marquezID.(int64),
	})

	publication.Append(publicationType{
		Title:    `I, Robot`,
		AuthorID: asimovID.(int64),
	})

	var foundationID interface{}
	foundationID, err = publication.Append(publicationType{
		Title:    `Foundation`,
		AuthorID: asimovID.(int64),
	})
	if err != nil {
		t.Fatal(err)
	}

	publication.Append(publicationType{
		Title:    `The Robots of Dawn`,
		AuthorID: asimovID.(int64),
	})

	// Adding reviews for foundation.
	review.Append(reviewType{
		PublicationID: foundationID.(int64),
		Name:          "John Doe",
		Comments:      "I love The Foundation series.",
		Created:       time.Now(),
	})

	review.Append(reviewType{
		PublicationID: foundationID.(int64),
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

	type artistPublicationType struct {
		ID               int64  `db:"id"`
		PublicationTitle string `db:"publication_title"`
		ArtistName       string `db:"artist_name"`
	}

	all := []artistPublicationType{}

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

	type publicationType struct {
		ID       int64  `db:"id,omitempty"`
		Title    string `db:"title"`
		AuthorID int64  `db:"author_id"`
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

	var all []publicationType

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

	type artistType struct {
		ID   int64  `db:"id,omitempty"`
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
	if _, err = artist.Append(artistType{1, "First"}); err != nil {
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
	if _, err = artist.Append(artistType{2, "Second"}); err != nil {
		t.Fatal(err)
	}

	// Won't fail.
	if _, err = artist.Append(artistType{3, "Third"}); err != nil {
		t.Fatal(err)
	}

	// Will fail.
	if _, err = artist.Append(artistType{1, "Duplicated"}); err == nil {
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
	if _, err = artist.Append(artistType{2, "Second"}); err != nil {
		t.Fatal(err)
	}

	// Won't fail.
	if _, err = artist.Append(artistType{3, "Third"}); err != nil {
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
	if _, err = artist.Append(artistType{2, "Second"}); err != nil {
		t.Fatal(err)
	}

	// Won't fail.
	if _, err = artist.Append(artistType{3, "Third"}); err != nil {
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
		t.Fatalf("Expecting 3 elements.")
	}

}

// Attempts to test composite keys.
func TestCompositeKeys(t *testing.T) {
	var err error
	var sess db.Database
	var compositeKeys db.Collection

	if sess, err = db.Open(Adapter, settings); err != nil {
		t.Fatal(err)
	}

	defer sess.Close()

	if compositeKeys, err = sess.Collection("composite_keys"); err != nil {
		t.Fatal(err)
	}

	n := rand.Intn(100000)

	item := itemWithKey{
		"ABCDEF",
		strconv.Itoa(n),
		"Some value",
	}

	if _, err = compositeKeys.Append(&item); err != nil {
		t.Fatal(err)
	}

	// Using constraint interface.

	var item2 itemWithKey

	if item2.SomeVal == item.SomeVal {
		t.Fatal(`Values must be different before query.`)
	}

	res := compositeKeys.Find(item)

	if err := res.One(&item2); err != nil {
		t.Fatal(err)
	}

	if item2.SomeVal != item.SomeVal {
		t.Fatal(`Values must be equal after query.`)
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

	if _, err = driver.Exec("TRUNCATE TABLE `artist`"); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err = driver.Exec("INSERT INTO `artist` (`name`) VALUES('Hayao Miyazaki')"); err != nil {
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

	if _, err = tx.Exec("TRUNCATE TABLE `artist`"); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err = tx.Exec("INSERT INTO `artist` (`name`) VALUES('Hayao Miyazaki')"); err != nil {
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
