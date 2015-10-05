// Copyright (c) 2012-2015 The upper.io/db authors. All rights reserved.
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

package postgresql

import (
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"upper.io/db"
	"upper.io/db/util/sqlutil"
)

const (
	databaseName = "upperio_tests"
	username     = "upperio_tests"
	password     = "upperio_secret"
)

const (
	testTimeZone = "Canada/Eastern"
)

var settings = ConnectionURL{
	Database: databaseName,
	User:     username,
	Password: password,
	Options: map[string]string{
		"timezone": testTimeZone,
	},
}

var host string

// Structure for testing conversions and datatypes.
type testValuesStruct struct {
	Uint   uint   `db:"_uint"`
	Uint8  uint8  `db:"_uint8"`
	Uint16 uint16 `db:"_uint16"`
	Uint32 uint32 `db:"_uint32"`
	Uint64 uint64 `db:"_uint64"`

	Int   int   `db:"_int"`
	Int8  int8  `db:"_int8"`
	Int16 int16 `db:"_int16"`
	Int32 int32 `db:"_int32"`
	Int64 int64 `db:"_int64"`

	Float32 float32 `db:"_float32"`
	Float64 float64 `db:"_float64"`

	Bool   bool   `db:"_bool"`
	String string `db:"_string"`

	Date  time.Time  `db:"_date"`
	DateN *time.Time `db:"_nildate"`
	DateP *time.Time `db:"_ptrdate"`
	DateD *time.Time `db:"_defaultdate,omitempty"`
	Time  int64      `db:"_time"`
}

type artistType struct {
	ID   int64  `db:"id,omitempty"`
	Name string `db:"name"`
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
		item.Code = string(keys["code"].([]byte))
		item.UserID = string(keys["user_id"].([]byte))
		return nil
	}
	return errors.New(`Expecting exactly two keys.`)
}

var testValues testValuesStruct

func init() {
	loc, err := time.LoadLocation(testTimeZone)

	if err != nil {
		panic(err.Error())
	}

	t := time.Date(2011, 7, 28, 1, 2, 3, 0, loc)                     // timestamp with time zone
	tnz := time.Date(2012, 7, 28, 1, 2, 3, 0, time.FixedZone("", 0)) // timestamp without time zone

	testValues = testValuesStruct{
		1, 1, 1, 1, 1,
		-1, -1, -1, -1, -1,
		1.337, 1.337,
		true,
		"Hello world!",
		t,
		nil,
		&tnz,
		nil,
		int64(time.Second * time.Duration(7331)),
	}

	if host = os.Getenv("TEST_HOST"); host == "" {
		host = "localhost"
	}

	settings.Address = db.ParseAddress(host)
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
func SkipTestOpenWithWrongData(t *testing.T) {
	var err error
	var rightSettings, wrongSettings db.Settings

	// Attempt to open with safe settings.
	rightSettings = db.Settings{
		Database: databaseName,
		Host:     host,
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
		Database: "fail",
		Host:     host,
		User:     "fail",
		Password: "fail",
	}

	if _, err = db.Open(Adapter, wrongSettings); err == nil {
		t.Fatalf("Expecting an error.")
	}

	// Attempt to open with wrong database.
	wrongSettings = db.Settings{
		Database: "fail",
		Host:     host,
		User:     username,
		Password: password,
	}

	if _, err = db.Open(Adapter, wrongSettings); err == nil {
		t.Fatalf("Expecting an error.")
	}

	// Attempt to open with wrong username.
	wrongSettings = db.Settings{
		Database: databaseName,
		Host:     host,
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
		Database: databaseName,
		User:     username,
		Password: password,
		Host:     host,
	}

	// Opening database.
	if sess, err = db.Open(Adapter, oldSettings); err != nil {
		t.Fatal(err)
	}

	// Closing database.
	sess.Close()
}

// Test Use
func TestUse(t *testing.T) {
	var err error
	var sess db.Database

	// Opening database, no error expected.
	if sess, err = db.Open(Adapter, settings); err != nil {
		t.Fatal(err)
	}

	// Connecting to another database, error expected.
	if err = sess.Use("Another database"); err == nil {
		t.Fatal("This database should not exist!")
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

// Attempts to trigger a database error.
func TestSetCursorError(t *testing.T) {
	sess, err := db.Open(Adapter, settings)
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

	if pk, ok := id.(int64); !ok || pk == 0 {
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

	if pk, ok := id.(int64); !ok || pk == 0 {
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

	if pk, ok := id.(int64); !ok || pk == 0 {
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

// Attempts to count all rows in a table that does not exist.
func TestResultNonExistentCount(t *testing.T) {
	sess, err := db.Open(Adapter, settings)

	if err != nil {
		t.Fatal(err)
	}

	defer sess.Close()

	total, err := sess.C("notartist").Find().Count()

	if err != db.ErrCollectionDoesNotExist {
		t.Fatal("Expecting a specific error, got", err)
	}

	if total != 0 {
		t.Fatal("Counter should be zero")
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
			if id, ok := rowMap["id"].(int64); !ok || id == 0 {
				t.Fatalf("Expecting a not null ID.")
			}
			if name, ok := rowMap["name"].([]byte); !ok || len(name) == 0 {
				t.Fatalf("Expecting a name.")
			}
		} else {
			t.Fatal(err)
		}
	}

	res.Close()

	// Dumping into a tagged struct.
	rowStruct2 := struct {
		Value1 uint64 `db:"id"`
		Value2 string `db:"name"`
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

	// Dumping into a slice of maps.
	allRowsMap := []map[string]interface{}{}

	res = artist.Find()
	if err = res.All(&allRowsMap); err != nil {
		t.Fatal(err)
	}

	if len(allRowsMap) != 4 {
		t.Fatalf("Expecting 4 items.")
	}

	for _, singleRowMap := range allRowsMap {
		if pk, ok := singleRowMap["id"].(int64); !ok || pk == 0 {
			t.Fatalf("Expecting a not null ID.")
		}
	}

	// Dumping into a slice of structs.
	allRowsStruct := []struct {
		ID   uint64 `db:"id,omitempty"`
		Name string `db:"name"`
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

	// Dumping into a slice of tagged structs.
	allRowsStruct2 := []struct {
		Value1 uint64 `db:"id"`
		Value2 string `db:"name"`
	}{}

	res = artist.Find()

	if err = res.All(&allRowsStruct2); err != nil {
		t.Fatal(err)
	}

	if len(allRowsStruct2) != 4 {
		t.Fatalf("Expecting 4 items.")
	}

	for _, singleRowStruct := range allRowsStruct2 {
		if singleRowStruct.Value1 == 0 {
			t.Fatalf("Expecting a not null ID.")
		}
	}
}

func TestResultFetchOne(t *testing.T) {
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

	// Fetching one struct
	var someArtist artistType
	err = artist.Find().Limit(1).One(&someArtist)
	if err != nil {
		t.Fatal(err)
	}

	if someArtist.Name == "" {
		t.Fatal("Expecting an artist object with a name.")
	}
	if someArtist.ID <= 0 {
		t.Fatal("Expecting an artist to have an ID.")
	}

	// Fetching one object
	var someArtistObj *artistType
	err = artist.Find().Limit(1).One(&someArtistObj)
	if err != nil {
		t.Fatal(err)
	}

	if someArtistObj.Name == "" {
		t.Fatal("Expecting an artist object with a name.")
	}
	if someArtistObj.ID <= 0 {
		t.Fatal("Expecting an artist object to have an ID.")
	}
}

func TestResultFetchAll(t *testing.T) {
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

	// Fetching all artists into struct
	artists := []artistType{}
	err = artist.Find().All(&artists)
	if err != nil {
		t.Fatal(err)
	}

	if len(artists) == 0 {
		t.Fatal("Expecting some artists.")
	}
	if artists[0].Name == "" {
		t.Fatal("Expecting the first artist to have a name.")
	}
	if artists[0].ID <= 0 {
		t.Fatal("Expecting the first artist to have an ID.")
	}

	// Fetching all artists into struct objects
	artistObjs := []*artistType{}
	err = artist.Find().All(&artistObjs)
	if err != nil {
		t.Fatal(err)
	}

	if len(artistObjs) == 0 {
		t.Fatal("Expecting some artist objects.")
	}
	if artistObjs[0].Name == "" {
		t.Fatal("Expecting the first artist object to have a name.")
	}
	if artistObjs[0].ID <= 0 {
		t.Fatal("Expecting the first artist object to have an ID.")
	}
}

func TestInlineStructs(t *testing.T) {
	var sess db.Database
	var err error

	var review db.Collection

	type reviewTypeDetails struct {
		Name     string    `db:"name"`
		Comments string    `db:"comments"`
		Created  time.Time `db:"created"`
	}

	type reviewType struct {
		ID            int64             `db:"id,omitempty"`
		PublicationID int64             `db:"publication_id"`
		Details       reviewTypeDetails `db:",inline"`
	}

	if sess, err = db.Open(Adapter, settings); err != nil {
		t.Fatal(err)
	}

	defer sess.Close()

	if review, err = sess.Collection("review"); err != nil {
		t.Fatal(err)
	}

	if err = review.Truncate(); err != nil {
		t.Fatal(err)
	}

	rec := reviewType{
		PublicationID: 123,
		Details: reviewTypeDetails{
			Name: "..name..", Comments: "..comments..",
		},
	}

	id, err := review.Append(rec)
	if err != nil {
		t.Fatal(err)
	}
	if id.(int64) <= 0 {
		t.Fatal("bad id")
	}
	rec.ID = id.(int64)

	var recChk reviewType
	err = review.Find().One(&recChk)

	if err != nil {
		t.Fatal(err)
	}

	if recChk.ID != rec.ID {
		t.Fatal("ID of review does not match, expecting:", rec.ID, "got:", recChk.ID)
	}
	if recChk.Details.Name != rec.Details.Name {
		t.Fatal("Name of inline field does not match, expecting:",
			rec.Details.Name, "got:", recChk.Details.Name)
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
		ID   uint64 `db:"id,omitempty"`
		Name string `db:"name"`
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
		Name string `db:"name"`
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
	}{"john"}

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

	// Updating set with a tagged object
	rowStruct3 := &struct {
		Value1 string `db:"name"`
	}{"anderson"}

	if err = res.Update(rowStruct3); err != nil {
		t.Fatal(err)
	}

	// Pulling it again.
	if err = res.One(&value); err != nil {
		t.Fatal(err)
	}

	// Verifying
	if value.Name != rowStruct3.Value1 {
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

	// In PostgreSQL, how we can tell if this is an invalid null?

	// if test.NullStringTest.Valid {
	//  t.Fatalf(`Expecting invalid null.`)
	// }

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
		db.Raw{"a.name AS artist_name"},
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
	var rows *sqlx.Rows
	var err error
	var drv *sqlx.DB

	type publicationType struct {
		ID       int64  `db:"id,omitempty"`
		Title    string `db:"title"`
		AuthorID int64  `db:"author_id"`
	}

	if sess, err = db.Open(Adapter, settings); err != nil {
		t.Fatal(err)
	}

	defer sess.Close()

	drv = sess.Driver().(*sqlx.DB)

	rows, err = drv.Queryx(`
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

	// An attempt to use the same transaction must fail.
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

	// Won't fail
	sqlTx := tx.Driver().(*sqlx.Tx)
	if _, err = sqlTx.Exec(`INSERT INTO "artist" ("id", "name") VALUES($1, $2)`, 4, "Fourth"); err != nil {
		t.Fatal(err)
	}

	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}

	if err = tx.Rollback(); err == nil {
		t.Fatalf("Should have failed, as we've already commited.")
	}

	// Let's verify we have 4 rows.
	if artist, err = sess.Collection("artist"); err != nil {
		t.Fatal(err)
	}

	if count, err = artist.Find().Count(); err != nil {
		t.Fatal(err)
	}

	if count != 4 {
		t.Fatalf("Expecting exactly 4 results.")
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

	// Using constrainer interface.

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

	if err = res.One(&item); err != nil {
		t.Fatal(err)
	}

	if item.DateD == nil {
		t.Fatal("Expecting default date to have been set on append")
	}

	// Copy the default date (this value is set by the database)
	testValues.DateD = item.DateD

	// The original value and the test subject must match.
	if reflect.DeepEqual(item, testValues) == false {
		fmt.Printf("item1: %v\n", item)
		fmt.Printf("test2: %v\n", testValues)
		t.Fatalf("Struct is different.")
	}
}

func TestOptionTypes(t *testing.T) {
	var err error
	var sess db.Database
	var optionTypes db.Collection

	if sess, err = db.Open(Adapter, settings); err != nil {
		t.Fatal(err)
	}

	defer sess.Close()

	if optionTypes, err = sess.Collection("option_types"); err != nil {
		t.Fatal(err)
	}

	if err = optionTypes.Truncate(); err != nil {
		t.Fatal(err)
	}

	// TODO: lets do some benchmarking on these auto-wrapped option types..

	// TODO: add nullable jsonb field mapped to a []string

	// A struct with wrapped option types defined in the struct tags
	// for postgres string array and jsonb types
	type optionType struct {
		ID       int64                  `db:"id,omitempty"`
		Name     string                 `db:"name"`
		Tags     []string               `db:"tags,stringarray"`
		Settings map[string]interface{} `db:"settings,jsonb"`
	}

	// Item 1
	item1 := optionType{
		Name:     "Food",
		Tags:     []string{"toronto", "pizza"},
		Settings: map[string]interface{}{"a": 1, "b": 2},
	}

	id, err := optionTypes.Append(item1)
	if err != nil {
		t.Fatal(err)
	}

	if pk, ok := id.(int64); !ok || pk == 0 {
		t.Fatalf("Expecting an ID.")
	}

	var item1Chk optionType
	if err := optionTypes.Find(db.Cond{"id": id}).One(&item1Chk); err != nil {
		t.Fatal(err)
	}

	if item1Chk.Settings["a"].(float64) != 1 { // float64 because of json..
		t.Fatalf("Expecting Settings['a'] of jsonb value to be 1")
	}

	if item1Chk.Tags[0] != "toronto" {
		t.Fatalf("Expecting first element of Tags stringarray to be 'toronto'")
	}

	// Item 1 B
	item1b := &optionType{
		Name:     "Golang",
		Tags:     []string{"love", "it"},
		Settings: map[string]interface{}{"go": 1, "lang": 2},
	}

	id, err = optionTypes.Append(item1b)
	if err != nil {
		t.Fatal(err)
	}

	if pk, ok := id.(int64); !ok || pk == 0 {
		t.Fatalf("Expecting an ID.")
	}

	var item1bChk optionType
	if err := optionTypes.Find(db.Cond{"id": id}).One(&item1bChk); err != nil {
		t.Fatal(err)
	}

	if item1bChk.Settings["go"].(float64) != 1 { // float64 because of json..
		t.Fatalf("Expecting Settings['go'] of jsonb value to be 1")
	}

	if item1bChk.Tags[0] != "love" {
		t.Fatalf("Expecting first element of Tags stringarray to be 'love'")
	}

	// Item 1 C
	item1c := &optionType{
		Name: "Sup", Tags: []string{}, Settings: map[string]interface{}{},
	}

	id, err = optionTypes.Append(item1c)
	if err != nil {
		t.Fatal(err)
	}

	if pk, ok := id.(int64); !ok || pk == 0 {
		t.Fatalf("Expecting an ID.")
	}

	var item1cChk optionType
	if err := optionTypes.Find(db.Cond{"id": id}).One(&item1cChk); err != nil {
		t.Fatal(err)
	}

	if len(item1cChk.Tags) != 0 {
		t.Fatalf("Expecting tags array to be empty but is %v", item1cChk.Tags)
	}

	if len(item1cChk.Settings) != 0 {
		t.Fatalf("Expecting Settings map to be empty")
	}

	// An option type to pointer jsonb field
	type optionType2 struct {
		ID       int64                   `db:"id,omitempty"`
		Name     string                  `db:"name"`
		Tags     []string                `db:"tags,stringarray"`
		Settings *map[string]interface{} `db:"settings,jsonb"`
	}

	item2 := optionType2{
		Name: "JS", Tags: []string{"hi", "bye"}, Settings: nil,
	}

	id, err = optionTypes.Append(item2)
	if err != nil {
		t.Fatal(err)
	}

	if pk, ok := id.(int64); !ok || pk == 0 {
		t.Fatalf("Expecting an ID.")
	}

	var item2Chk optionType2
	res := optionTypes.Find(db.Cond{"id": id})
	if err := res.One(&item2Chk); err != nil {
		t.Fatal(err)
	}

	if item2Chk.ID != id.(int64) {
		t.Fatalf("Expecting id to match")
	}

	if item2Chk.Name != item2.Name {
		t.Fatalf("Expecting Name to match")
	}

	if item2Chk.Tags[0] != item2.Tags[0] || len(item2Chk.Tags) != len(item2.Tags) {
		t.Fatalf("Expecting tags to match")
	}

	// Update the value
	m := map[string]interface{}{}
	m["lang"] = "javascript"
	m["num"] = 31337
	item2.Settings = &m
	err = res.Update(item2)
	if err != nil {
		t.Fatal(err)
	}

	if err := res.One(&item2Chk); err != nil {
		t.Fatal(err)
	}

	if (*item2Chk.Settings)["num"].(float64) != 31337 { // float64 because of json..
		t.Fatalf("Expecting Settings['num'] of jsonb value to be 31337")
	}

	if (*item2Chk.Settings)["lang"] != "javascript" {
		t.Fatalf("Expecting Settings['lang'] of jsonb value to be 'javascript'")
	}

	// An option type to pointer string array field
	type optionType3 struct {
		ID       int64                  `db:"id,omitempty"`
		Name     string                 `db:"name"`
		Tags     *[]string              `db:"tags,stringarray"`
		Settings map[string]interface{} `db:"settings,jsonb"`
	}

	item3 := optionType3{
		Name: "Julia", Tags: nil, Settings: map[string]interface{}{"girl": true, "lang": true},
	}

	id, err = optionTypes.Append(item3)
	if err != nil {
		t.Fatal(err)
	}

	if pk, ok := id.(int64); !ok || pk == 0 {
		t.Fatalf("Expecting an ID.")
	}

	var item3Chk optionType2
	if err := optionTypes.Find(db.Cond{"id": id}).One(&item3Chk); err != nil {
		t.Fatal(err)
	}
}

func TestOptionTypeJsonbStruct(t *testing.T) {
	var err error
	var sess db.Database
	var optionTypes db.Collection

	if sess, err = db.Open(Adapter, settings); err != nil {
		t.Fatal(err)
	}

	defer sess.Close()

	if optionTypes, err = sess.Collection("option_types"); err != nil {
		t.Fatal(err)
	}

	if err = optionTypes.Truncate(); err != nil {
		t.Fatal(err)
	}

	// A struct with wrapped option types defined in the struct tags
	// for postgres string array and jsonb types
	type Settings struct {
		Name string `json:"name"`
		Num  int64  `json:"num"`
	}

	type OptionType struct {
		ID       int64    `db:"id,omitempty"`
		Name     string   `db:"name"`
		Tags     []string `db:"tags,stringarray"`
		Settings Settings `db:"settings,jsonb"`
	}

	item1 := &OptionType{
		Name:     "Hi",
		Tags:     []string{"aah", "ok"},
		Settings: Settings{Name: "a", Num: 123},
	}

	id, err := optionTypes.Append(item1)
	if err != nil {
		t.Fatal(err)
	}

	if pk, ok := id.(int64); !ok || pk == 0 {
		t.Fatalf("Expecting an ID.")
	}

	var item1Chk OptionType
	if err := optionTypes.Find(db.Cond{"id": id}).One(&item1Chk); err != nil {
		t.Fatal(err)
	}

	if len(item1Chk.Tags) != 2 {
		t.Fatalf("Expecting 2 tags")
	}

	if item1Chk.Tags[0] != "aah" {
		t.Fatalf("Expecting first tag to be 0")
	}

	if item1Chk.Settings.Name != "a" {
		t.Fatalf("Expecting Name to be 'a'")
	}

	if item1Chk.Settings.Num != 123 {
		t.Fatalf("Expecting Num to be 123")
	}
}

func TestQueryBuilder(t *testing.T) {
	var sess db.Database
	var err error

	if sess, err = db.Open(Adapter, settings); err != nil {
		t.Fatal(err)
	}

	defer sess.Close()

	b := sess.Builder()

	assert := assert.New(t)

	// Testing SELECT.

	assert.Equal(
		`SELECT * FROM "artist"`,
		b.SelectAllFrom("artist").String(),
	)

	assert.Equal(
		`SELECT * FROM "artist"`,
		b.Select().From("artist").String(),
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
		b.Select(db.Raw{"DATE()"}).String(),
	)

	// Testing INSERT.

	assert.Equal(
		`INSERT INTO "artist" VALUES ($1, $2), ($3, $4), ($5, $6)`,
		b.InsertInto("artist").
			Values(10, "Ryuichi Sakamoto").
			Values(11, "Alondra de la Parra").
			Values(12, "Haruki Murakami").
			String(),
	)

	assert.Equal(
		`INSERT INTO "artist" ("name", "id") VALUES ($1, $2)`,
		b.InsertInto("artist").Values(map[string]string{"id": "12", "name": "Chavela Vargas"}).String(),
	)

	assert.Equal(
		`INSERT INTO "artist" ("name", "id") VALUES ($1, $2)`,
		b.InsertInto("artist").Values(struct {
			ID   int    `db:"id"`
			Name string `db:"name"`
		}{12, "Chavela Vargas"}).String(),
	)

	assert.Equal(
		`INSERT INTO "artist" ("name", "id") VALUES ($1, $2)`,
		b.InsertInto("artist").Columns("name", "id").Values("Chavela Vargas", 12).String(),
	)

	// Testing DELETE.

	assert.Equal(
		`DELETE FROM "artist" WHERE (name = $1) LIMIT 1`,
		b.DeleteFrom("artist").Where("name = ?", "Chavela Vargas").Limit(1).String(),
	)

	assert.Equal(
		`DELETE FROM "artist" WHERE (id > 5)`,
		b.DeleteFrom("artist").Where("id > 5").String(),
	)

	// Testing UPDATE.

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
		b.Update("artist").Set(map[string]string{"name": "Artist"}).Where(db.Cond{"id <": 5}).String(),
	)

	assert.Equal(
		`UPDATE "artist" SET "name" = $1 WHERE ("id" < $2)`,
		b.Update("artist").Set(struct {
			Nombre string `db:"name"`
		}{"Artist"}).Where(db.Cond{"id <": 5}).String(),
	)

	assert.Equal(
		`UPDATE "artist" SET "name" = $1, "last_name" = $2 WHERE ("id" < $3)`,
		b.Update("artist").Set(struct {
			Nombre string `db:"name"`
		}{"Artist"}).Set(map[string]string{"last_name": "Foo"}).Where(db.Cond{"id <": 5}).String(),
	)

	assert.Equal(
		`UPDATE "artist" SET "name" = $1 || ' ' || $2 || id, "id" = id + $3 WHERE (id > $4)`,
		b.Update("artist").Set(
			"name = ? || ' ' || ? || id", "Artist", "#",
			"id = id + ?", 10,
		).Where("id > ?", 0).String(),
	)

	/*
		// INSERT INTO artist (name) VALUES(? || ?)
		if err = b.InsertInto("artist").Columns("name").Values(db.Expr("? || ' ' || ?", "Tom", "Yorke")).Exec(); err != nil {
			t.Fatal(err)
		}
		// INSERT INTO artist ("name") VALUES('Michael Jackson')
		if err = b.InsertInto("artist").Columns("name").Record(map[string]string{"no": "Not me!", "name": "Michael Jackson"}).Exec(); err != nil {
			t.Fatal(err)
		}

		// INSERT INTO artist ("id", "name") VALUES(20, 'Francisco Toledo')
		if err = b.InsertInto("artist").Value(map[string]string{"id": 20, "name": "Francisco Toledo"}).Exec(); err != nil {
			t.Fatal(err)
		}

		// INSERT INTO artist ("name") VALUES('Mads Mikkelsen')
		if err = b.InsertInto("artist").Value(artistType{"Mads Mikkelsen"}).Exec(); err != nil {
			t.Fatal(err)
		}
	*/

	// Testing actual queries.

	/*
		var artist artistType
		var artists []artistType

		err = b.SelectAllFrom("artist").Iterator().All(&artists)
		assert.NoError(err)
		assert.True(len(artists) > 0)

		err = b.SelectAllFrom("artist").Iterator().One(&artist)
		assert.NoError(err)
		assert.NotNil(artist)

		var qs db.QuerySelector

		qs = b.SelectAllFrom("artist")
		iter := qs.Iterator()
		for iter.Next(&artist) {
			assert.Nil(iter.Err())
			assert.NotNil(artist)
		}

		assert.Nil(iter.Close())

		qs = b.Select().From("artist a").Join("publications p").On("p1.id = a.id").Using("id")
		assert.Error(qs.Iterator().One(&artist), `Should not work because it attempts to use both "On()" and "Using()" in the same JOIN.`)

		qs = b.Select().From("artist a").On("p1.id = a.id")
		assert.Error(qs.Iterator().One(&artist), `Should not work because it should put a "Join()" before "On()".`)
	*/
}

// TestExhaustConnections simulates a "too many connections" situation
// triggered by opening more transactions than available connections.
// upper.io/db deals with this problem by waiting a bit more for the connection
// to be established.
func TestExhaustConnections(t *testing.T) {
	var err error
	var sess db.Database
	var wg sync.WaitGroup

	if sess, err = db.Open(Adapter, settings); err != nil {
		t.Fatal(err)
	}

	// By default, PostgreSQL accepts 100 connections only.
	for i := 0; i < 500; i++ {
		wg.Add(1)
		t.Logf("Tx %d: Pending", i)
		go func(t *testing.T, wg *sync.WaitGroup, i int) {
			var tx db.Tx
			defer wg.Done()

			start := time.Now()

			// Requesting a new transaction session.
			if tx, err = sess.Transaction(); err != nil {
				t.Fatal(err)
			}

			t.Logf("Tx %d: OK (waiting time: %v)", i, time.Now().Sub(start))

			// Let's suppose that we do some complex stuff and that the transaction
			// lasts 3 seconds.
			time.Sleep(time.Second * 3)

			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}

			t.Logf("Tx %d: Done", i)
		}(t, &wg, i)
	}

	wg.Wait()

	sess.Close()
}
