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

package ql

// In order to execute these tests you must initialize the database first:
//
// cd _dumps
// make
// cd ..
// go test

import (
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"database/sql"

	"upper.io/db.v2"
)

const (
	databaseName = `_dumps/test.db`
)

const (
	testTimeZone = "Canada/Eastern"
)

var settings = ConnectionURL{
	Database: databaseName,
}

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

type itemWithKey struct {
	ID      int64  `db:",omitempty"`
	SomeVal string `db:"some_val"`
}

func (item itemWithKey) Constraints() db.Cond {
	cond := db.Cond{
		"id()": item.ID,
	}
	return cond
}

/*
func (item *itemWithKey) SetID(keys map[string]interface{}) error {
	if len(keys) == 1 {
		item.ID = keys["id"].(int64)
		return nil
	}
	return errors.New(`Expecting exactly two keys.`)
}
*/

var testValues testValuesStruct

func init() {
	loc, err := time.LoadLocation(testTimeZone)

	if err != nil {
		panic(err.Error())
	}

	t := time.Date(2011, 7, 28, 1, 2, 3, 0, loc)
	tnz := time.Date(2012, 7, 28, 1, 2, 3, 0, time.UTC)

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

	// Append to append a tagged struct.
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

	// Counting elements, must be exactly 3 elements.
	if total, err = artist.Find().Count(); err != nil {
		t.Fatal(err)
	}

	if total != 3 {
		t.Fatalf("Expecting exactly 3 rows.")
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
		db.Raw(`count(1) AS counter`),
		db.Raw(`sum(value) AS total`),
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

	res = artist.Find().Select("id() AS id", "name")

	for {
		err = res.Next(&rowMap)

		if err == db.ErrNoMoreRows {
			break
		}

		if err == nil {
			if pk, ok := rowMap["id"].(int64); !ok || pk == 0 {
				t.Fatalf("Expecting a not null ID.")
			}
			if name, ok := rowMap["name"].([]byte); !ok || string(name) == "" {
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

	res = artist.Find().Select("id() AS id", "name")

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

	res = artist.Find().Select("id() AS id", "name")
	if err = res.All(&allRowsMap); err != nil {
		t.Fatal(err)
	}

	if len(allRowsMap) != 3 {
		t.Fatalf("Expecting 3 items.")
	}

	for _, singleRowMap := range allRowsMap {
		if pk, ok := singleRowMap["id"].(int64); !ok || pk == 0 {
			t.Fatalf("Expecting a not null ID.")
		}
	}

	// Dumping into a slice of structs.
	allRowsStruct := []struct {
		ID   uint64 `db:"id"`
		Name string `db:"name"`
	}{}

	res = artist.Find().Select("id() AS id", "name")
	if err = res.All(&allRowsStruct); err != nil {
		t.Fatal(err)
	}

	if len(allRowsStruct) != 3 {
		t.Fatalf("Expecting 3 items.")
	}

	for _, singleRowStruct := range allRowsStruct {
		if singleRowStruct.ID == 0 {
			t.Fatalf("Expecting a not null ID.")
		}
	}

	// Dumping into an slice of tagged structs.
	allRowsStruct2 := []struct {
		Value1 uint64 `db:"id"`
		Value2 string `db:"name"`
	}{}

	res = artist.Find().Select("id() AS id", "name")

	if err = res.All(&allRowsStruct2); err != nil {
		t.Fatal(err)
	}

	if len(allRowsStruct2) != 3 {
		t.Fatalf("Expecting 3 items.")
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
	res := artist.Find(db.Cond{"id() !=": 0}).Limit(1)

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

	res = artist.Find(db.Cond{"id() NOT IN": []int{0, -1}})

	if err = res.One(&rowStruct); err != nil {
		t.Fatal(err)
	}

	if total, err = res.Count(); err != nil {
		t.Fatal(err)
	}

	if total != 3 {
		t.Fatalf("Expecting 3 items.")
	}

	// Testing conditions
	res = artist.Find(db.Cond{"id() NOT": db.Func("IN", 0, -1)})

	if err = res.One(&rowStruct); err != nil {
		t.Fatal(err)
	}

	if total, err = res.Count(); err != nil {
		t.Fatal(err)
	}

	if total != 3 {
		t.Fatalf("Expecting 3 items.")
	}

	// Testing DISTINCT (function)
	res = artist.Find().Select(
		db.Func(`DISTINCT`, `name`),
	)

	if err = res.One(&rowMap); err != nil {
		t.Fatal(err)
	}

	if total, err = res.Count(); err != nil {
		t.Fatal(err)
	}

	if total != 3 {
		t.Fatalf("Expecting 3 items.")
	}

	// Testing DISTINCT (raw)
	res = artist.Find().Select(
		db.Raw(`DISTINCT(name)`),
	)

	if err = res.One(&rowMap); err != nil {
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
	res = artist.Find(db.Cond{"id()": 1})

	// Trying to remove the row.
	if err = res.Remove(); err != nil {
		t.Fatal(err)
	}
}

func TestTransactionsAndRollback(t *testing.T) {
	var sess db.Database
	var err error

	type artistT struct {
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
	if _, err = artist.Append(artistT{Name: "First"}); err != nil {
		t.Fatal(err)
	}

	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// Attempt to use the same transaction should fail.
	if _, err = tx.Collection("artist"); err == nil {
		t.Fatalf("Illegal, transaction has already been commited.")
	}

	tx.Close()

	// Use another transaction.
	if tx, err = sess.Transaction(); err != nil {
		t.Fatal(err)
	}

	if artist, err = tx.Collection("artist"); err != nil {
		t.Fatal(err)
	}

	// Won't fail.
	if _, err = artist.Append(artistT{Name: "Second"}); err != nil {
		t.Fatal(err)
	}

	// Won't fail.
	if _, err = artist.Append(artistT{Name: "Third"}); err != nil {
		t.Fatal(err)
	}

	// Will fail.
	//if _, err = artist.Append(artistT{1, "Duplicated"}); err == nil {
	//	t.Fatal("Should have failed, as we have already inserted ID 1.")
	//}

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

	tx.Close()

	// Attempt to add some rows.
	if tx, err = sess.Transaction(); err != nil {
		t.Fatal(err)
	}

	if artist, err = tx.Collection("artist"); err != nil {
		t.Fatal(err)
	}

	// Won't fail.
	if _, err = artist.Append(artistT{Name: "Second"}); err != nil {
		t.Fatal(err)
	}

	// Won't fail.
	if _, err = artist.Append(artistT{Name: "Third"}); err != nil {
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

	tx.Close()

	// Attempt to add some rows.
	if tx, err = sess.Transaction(); err != nil {
		t.Fatal(err)
	}

	if artist, err = tx.Collection("artist"); err != nil {
		t.Fatal(err)
	}

	// Won't fail.
	if _, err = artist.Append(artistT{Name: "Second"}); err != nil {
		t.Fatal(err)
	}

	// Won't fail.
	if _, err = artist.Append(artistT{Name: "Third"}); err != nil {
		t.Fatal(err)
	}

	// Won't fail
	sqlTx := tx.Driver().(*sql.Tx)
	if _, err = sqlTx.Exec(`INSERT INTO artist (name) VALUES($1)`, "Fourth"); err != nil {
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

// QL: Does not support composite keys, so we're testing db.Constrainer and
// db.IDSetter interface.
func TestCompositeKeys(t *testing.T) {
	var err error
	var id interface{}
	var sess db.Database
	var compositeKeys db.Collection

	if sess, err = db.Open(Adapter, settings); err != nil {
		t.Fatal(err)
	}

	defer sess.Close()

	if compositeKeys, err = sess.Collection("composite_keys"); err != nil {
		t.Fatal(err)
	}

	//n := rand.Intn(100000)

	item := itemWithKey{
		// 		"ABCDEF",
		// 		strconv.Itoa(n),
		0,
		"Some value",
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
	res := compositeKeys.Find(itemWithKey{ID: id.(int64)})

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

/*
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
	res = dataTypes.Find(db.Cond{"id()": id})

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
*/

// TestExhaustConnections simulates a "too many connections" situation
// triggered by opening more transactions than available connections.
// upper.io/db.v2 deals with this problem by waiting a bit more for the connection
// to be established.
func TestExhaustConnections(t *testing.T) {
	var err error
	var sess db.Database
	var wg sync.WaitGroup

	originalFileOpenCount := fileOpenCount

	if sess, err = db.Open(Adapter, settings); err != nil {
		t.Fatal(err)
	}

	// Putting less stress into QL. See https://github.com/cznic/ql/issues/107
	for i := 0; i < 5; i++ {
		wg.Add(1)
		t.Logf("Tx %d: Pending", i)
		go func(t *testing.T, wg *sync.WaitGroup, i int) {
			var tx db.Tx
			defer wg.Done()

			start := time.Now()

			// Requesting a new transaction session.
			if tx, err = sess.Transaction(); err != nil {
				panic(err.Error())
			}

			t.Logf("Tx %d: OK (waiting time: %v)", i, time.Now().Sub(start))

			// We have to use a shorten time here.
			time.Sleep(time.Millisecond * 500)

			if err := tx.Close(); err != nil {
				panic(err.Error())
			}

			t.Logf("Tx %d: Done", i)
		}(t, &wg, i)
	}

	wg.Wait()

	sess.Close()

	if fileOpenCount != originalFileOpenCount {
		t.Fatalf("File open count must be %d.", originalFileOpenCount)
	}
}
