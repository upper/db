// Copyright (c) 2012-present The upper.io/db authors. All rights reserved.
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
	"fmt"
	"log"
	"math/rand"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"gopkg.in/mgo.v2/bson"
	"upper.io/db.v3"
)

type artistType struct {
	ID   bson.ObjectId `bson:"_id,omitempty"`
	Name string        `bson:"name"`
}

// Global settings for tests.
var settings = ConnectionURL{
	Database: os.Getenv("DB_NAME"),
	User:     os.Getenv("DB_USERNAME"),
	Password: os.Getenv("DB_PASSWORD"),
	Host:     os.Getenv("DB_HOST") + ":" + os.Getenv("DB_PORT"),
}

var host string

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
}

// Attempts to open an empty datasource.
func TestOpenWithWrongData(t *testing.T) {
	var err error
	var rightSettings, wrongSettings ConnectionURL

	// Attempt to open with safe settings.
	rightSettings = ConnectionURL{
		Database: settings.Database,
		Host:     settings.Host,
		User:     settings.User,
		Password: settings.Password,
	}

	// Attempt to open an empty database.
	if _, err = Open(rightSettings); err != nil {
		// Must fail.
		t.Fatal(err)
	}

	// Attempt to open with wrong password.
	wrongSettings = ConnectionURL{
		Database: settings.Database,
		Host:     settings.Host,
		User:     settings.User,
		Password: "fail",
	}

	if _, err = Open(wrongSettings); err == nil {
		t.Fatalf("Expecting an error.")
	}

	// Attempt to open with wrong database.
	wrongSettings = ConnectionURL{
		Database: "fail",
		Host:     settings.Host,
		User:     settings.User,
		Password: settings.Password,
	}

	if _, err = Open(wrongSettings); err == nil {
		t.Fatalf("Expecting an error.")
	}

	// Attempt to open with wrong username.
	wrongSettings = ConnectionURL{
		Database: settings.Database,
		Host:     settings.Host,
		User:     "fail",
		Password: settings.Password,
	}

	if _, err = Open(wrongSettings); err == nil {
		t.Fatalf("Expecting an error.")
	}
}

// Truncates all collections.
func TestTruncate(t *testing.T) {

	var err error

	// Opening database.
	sess, err := Open(settings)

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
		col := sess.Collection(name)

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
func TestInsert(t *testing.T) {

	var err error
	var id interface{}

	// Opening database.
	sess, err := Open(settings)

	if err != nil {
		t.Fatal(err)
	}

	// We should close the database when it's no longer in use.
	defer sess.Close()

	// Getting a pointer to the "artist" collection.
	artist := sess.Collection("artist")

	// Inserting a map.
	id, err = artist.Insert(map[string]string{
		"name": "Ozzie",
	})

	if err != nil {
		t.Fatalf("Insert(): %s", err.Error())
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

	// Inserting a struct.
	id, err = artist.Insert(struct {
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

	// Inserting a struct (using tags to specify the field name).
	id, err = artist.Insert(struct {
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

	// Inserting a pointer to a struct
	id, err = artist.Insert(&struct {
		ArtistName string `bson:"name"`
	}{
		"Metallica",
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

	// Inserting a pointer to a map
	id, err = artist.Insert(&map[string]string{
		"name": "Freddie",
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

	var total uint64

	// Counting elements, must be exactly 6 elements.
	if total, err = artist.Find().Count(); err != nil {
		t.Fatal(err)
	}

	if total != 5 {
		t.Fatalf("Expecting exactly 5 rows.")
	}
}

func TestGetNonExistentRow_Issue426(t *testing.T) {
	// Opening database.
	sess, err := Open(settings)
	if err != nil {
		t.Fatal(err)
	}

	defer sess.Close()

	artist := sess.Collection("artist")

	var one artistType
	err = artist.Find(db.Cond{"name": "nothing"}).One(&one)

	assert.NotZero(t, err)
	assert.Equal(t, db.ErrNoMoreRows, err)

	var all []artistType
	err = artist.Find(db.Cond{"name": "nothing"}).All(&all)

	assert.Zero(t, err, "All should not return mgo.ErrNotFound")
	assert.Equal(t, 0, len(all))
}

// This test tries to use an empty filter and count how many elements were
// added into the artist collection.
func TestResultCount(t *testing.T) {

	var err error
	var res db.Result

	// Opening database.
	sess, err := Open(settings)

	if err != nil {
		t.Fatal(err)
	}

	defer sess.Close()

	// We should close the database when it's no longer in use.
	artist := sess.Collection("artist")

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

	if sess, err = Open(settings); err != nil {
		t.Fatal(err)
	}

	type statsT struct {
		Numeric int `db:"numeric" bson:"numeric"`
		Value   int `db:"value" bson:"value"`
	}

	defer sess.Close()

	stats = sess.Collection("statsTest")

	// Truncating table.
	stats.Truncate()

	// Adding row append.
	for i := 0; i < 1000; i++ {
		numeric, value := rand.Intn(10), rand.Intn(100)
		if _, err = stats.Insert(statsT{numeric, value}); err != nil {
			t.Fatal(err)
		}
	}

	// db.statsTest.group({key: {numeric: true}, initial: {sum: 0}, reduce: function(doc, prev) { prev.sum += 1}});

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

// Attempts to count all rows in a table that does not exist.
func TestResultNonExistentCount(t *testing.T) {
	sess, err := Open(settings)

	if err != nil {
		t.Fatal(err)
	}

	defer sess.Close()

	total, err := sess.Collection("notartist").Find().Count()

	if err != nil {
		t.Fatal("MongoDB should not care about a non-existent collecton.", err)
	}

	if total != 0 {
		t.Fatal("Counter should be zero")
	}
}

// This test uses and result and tries to fetch items one by one.
func TestResultFetch(t *testing.T) {

	var err error
	var res db.Result

	// Opening database.
	sess, err := Open(settings)

	if err != nil {
		t.Fatal(err)
	}

	// We should close the database when it's no longer in use.
	defer sess.Close()

	artist := sess.Collection("artist")

	// Testing map
	res = artist.Find()

	rowM := map[string]interface{}{}

	for res.Next(&rowM) {
		if rowM["_id"] == nil {
			t.Fatalf("Expecting an ID.")
		}
		if _, ok := rowM["_id"].(bson.ObjectId); ok != true {
			t.Fatalf("Expecting a bson.ObjectId.")
		}

		if rowM["_id"].(bson.ObjectId).Valid() != true {
			t.Fatalf("Expecting a valid bson.ObjectId.")
		}
		if name, ok := rowM["name"].(string); !ok || name == "" {
			t.Fatalf("Expecting a name.")
		}
	}

	res.Close()

	// Testing struct
	rowS := struct {
		ID   bson.ObjectId `bson:"_id"`
		Name string        `bson:"name"`
	}{}

	res = artist.Find()

	for res.Next(&rowS) {
		if rowS.ID.Valid() == false {
			t.Fatalf("Expecting a not null ID.")
		}
		if rowS.Name == "" {
			t.Fatalf("Expecting a name.")
		}
	}

	res.Close()

	// Testing tagged struct
	rowT := struct {
		Value1 bson.ObjectId `bson:"_id"`
		Value2 string        `bson:"name"`
	}{}

	res = artist.Find()

	for res.Next(&rowT) {
		if rowT.Value1.Valid() == false {
			t.Fatalf("Expecting a not null ID.")
		}
		if rowT.Value2 == "" {
			t.Fatalf("Expecting a name.")
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
		if singleRowM["_id"] == nil {
			t.Fatalf("Expecting a not null ID.")
		}
	}

	// Testing Result.All() with a slice of structs.
	res = artist.Find()

	allRowsS := []struct {
		ID   bson.ObjectId `bson:"_id"`
		Name string
	}{}
	err = res.All(&allRowsS)

	if err != nil {
		t.Fatal(err)
	}

	for _, singleRowS := range allRowsS {
		if singleRowS.ID.Valid() == false {
			t.Fatalf("Expecting a not null ID.")
		}
	}

	// Testing Result.All() with a slice of tagged structs.
	res = artist.Find()

	allRowsT := []struct {
		Value1 bson.ObjectId `bson:"_id"`
		Value2 string        `bson:"name"`
	}{}
	err = res.All(&allRowsT)

	if err != nil {
		t.Fatal(err)
	}

	for _, singleRowT := range allRowsT {
		if singleRowT.Value1.Valid() == false {
			t.Fatalf("Expecting a not null ID.")
		}
	}
}

// This test tries to update some previously added rows.
func TestUpdate(t *testing.T) {
	var err error

	// Opening database.
	sess, err := Open(settings)

	if err != nil {
		t.Fatal(err)
	}

	// We should close the database when it's no longer in use.
	defer sess.Close()

	// Getting a pointer to the "artist" collection.
	artist := sess.Collection("artist")

	// Value
	value := struct {
		ID   bson.ObjectId `bson:"_id"`
		Name string
	}{}

	// Getting the first artist.
	res := artist.Find(db.Cond{"_id": db.NotEq(nil)}).Limit(1)

	err = res.One(&value)

	if err != nil {
		t.Fatal(err)
	}

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
		Value1 string `bson:"name"`
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

func TestOperators(t *testing.T) {
	var err error
	var res db.Result

	// Opening database.
	sess, err := Open(settings)

	if err != nil {
		t.Fatal(err)
	}

	// We should close the database when it's no longer in use.
	defer sess.Close()

	// Getting a pointer to the "artist" collection.
	artist := sess.Collection("artist")

	rowS := struct {
		ID   uint64
		Name string
	}{}

	res = artist.Find(db.Cond{"_id": db.NotIn([]int{0, -1})})

	if err = res.One(&rowS); err != nil {
		t.Fatalf("One: %q", err)
	}

	res.Close()
}

// This test tries to remove some previously added rows.
func TestDelete(t *testing.T) {

	var err error

	// Opening database.
	sess, err := Open(settings)

	if err != nil {
		t.Fatal(err)
	}

	// We should close the database when it's no longer in use.
	defer sess.Close()

	// Getting a pointer to the "artist" collection.
	artist := sess.Collection("artist")

	// Getting the first artist.
	res := artist.Find(db.Cond{"_id": db.NotEq(nil)}).Limit(1)

	var first struct {
		ID bson.ObjectId `bson:"_id"`
	}

	err = res.One(&first)

	if err != nil {
		t.Fatal(err)
	}

	res = artist.Find(db.Cond{"_id": db.Eq(first.ID)})

	// Trying to remove the row.
	err = res.Delete()

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
	sess, err := Open(settings)

	if err != nil {
		t.Fatal(err)
	}

	// We should close the database when it's no longer in use.
	defer sess.Close()

	// Getting a pointer to the "data_types" collection.
	dataTypes := sess.Collection("data_types")

	// Inserting our test subject.
	id, err := dataTypes.Insert(testValues)

	if err != nil {
		t.Fatal(err)
	}

	// Trying to get the same subject we added.
	res = dataTypes.Find(db.Cond{"_id": db.Eq(id)})

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

func TestPaginator(t *testing.T) {

	// Opening database.
	sess, err := Open(settings)
	assert.NoError(t, err)

	// We should close the database when it's no longer in use.
	defer sess.Close()

	// Getting a pointer to the "artist" collection.
	artist := sess.Collection("artist")

	err = artist.Truncate()
	assert.NoError(t, err)

	for i := 0; i < 999; i++ {
		_, err = artist.Insert(artistType{
			Name: fmt.Sprintf("artist-%d", i),
		})
		assert.NoError(t, err)
	}

	q := sess.Collection("artist").Find().Paginate(15)
	paginator := q.Paginate(13)

	var zerothPage []artistType
	err = paginator.Page(0).All(&zerothPage)
	assert.NoError(t, err)
	assert.Equal(t, 13, len(zerothPage))

	var secondPage []artistType
	err = paginator.Page(2).All(&secondPage)
	assert.NoError(t, err)
	assert.Equal(t, 13, len(secondPage))

	tp, err := paginator.TotalPages()
	assert.NoError(t, err)
	assert.NotZero(t, tp)
	assert.Equal(t, uint(77), tp)

	ti, err := paginator.TotalEntries()
	assert.NoError(t, err)
	assert.NotZero(t, ti)
	assert.Equal(t, uint64(999), ti)

	var seventySixthPage []artistType
	err = paginator.Page(76).All(&seventySixthPage)
	assert.NoError(t, err)
	assert.Equal(t, 11, len(seventySixthPage))

	var seventySeventhPage []artistType
	err = paginator.Page(77).All(&seventySeventhPage)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(seventySeventhPage))

	var hundredthPage []artistType
	err = paginator.Page(100).All(&hundredthPage)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(hundredthPage))

	for i := uint(0); i < tp; i++ {
		current := paginator.Page(i)

		var items []artistType
		err := current.All(&items)
		if err != nil {
			t.Fatal(err)
		}
		if len(items) < 1 {
			break
		}
		for j := 0; j < len(items); j++ {
			assert.Equal(t, fmt.Sprintf("artist-%d", int64(13*int(i)+j)), items[j].Name)
		}
	}

	paginator = paginator.Cursor("_id")
	{
		current := paginator.Page(0)
		for i := 0; ; i++ {
			var items []artistType
			err := current.All(&items)
			if err != nil {
				t.Fatal(err)
			}
			if len(items) < 1 {
				break
			}

			for j := 0; j < len(items); j++ {
				assert.Equal(t, fmt.Sprintf("artist-%d", int64(13*int(i)+j)), items[j].Name)
			}
			current = current.NextPage(items[len(items)-1].ID)
		}
	}

	{
		log.Printf("Page 76")
		current := paginator.Page(76)
		for i := 76; ; i-- {
			var items []artistType

			err := current.All(&items)
			assert.NoError(t, err)

			if len(items) < 1 {
				assert.Equal(t, 0, len(items))
				break
			}
			for j := 0; j < len(items); j++ {
				assert.Equal(t, fmt.Sprintf("artist-%d", 13*int(i)+j), items[j].Name)
			}

			current = current.PrevPage(items[0].ID)
		}
	}

	{
		resultPaginator := sess.Collection("artist").Find().Paginate(15)

		count, err := resultPaginator.TotalPages()
		assert.Equal(t, uint(67), count)
		assert.NoError(t, err)

		var items []artistType
		err = resultPaginator.Page(5).All(&items)
		assert.NoError(t, err)

		for j := 0; j < len(items); j++ {
			assert.Equal(t, fmt.Sprintf("artist-%d", 15*5+j), items[j].Name)
		}

		resultPaginator = resultPaginator.Cursor("_id").Page(0)
		for i := 0; ; i++ {
			var items []artistType

			err = resultPaginator.All(&items)
			assert.NoError(t, err)

			if len(items) < 1 {
				break
			}

			for j := 0; j < len(items); j++ {
				assert.Equal(t, fmt.Sprintf("artist-%d", 15*i+j), items[j].Name)
			}
			resultPaginator = resultPaginator.NextPage(items[len(items)-1].ID)
		}

		resultPaginator = resultPaginator.Cursor("_id").Page(66)
		for i := 66; ; i-- {
			var items []artistType

			err = resultPaginator.All(&items)
			assert.NoError(t, err)

			if len(items) < 1 {
				break
			}

			for j := 0; j < len(items); j++ {
				assert.Equal(t, fmt.Sprintf("artist-%d", 15*i+j), items[j].Name)
			}
			resultPaginator = resultPaginator.PrevPage(items[0].ID)
		}
	}
}
