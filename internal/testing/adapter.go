package ADAPTER

import (
	"database/sql"
	"flag"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"upper.io/db.v2"
)

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

func (item itemWithKey) Constraints() db.Cond {
	return db.Cond{
		"code":    item.Code,
		"user_id": item.UserID,
	}
}

func TestMain(m *testing.M) {
	flag.Parse()

	config()

	if err := tearUp(); err != nil {
		log.Fatal("tearUp", err)
	}

	os.Exit(m.Run())
}

func mustOpen() db.Database {
	sess, err := db.Open(Adapter, settings)
	if err != nil {
		panic(err.Error())
	}
	return sess
}

func TestOpenMustFail(t *testing.T) {
	_, err := db.Open(Adapter, ConnectionURL{})
	assert.Error(t, err)
}

func TestOpenMustSucceed(t *testing.T) {
	sess, err := db.Open(Adapter, settings)
	assert.NoError(t, err)
	assert.NotNil(t, sess)

	err = sess.Close()
	assert.NoError(t, err)
}

func TestTruncateAllCollections(t *testing.T) {
	sess, err := db.Open(Adapter, settings)
	assert.NoError(t, err)
	defer sess.Close()

	collections, err := sess.Collections()
	assert.NoError(t, err)
	assert.True(t, len(collections) > 0)

	for _, name := range collections {
		col := sess.Collection(name)

		if col.Exists() {
			if err = col.Truncate(); err != nil {
				assert.NoError(t, err)
			}
		}
	}
}

func TestExpectCursorError(t *testing.T) {
	sess := mustOpen()
	defer sess.Close()

	artist := sess.Collection("artist")

	res := artist.Find(db.Cond{"id": "X"})
	c, err := res.Count()

	assert.Error(t, err)
	assert.Equal(t, uint64(0), c)
}

func TestAppendToArtistsTable(t *testing.T) {
	sess := mustOpen()
	defer sess.Close()

	artist := sess.Collection("artist")

	itemMap := map[string]string{
		"name": "Ozzie",
	}

	id, err := artist.Append(itemMap)
	assert.NoError(t, err)
	assert.NotNil(t, id)

	if pk, ok := id.(int64); !ok || pk == 0 {
		t.Fatalf("Expecting an ID.")
	}

	// Attempt to append a struct.
	itemStruct := struct {
		Name string `db:"name"`
	}{
		"Flea",
	}

	id, err = artist.Append(itemStruct)
	assert.NoError(t, err)
	assert.NotNil(t, id)

	if pk, ok := id.(int64); !ok || pk == 0 {
		t.Fatalf("Expecting an ID.")
	}

	// Attempt to append a tagged struct.
	itemStruct2 := struct {
		ArtistName string `db:"name"`
	}{
		"Slash",
	}

	id, err = artist.Append(itemStruct2)
	assert.NoError(t, err)
	assert.NotNil(t, id)

	if pk, ok := id.(int64); !ok || pk == 0 {
		t.Fatalf("Expecting an ID.")
	}

	// Attempt to append and update a private key
	itemStruct3 := artistWithInt64Key{
		Name: "Janus",
	}

	_, err = artist.Append(&itemStruct3)
	assert.NoError(t, err)
	assert.NotZero(t, itemStruct3.id)

	// Counting elements, must be exactly 4 elements.
	count, err := artist.Find().Count()
	assert.NoError(t, err)
	assert.Equal(t, uint64(4), count)
}

func TestQueryNonExistentCollection(t *testing.T) {
	sess := mustOpen()
	defer sess.Close()

	count, err := sess.Collection("doesnotexist").Find().Count()
	assert.Error(t, err)
	assert.Zero(t, count)
}

func TestGetOneResult(t *testing.T) {
	sess := mustOpen()
	defer sess.Close()

	artist := sess.Collection("artist")

	// Fetching one struct.
	var someArtist artistType
	err := artist.Find().Limit(1).One(&someArtist)
	assert.NoError(t, err)

	assert.NotZero(t, someArtist.Name)
	assert.NotZero(t, someArtist.ID)

	// Fetching a pointer to a pointer.
	var someArtistObj *artistType
	err = artist.Find().Limit(1).One(&someArtistObj)
	assert.NoError(t, err)
	assert.NotZero(t, someArtist.Name)
	assert.NotZero(t, someArtist.ID)
}

func TestGetResultsOneByOne(t *testing.T) {
	sess := mustOpen()
	defer sess.Close()

	artist := sess.Collection("artist")

	rowMap := map[string]interface{}{}

	res := artist.Find()

	for {
		err := res.Next(&rowMap)

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

	err := res.Close()
	assert.NoError(t, err)

	// Dumping into a tagged struct.
	rowStruct2 := struct {
		Value1 uint64 `db:"id"`
		Value2 string `db:"name"`
	}{}

	res = artist.Find()

	for {
		err := res.Next(&rowStruct2)
		if err == db.ErrNoMoreRows {
			break
		}

		if err == nil {
			assert.NotZero(t, rowStruct2.Value1)
			assert.NotZero(t, rowStruct2.Value2)
		} else {
			t.Fatal(err)
		}
	}

	err = res.Close()
	assert.NoError(t, err)

	// Dumping into a slice of maps.
	allRowsMap := []map[string]interface{}{}

	res = artist.Find()
	err = res.All(&allRowsMap)
	assert.NoError(t, err)
	assert.Equal(t, 4, len(allRowsMap))

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

	assert.Equal(t, 4, len(allRowsStruct))

	for _, singleRowStruct := range allRowsStruct {
		assert.NotZero(t, singleRowStruct.ID)
	}

	// Dumping into a slice of tagged structs.
	allRowsStruct2 := []struct {
		Value1 uint64 `db:"id"`
		Value2 string `db:"name"`
	}{}

	res = artist.Find()

	err = res.All(&allRowsStruct2)
	assert.NoError(t, err)

	assert.Equal(t, 4, len(allRowsStruct2))

	for _, singleRowStruct := range allRowsStruct2 {
		assert.NotZero(t, singleRowStruct.Value1)
	}
}

func TestGetAllResults(t *testing.T) {
	sess := mustOpen()
	defer sess.Close()

	artist := sess.Collection("artist")

	// Fetching all artists into struct
	artists := []artistType{}
	err := artist.Find().All(&artists)
	assert.NoError(t, err)
	assert.NotZero(t, len(artists))

	assert.NotZero(t, artists[0].Name)
	assert.NotZero(t, artists[0].ID)

	// Fetching all artists into struct objects
	artistObjs := []*artistType{}
	err = artist.Find().All(&artistObjs)
	assert.NoError(t, err)
	assert.NotZero(t, len(artistObjs))
	assert.NotZero(t, artistObjs[0].Name)
	assert.NotZero(t, artistObjs[0].ID)
}

func TestInlineStructs(t *testing.T) {
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

	sess := mustOpen()
	defer sess.Close()

	review := sess.Collection("review")

	err := review.Truncate()
	assert.NoError(t, err)

	rec := reviewType{
		PublicationID: 123,
		Details: reviewTypeDetails{
			Name:     "..name..",
			Comments: "..comments..",
		},
	}

	id, err := review.Append(rec)
	assert.NoError(t, err)
	assert.NotZero(t, id.(int64))

	rec.ID = id.(int64)

	var recChk reviewType
	err = review.Find().One(&recChk)
	assert.NoError(t, err)

	assert.NotZero(t, recChk.Details.Name)

	assert.Equal(t, recChk.ID, rec.ID)
	assert.Equal(t, recChk.Details.Name, rec.Details.Name)
}

func TestUpdate(t *testing.T) {
	sess := mustOpen()
	defer sess.Close()

	artist := sess.Collection("artist")

	// Defining destination struct
	value := struct {
		ID   uint64 `db:"id,omitempty"`
		Name string `db:"name"`
	}{}

	// Getting the first artist.
	res := artist.Find(db.Cond{"id !=": 0}).Limit(1)

	err := res.One(&value)
	assert.NoError(t, err)

	// Updating set with a map
	rowMap := map[string]interface{}{
		"name": strings.ToUpper(value.Name),
	}

	err = res.Update(rowMap)
	assert.NoError(t, err)

	// Pulling it again.
	err = res.One(&value)
	assert.NoError(t, err)

	// Verifying.
	assert.Equal(t, value.Name, rowMap["name"])

	// Updating set with a struct
	rowStruct := struct {
		Name string `db:"name"`
	}{strings.ToLower(value.Name)}

	err = res.Update(rowStruct)
	assert.NoError(t, err)

	// Pulling it again.
	err = res.One(&value)
	assert.NoError(t, err)

	// Verifying
	assert.Equal(t, value.Name, rowStruct.Name)

	// Updating set with a tagged struct
	rowStruct2 := struct {
		Value1 string `db:"name"`
	}{"john"}

	err = res.Update(rowStruct2)
	assert.NoError(t, err)

	// Pulling it again.
	err = res.One(&value)
	assert.NoError(t, err)

	// Verifying
	assert.Equal(t, value.Name, rowStruct2.Value1)

	// Updating set with a tagged object
	rowStruct3 := &struct {
		Value1 string `db:"name"`
	}{"anderson"}

	err = res.Update(rowStruct3)
	assert.NoError(t, err)

	// Pulling it again.
	err = res.One(&value)
	assert.NoError(t, err)

	// Verifying
	assert.Equal(t, value.Name, rowStruct3.Value1)
}

func TestFunction(t *testing.T) {
	sess := mustOpen()
	defer sess.Close()

	rowStruct := struct {
		ID   uint64
		Name string
	}{}

	artist := sess.Collection("artist")
	res := artist.Find(db.Cond{"id NOT IN": []int{0, -1}})

	err := res.One(&rowStruct)
	assert.NoError(t, err)

	total, err := res.Count()
	assert.NoError(t, err)
	assert.Equal(t, uint64(4), total)

	// Testing conditions
	res = artist.Find(db.Cond{"id NOT": db.Func("IN", 0, -1)})

	err = res.One(&rowStruct)
	assert.NoError(t, err)

	total, err = res.Count()
	assert.NoError(t, err)
	assert.Equal(t, uint64(4), total)

	// Testing DISTINCT (function)
	res = artist.Find().Select(
		db.Func("DISTINCT", "name"),
	)

	var rowMap map[string]interface{}
	err = res.One(&rowMap)
	assert.NoError(t, err)

	total, err = res.Count()
	assert.NoError(t, err)
	assert.Equal(t, uint64(4), total)

	// Testing DISTINCT (raw)
	res = artist.Find().Select(
		db.Raw("DISTINCT(name)"),
	)

	err = res.One(&rowMap)
	assert.NoError(t, err)

	total, err = res.Count()
	assert.NoError(t, err)
	assert.Equal(t, uint64(4), total)
}

func TestNullableFields(t *testing.T) {
	sess := mustOpen()
	defer sess.Close()

	type testType struct {
		ID              int64           `db:"id,omitempty"`
		NullStringTest  sql.NullString  `db:"_string"`
		NullInt64Test   sql.NullInt64   `db:"_int64"`
		NullFloat64Test sql.NullFloat64 `db:"_float64"`
		NullBoolTest    sql.NullBool    `db:"_bool"`
	}

	col := sess.Collection(`data_types`)

	err := col.Truncate()
	assert.NoError(t, err)

	// Testing insertion of invalid nulls.
	test := testType{
		NullStringTest:  sql.NullString{"", false},
		NullInt64Test:   sql.NullInt64{0, false},
		NullFloat64Test: sql.NullFloat64{0.0, false},
		NullBoolTest:    sql.NullBool{false, false},
	}

	id, err := col.Append(testType{})
	assert.NoError(t, err)

	// Testing fetching of invalid nulls.
	err = col.Find(db.Cond{"id": id}).One(&test)
	assert.NoError(t, err)

	assert.False(t, test.NullInt64Test.Valid)
	assert.False(t, test.NullFloat64Test.Valid)
	assert.False(t, test.NullBoolTest.Valid)

	// Testing insertion of valid nulls.
	test = testType{
		NullStringTest:  sql.NullString{"", true},
		NullInt64Test:   sql.NullInt64{0, true},
		NullFloat64Test: sql.NullFloat64{0.0, true},
		NullBoolTest:    sql.NullBool{false, true},
	}

	id, err = col.Append(test)
	assert.NoError(t, err)

	// Testing fetching of valid nulls.
	err = col.Find(db.Cond{"id": id}).One(&test)
	assert.NoError(t, err)

	assert.True(t, test.NullInt64Test.Valid)
	assert.True(t, test.NullBoolTest.Valid)
	assert.True(t, test.NullStringTest.Valid)
}

func TestGroup(t *testing.T) {
	sess := mustOpen()
	defer sess.Close()

	type statsType struct {
		Numeric int `db:"numeric"`
		Value   int `db:"value"`
	}

	stats := sess.Collection("stats_test")

	err := stats.Truncate()
	assert.NoError(t, err)

	// Adding row append.
	for i := 0; i < 100; i++ {
		numeric, value := rand.Intn(5), rand.Intn(100)
		_, err := stats.Append(statsType{numeric, value})
		assert.NoError(t, err)
	}

	// Testing GROUP BY
	res := stats.Find().Select(
		"numeric",
		db.Raw("COUNT(1) AS counter"),
		db.Raw("SUM(value) AS total"),
	).Group("numeric")

	var results []map[string]interface{}

	err = res.All(&results)
	assert.NoError(t, err)

	assert.Equal(t, 5, len(results))
}

func TestRemove(t *testing.T) {
	sess := mustOpen()
	defer sess.Close()

	artist := sess.Collection("artist")

	res := artist.Find(db.Cond{"id": 1})

	total, err := res.Count()
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), total)

	err = res.Remove()
	assert.NoError(t, err)

	total, err = res.Count()
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), total)
}

func TestCompositeKeys(t *testing.T) {
	sess := mustOpen()
	defer sess.Close()

	compositeKeys := sess.Collection("composite_keys")

	n := rand.Intn(100000)

	item := itemWithKey{
		"ABCDEF",
		strconv.Itoa(n),
		"Some value",
	}

	_, err := compositeKeys.Append(&item)
	assert.NoError(t, err)

	// Using constrainer interface.
	var item2 itemWithKey
	assert.NotEqual(t, item2.SomeVal, item.SomeVal)

	res := compositeKeys.Find(item)
	err = res.One(&item2)
	assert.NoError(t, err)

	assert.Equal(t, item2.SomeVal, item.SomeVal)
}

// Attempts to test database transactions.
func TestTransactionsAndRollback(t *testing.T) {
	sess := mustOpen()
	defer sess.Close()

	// Simple transaction that should not fail.
	tx, err := sess.Transaction()
	assert.NoError(t, err)

	artist := tx.Collection("artist")
	err = artist.Truncate()
	assert.NoError(t, err)

	// Simple transaction
	_, err = artist.Append(artistType{1, "First"})
	assert.NoError(t, err)

	err = tx.Commit()
	assert.NoError(t, err)

	// An attempt to use the same transaction must fail.
	err = tx.Commit()
	assert.Error(t, err)

	err = tx.Close()
	assert.NoError(t, err)

	// Use another transaction.
	tx, err = sess.Transaction()

	artist = tx.Collection("artist")

	_, err = artist.Append(artistType{2, "Second"})
	assert.NoError(t, err)

	// Won't fail.
	_, err = artist.Append(artistType{3, "Third"})
	assert.NoError(t, err)

	// Will fail.
	_, err = artist.Append(artistType{1, "Duplicated"})
	assert.Error(t, err)

	err = tx.Rollback()
	assert.NoError(t, err)

	err = tx.Commit()
	assert.Error(t, err, "Already rolled back.")

	// Let's verify we still have one element.
	artist = sess.Collection("artist")

	count, err := artist.Find().Count()
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), count)

	err = tx.Close()
	assert.NoError(t, err)

	// Attempt to add some rows.
	tx, err = sess.Transaction()
	assert.NoError(t, err)

	artist = tx.Collection("artist")

	// Won't fail.
	_, err = artist.Append(artistType{2, "Second"})
	assert.NoError(t, err)

	// Won't fail.
	_, err = artist.Append(artistType{3, "Third"})
	assert.NoError(t, err)

	// Then rollback for no reason.
	err = tx.Rollback()
	assert.NoError(t, err)

	err = tx.Commit()
	assert.Error(t, err, "Already rolled back.")

	// Let's verify we still have one element.
	artist = sess.Collection("artist")

	count, err = artist.Find().Count()
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), count)

	err = tx.Close()
	assert.NoError(t, err)

	// Attempt to add some rows.
	tx, err = sess.Transaction()
	assert.NoError(t, err)

	artist = tx.Collection("artist")

	// Won't fail.
	_, err = artist.Append(artistType{2, "Second"})
	assert.NoError(t, err)

	// Won't fail.
	_, err = artist.Append(artistType{3, "Third"})
	assert.NoError(t, err)

	err = tx.Commit()
	assert.NoError(t, err)

	err = tx.Rollback()
	assert.Error(t, err, "Already commited")

	// Let's verify we have 3 rows.
	artist = sess.Collection("artist")

	count, err = artist.Find().Count()
	assert.NoError(t, err)
	assert.Equal(t, uint64(3), count)
}

func TestDataTypes(t *testing.T) {
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

	sess := mustOpen()
	defer sess.Close()

	// Getting a pointer to the "data_types" collection.
	dataTypes := sess.Collection("data_types")

	// Removing all data.
	err := dataTypes.Truncate()
	assert.NoError(t, err)

	// Appending our test subject.
	loc, err := time.LoadLocation(testTimeZone)
	assert.NoError(t, err)

	ts := time.Date(2011, 7, 28, 1, 2, 3, 0, loc)                    // timestamp with time zone
	tnz := time.Date(2012, 7, 28, 1, 2, 3, 0, time.FixedZone("", 0)) // timestamp without time zone

	testValues := testValuesStruct{
		1, 1, 1, 1, 1,
		-1, -1, -1, -1, -1,
		1.337, 1.337,
		true,
		"Hello world!",
		ts,
		nil,
		&tnz,
		nil,
		int64(time.Second * time.Duration(7331)),
	}
	id, err := dataTypes.Append(testValues)
	assert.NoError(t, err)
	assert.NotNil(t, id)

	// Defining our set.
	res := dataTypes.Find(db.Cond{"id": id})

	count, err := res.Count()
	assert.NoError(t, err)
	assert.NotZero(t, count)

	// Trying to dump the subject into an empty structure of the same type.
	var item testValuesStruct

	err = res.One(&item)
	assert.NoError(t, err)
	assert.NotNil(t, item.DateD)

	// Copy the default date (this value is set by the database)
	testValues.DateD = item.DateD

	// The original value and the test subject must match.
	assert.Equal(t, testValues, item)
}

func TestOptionTypes(t *testing.T) {
	sess := mustOpen()
	defer sess.Close()

	optionTypes := sess.Collection("option_types")
	err := optionTypes.Truncate()
	assert.NoError(t, err)

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
	assert.NoError(t, err)

	if pk, ok := id.(int64); !ok || pk == 0 {
		t.Fatalf("Expecting an ID.")
	}

	var item1Chk optionType
	err = optionTypes.Find(db.Cond{"id": id}).One(&item1Chk)
	assert.NoError(t, err)

	assert.Equal(t, float64(1), item1Chk.Settings["a"])
	assert.Equal(t, "toronto", item1Chk.Tags[0])

	// Item 1 B
	item1b := &optionType{
		Name:     "Golang",
		Tags:     []string{"love", "it"},
		Settings: map[string]interface{}{"go": 1, "lang": 2},
	}

	id, err = optionTypes.Append(item1b)
	assert.NoError(t, err)

	if pk, ok := id.(int64); !ok || pk == 0 {
		t.Fatalf("Expecting an ID.")
	}

	var item1bChk optionType
	err = optionTypes.Find(db.Cond{"id": id}).One(&item1bChk)
	assert.NoError(t, err)

	assert.Equal(t, float64(1), item1bChk.Settings["go"])
	assert.Equal(t, "love", item1bChk.Tags[0])

	// Item 1 C
	item1c := &optionType{
		Name: "Sup", Tags: []string{}, Settings: map[string]interface{}{},
	}

	id, err = optionTypes.Append(item1c)
	assert.NoError(t, err)

	if pk, ok := id.(int64); !ok || pk == 0 {
		t.Fatalf("Expecting an ID.")
	}

	var item1cChk optionType
	err = optionTypes.Find(db.Cond{"id": id}).One(&item1cChk)
	assert.NoError(t, err)

	assert.Zero(t, len(item1cChk.Tags))
	assert.Zero(t, len(item1cChk.Settings))

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
	assert.NoError(t, err)

	if pk, ok := id.(int64); !ok || pk == 0 {
		t.Fatalf("Expecting an ID.")
	}

	var item2Chk optionType2
	res := optionTypes.Find(db.Cond{"id": id})
	err = res.One(&item2Chk)
	assert.NoError(t, err)

	assert.Equal(t, id.(int64), item2Chk.ID)

	assert.Equal(t, item2Chk.Name, item2.Name)

	assert.Equal(t, item2Chk.Tags[0], item2.Tags[0])
	assert.Equal(t, len(item2Chk.Tags), len(item2.Tags))

	// Update the value
	m := map[string]interface{}{}
	m["lang"] = "javascript"
	m["num"] = 31337
	item2.Settings = &m
	err = res.Update(item2)
	assert.NoError(t, err)

	err = res.One(&item2Chk)
	assert.NoError(t, err)

	assert.Equal(t, float64(31337), (*item2Chk.Settings)["num"].(float64))

	assert.Equal(t, "javascript", (*item2Chk.Settings)["lang"])

	// An option type to pointer string array field
	type optionType3 struct {
		ID       int64                  `db:"id,omitempty"`
		Name     string                 `db:"name"`
		Tags     *[]string              `db:"tags,stringarray"`
		Settings map[string]interface{} `db:"settings,jsonb"`
	}

	item3 := optionType3{
		Name:     "Julia",
		Tags:     nil,
		Settings: map[string]interface{}{"girl": true, "lang": true},
	}

	id, err = optionTypes.Append(item3)
	assert.NoError(t, err)

	if pk, ok := id.(int64); !ok || pk == 0 {
		t.Fatalf("Expecting an ID.")
	}

	var item3Chk optionType2
	err = optionTypes.Find(db.Cond{"id": id}).One(&item3Chk)
	assert.NoError(t, err)
}

func TestOptionTypeJsonbStruct(t *testing.T) {
	sess := mustOpen()
	defer sess.Close()

	optionTypes := sess.Collection("option_types")

	err := optionTypes.Truncate()
	assert.NoError(t, err)

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
	assert.NoError(t, err)

	if pk, ok := id.(int64); !ok || pk == 0 {
		t.Fatalf("Expecting an ID.")
	}

	var item1Chk OptionType
	err = optionTypes.Find(db.Cond{"id": id}).One(&item1Chk)
	assert.NoError(t, err)

	assert.Equal(t, 2, len(item1Chk.Tags))
	assert.Equal(t, "aah", item1Chk.Tags[0])
	assert.Equal(t, "a", item1Chk.Settings.Name)
	assert.Equal(t, int64(123), item1Chk.Settings.Num)
}

func TestExhaustConnectionPool(t *testing.T) {
	sess := mustOpen()
	defer sess.Close()

	var wg sync.WaitGroup
	for i := 0; i < 300; i++ {
		wg.Add(1)
		t.Logf("Tx %d: Pending", i)

		go func(t *testing.T, wg *sync.WaitGroup, i int) {
			var tx db.Tx
			defer wg.Done()

			start := time.Now()

			// Requesting a new transaction session.
			tx, err := sess.Transaction()
			if err != nil {
				t.Fatal(err)
			}

			t.Logf("Tx %d: OK (waiting time: %v)", i, time.Now().Sub(start))

			// Let's suppose that we do some complex stuff and that the transaction
			// lasts 3 seconds.
			time.Sleep(time.Second * 3)

			if err := tx.Close(); err != nil {
				t.Fatal(err)
			}

			t.Logf("Tx %d: Done", i)
		}(t, &wg, i)
	}

	wg.Wait()
}
