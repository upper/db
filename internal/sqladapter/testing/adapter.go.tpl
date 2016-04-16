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

type itemWithCompoundKey struct {
	Code    string `db:"code"`
	UserID  string `db:"user_id"`
	SomeVal string `db:"some_val"`
}

func TestMain(m *testing.M) {
	flag.Parse()

	if err := tearUp(); err != nil {
		log.Fatal("tearUp", err)
	}

	os.Exit(m.Run())
}

func mustOpen() Database {
	sess, err := Open(settings)
	if err != nil {
		panic(err.Error())
	}
	return sess
}

func TestOpenMustFail(t *testing.T) {
	_, err := Open(ConnectionURL{})
	assert.Error(t, err)
}

func TestOpenMustSucceed(t *testing.T) {
	sess, err := Open(settings)
	assert.NoError(t, err)
	assert.NotNil(t, sess)

	err = sess.Close()
	assert.NoError(t, err)
}

func TestTruncateAllCollections(t *testing.T) {
	sess, err := Open(settings)
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

	var cond db.Cond

	switch Adapter {
	case "ql":
		cond = db.Cond{"id()": -1}
	default:
		cond = db.Cond{"id": 0}
	}

	res := artist.Find(cond)
	c, err := res.Count()
	assert.Equal(t, uint64(0), c)
	assert.NoError(t, err)

	var item map[string]interface{}
	err = res.One(&item)
	assert.Error(t, err)
}

func TestInsertReturning(t *testing.T) {
	sess := mustOpen()
	defer sess.Close()

	artist := sess.Collection("artist")

	err := artist.Truncate()
	assert.NoError(t, err)

	itemMap := map[string]string{
		"name": "Ozzie",
	}
	assert.Zero(t, itemMap["id"], "Must be zero before inserting")
	err = artist.InsertReturning(&itemMap)
	assert.NoError(t, err)
	assert.NotZero(t, itemMap["id"], "Must not be zero after inserting")

	itemStruct := struct {
		ID int `db:"id,omitempty"`
		Name string `db:"name"`
	}{
		0,
		"Flea",
	}
	assert.Zero(t, itemStruct.ID, "Must be zero before inserting")
	err = artist.InsertReturning(&itemStruct)
	assert.NoError(t, err)
	assert.NotZero(t, itemStruct.ID, "Must not be zero after inserting")

	count, err := artist.Find().Count()
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), count, "Expecting 2 elements")

	itemStruct2 := struct {
		ID int `db:"id,omitempty"`
		Name string `db:"name"`
	}{
		0,
		"Slash",
	}
	assert.Zero(t, itemStruct2.ID, "Must be zero before inserting")
	err = artist.InsertReturning(itemStruct2)
	assert.Error(t, err, "Should not happen, using a pointer should be enforced")
	assert.Zero(t, itemStruct2.ID, "Must still be zero because there was no insertion")

	itemMap2 := map[string]string{
		"name": "Janus",
	}
	assert.Zero(t, itemMap2["id"], "Must be zero before inserting")
	err = artist.InsertReturning(itemMap2)
	assert.Error(t, err, "Should not happen, using a pointer should be enforced")
	assert.Zero(t, itemMap2["id"], "Must still be zero because there was no insertion")

	// Counting elements, must be exactly 2 elements.
	count, err = artist.Find().Count()
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), count, "Expecting 2 elements")
}

func TestInsertReturningWithinTransaction(t *testing.T) {
	sess := mustOpen()
	defer sess.Close()

	err := sess.Collection("artist").Truncate()
	assert.NoError(t, err)

	tx, err := sess.Transaction()
	assert.NoError(t, err)

	artist := tx.Collection("artist")

	itemMap := map[string]string{
		"name": "Ozzie",
	}
	assert.Zero(t, itemMap["id"], "Must be zero before inserting")
	err = artist.InsertReturning(&itemMap)
	assert.NoError(t, err)
	assert.NotZero(t, itemMap["id"], "Must not be zero after inserting")

	itemStruct := struct {
		ID int `db:"id,omitempty"`
		Name string `db:"name"`
	}{
		0,
		"Flea",
	}
	assert.Zero(t, itemStruct.ID, "Must be zero before inserting")
	err = artist.InsertReturning(&itemStruct)
	assert.NoError(t, err)
	assert.NotZero(t, itemStruct.ID, "Must not be zero after inserting")

	count, err := artist.Find().Count()
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), count, "Expecting 2 elements")

	itemStruct2 := struct {
		ID int `db:"id,omitempty"`
		Name string `db:"name"`
	}{
		0,
		"Slash",
	}
	assert.Zero(t, itemStruct2.ID, "Must be zero before inserting")
	err = artist.InsertReturning(itemStruct2)
	assert.Error(t, err, "Should not happen, using a pointer should be enforced")
	assert.Zero(t, itemStruct2.ID, "Must still be zero because there was no insertion")

	itemMap2 := map[string]string{
		"name": "Janus",
	}
	assert.Zero(t, itemMap2["id"], "Must be zero before inserting")
	err = artist.InsertReturning(itemMap2)
	assert.Error(t, err, "Should not happen, using a pointer should be enforced")
	assert.Zero(t, itemMap2["id"], "Must still be zero because there was no insertion")

	// Counting elements, must be exactly 2 elements.
	count, err = artist.Find().Count()
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), count, "Expecting 2 elements")

	// Rolling back everything
	err = tx.Rollback()
	assert.NoError(t, err)

	// Expecting no elements.
	count, err = sess.Collection("artist").Find().Count()
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), count, "Expecting 0 elements, everything was rolled back!")
}

func TestInsertIntoArtistsTable(t *testing.T) {
	sess := mustOpen()
	defer sess.Close()

	artist := sess.Collection("artist")

	err := artist.Truncate()
	assert.NoError(t, err)

	itemMap := map[string]string{
		"name": "Ozzie",
	}

	id, err := artist.Insert(itemMap)
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

	id, err = artist.Insert(itemStruct)
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

	id, err = artist.Insert(itemStruct2)
	assert.NoError(t, err)
	assert.NotNil(t, id)

	if pk, ok := id.(int64); !ok || pk == 0 {
		t.Fatalf("Expecting an ID.")
	}

	// Attempt to append and update a private key
	itemStruct3 := artistType{
		Name: "Janus",
	}

	id, err = artist.Insert(&itemStruct3)
	assert.NoError(t, err)
	if Adapter != "ql" {
		assert.NotZero(t, id)
	}

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
	if Adapter != "ql" {
		assert.NotZero(t, someArtist.ID)
	}

	// Fetching a pointer to a pointer.
	var someArtistObj *artistType
	err = artist.Find().Limit(1).One(&someArtistObj)
	assert.NoError(t, err)
	assert.NotZero(t, someArtist.Name)
	if Adapter != "ql" {
		assert.NotZero(t, someArtist.ID)
	}
}

func TestGetResultsOneByOne(t *testing.T) {
	sess := mustOpen()
	defer sess.Close()

	artist := sess.Collection("artist")

	rowMap := map[string]interface{}{}

	res := artist.Find()

	if Adapter == "ql" {
		res = res.Select("id() as id", "name")
	}

	for {
		err := res.Next(&rowMap)
		if err == db.ErrNoMoreRows {
			break
		}

		if err != nil {
			t.Fatal(err)
		}

		assert.NotZero(t, rowMap["id"])
		assert.NotZero(t, rowMap["name"])
	}

	err := res.Close()
	assert.NoError(t, err)

	// Dumping into a tagged struct.
	rowStruct2 := struct {
		Value1 int64 `db:"id"`
		Value2 string `db:"name"`
	}{}

	res = artist.Find()

	if Adapter == "ql" {
		res = res.Select("id() as id", "name")
	}

	for {
		err := res.Next(&rowStruct2)
		if err == db.ErrNoMoreRows {
			break
		}

		if err != nil {
			t.Fatal(err)
		}

		assert.NotZero(t, rowStruct2.Value1)
		assert.NotZero(t, rowStruct2.Value2)
	}

	err = res.Close()
	assert.NoError(t, err)

	// Dumping into a slice of maps.
	allRowsMap := []map[string]interface{}{}

	res = artist.Find()

	if Adapter == "ql" {
		res.Select("id() as id")
	}

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
		ID   int64 `db:"id,omitempty"`
		Name string `db:"name"`
	}{}

	res = artist.Find()

	if Adapter == "ql" {
		res.Select("id() as id")
	}

	if err = res.All(&allRowsStruct); err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, 4, len(allRowsStruct))

	for _, singleRowStruct := range allRowsStruct {
		assert.NotZero(t, singleRowStruct.ID)
	}

	// Dumping into a slice of tagged structs.
	allRowsStruct2 := []struct {
		Value1 int64 `db:"id"`
		Value2 string `db:"name"`
	}{}

	res = artist.Find()

	if Adapter == "ql" {
		res.Select("id() as id", "name")
	}

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

	res := artist.Find()
	if Adapter == "ql" {
		res.Select("id() as id", "name")
	}

	err := res.All(&artists)
	assert.NoError(t, err)
	assert.NotZero(t, len(artists))

	assert.NotZero(t, artists[0].Name)
	assert.NotZero(t, artists[0].ID)

	// Fetching all artists into struct objects
	artistObjs := []*artistType{}
	res = artist.Find()
	if Adapter == "ql" {
		res.Select("id() as id", "name")
	}
	err = res.All(&artistObjs)
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

	if Adapter == "postgresql" {
		rec.Details.Created = time.Date(2016, time.January, 1, 2, 3, 4, 0, time.FixedZone("", 0))
	} else {
		rec.Details.Created = time.Date(2016, time.January, 1, 2, 3, 4, 0, time.UTC)
	}

	id, err := review.Insert(rec)
	assert.NoError(t, err)
	assert.NotZero(t, id.(int64))

	rec.ID = id.(int64)

	var recChk reviewType
	res := review.Find()
	if Adapter == "ql" {
		res.Select("id() as id", "publication_id", "comments", "name", "created")
	}
	err = res.One(&recChk)
	assert.NoError(t, err)
	log.Printf("rec: %#v", rec.Details.Created)
	log.Printf("recChj: %#v", recChk.Details.Created)

	assert.Equal(t, rec, recChk)
}

func TestUpdate(t *testing.T) {
	sess := mustOpen()
	defer sess.Close()

	artist := sess.Collection("artist")

	// Defining destination struct
	value := struct {
		ID   int64 `db:"id,omitempty"`
		Name string `db:"name"`
	}{}

	// Getting the first artist.
	cond := db.Cond{"id !=": 0}
	if Adapter == "ql" {
		cond = db.Cond{"id() !=": 0}
	}
	res := artist.Find(cond).Limit(1)

	err := res.One(&value)
	assert.NoError(t, err)

	res = artist.Find(value.ID)

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
		ID   int64
		Name string
	}{}

	artist := sess.Collection("artist")
	cond := db.Cond{"id NOT IN": []int{0, -1}}
	if Adapter == "ql" {
		cond = db.Cond{"id() NOT IN": []int{0, -1}}
	}
	res := artist.Find(cond)

	err := res.One(&rowStruct)
	assert.NoError(t, err)

	total, err := res.Count()
	assert.NoError(t, err)
	assert.Equal(t, uint64(4), total)

	// Testing conditions
	cond = db.Cond{"id NOT": db.Func("IN", 0, -1)}
	if Adapter == "ql" {
		cond = db.Cond{"id() NOT": db.Func("IN", 0, -1)}
	}
	res = artist.Find(cond)

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

	id, err := col.Insert(testType{})
	assert.NoError(t, err)

	// Testing fetching of invalid nulls.
	cond := db.Cond{"id": id}
	if Adapter == "ql" {
		cond = db.Cond{"id()": id}
	}
	err = col.Find(cond).One(&test)
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

	id, err = col.Insert(test)
	assert.NoError(t, err)

	// Testing fetching of valid nulls.
	cond = db.Cond{"id": id}
	if Adapter == "ql" {
		cond = db.Cond{"id()": id}
	}
	err = col.Find(cond).One(&test)
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
		_, err := stats.Insert(statsType{numeric, value})
		assert.NoError(t, err)
	}

	// Testing GROUP BY
	res := stats.Find().Select(
		"numeric",
		db.Raw("count(1) AS counter"),
		db.Raw("sum(value) AS total"),
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
	res := artist.Find()

	total, err := res.Count()
	assert.NoError(t, err)
	assert.Equal(t, uint64(4), total)

	err = res.Remove()
	assert.NoError(t, err)

	total, err = res.Count()
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), total)
}

func TestCompositeKeys(t *testing.T) {
	if Adapter == "ql" {
		t.Logf("Unsupported, skipped")
		return
	}

	sess := mustOpen()
	defer sess.Close()

	compositeKeys := sess.Collection("composite_keys")

	n := rand.Intn(100000)

	item := itemWithCompoundKey{
		"ABCDEF",
		strconv.Itoa(n),
		"Some value",
	}

	id, err := compositeKeys.Insert(&item)
	assert.NoError(t, err)
	assert.NotZero(t, id)

	var item2 itemWithCompoundKey
	assert.NotEqual(t, item2.SomeVal, item.SomeVal)

	// Finding by ID
	err = compositeKeys.Find(id).One(&item2)
	assert.NoError(t, err)

	assert.Equal(t, item2.SomeVal, item.SomeVal)
}

// Attempts to test database transactions.
func TestTransactionsAndRollback(t *testing.T) {
	if Adapter == "ql" {
		t.Logf("Skipped.")
		return
	}

	sess := mustOpen()
	defer sess.Close()

	// Simple transaction that should not fail.
	tx, err := sess.Transaction()
	assert.NoError(t, err)

	artist := tx.Collection("artist")
	err = artist.Truncate()
	assert.NoError(t, err)

	// Simple transaction
	_, err = artist.Insert(artistType{1, "First"})
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

	_, err = artist.Insert(artistType{2, "Second"})
	assert.NoError(t, err)

	// Won't fail.
	_, err = artist.Insert(artistType{3, "Third"})
	assert.NoError(t, err)

	// Will fail.
	_, err = artist.Insert(artistType{1, "Duplicated"})
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
	_, err = artist.Insert(artistType{2, "Second"})
	assert.NoError(t, err)

	// Won't fail.
	_, err = artist.Insert(artistType{3, "Third"})
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
	_, err = artist.Insert(artistType{2, "Second"})
	assert.NoError(t, err)

	// Won't fail.
	_, err = artist.Insert(artistType{3, "Third"})
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
	if Adapter == "ql" {
		t.Logf("Skipped.")
		return
	}

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

	// Inserting our test subject.
	loc, err := time.LoadLocation(testTimeZone)
	assert.NoError(t, err)

	ts := time.Date(2011, 7, 28, 1, 2, 3, 0, loc) // timestamp with time zone

	var tnz time.Time
	if Adapter == "postgresql" {
		tnz = time.Date(2012, 7, 28, 1, 2, 3, 0, time.FixedZone("", 0)) // timestamp without time zone
	} else {
		tnz = time.Date(2012, 7, 28, 1, 2, 3, 0, time.UTC) // timestamp without time zone
	}

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
	id, err := dataTypes.Insert(testValues)
	assert.NoError(t, err)
	assert.NotNil(t, id)

	// Defining our set.
	cond := db.Cond{"id": id}
	if Adapter == "ql" {
		cond = db.Cond{"id()": id}
	}
	res := dataTypes.Find(cond)

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
	item.Date = item.Date.In(loc)

	// The original value and the test subject must match.
	assert.Equal(t, testValues, item)
}

func TestBuilder(t *testing.T) {
	sess := mustOpen()
	defer sess.Close()

	var all []map[string]interface{}

	iter := sess.Builder().SelectAllFrom("artist").Iterator()
	err := iter.All(&all)

	assert.NoError(t, err)
	assert.NotZero(t, all)
}

func TestExhaustConnectionPool(t *testing.T) {
	if Adapter == "ql" {
		t.Logf("Skipped.")
		return
	}

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

			// Let's suppose that we do a bunch of complex stuff and that the
			// transaction lasts 3 seconds.
			time.Sleep(time.Second * 3)

			if err := tx.Close(); err != nil {
				t.Fatal(err)
			}

			t.Logf("Tx %d: Done", i)
		}(t, &wg, i)
	}

	wg.Wait()
}
