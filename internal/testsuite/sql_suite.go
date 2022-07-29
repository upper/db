package testsuite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	detectrace "github.com/ipfs/go-detect-race"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
	db "github.com/upper/db/v4"
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

type customType struct {
	Val []byte
}

type artistWithCustomType struct {
	Custom customType `db:"name"`
}

func (f customType) String() string {
	return fmt.Sprintf("foo: %s", string(f.Val))
}

func (f customType) MarshalDB() (interface{}, error) {
	return f.String(), nil
}

func (f *customType) UnmarshalDB(in interface{}) error {
	switch t := in.(type) {
	case []byte:
		f.Val = t
	case string:
		f.Val = []byte(t)
	}
	return nil
}

var (
	_ = db.Marshaler(&customType{})
	_ = db.Unmarshaler(&customType{})
)

type SQLTestSuite struct {
	suite.Suite

	Helper
}

func (s *SQLTestSuite) AfterTest(suiteName, testName string) {
	err := s.TearDown()
	s.NoError(err)
}

func (s *SQLTestSuite) BeforeTest(suiteName, testName string) {
	err := s.TearUp()
	s.NoError(err)

	sess := s.Session()

	// Creating test data
	artist := sess.Collection("artist")

	artistNames := []string{"Ozzie", "Flea", "Slash", "Chrono"}
	for _, artistName := range artistNames {
		_, err := artist.Insert(map[string]string{
			"name": artistName,
		})
		s.NoError(err)
	}
}

func (s *SQLTestSuite) TestPreparedStatementsCache() {
	sess := s.Session()

	sess.SetPreparedStatementCache(true)
	defer sess.SetPreparedStatementCache(false)

	var tMu sync.Mutex
	tFatal := func(err error) {
		tMu.Lock()
		defer tMu.Unlock()

		s.T().Errorf("tmu: %v", err)
	}

	// This limit was chosen because, by default, MySQL accepts 16k statements
	// and dies. See https://github.com/upper/db/issues/287
	limit := 20000

	if detectrace.WithRace() {
		// When running this test under the Go race detector we quickly reach the limit
		// of 8128 alive goroutines it can handle, so we set it to a safer number.
		//
		// Note that in order to fully stress this feature you'll have to run this
		// test without the race detector.
		limit = 100
	}

	var wg sync.WaitGroup

	for i := 0; i < limit; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			// This query is different on each iteration and generates a new
			// prepared statement everytime it's called.
			res := sess.Collection("artist").Find().Select(db.Raw(fmt.Sprintf("count(%d) AS c", i)))

			var count map[string]uint64
			err := res.One(&count)
			if err != nil {
				tFatal(err)
			}
		}(i)
	}
	wg.Wait()

	// Concurrent Insert can open many connections on MySQL / PostgreSQL, this
	// sets a limit on them.
	sess.SetMaxOpenConns(90)

	switch s.Adapter() {
	case "ql":
		limit = 1000
	}

	for i := 0; i < limit; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// The same prepared query on every iteration.
			_, err := sess.Collection("artist").Insert(artistType{
				Name: fmt.Sprintf("artist-%d", i),
			})
			if err != nil {
				tFatal(err)
			}
		}(i)
	}
	wg.Wait()

	// Insert returning creates a transaction.
	for i := 0; i < limit; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// The same prepared query on every iteration.
			artist := artistType{
				Name: fmt.Sprintf("artist-%d", i),
			}
			err := sess.Collection("artist").InsertReturning(&artist)
			if err != nil {
				tFatal(err)
			}
		}(i)
	}
	wg.Wait()

	// Removing the limit.
	sess.SetMaxOpenConns(0)
}

func (s *SQLTestSuite) TestTruncateAllCollections() {
	sess := s.Session()

	collections, err := sess.Collections()
	s.NoError(err)
	s.True(len(collections) > 0)

	for _, col := range collections {
		if ok, _ := col.Exists(); ok {
			if err = col.Truncate(); err != nil {
				s.NoError(err)
			}
		}
	}
}

func (s *SQLTestSuite) TestQueryLogger() {
	logLevel := db.LC().Level()

	db.LC().SetLogger(logrus.New())
	db.LC().SetLevel(db.LogLevelDebug)

	defer func() {
		db.LC().SetLogger(nil)
		db.LC().SetLevel(logLevel)
	}()

	sess := s.Session()

	_, err := sess.Collection("artist").Find().Count()
	s.Equal(nil, err)

	_, err = sess.Collection("artist_x").Find().Count()
	s.NotEqual(nil, err)
}

func (s *SQLTestSuite) TestExpectCursorError() {
	sess := s.Session()

	artist := sess.Collection("artist")

	res := artist.Find(-1)
	c, err := res.Count()
	s.Equal(uint64(0), c)
	s.NoError(err)

	var item map[string]interface{}
	err = res.One(&item)
	s.Error(err)
}

func (s *SQLTestSuite) TestInsertDefault() {
	if s.Adapter() == "ql" {
		s.T().Skip("Currently not supported.")
	}

	sess := s.Session()

	artist := sess.Collection("artist")

	err := artist.Truncate()
	s.NoError(err)

	id, err := artist.Insert(&artistType{})
	s.NoError(err)
	s.NotNil(id)

	err = artist.Truncate()
	s.NoError(err)

	id, err = artist.Insert(nil)
	s.NoError(err)
	s.NotNil(id)
}

func (s *SQLTestSuite) TestInsertReturning() {
	sess := s.Session()

	artist := sess.Collection("artist")

	err := artist.Truncate()
	s.NoError(err)

	itemMap := map[string]string{
		"name": "Ozzie",
	}
	s.Zero(itemMap["id"], "Must be zero before inserting")
	err = artist.InsertReturning(&itemMap)
	s.NoError(err)
	s.NotZero(itemMap["id"], "Must not be zero after inserting")

	itemStruct := struct {
		ID   int    `db:"id,omitempty"`
		Name string `db:"name"`
	}{
		0,
		"Flea",
	}
	s.Zero(itemStruct.ID, "Must be zero before inserting")
	err = artist.InsertReturning(&itemStruct)
	s.NoError(err)
	s.NotZero(itemStruct.ID, "Must not be zero after inserting")

	count, err := artist.Find().Count()
	s.NoError(err)
	s.Equal(uint64(2), count, "Expecting 2 elements")

	itemStruct2 := struct {
		ID   int    `db:"id,omitempty"`
		Name string `db:"name"`
	}{
		0,
		"Slash",
	}
	s.Zero(itemStruct2.ID, "Must be zero before inserting")
	err = artist.InsertReturning(itemStruct2)
	s.Error(err, "Should not happen, using a pointer should be enforced")
	s.Zero(itemStruct2.ID, "Must still be zero because there was no insertion")

	itemMap2 := map[string]string{
		"name": "Janus",
	}
	s.Zero(itemMap2["id"], "Must be zero before inserting")
	err = artist.InsertReturning(itemMap2)
	s.Error(err, "Should not happen, using a pointer should be enforced")
	s.Zero(itemMap2["id"], "Must still be zero because there was no insertion")

	// Counting elements, must be exactly 2 elements.
	count, err = artist.Find().Count()
	s.NoError(err)
	s.Equal(uint64(2), count, "Expecting 2 elements")
}

func (s *SQLTestSuite) TestInsertReturningWithinTransaction() {
	sess := s.Session()

	err := sess.Collection("artist").Truncate()
	s.NoError(err)

	err = sess.Tx(func(tx db.Session) error {
		artist := tx.Collection("artist")

		itemMap := map[string]string{
			"name": "Ozzie",
		}
		s.Zero(itemMap["id"], "Must be zero before inserting")
		err = artist.InsertReturning(&itemMap)
		s.NoError(err)
		s.NotZero(itemMap["id"], "Must not be zero after inserting")

		itemStruct := struct {
			ID   int    `db:"id,omitempty"`
			Name string `db:"name"`
		}{
			0,
			"Flea",
		}
		s.Zero(itemStruct.ID, "Must be zero before inserting")
		err = artist.InsertReturning(&itemStruct)
		s.NoError(err)
		s.NotZero(itemStruct.ID, "Must not be zero after inserting")

		count, err := artist.Find().Count()
		s.NoError(err)
		s.Equal(uint64(2), count, "Expecting 2 elements")

		itemStruct2 := struct {
			ID   int    `db:"id,omitempty"`
			Name string `db:"name"`
		}{
			0,
			"Slash",
		}
		s.Zero(itemStruct2.ID, "Must be zero before inserting")
		err = artist.InsertReturning(itemStruct2)
		s.Error(err, "Should not happen, using a pointer should be enforced")
		s.Zero(itemStruct2.ID, "Must still be zero because there was no insertion")

		itemMap2 := map[string]string{
			"name": "Janus",
		}
		s.Zero(itemMap2["id"], "Must be zero before inserting")
		err = artist.InsertReturning(itemMap2)
		s.Error(err, "Should not happen, using a pointer should be enforced")
		s.Zero(itemMap2["id"], "Must still be zero because there was no insertion")

		// Counting elements, must be exactly 2 elements.
		count, err = artist.Find().Count()
		s.NoError(err)
		s.Equal(uint64(2), count, "Expecting 2 elements")

		return fmt.Errorf("rolling back for no reason")
	})
	s.Error(err)

	// Expecting no elements.
	count, err := sess.Collection("artist").Find().Count()
	s.NoError(err)
	s.Equal(uint64(0), count, "Expecting 0 elements, everything was rolled back!")
}

func (s *SQLTestSuite) TestInsertIntoArtistsTable() {
	sess := s.Session()

	artist := sess.Collection("artist")

	err := artist.Truncate()
	s.NoError(err)

	itemMap := map[string]string{
		"name": "Ozzie",
	}

	record, err := artist.Insert(itemMap)
	s.NoError(err)
	s.NotNil(record)

	if pk, ok := record.ID().(int64); !ok || pk == 0 {
		s.T().Errorf("Expecting an ID.")
	}

	// Attempt to append a struct.
	itemStruct := struct {
		Name string `db:"name"`
	}{
		"Flea",
	}

	record, err = artist.Insert(itemStruct)
	s.NoError(err)
	s.NotNil(record)

	if pk, ok := record.ID().(int64); !ok || pk == 0 {
		s.T().Errorf("Expecting an ID.")
	}

	// Attempt to append a tagged struct.
	itemStruct2 := struct {
		ArtistName string `db:"name"`
	}{
		"Slash",
	}

	record, err = artist.Insert(&itemStruct2)
	s.NoError(err)
	s.NotNil(record)

	if pk, ok := record.ID().(int64); !ok || pk == 0 {
		s.T().Errorf("Expecting an ID.")
	}

	itemStruct3 := artistType{
		Name: "Janus",
	}
	record, err = artist.Insert(&itemStruct3)
	s.NoError(err)
	if s.Adapter() != "ql" {
		s.NotZero(record) // QL always inserts an ID.
	}

	// Counting elements, must be exactly 4 elements.
	count, err := artist.Find().Count()
	s.NoError(err)
	s.Equal(uint64(4), count)

	count, err = artist.Find(db.Cond{"name": db.Eq("Ozzie")}).Count()
	s.NoError(err)
	s.Equal(uint64(1), count)

	count, err = artist.Find("name", "Ozzie").And("name", "Flea").Count()
	s.NoError(err)
	s.Equal(uint64(0), count)

	count, err = artist.Find(db.Or(db.Cond{"name": "Ozzie"}, db.Cond{"name": "Flea"})).Count()
	s.NoError(err)
	s.Equal(uint64(2), count)

	count, err = artist.Find(db.And(db.Cond{"name": "Ozzie"}, db.Cond{"name": "Flea"})).Count()
	s.NoError(err)
	s.Equal(uint64(0), count)

	count, err = artist.Find(db.Cond{"name": "Ozzie"}).And(db.Cond{"name": "Flea"}).Count()
	s.NoError(err)
	s.Equal(uint64(0), count)
}

func (s *SQLTestSuite) TestQueryNonExistentCollection() {
	sess := s.Session()

	count, err := sess.Collection("doesnotexist").Find().Count()
	s.Error(err)
	s.Zero(count)
}

func (s *SQLTestSuite) TestGetOneResult() {
	sess := s.Session()

	artist := sess.Collection("artist")

	for i := 0; i < 5; i++ {
		_, err := artist.Insert(map[string]string{
			"name": fmt.Sprintf("Artist %d", i),
		})
		s.NoError(err)
	}

	// Fetching one struct.
	var someArtist artistType
	err := artist.Find().Limit(1).One(&someArtist)
	s.NoError(err)

	s.NotZero(someArtist.Name)
	if s.Adapter() != "ql" {
		s.NotZero(someArtist.ID)
	}

	// Fetching a pointer to a pointer.
	var someArtistObj *artistType
	err = artist.Find().Limit(1).One(&someArtistObj)
	s.NoError(err)
	s.NotZero(someArtist.Name)
	if s.Adapter() != "ql" {
		s.NotZero(someArtist.ID)
	}
}

func (s *SQLTestSuite) TestGetWithOffset() {
	sess := s.Session()

	artist := sess.Collection("artist")

	// Fetching one struct.
	var artists []artistType
	err := artist.Find().Offset(1).All(&artists)
	s.NoError(err)

	s.Equal(3, len(artists))
}

func (s *SQLTestSuite) TestGetResultsOneByOne() {
	sess := s.Session()

	artist := sess.Collection("artist")

	rowMap := map[string]interface{}{}

	res := artist.Find()

	err := res.Err()
	s.NoError(err)

	for res.Next(&rowMap) {
		s.NotZero(rowMap["id"])
		s.NotZero(rowMap["name"])
	}
	err = res.Err()
	s.NoError(err)

	err = res.Close()
	s.NoError(err)

	// Dumping into a tagged struct.
	rowStruct2 := struct {
		Value1 int64  `db:"id"`
		Value2 string `db:"name"`
	}{}

	res = artist.Find()

	for res.Next(&rowStruct2) {
		s.NotZero(rowStruct2.Value1)
		s.NotZero(rowStruct2.Value2)
	}
	err = res.Err()
	s.NoError(err)

	err = res.Close()
	s.NoError(err)

	// Dumping into a slice of maps.
	allRowsMap := []map[string]interface{}{}

	res = artist.Find()

	err = res.All(&allRowsMap)
	s.NoError(err)
	s.Equal(4, len(allRowsMap))

	for _, singleRowMap := range allRowsMap {
		if fmt.Sprintf("%d", singleRowMap["id"]) == "0" {
			s.T().Errorf("Expecting a not null ID.")
		}
	}

	// Dumping into a slice of structs.
	allRowsStruct := []struct {
		ID   int64  `db:"id,omitempty"`
		Name string `db:"name"`
	}{}

	res = artist.Find()

	if err = res.All(&allRowsStruct); err != nil {
		s.T().Errorf("%v", err)
	}

	s.Equal(4, len(allRowsStruct))

	for _, singleRowStruct := range allRowsStruct {
		s.NotZero(singleRowStruct.ID)
	}

	// Dumping into a slice of tagged structs.
	allRowsStruct2 := []struct {
		Value1 int64  `db:"id"`
		Value2 string `db:"name"`
	}{}

	res = artist.Find()

	err = res.All(&allRowsStruct2)
	s.NoError(err)

	s.Equal(4, len(allRowsStruct2))

	for _, singleRowStruct := range allRowsStruct2 {
		s.NotZero(singleRowStruct.Value1)
	}
}

func (s *SQLTestSuite) TestGetAllResults() {
	sess := s.Session()

	artist := sess.Collection("artist")

	total, err := artist.Find().Count()
	s.NoError(err)
	s.NotZero(total)

	// Fetching all artists into struct
	artists := []artistType{}

	res := artist.Find()

	err = res.All(&artists)
	s.NoError(err)
	s.Equal(len(artists), int(total))

	s.NotZero(artists[0].Name)
	s.NotZero(artists[0].ID)

	// Fetching all artists into struct pointers
	artistObjs := []*artistType{}
	res = artist.Find()

	err = res.All(&artistObjs)
	s.NoError(err)
	s.Equal(len(artistObjs), int(total))

	s.NotZero(artistObjs[0].Name)
	s.NotZero(artistObjs[0].ID)
}

func (s *SQLTestSuite) TestInlineStructs() {
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

	sess := s.Session()

	review := sess.Collection("review")

	err := review.Truncate()
	s.NoError(err)

	rec := reviewType{
		PublicationID: 123,
		Details: reviewTypeDetails{
			Name:     "..name..",
			Comments: "..comments..",
		},
	}

	testTimeZone := time.UTC
	switch s.Adapter() {
	case "mysql": // MySQL uses a global time zone
		testTimeZone = defaultTimeLocation
	}

	createdAt := time.Date(2016, time.January, 1, 2, 3, 4, 0, testTimeZone)
	rec.Details.Created = createdAt

	record, err := review.Insert(rec)
	s.NoError(err)
	s.NotZero(record.ID().(int64))

	rec.ID = record.ID().(int64)

	var recChk reviewType
	res := review.Find()

	err = res.One(&recChk)
	s.NoError(err)

	s.Equal(rec, recChk)
}

func (s *SQLTestSuite) TestUpdate() {
	sess := s.Session()

	artist := sess.Collection("artist")

	_, err := artist.Insert(map[string]string{
		"name": "Ozzie",
	})
	s.NoError(err)

	// Defining destination struct
	value := struct {
		ID   int64  `db:"id,omitempty"`
		Name string `db:"name"`
	}{}

	// Getting the first artist.
	cond := db.Cond{"id !=": db.NotEq(0)}
	if s.Adapter() == "ql" {
		cond = db.Cond{"id() !=": 0}
	}
	res := artist.Find(cond).Limit(1)

	err = res.One(&value)
	s.NoError(err)

	res = artist.Find(value.ID)

	// Updating set with a map
	rowMap := map[string]interface{}{
		"name": strings.ToUpper(value.Name),
	}

	err = res.Update(rowMap)
	s.NoError(err)

	// Pulling it again.
	err = res.One(&value)
	s.NoError(err)

	// Verifying.
	s.Equal(value.Name, rowMap["name"])

	if s.Adapter() != "ql" {

		// Updating using raw
		if err = res.Update(map[string]interface{}{"name": db.Raw("LOWER(name)")}); err != nil {
			s.T().Errorf("%v", err)
		}

		// Pulling it again.
		err = res.One(&value)
		s.NoError(err)

		// Verifying.
		s.Equal(value.Name, strings.ToLower(rowMap["name"].(string)))

		// Updating using raw
		if err = res.Update(struct {
			Name *db.RawExpr `db:"name"`
		}{db.Raw(`UPPER(name)`)}); err != nil {
			s.T().Errorf("%v", err)
		}

		// Pulling it again.
		err = res.One(&value)
		s.NoError(err)

		// Verifying.
		s.Equal(value.Name, strings.ToUpper(rowMap["name"].(string)))

		// Updating using raw
		if err = res.Update(struct {
			Name *db.FuncExpr `db:"name"`
		}{db.Func("LOWER", db.Raw("name"))}); err != nil {
			s.T().Errorf("%v", err)
		}

		// Pulling it again.
		err = res.One(&value)
		s.NoError(err)

		// Verifying.
		s.Equal(value.Name, strings.ToLower(rowMap["name"].(string)))
	}

	// Updating set with a struct
	rowStruct := struct {
		Name string `db:"name"`
	}{strings.ToLower(value.Name)}

	err = res.Update(rowStruct)
	s.NoError(err)

	// Pulling it again.
	err = res.One(&value)
	s.NoError(err)

	// Verifying
	s.Equal(value.Name, rowStruct.Name)

	// Updating set with a tagged struct
	rowStruct2 := struct {
		Value1 string `db:"name"`
	}{"john"}

	err = res.Update(rowStruct2)
	s.NoError(err)

	// Pulling it again.
	err = res.One(&value)
	s.NoError(err)

	// Verifying
	s.Equal(value.Name, rowStruct2.Value1)

	// Updating set with a tagged object
	rowStruct3 := &struct {
		Value1 string `db:"name"`
	}{"anderson"}

	err = res.Update(rowStruct3)
	s.NoError(err)

	// Pulling it again.
	err = res.One(&value)
	s.NoError(err)

	// Verifying
	s.Equal(value.Name, rowStruct3.Value1)
}

func (s *SQLTestSuite) TestFunction() {
	sess := s.Session()

	rowStruct := struct {
		ID   int64
		Name string
	}{}

	artist := sess.Collection("artist")

	cond := db.Cond{"id NOT IN": []int{0, -1}}
	if s.Adapter() == "ql" {
		cond = db.Cond{"id() NOT IN": []int{0, -1}}
	}
	res := artist.Find(cond)

	err := res.One(&rowStruct)
	s.NoError(err)

	total, err := res.Count()
	s.NoError(err)
	s.Equal(uint64(4), total)

	// Testing conditions
	cond = db.Cond{"id NOT IN": []interface{}{0, -1}}
	if s.Adapter() == "ql" {
		cond = db.Cond{"id() NOT IN": []interface{}{0, -1}}
	}
	res = artist.Find(cond)

	err = res.One(&rowStruct)
	s.NoError(err)

	total, err = res.Count()
	s.NoError(err)
	s.Equal(uint64(4), total)

	res = artist.Find().Select("name")

	var rowMap map[string]interface{}
	err = res.One(&rowMap)
	s.NoError(err)

	total, err = res.Count()
	s.NoError(err)
	s.Equal(uint64(4), total)

	res = artist.Find().Select("name")

	err = res.One(&rowMap)
	s.NoError(err)

	total, err = res.Count()
	s.NoError(err)
	s.Equal(uint64(4), total)
}

func (s *SQLTestSuite) TestNullableFields() {
	sess := s.Session()

	type testType struct {
		ID              int64           `db:"id,omitempty"`
		NullStringTest  sql.NullString  `db:"_string"`
		NullInt64Test   sql.NullInt64   `db:"_int64"`
		NullFloat64Test sql.NullFloat64 `db:"_float64"`
		NullBoolTest    sql.NullBool    `db:"_bool"`
	}

	col := sess.Collection(`data_types`)

	err := col.Truncate()
	s.NoError(err)

	// Testing insertion of invalid nulls.
	test := testType{
		NullStringTest:  sql.NullString{String: "", Valid: false},
		NullInt64Test:   sql.NullInt64{Int64: 0, Valid: false},
		NullFloat64Test: sql.NullFloat64{Float64: 0.0, Valid: false},
		NullBoolTest:    sql.NullBool{Bool: false, Valid: false},
	}

	id, err := col.Insert(testType{})
	s.NoError(err)

	// Testing fetching of invalid nulls.
	err = col.Find(id).One(&test)
	s.NoError(err)

	s.False(test.NullInt64Test.Valid)
	s.False(test.NullFloat64Test.Valid)
	s.False(test.NullBoolTest.Valid)

	// Testing insertion of valid nulls.
	test = testType{
		NullStringTest:  sql.NullString{String: "", Valid: true},
		NullInt64Test:   sql.NullInt64{Int64: 0, Valid: true},
		NullFloat64Test: sql.NullFloat64{Float64: 0.0, Valid: true},
		NullBoolTest:    sql.NullBool{Bool: false, Valid: true},
	}

	id, err = col.Insert(test)
	s.NoError(err)

	// Testing fetching of valid nulls.
	err = col.Find(id).One(&test)
	s.NoError(err)

	s.True(test.NullInt64Test.Valid)
	s.True(test.NullBoolTest.Valid)
	s.True(test.NullStringTest.Valid)
}

func (s *SQLTestSuite) TestGroup() {
	sess := s.Session()

	type statsType struct {
		Numeric int `db:"numeric"`
		Value   int `db:"value"`
	}

	stats := sess.Collection("stats_test")

	err := stats.Truncate()
	s.NoError(err)

	// Adding row append.
	for i := 0; i < 100; i++ {
		numeric, value := rand.Intn(5), rand.Intn(100)
		_, err := stats.Insert(statsType{numeric, value})
		s.NoError(err)
	}

	// Testing GROUP BY
	res := stats.Find().Select(
		"numeric",
		db.Raw("count(1) AS counter"),
		db.Raw("sum(value) AS total"),
	).GroupBy("numeric")

	var results []map[string]interface{}

	err = res.All(&results)
	s.NoError(err)

	s.Equal(5, len(results))
}

func (s *SQLTestSuite) TestInsertAndDelete() {
	sess := s.Session()

	artist := sess.Collection("artist")
	res := artist.Find()

	total, err := res.Count()
	s.NoError(err)
	s.Greater(total, uint64(0))

	err = res.Delete()
	s.NoError(err)

	total, err = res.Count()
	s.NoError(err)
	s.Equal(uint64(0), total)
}

func (s *SQLTestSuite) TestCompositeKeys() {
	if s.Adapter() == "ql" {
		s.T().Skip("Currently not supported.")
	}

	sess := s.Session()

	compositeKeys := sess.Collection("composite_keys")

	{
		n := rand.Intn(100000)

		item := itemWithCompoundKey{
			"ABCDEF",
			strconv.Itoa(n),
			"Some value",
		}

		id, err := compositeKeys.Insert(&item)
		s.NoError(err)
		s.NotZero(id)

		var item2 itemWithCompoundKey
		s.NotEqual(item2.SomeVal, item.SomeVal)

		// Finding by ID
		err = compositeKeys.Find(id).One(&item2)
		s.NoError(err)

		s.Equal(item2.SomeVal, item.SomeVal)
	}

	{
		n := rand.Intn(100000)

		item := itemWithCompoundKey{
			"ABCDEF",
			strconv.Itoa(n),
			"Some value",
		}

		err := compositeKeys.InsertReturning(&item)
		s.NoError(err)
	}
}

// Attempts to test database transactions.
func (s *SQLTestSuite) TestTransactionsAndRollback() {
	if s.Adapter() == "ql" {
		s.T().Skip("Currently not supported.")
	}

	sess := s.Session()

	err := sess.Tx(func(tx db.Session) error {
		artist := tx.Collection("artist")
		err := artist.Truncate()
		s.NoError(err)

		_, err = artist.Insert(artistType{1, "First"})
		s.NoError(err)

		return nil
	})
	s.NoError(err)

	err = sess.Tx(func(tx db.Session) error {
		artist := tx.Collection("artist")

		_, err = artist.Insert(artistType{2, "Second"})
		s.NoError(err)

		// Won't fail.
		_, err = artist.Insert(artistType{3, "Third"})
		s.NoError(err)

		// Will fail.
		_, err = artist.Insert(artistType{1, "Duplicated"})
		s.Error(err)

		return err
	})
	s.Error(err)

	// Let's verify we still have one element.
	artist := sess.Collection("artist")

	count, err := artist.Find().Count()
	s.NoError(err)
	s.Equal(uint64(1), count)

	err = sess.Tx(func(tx db.Session) error {
		artist := tx.Collection("artist")

		// Won't fail.
		_, err = artist.Insert(artistType{2, "Second"})
		s.NoError(err)

		// Won't fail.
		_, err = artist.Insert(artistType{3, "Third"})
		s.NoError(err)

		return fmt.Errorf("rollback for no reason")
	})
	s.Error(err)

	// Let's verify we still have one element.
	artist = sess.Collection("artist")

	count, err = artist.Find().Count()
	s.NoError(err)
	s.Equal(uint64(1), count)

	// Attempt to add some rows.
	err = sess.Tx(func(tx db.Session) error {
		artist = tx.Collection("artist")

		// Won't fail.
		_, err = artist.Insert(artistType{2, "Second"})
		s.NoError(err)

		// Won't fail.
		_, err = artist.Insert(artistType{3, "Third"})
		s.NoError(err)

		return nil
	})
	s.NoError(err)

	// Let's verify we have 3 rows.
	artist = sess.Collection("artist")

	count, err = artist.Find().Count()
	s.NoError(err)
	s.Equal(uint64(3), count)
}

func (s *SQLTestSuite) TestDataTypes() {
	if s.Adapter() == "ql" {
		s.T().Skip("Currently not supported.")
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
		Blob   []byte `db:"_blob"`

		Date  time.Time  `db:"_date"`
		DateN *time.Time `db:"_nildate"`
		DateP *time.Time `db:"_ptrdate"`
		DateD *time.Time `db:"_defaultdate,omitempty"`
		Time  int64      `db:"_time"`
	}

	sess := s.Session()

	// Getting a pointer to the "data_types" collection.
	dataTypes := sess.Collection("data_types")

	// Removing all data.
	err := dataTypes.Truncate()
	s.NoError(err)

	testTimeZone := time.Local
	switch s.Adapter() {
	case "mysql", "postgresql": // MySQL uses a global time zone
		testTimeZone = defaultTimeLocation
	}

	ts := time.Date(2011, 7, 28, 1, 2, 3, 0, testTimeZone)
	tnz := ts.In(time.UTC)

	switch s.Adapter() {
	case "mysql":
		// MySQL uses a global timezone
		tnz = ts.In(defaultTimeLocation)
	}

	testValues := testValuesStruct{
		1, 1, 1, 1, 1,
		-1, -1, -1, -1, -1,

		1.337, 1.337,

		true,
		"Hello world!",
		[]byte("Hello world!"),

		ts,   // Date
		nil,  // DateN
		&tnz, // DateP
		nil,  // DateD
		int64(time.Second * time.Duration(7331)),
	}
	id, err := dataTypes.Insert(testValues)
	s.NoError(err)
	s.NotNil(id)

	// Defining our set.
	cond := db.Cond{"id": id}
	if s.Adapter() == "ql" {
		cond = db.Cond{"id()": id}
	}
	res := dataTypes.Find(cond)

	count, err := res.Count()
	s.NoError(err)
	s.NotZero(count)

	// Trying to dump the subject into an empty structure of the same type.
	var item testValuesStruct

	err = res.One(&item)
	s.NoError(err)

	s.NotNil(item.DateD)
	s.NotNil(item.Date)

	// Copy the default date (this value is set by the database)
	testValues.DateD = item.DateD
	item.Date = item.Date.In(testTimeZone)

	s.Equal(testValues.Date, item.Date)
	s.Equal(testValues.DateN, item.DateN)
	s.Equal(testValues.DateP, item.DateP)
	s.Equal(testValues.DateD, item.DateD)

	// The original value and the test subject must match.
	s.Equal(testValues, item)
}

func (s *SQLTestSuite) TestUpdateWithNullColumn() {
	sess := s.Session()

	artist := sess.Collection("artist")
	err := artist.Truncate()
	s.NoError(err)

	type Artist struct {
		ID   int64   `db:"id,omitempty"`
		Name *string `db:"name"`
	}

	name := "José"
	id, err := artist.Insert(Artist{0, &name})
	s.NoError(err)

	var item Artist
	err = artist.Find(id).One(&item)
	s.NoError(err)

	s.NotEqual(nil, item.Name)
	s.Equal(name, *item.Name)

	err = artist.Find(id).Update(Artist{Name: nil})
	s.NoError(err)

	var item2 Artist
	err = artist.Find(id).One(&item2)
	s.NoError(err)

	s.Equal((*string)(nil), item2.Name)
}

func (s *SQLTestSuite) TestBatchInsert() {
	sess := s.Session()

	for batchSize := 0; batchSize < 17; batchSize++ {
		err := sess.Collection("artist").Truncate()
		s.NoError(err)

		q := sess.SQL().InsertInto("artist").Columns("name")

		switch s.Adapter() {
		case "postgresql", "cockroachdb":
			q = q.Amend(func(query string) string {
				return query + ` ON CONFLICT DO NOTHING`
			})
		}

		batch := q.Batch(batchSize)

		totalItems := int(rand.Int31n(21))

		go func() {
			defer batch.Done()
			for i := 0; i < totalItems; i++ {
				batch.Values(fmt.Sprintf("artist-%d", i))
			}
		}()

		err = batch.Wait()
		s.NoError(err)
		s.NoError(batch.Err())

		c, err := sess.Collection("artist").Find().Count()
		s.NoError(err)
		s.Equal(uint64(totalItems), c)

		for i := 0; i < totalItems; i++ {
			c, err := sess.Collection("artist").Find(db.Cond{"name": fmt.Sprintf("artist-%d", i)}).Count()
			s.NoError(err)
			s.Equal(uint64(1), c)
		}
	}
}

func (s *SQLTestSuite) TestBatchInsertNoColumns() {
	sess := s.Session()

	for batchSize := 0; batchSize < 17; batchSize++ {
		err := sess.Collection("artist").Truncate()
		s.NoError(err)

		batch := sess.SQL().InsertInto("artist").Batch(batchSize)

		totalItems := int(rand.Int31n(21))

		go func() {
			defer batch.Done()
			for i := 0; i < totalItems; i++ {
				value := struct {
					Name string `db:"name"`
				}{fmt.Sprintf("artist-%d", i)}
				batch.Values(value)
			}
		}()

		err = batch.Wait()
		s.NoError(err)
		s.NoError(batch.Err())

		c, err := sess.Collection("artist").Find().Count()
		s.NoError(err)
		s.Equal(uint64(totalItems), c)

		for i := 0; i < totalItems; i++ {
			c, err := sess.Collection("artist").Find(db.Cond{"name": fmt.Sprintf("artist-%d", i)}).Count()
			s.NoError(err)
			s.Equal(uint64(1), c)
		}
	}
}

func (s *SQLTestSuite) TestBatchInsertReturningKeys() {
	switch s.Adapter() {
	case "postgresql", "cockroachdb":
		// pass
	default:
		s.T().Skip("Currently not supported.")
		return
	}

	sess := s.Session()

	err := sess.Collection("artist").Truncate()
	s.NoError(err)

	batchSize, totalItems := 7, 12

	batch := sess.SQL().InsertInto("artist").Columns("name").Returning("id").Batch(batchSize)

	go func() {
		defer batch.Done()
		for i := 0; i < totalItems; i++ {
			batch.Values(fmt.Sprintf("artist-%d", i))
		}
	}()

	var keyMap []struct {
		ID int `db:"id"`
	}
	for batch.NextResult(&keyMap) {
		// Each insertion must produce new keys.
		s.True(len(keyMap) > 0)
		s.True(len(keyMap) <= batchSize)

		// Find the elements we've just inserted
		keys := make([]int, 0, len(keyMap))
		for i := range keyMap {
			keys = append(keys, keyMap[i].ID)
		}

		// Make sure count matches.
		c, err := sess.Collection("artist").Find(db.Cond{"id": keys}).Count()
		s.NoError(err)
		s.Equal(uint64(len(keyMap)), c)
	}
	s.NoError(batch.Err())

	// Count all new elements
	c, err := sess.Collection("artist").Find().Count()
	s.NoError(err)
	s.Equal(uint64(totalItems), c)
}

func (s *SQLTestSuite) TestPaginator() {
	sess := s.Session()

	err := sess.Collection("artist").Truncate()
	s.NoError(err)

	batch := sess.SQL().InsertInto("artist").Batch(100)

	go func() {
		defer batch.Done()
		for i := 0; i < 999; i++ {
			value := struct {
				Name string `db:"name"`
			}{fmt.Sprintf("artist-%d", i)}
			batch.Values(value)
		}
	}()

	err = batch.Wait()
	s.NoError(err)
	s.NoError(batch.Err())

	q := sess.SQL().SelectFrom("artist")
	if s.Adapter() == "ql" {
		q = sess.SQL().SelectFrom(sess.SQL().Select("id() AS id", "name").From("artist"))
	}

	const pageSize = 13
	cursorColumn := "id"

	paginator := q.Paginate(pageSize)

	var zerothPage []artistType
	err = paginator.Page(0).All(&zerothPage)
	s.NoError(err)
	s.Equal(pageSize, len(zerothPage))

	var firstPage []artistType
	err = paginator.Page(1).All(&firstPage)
	s.NoError(err)
	s.Equal(pageSize, len(firstPage))

	s.Equal(zerothPage, firstPage)

	var secondPage []artistType
	err = paginator.Page(2).All(&secondPage)
	s.NoError(err)
	s.Equal(pageSize, len(secondPage))

	totalPages, err := paginator.TotalPages()
	s.NoError(err)
	s.NotZero(totalPages)
	s.Equal(uint(77), totalPages)

	totalEntries, err := paginator.TotalEntries()
	s.NoError(err)
	s.NotZero(totalEntries)
	s.Equal(uint64(999), totalEntries)

	var lastPage []artistType
	err = paginator.Page(totalPages).All(&lastPage)
	s.NoError(err)
	s.Equal(11, len(lastPage))

	var beyondLastPage []artistType
	err = paginator.Page(totalPages + 1).All(&beyondLastPage)
	s.NoError(err)
	s.Equal(0, len(beyondLastPage))

	var hundredthPage []artistType
	err = paginator.Page(100).All(&hundredthPage)
	s.NoError(err)
	s.Equal(0, len(hundredthPage))

	for i := uint(0); i < totalPages; i++ {
		current := paginator.Page(i + 1)

		var items []artistType
		err := current.All(&items)
		if err != nil {
			s.T().Errorf("%v", err)
		}
		s.NoError(err)
		if len(items) < 1 {
			s.Equal(totalPages+1, i)
			break
		}
		for j := 0; j < len(items); j++ {
			s.Equal(fmt.Sprintf("artist-%d", int64(pageSize*int(i)+j)), items[j].Name)
		}
	}

	paginator = paginator.Cursor(cursorColumn)
	{
		current := paginator.Page(1)
		for i := 0; ; i++ {
			var items []artistType
			err := current.All(&items)
			if err != nil {
				s.T().Errorf("%v", err)
			}
			if len(items) < 1 {
				s.Equal(int(totalPages), i)
				break
			}

			for j := 0; j < len(items); j++ {
				s.Equal(fmt.Sprintf("artist-%d", int64(pageSize*int(i)+j)), items[j].Name)
			}
			current = current.NextPage(items[len(items)-1].ID)
		}
	}

	{
		current := paginator.Page(totalPages)
		for i := totalPages; ; i-- {
			var items []artistType

			err := current.All(&items)
			s.NoError(err)

			if len(items) < 1 {
				s.Equal(uint(0), i)
				break
			}
			for j := 0; j < len(items); j++ {
				s.Equal(fmt.Sprintf("artist-%d", pageSize*int(i-1)+j), items[j].Name)
			}

			current = current.PrevPage(items[0].ID)
		}
	}

	if s.Adapter() == "ql" {
		s.T().Skip("Unsupported, see https://github.com/cznic/ql/issues/182")
		return
	}

	{
		result := sess.Collection("artist").Find()

		fifteenResults := 15
		resultPaginator := result.Paginate(uint(fifteenResults))

		count, err := resultPaginator.TotalPages()
		s.Equal(uint(67), count)
		s.NoError(err)

		var items []artistType
		fifthPage := 5
		err = resultPaginator.Page(uint(fifthPage)).All(&items)
		s.NoError(err)

		for j := 0; j < len(items); j++ {
			s.Equal(fmt.Sprintf("artist-%d", int(fifteenResults)*(fifthPage-1)+j), items[j].Name)
		}

		resultPaginator = resultPaginator.Cursor(cursorColumn).Page(1)
		for i := 0; ; i++ {
			var items []artistType

			err = resultPaginator.All(&items)
			s.NoError(err)

			if len(items) < 1 {
				break
			}

			for j := 0; j < len(items); j++ {
				s.Equal(fmt.Sprintf("artist-%d", fifteenResults*i+j), items[j].Name)
			}
			resultPaginator = resultPaginator.NextPage(items[len(items)-1].ID)
		}

		resultPaginator = resultPaginator.Cursor(cursorColumn).Page(count)
		for i := count; ; i-- {
			var items []artistType

			err = resultPaginator.All(&items)
			s.NoError(err)

			if len(items) < 1 {
				s.Equal(uint(0), i)
				break
			}

			for j := 0; j < len(items); j++ {
				s.Equal(fmt.Sprintf("artist-%d", fifteenResults*(int(i)-1)+j), items[j].Name)
			}
			resultPaginator = resultPaginator.PrevPage(items[0].ID)
		}
	}

	{
		// Testing page size 0.
		paginator := q.Paginate(0)

		totalPages, err := paginator.TotalPages()
		s.NoError(err)
		s.Equal(uint(1), totalPages)

		totalEntries, err := paginator.TotalEntries()
		s.NoError(err)
		s.Equal(uint64(999), totalEntries)

		var allItems []artistType
		err = paginator.Page(0).All(&allItems)
		s.NoError(err)
		s.Equal(totalEntries, uint64(len(allItems)))

	}
}

func (s *SQLTestSuite) TestPaginator_Issue607() {
	sess := s.Session()

	err := sess.Collection("artist").Truncate()
	s.NoError(err)

	// Add first batch
	{
		batch := sess.SQL().InsertInto("artist").Batch(50)

		go func() {
			defer batch.Done()
			for i := 0; i < 49; i++ {
				value := struct {
					Name string `db:"name"`
				}{fmt.Sprintf("artist-1.%d", i)}
				batch.Values(value)
			}
		}()

		err = batch.Wait()
		s.NoError(err)
		s.NoError(batch.Err())
	}

	artists := []*artistType{}
	paginator := sess.SQL().Select("name").From("artist").Paginate(10)

	err = paginator.Page(1).All(&artists)
	s.NoError(err)

	{
		totalPages, err := paginator.TotalPages()
		s.NoError(err)
		s.NotZero(totalPages)
		s.Equal(uint(5), totalPages)
	}

	// Add second batch
	{
		batch := sess.SQL().InsertInto("artist").Batch(50)

		go func() {
			defer batch.Done()
			for i := 0; i < 49; i++ {
				value := struct {
					Name string `db:"name"`
				}{fmt.Sprintf("artist-2.%d", i)}
				batch.Values(value)
			}
		}()

		err = batch.Wait()
		s.NoError(err)
		s.NoError(batch.Err())
	}

	{
		totalPages, err := paginator.TotalPages()
		s.NoError(err)
		s.NotZero(totalPages)
		s.Equal(uint(10), totalPages, "expect number of pages to change")
	}

	artists = []*artistType{}

	cond := db.Cond{"name": db.Like("artist-1.%")}
	if s.Adapter() == "ql" {
		cond = db.Cond{"name": db.Like("artist-1.")}
	}

	paginator = sess.SQL().Select("name").From("artist").Where(cond).Paginate(10)

	err = paginator.Page(1).All(&artists)
	s.NoError(err)

	{
		totalPages, err := paginator.TotalPages()
		s.NoError(err)
		s.NotZero(totalPages)
		s.Equal(uint(5), totalPages, "expect same 5 pages from the first batch")
	}

}

func (s *SQLTestSuite) TestSession() {
	sess := s.Session()

	var all []map[string]interface{}

	err := sess.Collection("artist").Truncate()
	s.NoError(err)

	_, err = sess.SQL().InsertInto("artist").Values(struct {
		Name string `db:"name"`
	}{"Rinko Kikuchi"}).Exec()
	s.NoError(err)

	// Using explicit iterator.
	iter := sess.SQL().SelectFrom("artist").Iterator()
	err = iter.All(&all)

	s.NoError(err)
	s.NotZero(all)

	// Using explicit iterator to fetch one item.
	var item map[string]interface{}
	iter = sess.SQL().SelectFrom("artist").Iterator()
	err = iter.One(&item)

	s.NoError(err)
	s.NotZero(item)

	// Using explicit iterator and NextScan.
	iter = sess.SQL().SelectFrom("artist").Iterator()
	var id int
	var name string

	if s.Adapter() == "ql" {
		err = iter.NextScan(&name)
		id = 1
	} else {
		err = iter.NextScan(&id, &name)
	}

	s.NoError(err)
	s.NotZero(id)
	s.NotEmpty(name)
	s.NoError(iter.Close())

	err = iter.NextScan(&id, &name)
	s.Error(err)

	// Using explicit iterator and ScanOne.
	iter = sess.SQL().SelectFrom("artist").Iterator()
	id, name = 0, ""
	if s.Adapter() == "ql" {
		err = iter.ScanOne(&name)
		id = 1
	} else {
		err = iter.ScanOne(&id, &name)
	}

	s.NoError(err)
	s.NotZero(id)
	s.NotEmpty(name)

	err = iter.ScanOne(&id, &name)
	s.Error(err)

	// Using explicit iterator and Next.
	iter = sess.SQL().SelectFrom("artist").Iterator()

	var artist map[string]interface{}
	for iter.Next(&artist) {
		if s.Adapter() != "ql" {
			s.NotZero(artist["id"])
		}
		s.NotEmpty(artist["name"])
	}
	// We should not have any error after finishing successfully exiting a Next() loop.
	s.Empty(iter.Err())

	for i := 0; i < 5; i++ {
		// But we'll get errors if we attempt to continue using Next().
		s.False(iter.Next(&artist))
		s.Error(iter.Err())
	}

	// Using implicit iterator.
	q := sess.SQL().SelectFrom("artist")
	err = q.All(&all)

	s.NoError(err)
	s.NotZero(all)

	err = sess.Tx(func(tx db.Session) error {
		q := tx.SQL().SelectFrom("artist")
		s.NotZero(iter)

		err = q.All(&all)
		s.NoError(err)
		s.NotZero(all)

		return nil
	})

	s.NoError(err)
}

func (s *SQLTestSuite) TestExhaustConnectionPool() {
	if s.Adapter() == "ql" {
		s.T().Skip("Currently not supported.")
		return
	}

	sess := s.Session()
	errRolledBack := errors.New("rolled back")

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		s.T().Logf("Tx %d: Pending", i)

		wg.Add(1)
		go func(wg *sync.WaitGroup, i int) {
			defer wg.Done()

			// Requesting a new transaction session.
			start := time.Now()
			s.T().Logf("Tx: %d: NewTx", i)

			expectError := false
			if i%2 == 1 {
				expectError = true
			}

			err := sess.Tx(func(tx db.Session) error {
				s.T().Logf("Tx %d: OK (time to connect: %v)", i, time.Since(start))
				// Let's suppose that we do a bunch of complex stuff and that the
				// transaction lasts 3 seconds.
				time.Sleep(time.Second * 3)

				if expectError {
					if _, err := tx.SQL().DeleteFrom("artist").Exec(); err != nil {
						return err
					}
					return errRolledBack
				}

				var account map[string]interface{}
				if err := tx.Collection("artist").Find().One(&account); err != nil {
					return err
				}
				return nil
			})
			if expectError {
				s.Error(err)
				s.True(errors.Is(err, errRolledBack))
			} else {
				s.NoError(err)
			}
		}(&wg, i)
	}

	wg.Wait()
}

func (s *SQLTestSuite) TestCustomType() {
	// See https://github.com/upper/db/issues/332
	sess := s.Session()

	artist := sess.Collection("artist")

	err := artist.Truncate()
	s.NoError(err)

	id, err := artist.Insert(artistWithCustomType{
		Custom: customType{Val: []byte("some name")},
	})
	s.NoError(err)
	s.NotNil(id)

	var bar artistWithCustomType
	err = artist.Find(id).One(&bar)
	s.NoError(err)

	s.Equal("foo: some name", string(bar.Custom.Val))
}

func (s *SQLTestSuite) Test_Issue565() {
	s.Session().Collection("birthdays").Insert(&birthday{
		Name: "Lucy",
		Born: time.Now(),
	})

	parentCtx := context.WithValue(s.Session().Context(), "carry", 1)
	s.NotZero(parentCtx.Value("carry"))

	{
		ctx, cancel := context.WithTimeout(parentCtx, time.Nanosecond)
		defer cancel()

		sess := s.Session()

		sess = sess.WithContext(ctx)

		var result birthday
		err := sess.Collection("birthdays").Find().Select("name").One(&result)

		s.Error(err)
		s.Zero(result.Name)

		s.NotZero(ctx.Value("carry"))
	}

	{
		ctx, cancel := context.WithTimeout(parentCtx, time.Second*10)
		cancel() // cancel before passing

		sess := s.Session().WithContext(ctx)

		var result birthday
		err := sess.Collection("birthdays").Find().Select("name").One(&result)

		s.Error(err)
		s.Zero(result.Name)

		s.NotZero(ctx.Value("carry"))
	}

	{
		ctx, cancel := context.WithTimeout(parentCtx, time.Second)
		defer cancel()

		sess := s.Session().WithContext(ctx)

		var result birthday
		err := sess.Collection("birthdays").Find().Select("name").One(&result)

		s.NoError(err)
		s.NotZero(result.Name)

		s.NotZero(ctx.Value("carry"))
	}
}

func (s *SQLTestSuite) TestSelectFromSubquery() {
	sess := s.Session()

	{
		var artists []artistType
		q := sess.SQL().SelectFrom(
			sess.SQL().SelectFrom("artist").Where(db.Cond{
				"name": db.IsNotNull(),
			}),
		).As("_q")
		err := q.All(&artists)
		s.NoError(err)

		s.NotZero(len(artists))
	}

	{
		var artists []artistType
		q := sess.SQL().SelectFrom(
			sess.Collection("artist").Find(db.Cond{
				"name": db.IsNotNull(),
			}),
		).As("_q")
		err := q.All(&artists)
		s.NoError(err)

		s.NotZero(len(artists))
	}

}
