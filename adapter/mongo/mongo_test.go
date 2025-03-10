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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	db "github.com/upper/db/v4"
	"github.com/upper/db/v4/internal/testsuite"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type artistType struct {
	ID   primitive.ObjectID `bson:"_id,omitempty"`
	Name string             `bson:"name"`
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

type AdapterTests struct {
	testsuite.Suite
}

func (s *AdapterTests) SetupSuite() {
	s.Helper = &Helper{}
}

func (s *AdapterTests) TestOpenWithWrongData() {
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
	_, err = Open(rightSettings)
	s.Require().NoError(err)

	// Attempt to open with wrong password.
	wrongSettings = ConnectionURL{
		Database: settings.Database,
		Host:     settings.Host,
		User:     settings.User,
		Password: "fail",
	}

	_, err = Open(wrongSettings)
	s.Error(err)

	// Attempt to open with wrong database.
	wrongSettings = ConnectionURL{
		Database: "fail",
		Host:     settings.Host,
		User:     settings.User,
		Password: settings.Password,
	}

	_, err = Open(wrongSettings)
	s.Error(err)

	// Attempt to open with wrong username.
	wrongSettings = ConnectionURL{
		Database: settings.Database,
		Host:     settings.Host,
		User:     "fail",
		Password: settings.Password,
	}

	_, err = Open(wrongSettings)
	s.Error(err)
}

func (s *AdapterTests) TestTruncate() {
	// Opening database.
	sess, err := Open(settings)
	s.Require().NoError(err)

	// We should close the database when it's no longer in use.
	defer sess.Close()

	// Getting a list of all collections in this database.
	collections, err := sess.Collections()
	s.Require().NoError(err)

	for _, col := range collections {
		// The collection may ot may not exists.
		if ok, _ := col.Exists(); ok {
			// Truncating the structure, if exists.
			err = col.Truncate()
			s.Require().NoError(err)
		}
	}
}

func (s *AdapterTests) TestInsert() {
	// Opening database.
	sess, err := Open(settings)
	s.Require().NoError(err)

	// We should close the database when it's no longer in use.
	defer sess.Close()

	// Getting a pointer to the "artist" collection.
	artist := sess.Collection("artist")
	_ = artist.Truncate()

	// Inserting a map.
	record, err := artist.Insert(map[string]string{
		"name": "Ozzie",
	})
	s.Require().NoError(err)

	id := record.ID()
	s.NotZero(record.ID())

	_, ok := id.(primitive.ObjectID)
	s.True(ok)

	_, err = primitive.ObjectIDFromHex(id.(primitive.ObjectID).Hex())
	s.Require().NoError(err)

	// Inserting a struct.
	record, err = artist.Insert(struct {
		Name string
	}{
		"Flea",
	})
	s.Require().NoError(err)

	id = record.ID()
	s.NotZero(id)

	_, ok = id.(primitive.ObjectID)
	s.True(ok)

	_, err = primitive.ObjectIDFromHex(id.(primitive.ObjectID).Hex())
	s.Require().NoError(err)

	// Inserting a struct (using tags to specify the field name).
	record, err = artist.Insert(struct {
		ArtistName string `bson:"name"`
	}{
		"Slash",
	})
	s.Require().NoError(err)

	id = record.ID()
	s.NotNil(id)

	_, ok = id.(primitive.ObjectID)

	s.True(ok)

	_, err = primitive.ObjectIDFromHex(id.(primitive.ObjectID).Hex())
	s.Require().NoError(err)

	// Inserting a pointer to a struct
	record, err = artist.Insert(&struct {
		ArtistName string `bson:"name"`
	}{
		"Metallica",
	})
	s.Require().NoError(err)

	id = record.ID()
	s.NotNil(id)

	_, ok = id.(primitive.ObjectID)
	s.True(ok)

	_, err = primitive.ObjectIDFromHex(id.(primitive.ObjectID).Hex())
	s.Require().NoError(err)

	// Inserting a pointer to a map
	record, err = artist.Insert(&map[string]string{
		"name": "Freddie",
	})
	s.Require().NoError(err)
	s.NotZero(id)

	_, ok = id.(primitive.ObjectID)
	s.True(ok)

	id = record.ID()
	s.NotNil(id)

	_, err = primitive.ObjectIDFromHex(id.(primitive.ObjectID).Hex())
	s.Require().NoError(err)

	// Counting elements, must be exactly 6 elements.
	total, err := artist.Find().Count()
	s.Require().NoError(err)
	s.Equal(uint64(5), total)
}

func (s *AdapterTests) TestGetNonExistentRow_Issue426() {
	// Opening database.
	sess, err := Open(settings)
	s.Require().NoError(err)

	defer sess.Close()

	artist := sess.Collection("artist")

	var one artistType
	err = artist.Find(db.Cond{"name": "nothing"}).One(&one)

	s.NotZero(err)
	s.Equal(db.ErrNoMoreRows, err)

	var all []artistType
	err = artist.Find(db.Cond{"name": "nothing"}).All(&all)

	s.Zero(len(all), "All should return an empty slice")
	s.Equal(0, len(all))
}

func (s *AdapterTests) TestResultCount() {
	var err error
	var res db.Result

	// Opening database.
	sess, err := Open(settings)
	s.Require().NoError(err)

	defer sess.Close()

	// We should close the database when it's no longer in use.
	artist := sess.Collection("artist")

	res = artist.Find()

	// Counting all the matching rows.
	total, err := res.Count()
	s.Require().NoError(err)
	s.NotZero(total)
}

func (s *AdapterTests) TestGroup() {
	var stats db.Collection

	sess, err := Open(settings)
	s.Require().NoError(err)

	type statsT struct {
		Numeric int `db:"numeric" bson:"numeric"`
		Value   int `db:"value" bson:"value"`
	}

	defer sess.Close()

	stats = sess.Collection("statsTest")

	// Truncating table.
	_ = stats.Truncate()

	// Adding row append.
	for i := 0; i < 1000; i++ {
		numeric, value := rand.Intn(10), rand.Intn(100)
		_, err = stats.Insert(statsT{numeric, value})
		s.Require().NoError(err)
	}

	// db.statsTest.group({key: {numeric: true}, initial: {sum: 0}, reduce: function(doc, prev) { prev.sum += 1}});

	// Testing GROUP BY
	res := stats.Find().GroupBy(bson.M{
		"key":     bson.M{"numeric": true},
		"initial": bson.M{"sum": 0},
		"reduce":  `function(doc, prev) { prev.sum += 1}`,
	})

	var results []map[string]interface{}

	err = res.All(&results)
	s.Equal(db.ErrUnsupported, err)
}

func (s *AdapterTests) TestResultNonExistentCount() {
	sess, err := Open(settings)
	s.Require().NoError(err)

	defer sess.Close()

	total, err := sess.Collection("notartist").Find().Count()
	s.Require().NoError(err)
	s.Zero(total)
}

func (s *AdapterTests) TestResultFetch() {

	// Opening database.
	sess, err := Open(settings)
	s.Require().NoError(err)

	// We should close the database when it's no longer in use.
	defer sess.Close()

	artist := sess.Collection("artist")

	// Testing map
	res := artist.Find()

	rowM := map[string]interface{}{}

	for res.Next(&rowM) {
		s.NotZero(rowM["_id"])

		_, ok := rowM["_id"].(primitive.ObjectID)
		s.True(ok)

		_, err := primitive.ObjectIDFromHex(rowM["_id"].(primitive.ObjectID).Hex())
		s.Require().NoError(err)

		name, ok := rowM["name"].(string)
		s.True(ok)
		s.NotZero(name)
	}

	err = res.Close()
	s.Require().NoError(err)

	// Testing struct
	rowS := struct {
		ID   primitive.ObjectID `bson:"_id"`
		Name string             `bson:"name"`
	}{}

	res = artist.Find()

	for res.Next(&rowS) {
		_, err := primitive.ObjectIDFromHex(rowS.ID.Hex())
		s.Require().NoError(err)

		s.NotZero(rowS.Name)
	}

	err = res.Close()
	s.Require().NoError(err)

	// Testing tagged struct
	rowT := struct {
		Value1 primitive.ObjectID `bson:"_id"`
		Value2 string             `bson:"name"`
	}{}

	res = artist.Find()

	for res.Next(&rowT) {
		_, err := primitive.ObjectIDFromHex(rowT.Value1.Hex())
		s.Require().NoError(err)

		s.NotZero(rowT.Value2)
	}

	err = res.Close()
	s.Require().NoError(err)

	// Testing Result.All() with a slice of maps.
	res = artist.Find()

	allRowsM := []map[string]interface{}{}
	err = res.All(&allRowsM)
	s.Require().NoError(err)

	for _, singleRowM := range allRowsM {
		s.NotZero(singleRowM["_id"])
	}

	// Testing Result.All() with a slice of structs.
	res = artist.Find()

	allRowsS := []struct {
		ID   primitive.ObjectID `bson:"_id"`
		Name string
	}{}
	err = res.All(&allRowsS)
	s.Require().NoError(err)

	for _, singleRowS := range allRowsS {
		_, err := primitive.ObjectIDFromHex(singleRowS.ID.Hex())
		s.Require().NoError(err)
	}

	// Testing Result.All() with a slice of tagged structs.
	res = artist.Find()

	allRowsT := []struct {
		Value1 primitive.ObjectID `bson:"_id"`
		Value2 string             `bson:"name"`
	}{}
	err = res.All(&allRowsT)
	s.Require().NoError(err)

	for _, singleRowT := range allRowsT {
		_, err := primitive.ObjectIDFromHex(singleRowT.Value1.Hex())
		s.Require().NoError(err)
	}
}

func (s *AdapterTests) TestUpdate() {
	// Opening database.
	sess, err := Open(settings)
	s.Require().NoError(err)

	// We should close the database when it's no longer in use.
	defer sess.Close()

	// Getting a pointer to the "artist" collection.
	artist := sess.Collection("artist")

	// Value
	value := struct {
		ID   primitive.ObjectID `bson:"_id"`
		Name string
	}{}

	// Getting the first artist.
	res := artist.Find(db.Cond{"_id": db.NotEq(nil)}).Limit(1)

	err = res.One(&value)
	s.Require().NoError(err)

	// Updating with a map
	rowM := map[string]interface{}{
		"name": strings.ToUpper(value.Name),
	}

	err = res.Update(rowM)
	s.Require().NoError(err)

	err = res.One(&value)
	s.Require().NoError(err)

	s.Equal(value.Name, rowM["name"])

	// Updating with a struct
	rowS := struct {
		Name string
	}{strings.ToLower(value.Name)}

	err = res.Update(rowS)
	s.Require().NoError(err)

	err = res.One(&value)
	s.Require().NoError(err)

	s.Equal(value.Name, rowS.Name)

	// Updating with a tagged struct
	rowT := struct {
		Value1 string `bson:"name"`
	}{strings.Replace(value.Name, "z", "Z", -1)}

	err = res.Update(rowT)
	s.Require().NoError(err)

	err = res.One(&value)
	s.Require().NoError(err)

	s.Equal(value.Name, rowT.Value1)
}

func (s *AdapterTests) TestOperators() {
	// Opening database.
	sess, err := Open(settings)
	s.Require().NoError(err)

	// We should close the database when it's no longer in use.
	defer sess.Close()

	// Getting a pointer to the "artist" collection.
	artist := sess.Collection("artist")

	rowS := struct {
		ID   uint64
		Name string
	}{}

	res := artist.Find(db.Cond{"_id": db.NotIn(0, -1)})

	err = res.One(&rowS)
	s.Require().NoError(err)

	err = res.Close()
	s.Require().NoError(err)
}

func (s *AdapterTests) TestDelete() {
	// Opening database.
	sess, err := Open(settings)
	s.Require().NoError(err)

	// We should close the database when it's no longer in use.
	defer sess.Close()

	// Getting a pointer to the "artist" collection.
	artist := sess.Collection("artist")

	// Getting the first artist.
	res := artist.Find(db.Cond{"_id": db.NotEq(nil)}).Limit(1)

	var first struct {
		ID primitive.ObjectID `bson:"_id"`
	}

	err = res.One(&first)
	s.Require().NoError(err)

	res = artist.Find(db.Cond{"_id": db.Eq(first.ID)})

	// Trying to remove the row.
	err = res.Delete()
	s.Require().NoError(err)
}

func (s *AdapterTests) TestDataTypes() {
	// Opening database.
	sess, err := Open(settings)
	s.Require().NoError(err)

	// We should close the database when it's no longer in use.
	defer sess.Close()

	// Getting a pointer to the "data_types" collection.
	dataTypes := sess.Collection("data_types")

	// Inserting our test subject.
	record, err := dataTypes.Insert(testValues)
	s.Require().NoError(err)

	id := record.ID()
	s.NotZero(id)

	// Trying to get the same subject we added.
	res := dataTypes.Find(db.Cond{"_id": db.Eq(id)})

	exists, err := res.Count()
	s.Require().NoError(err)
	s.NotZero(exists)

	// Trying to dump the subject into an empty structure of the same type.
	var item testValuesStruct
	err = res.One(&item)
	s.Require().NoError(err)

	// Convert dates to local time for comparison.
	testValues.Date = testValues.Date.Local()
	item.Date = item.Date.Local()
	*item.DateP = item.DateP.Local()

	// The original value and the test subject must match.
	s.Equal(testValues, item)
}

func (s *AdapterTests) TestPaginator() {
	// Opening database.
	sess, err := Open(settings)
	s.Require().NoError(err)

	// We should close the database when it's no longer in use.
	defer sess.Close()

	// Getting a pointer to the "artist" collection.
	artist := sess.Collection("artist")

	err = artist.Truncate()
	s.Require().NoError(err)

	for i := 0; i < 999; i++ {
		_, err = artist.Insert(artistType{
			Name: fmt.Sprintf("artist-%d", i),
		})
		s.Require().NoError(err)
	}

	q := sess.Collection("artist").Find().Paginate(15)
	paginator := q.Paginate(13)

	var zerothPage []artistType
	err = paginator.Page(0).All(&zerothPage)
	s.Require().NoError(err)
	s.Equal(13, len(zerothPage))

	var secondPage []artistType
	err = paginator.Page(2).All(&secondPage)
	s.Require().NoError(err)
	s.Equal(13, len(secondPage))

	tp, err := paginator.TotalPages()
	s.Require().NoError(err)
	s.NotZero(tp)
	s.Equal(uint(77), tp)

	ti, err := paginator.TotalEntries()
	s.Require().NoError(err)
	s.NotZero(ti)
	s.Equal(uint64(999), ti)

	var seventySixthPage []artistType
	err = paginator.Page(76).All(&seventySixthPage)
	s.Require().NoError(err)
	s.Equal(11, len(seventySixthPage))

	var seventySeventhPage []artistType
	err = paginator.Page(77).All(&seventySeventhPage)
	s.Require().NoError(err)
	s.Equal(0, len(seventySeventhPage))

	var hundredthPage []artistType
	err = paginator.Page(100).All(&hundredthPage)
	s.Require().NoError(err)
	s.Equal(0, len(hundredthPage))

	for i := uint(0); i < tp; i++ {
		current := paginator.Page(i)

		var items []artistType
		err := current.All(&items)
		s.Require().NoError(err)
		if len(items) < 1 {
			break
		}
		for j := 0; j < len(items); j++ {
			s.Equal(fmt.Sprintf("artist-%d", int64(13*int(i)+j)), items[j].Name)
		}
	}

	paginator = paginator.Cursor("_id")
	{
		current := paginator.Page(0)
		for i := 0; ; i++ {
			var items []artistType
			err := current.All(&items)
			s.Require().NoError(err)

			if len(items) < 1 {
				break
			}

			for j := 0; j < len(items); j++ {
				s.Equal(fmt.Sprintf("artist-%d", int64(13*int(i)+j)), items[j].Name)
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
			s.Require().NoError(err)

			if len(items) < 1 {
				s.Equal(0, len(items))
				break
			}
			for j := 0; j < len(items); j++ {
				s.Equal(fmt.Sprintf("artist-%d", 13*int(i)+j), items[j].Name)
			}

			current = current.PrevPage(items[0].ID)
		}
	}

	{
		resultPaginator := sess.Collection("artist").Find().Paginate(15)

		count, err := resultPaginator.TotalPages()
		s.Equal(uint(67), count)
		s.Require().NoError(err)

		var items []artistType
		err = resultPaginator.Page(5).All(&items)
		s.Require().NoError(err)

		for j := 0; j < len(items); j++ {
			s.Equal(fmt.Sprintf("artist-%d", 15*5+j), items[j].Name)
		}

		resultPaginator = resultPaginator.Cursor("_id").Page(0)
		for i := 0; ; i++ {
			var items []artistType

			err = resultPaginator.All(&items)
			s.Require().NoError(err)

			if len(items) < 1 {
				break
			}

			for j := 0; j < len(items); j++ {
				s.Equal(fmt.Sprintf("artist-%d", 15*i+j), items[j].Name)
			}
			resultPaginator = resultPaginator.NextPage(items[len(items)-1].ID)
		}

		resultPaginator = resultPaginator.Cursor("_id").Page(66)
		for i := 66; ; i-- {
			var items []artistType

			err = resultPaginator.All(&items)
			s.Require().NoError(err)

			if len(items) < 1 {
				break
			}

			for j := 0; j < len(items); j++ {
				s.Equal(fmt.Sprintf("artist-%d", 15*i+j), items[j].Name)
			}
			resultPaginator = resultPaginator.PrevPage(items[0].ID)
		}
	}
}

func TestAdapter(t *testing.T) {
	suite.Run(t, &AdapterTests{})
}
