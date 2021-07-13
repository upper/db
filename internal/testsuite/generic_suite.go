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

package testsuite

import (
	"database/sql/driver"
	"time"

	"github.com/stretchr/testify/suite"
	db "github.com/upper/db/v4"
	"gopkg.in/mgo.v2/bson"
)

type birthday struct {
	Name   string         `db:"name"`
	Born   time.Time      `db:"born"`
	BornUT *unixTimestamp `db:"born_ut,omitempty"`
	OmitMe bool           `json:"omit_me" db:"-" bson:"-"`
}

type fibonacci struct {
	Input  uint64 `db:"input"`
	Output uint64 `db:"output"`
	// Test for BSON option.
	OmitMe bool `json:"omit_me" db:"omit_me,bson,omitempty" bson:"omit_me,omitempty"`
}

type oddEven struct {
	// Test for JSON option.
	Input int `json:"input" db:"input"`
	// Test for JSON option.
	// The "bson" tag is required by mgo.
	IsEven bool `json:"is_even" db:"is_even,json" bson:"is_even"`
	OmitMe bool `json:"omit_me" db:"-" bson:"-"`
}

// Struct that relies on explicit mapping.
type mapE struct {
	ID       uint          `db:"id,omitempty" bson:"-"`
	MongoID  bson.ObjectId `db:"-" bson:"_id,omitempty"`
	CaseTest string        `db:"case_test" bson:"case_test"`
}

// Struct that will fallback to default mapping.
type mapN struct {
	ID        uint          `db:"id,omitempty"`
	MongoID   bson.ObjectId `db:"-" bson:"_id,omitempty"`
	Case_TEST string        `db:"case_test"`
}

// Struct for testing marshalling.
type unixTimestamp struct {
	// Time is handled internally as time.Time but saved as an (integer) unix
	// timestamp.
	value time.Time
}

func (u unixTimestamp) Value() (driver.Value, error) {
	return u.value.UTC().Unix(), nil
}

func (u *unixTimestamp) Scan(v interface{}) error {
	var unixTime int64

	switch t := v.(type) {
	case int64:
		unixTime = t
	case nil:
		return nil
	default:
		return db.ErrUnsupportedValue
	}

	t := time.Unix(unixTime, 0).In(time.UTC)
	*u = unixTimestamp{t}

	return nil
}

func newUnixTimestamp(t time.Time) *unixTimestamp {
	return &unixTimestamp{t.UTC()}
}

func even(i int) bool {
	return i%2 == 0
}

func fib(i uint64) uint64 {
	if i == 0 {
		return 0
	} else if i == 1 {
		return 1
	}
	return fib(i-1) + fib(i-2)
}

type GenericTestSuite struct {
	suite.Suite

	Helper
}

func (s *GenericTestSuite) AfterTest(suiteName, testName string) {
	err := s.TearDown()
	s.NoError(err)
}

func (s *GenericTestSuite) BeforeTest(suiteName, testName string) {
	err := s.TearUp()
	s.NoError(err)
}

func (s *GenericTestSuite) TestDatesAndUnicode() {
	sess := s.Session()

	testTimeZone := time.Local
	switch s.Adapter() {
	case "mysql", "cockroachdb", "postgresql":
		testTimeZone = defaultTimeLocation
	case "sqlite", "ql", "mssql":
		testTimeZone = time.UTC
	}

	born := time.Date(1941, time.January, 5, 0, 0, 0, 0, testTimeZone)

	controlItem := birthday{
		Name:   "Hayao Miyazaki",
		Born:   born,
		BornUT: newUnixTimestamp(born),
	}

	col := sess.Collection(`birthdays`)

	record, err := col.Insert(controlItem)
	s.NoError(err)
	s.NotZero(record.ID())

	var res db.Result
	switch s.Adapter() {
	case "mongo":
		res = col.Find(db.Cond{"_id": record.ID().(bson.ObjectId)})
	case "ql":
		res = col.Find(db.Cond{"id()": record.ID()})
	default:
		res = col.Find(db.Cond{"id": record.ID()})
	}

	var total uint64
	total, err = res.Count()
	s.NoError(err)
	s.Equal(uint64(1), total)

	switch s.Adapter() {
	case "mongo":
		s.T().Skip()
	}

	var testItem birthday
	err = res.One(&testItem)
	s.NoError(err)

	switch s.Adapter() {
	case "sqlite", "ql", "mssql":
		testItem.Born = testItem.Born.In(time.UTC)
	}
	s.Equal(controlItem.Born, testItem.Born)

	s.Equal(controlItem.BornUT, testItem.BornUT)
	s.Equal(controlItem, testItem)

	var testItems []birthday
	err = res.All(&testItems)
	s.NoError(err)
	s.NotZero(len(testItems))

	for _, testItem = range testItems {
		switch s.Adapter() {
		case "sqlite", "ql", "mssql":
			testItem.Born = testItem.Born.In(time.UTC)
		}
		s.Equal(controlItem, testItem)
	}

	controlItem.Name = `宮崎駿`
	err = res.Update(controlItem)
	s.NoError(err)

	err = res.One(&testItem)
	s.NoError(err)

	switch s.Adapter() {
	case "sqlite", "ql", "mssql":
		testItem.Born = testItem.Born.In(time.UTC)
	}

	s.Equal(controlItem, testItem)

	err = res.Delete()
	s.NoError(err)

	total, err = res.Count()
	s.NoError(err)
	s.Zero(total)

	err = res.Close()
	s.NoError(err)
}

func (s *GenericTestSuite) TestFibonacci() {
	var err error
	var res db.Result
	var total uint64

	sess := s.Session()

	col := sess.Collection("fibonacci")

	// Adding some items.
	var i uint64
	for i = 0; i < 10; i++ {
		item := fibonacci{Input: i, Output: fib(i)}
		_, err = col.Insert(item)
		s.NoError(err)
	}

	// Testing sorting by function.
	res = col.Find(
		// 5, 6, 7, 3
		db.Or(
			db.And(
				db.Cond{"input": db.Gte(5)},
				db.Cond{"input": db.Lte(7)},
			),
			db.Cond{"input": db.Eq(3)},
		),
	)

	// Testing sort by function.
	switch s.Adapter() {
	case "postgresql":
		res = res.OrderBy(db.Raw("RANDOM()"))
	case "sqlite":
		res = res.OrderBy(db.Raw("RANDOM()"))
	case "mysql":
		res = res.OrderBy(db.Raw("RAND()"))
	case "mssql":
		res = res.OrderBy(db.Raw("NEWID()"))
	}

	total, err = res.Count()
	s.NoError(err)
	s.Equal(uint64(4), total)

	// Find() with IN/$in
	res = col.Find(db.Cond{"input IN": []int{3, 5, 6, 7}}).OrderBy("input")

	total, err = res.Count()
	s.NoError(err)
	s.Equal(uint64(4), total)

	res = res.Offset(1).Limit(2)

	var item fibonacci
	for res.Next(&item) {
		switch item.Input {
		case 5:
		case 6:
			s.Equal(fib(item.Input), item.Output)
		default:
			s.T().Errorf(`Unexpected item: %v.`, item)
		}
	}
	s.NoError(res.Err())

	// Find() with range
	res = col.Find(
		// 5, 6, 7, 3
		db.Or(
			db.And(
				db.Cond{"input >=": 5},
				db.Cond{"input <=": 7},
			),
			db.Cond{"input": 3},
		),
	).OrderBy("-input")

	total, err = res.Count()
	s.NoError(err)
	s.Equal(uint64(4), total)

	// Skipping.
	res = res.Offset(1).Limit(2)

	var item2 fibonacci
	for res.Next(&item2) {
		switch item2.Input {
		case 5:
		case 6:
			s.Equal(fib(item2.Input), item2.Output)
		default:
			s.T().Errorf(`Unexpected item: %v.`, item2)
		}
	}
	err = res.Err()
	s.NoError(err)

	err = res.Delete()
	s.NoError(err)

	{
		total, err := res.Count()
		s.NoError(err)
		s.Zero(total)
	}

	// Find() with no arguments.
	res = col.Find()
	{
		total, err := res.Count()
		s.NoError(err)
		s.Equal(uint64(6), total)
	}

	// Skipping mongodb as the results of this are not defined there.
	if s.Adapter() != `mongo` {
		// Find() with empty db.Cond.
		{
			total, err := col.Find(db.Cond{}).Count()
			s.NoError(err)
			s.Equal(uint64(6), total)
		}

		// Find() with empty expression
		{
			total, err := col.Find(db.Or(db.And(db.Cond{}, db.Cond{}), db.Or(db.Cond{}))).Count()
			s.NoError(err)
			s.Equal(uint64(6), total)
		}

		// Find() with explicit IS NULL
		{
			total, err := col.Find(db.Cond{"input IS": nil}).Count()
			s.NoError(err)
			s.Equal(uint64(0), total)
		}

		// Find() with implicit IS NULL
		{
			total, err := col.Find(db.Cond{"input": nil}).Count()
			s.NoError(err)
			s.Equal(uint64(0), total)
		}

		// Find() with explicit = NULL
		{
			total, err := col.Find(db.Cond{"input =": nil}).Count()
			s.NoError(err)
			s.Equal(uint64(0), total)
		}

		// Find() with implicit IN
		{
			total, err := col.Find(db.Cond{"input": []int{1, 2, 3, 4}}).Count()
			s.NoError(err)
			s.Equal(uint64(3), total)
		}

		// Find() with implicit NOT IN
		{
			total, err := col.Find(db.Cond{"input NOT IN": []int{1, 2, 3, 4}}).Count()
			s.NoError(err)
			s.Equal(uint64(3), total)
		}
	}

	var items []fibonacci
	err = res.All(&items)
	s.NoError(err)

	for _, item := range items {
		switch item.Input {
		case 0:
		case 1:
		case 2:
		case 4:
		case 8:
		case 9:
			s.Equal(fib(item.Input), item.Output)
		default:
			s.T().Errorf(`Unexpected item: %v`, item)
		}
	}

	err = res.Close()
	s.NoError(err)
}

func (s *GenericTestSuite) TestOddEven() {
	sess := s.Session()

	col := sess.Collection("is_even")

	// Adding some items.
	var i int
	for i = 1; i < 100; i++ {
		item := oddEven{Input: i, IsEven: even(i)}
		_, err := col.Insert(item)
		s.NoError(err)
	}

	// Retrieving items
	res := col.Find(db.Cond{"is_even": true})

	var item oddEven
	for res.Next(&item) {
		s.Zero(item.Input % 2)
	}

	err := res.Err()
	s.NoError(err)

	err = res.Delete()
	s.NoError(err)

	// Testing named inputs (using tags).
	res = col.Find()

	var item2 struct {
		Value uint `db:"input" bson:"input"` // The "bson" tag is required by mgo.
	}
	for res.Next(&item2) {
		s.NotZero(item2.Value % 2)
	}
	err = res.Err()
	s.NoError(err)

	// Testing inline tag.
	res = col.Find()

	var item3 struct {
		OddEven oddEven `db:",inline" bson:",inline"`
	}
	for res.Next(&item3) {
		s.NotZero(item3.OddEven.Input % 2)
		s.NotZero(item3.OddEven.Input)
	}
	err = res.Err()
	s.NoError(err)

	// Testing inline tag.
	type OddEven oddEven
	res = col.Find()

	var item31 struct {
		OddEven `db:",inline" bson:",inline"`
	}
	for res.Next(&item31) {
		s.NotZero(item31.Input % 2)
		s.NotZero(item31.Input)
	}
	s.NoError(res.Err())

	// Testing omision tag.
	res = col.Find()

	var item4 struct {
		Value uint `db:"-"`
	}
	for res.Next(&item4) {
		s.Zero(item4.Value)
	}
	s.NoError(res.Err())
}

func (s *GenericTestSuite) TestExplicitAndDefaultMapping() {
	var err error
	var res db.Result

	var testE mapE
	var testN mapN

	sess := s.Session()

	col := sess.Collection("CaSe_TesT")

	if err = col.Truncate(); err != nil {
		if s.Adapter() != "mongo" {
			s.NoError(err)
		}
	}

	// Testing explicit mapping.
	testE = mapE{
		CaseTest: "Hello!",
	}

	_, err = col.Insert(testE)
	s.NoError(err)

	res = col.Find(db.Cond{"case_test": "Hello!"})
	if s.Adapter() == "ql" {
		res = res.Select("id() as id", "case_test")
	}

	err = res.One(&testE)
	s.NoError(err)

	if s.Adapter() == "mongo" {
		s.True(testE.MongoID.Valid())
	} else {
		s.NotZero(testE.ID)
	}

	// Testing default mapping.
	testN = mapN{
		Case_TEST: "World!",
	}

	_, err = col.Insert(testN)
	s.NoError(err)

	if s.Adapter() == `mongo` {
		res = col.Find(db.Cond{"case_test": "World!"})
	} else {
		res = col.Find(db.Cond{"case_test": "World!"})
	}

	if s.Adapter() == `ql` {
		res = res.Select(`id() as id`, `case_test`)
	}

	err = res.One(&testN)
	s.NoError(err)

	if s.Adapter() == `mongo` {
		s.True(testN.MongoID.Valid())
	} else {
		s.NotZero(testN.ID)
	}
}

func (s *GenericTestSuite) TestComparisonOperators() {
	sess := s.Session()

	birthdays := sess.Collection("birthdays")
	err := birthdays.Truncate()
	if err != nil {
		if s.Adapter() != "mongo" {
			s.NoError(err)
		}
	}

	// Insert data for testing
	birthdaysDataset := []birthday{
		{
			Name: "Marie Smith",
			Born: time.Date(1956, time.August, 5, 0, 0, 0, 0, defaultTimeLocation),
		},
		{
			Name: "Peter",
			Born: time.Date(1967, time.July, 23, 0, 0, 0, 0, defaultTimeLocation),
		},
		{
			Name: "Eve Smith",
			Born: time.Date(1911, time.February, 8, 0, 0, 0, 0, defaultTimeLocation),
		},
		{
			Name: "Alex López",
			Born: time.Date(2001, time.May, 5, 0, 0, 0, 0, defaultTimeLocation),
		},
		{
			Name: "Rose Smith",
			Born: time.Date(1944, time.December, 9, 0, 0, 0, 0, defaultTimeLocation),
		},
		{
			Name: "Daria López",
			Born: time.Date(1923, time.March, 23, 0, 0, 0, 0, defaultTimeLocation),
		},
		{
			Name: "",
			Born: time.Date(1945, time.December, 1, 0, 0, 0, 0, defaultTimeLocation),
		},
		{
			Name: "Colin",
			Born: time.Date(2010, time.May, 6, 0, 0, 0, 0, defaultTimeLocation),
		},
	}
	for _, birthday := range birthdaysDataset {
		_, err := birthdays.Insert(birthday)
		s.NoError(err)
	}

	// Test: equal
	{
		var item birthday
		err := birthdays.Find(db.Cond{
			"name": db.Eq("Colin"),
		}).One(&item)
		s.NoError(err)
		s.NotNil(item)

		s.Equal("Colin", item.Name)
	}

	// Test: not equal
	{
		var item birthday
		err := birthdays.Find(db.Cond{
			"name": db.NotEq("Colin"),
		}).One(&item)
		s.NoError(err)
		s.NotNil(item)

		s.NotEqual("Colin", item.Name)
	}

	// Test: greater than
	{
		var items []birthday
		ref := time.Date(1967, time.July, 23, 0, 0, 0, 0, defaultTimeLocation)
		err := birthdays.Find(db.Cond{
			"born": db.Gt(ref),
		}).All(&items)
		s.NoError(err)
		s.NotZero(len(items))
		s.Equal(2, len(items))
		for _, item := range items {
			s.True(item.Born.After(ref))
		}
	}
	return

	// Test: less than
	{
		var items []birthday
		ref := time.Date(1967, time.July, 23, 0, 0, 0, 0, defaultTimeLocation)
		err := birthdays.Find(db.Cond{
			"born": db.Lt(ref),
		}).All(&items)
		s.NoError(err)
		s.NotZero(len(items))
		s.Equal(5, len(items))
		for _, item := range items {
			s.True(item.Born.Before(ref))
		}
	}

	// Test: greater than or equal to
	{
		var items []birthday
		ref := time.Date(1967, time.July, 23, 0, 0, 0, 0, defaultTimeLocation)
		err := birthdays.Find(db.Cond{
			"born": db.Gte(ref),
		}).All(&items)
		s.NoError(err)
		s.NotZero(len(items))
		s.Equal(3, len(items))
		for _, item := range items {
			s.True(item.Born.After(ref) || item.Born.Equal(ref))
		}
	}

	// Test: less than or equal to
	{
		var items []birthday
		ref := time.Date(1967, time.July, 23, 0, 0, 0, 0, defaultTimeLocation)
		err := birthdays.Find(db.Cond{
			"born": db.Lte(ref),
		}).All(&items)
		s.NoError(err)
		s.NotZero(len(items))
		s.Equal(6, len(items))
		for _, item := range items {
			s.True(item.Born.Before(ref) || item.Born.Equal(ref))
		}
	}

	// Test: between
	{
		var items []birthday
		dateA := time.Date(1911, time.February, 8, 0, 0, 0, 0, defaultTimeLocation)
		dateB := time.Date(1967, time.July, 23, 0, 0, 0, 0, defaultTimeLocation)
		err := birthdays.Find(db.Cond{
			"born": db.Between(dateA, dateB),
		}).All(&items)
		s.NoError(err)
		s.Equal(6, len(items))
		for _, item := range items {
			s.True(item.Born.After(dateA) || item.Born.Equal(dateA))
			s.True(item.Born.Before(dateB) || item.Born.Equal(dateB))
		}
	}

	// Test: not between
	{
		var items []birthday
		dateA := time.Date(1911, time.February, 8, 0, 0, 0, 0, defaultTimeLocation)
		dateB := time.Date(1967, time.July, 23, 0, 0, 0, 0, defaultTimeLocation)
		err := birthdays.Find(db.Cond{
			"born": db.NotBetween(dateA, dateB),
		}).All(&items)
		s.NoError(err)
		s.Equal(2, len(items))
		for _, item := range items {
			s.False(item.Born.Before(dateA) || item.Born.Equal(dateA))
			s.False(item.Born.Before(dateB) || item.Born.Equal(dateB))
		}
	}

	// Test: in
	{
		var items []birthday
		names := []interface{}{"Peter", "Eve Smith", "Daria López", "Alex López"}
		err := birthdays.Find(db.Cond{
			"name": db.In(names...),
		}).All(&items)
		s.NoError(err)
		s.Equal(4, len(items))
		for _, item := range items {
			inArray := false
			for _, name := range names {
				if name == item.Name {
					inArray = true
				}
			}
			s.True(inArray)
		}
	}

	// Test: not in
	{
		var items []birthday
		names := []interface{}{"Peter", "Eve Smith", "Daria López", "Alex López"}
		err := birthdays.Find(db.Cond{
			"name": db.NotIn(names...),
		}).All(&items)
		s.NoError(err)
		s.Equal(4, len(items))
		for _, item := range items {
			inArray := false
			for _, name := range names {
				if name == item.Name {
					inArray = true
				}
			}
			s.False(inArray)
		}
	}

	// Test: not in
	{
		var items []birthday
		names := []interface{}{"Peter", "Eve Smith", "Daria López", "Alex López"}
		err := birthdays.Find(db.Cond{
			"name": db.NotIn(names...),
		}).All(&items)
		s.NoError(err)
		s.Equal(4, len(items))
		for _, item := range items {
			inArray := false
			for _, name := range names {
				if name == item.Name {
					inArray = true
				}
			}
			s.False(inArray)
		}
	}

	// Test: is and is not
	{
		var items []birthday
		err := birthdays.Find(db.And(
			db.Cond{"name": db.Is(nil)},
			db.Cond{"name": db.IsNot(nil)},
		)).All(&items)
		s.NoError(err)
		s.Equal(0, len(items))
	}

	// Test: is nil
	{
		var items []birthday
		err := birthdays.Find(db.And(
			db.Cond{"born_ut": db.IsNull()},
		)).All(&items)
		s.NoError(err)
		s.Equal(8, len(items))
	}

	// Test: like and not like
	{
		var items []birthday
		var q db.Result

		switch s.Adapter() {
		case "ql", "mongo":
			q = birthdays.Find(db.And(
				db.Cond{"name": db.Like(".*ari.*")},
				db.Cond{"name": db.NotLike(".*Smith")},
			))
		default:
			q = birthdays.Find(db.And(
				db.Cond{"name": db.Like("%ari%")},
				db.Cond{"name": db.NotLike("%Smith")},
			))
		}

		err := q.All(&items)
		s.NoError(err)
		s.Equal(1, len(items))

		s.Equal("Daria López", items[0].Name)
	}

	if s.Adapter() != "sqlite" && s.Adapter() != "mssql" {
		// Test: regexp
		{
			var items []birthday
			err := birthdays.Find(db.And(
				db.Cond{"name": db.RegExp("^[D|C|M]")},
			)).OrderBy("name").All(&items)
			s.NoError(err)
			s.Equal(3, len(items))

			s.Equal("Colin", items[0].Name)
			s.Equal("Daria López", items[1].Name)
			s.Equal("Marie Smith", items[2].Name)
		}

		// Test: not regexp
		{
			var items []birthday
			names := []string{"Daria López", "Colin", "Marie Smith"}
			err := birthdays.Find(db.And(
				db.Cond{"name": db.NotRegExp("^[D|C|M]")},
			)).OrderBy("name").All(&items)
			s.NoError(err)
			s.Equal(5, len(items))

			for _, item := range items {
				for _, name := range names {
					s.NotEqual(item.Name, name)
				}
			}
		}
	}

	// Test: after
	{
		ref := time.Date(1944, time.December, 9, 0, 0, 0, 0, defaultTimeLocation)
		var items []birthday
		err := birthdays.Find(db.Cond{
			"born": db.After(ref),
		}).All(&items)
		s.NoError(err)
		s.Equal(5, len(items))
	}

	// Test: on or after
	{
		ref := time.Date(1944, time.December, 9, 0, 0, 0, 0, defaultTimeLocation)
		var items []birthday
		err := birthdays.Find(db.Cond{
			"born": db.OnOrAfter(ref),
		}).All(&items)
		s.NoError(err)
		s.Equal(6, len(items))
	}

	// Test: before
	{
		ref := time.Date(1944, time.December, 9, 0, 0, 0, 0, defaultTimeLocation)
		var items []birthday
		err := birthdays.Find(db.Cond{
			"born": db.Before(ref),
		}).All(&items)
		s.NoError(err)
		s.Equal(2, len(items))
	}

	// Test: on or before
	{
		ref := time.Date(1944, time.December, 9, 0, 0, 0, 0, defaultTimeLocation)
		var items []birthday
		err := birthdays.Find(db.Cond{
			"born": db.OnOrBefore(ref),
		}).All(&items)
		s.NoError(err)
		s.Equal(3, len(items))
	}
}
