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

package db_test

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"upper.io/db.v3"
	"upper.io/db.v3/mongo"
	"upper.io/db.v3/mssql"
	"upper.io/db.v3/mysql"
	"upper.io/db.v3/postgresql"
	"upper.io/db.v3/ql"
	"upper.io/db.v3/sqlite"
)

var wrappers = []string{
	mongo.Adapter,
	mssql.Adapter,
	mysql.Adapter,
	postgresql.Adapter,
	ql.Adapter,
	sqlite.Adapter,
}

const (
	testAllWrappers = `all`
)

var (
	errDriverErr = errors.New(`Driver error`)
)

var settings map[string]db.ConnectionURL

func init() {

	// Getting settings from the environment.

	var host string
	if host = os.Getenv("DB_HOST"); host == "" {
		host = "localhost"
	}

	var wrapper string
	if wrapper = os.Getenv("WRAPPER"); wrapper == "" {
		wrapper = testAllWrappers
	}

	log.Printf("Running tests against host %s.\n", host)

	settings = map[string]db.ConnectionURL{
		`sqlite`: &sqlite.ConnectionURL{
			Database: `sqlite3-test.db`,
		},
		`mongo`: &mongo.ConnectionURL{
			Database: `upperio_tests`,
			Host:     host,
			User:     `upperio_tests`,
			Password: `upperio_secret`,
		},
		`mysql`: &mysql.ConnectionURL{
			Database: `upperio_tests`,
			Host:     host,
			User:     `upperio_tests`,
			Password: `upperio_secret`,
			Options: map[string]string{
				"parseTime": "true",
			},
		},
		`postgresql`: &postgresql.ConnectionURL{
			Database: `upperio_tests`,
			Host:     host,
			User:     `upperio_tests`,
			Password: `upperio_secret`,
			Options: map[string]string{
				"timezone": "UTC",
			},
		},
		`mssql`: &mssql.ConnectionURL{
			Database: `upperio_tests`,
			Host:     host,
			User:     `upperio_tests`,
			Password: `upperio_Secre3t`,
		},
		`ql`: &ql.ConnectionURL{
			Database: `ql-test.db`,
		},
	}

	if wrapper != testAllWrappers {
		wrappers = []string{wrapper}
		log.Printf("Testing wrapper %s.", wrapper)
	}

}

var setupFn = map[string]func(driver interface{}) error{
	`mongo`: func(driver interface{}) error {
		if mgod, ok := driver.(*mgo.Session); ok {
			var col *mgo.Collection
			col = mgod.DB("upperio_tests").C("birthdays")
			col.DropCollection()

			col = mgod.DB("upperio_tests").C("fibonacci")
			col.DropCollection()

			col = mgod.DB("upperio_tests").C("is_even")
			col.DropCollection()

			col = mgod.DB("upperio_tests").C("CaSe_TesT")
			col.DropCollection()
			return nil
		}
		return errDriverErr
	},
	`postgresql`: func(driver interface{}) error {
		if sqld, ok := driver.(*sql.DB); ok {
			var err error

			_, err = sqld.Exec(`DROP TABLE IF EXISTS "birthdays"`)
			if err != nil {
				return err
			}
			_, err = sqld.Exec(`CREATE TABLE "birthdays" (
					"id" serial primary key,
					"name" CHARACTER VARYING(50),
					"born" TIMESTAMP WITH TIME ZONE,
					"born_ut" INT
			)`)
			if err != nil {
				return err
			}

			_, err = sqld.Exec(`DROP TABLE IF EXISTS "fibonacci"`)
			if err != nil {
				return err
			}
			_, err = sqld.Exec(`CREATE TABLE "fibonacci" (
					"id" serial primary key,
					"input" NUMERIC,
					"output" NUMERIC
			)`)
			if err != nil {
				return err
			}

			_, err = sqld.Exec(`DROP TABLE IF EXISTS "is_even"`)
			if err != nil {
				return err
			}
			_, err = sqld.Exec(`CREATE TABLE "is_even" (
					"input" NUMERIC,
					"is_even" BOOL
			)`)
			if err != nil {
				return err
			}

			_, err = sqld.Exec(`DROP TABLE IF EXISTS "CaSe_TesT"`)
			if err != nil {
				return err
			}
			_, err = sqld.Exec(`CREATE TABLE "CaSe_TesT" (
					"id" SERIAL PRIMARY KEY,
					"case_test" VARCHAR(60)
			)`)
			if err != nil {
				return err
			}

			return nil
		}
		return fmt.Errorf("Expecting *sql.DB got %T (%#v).", driver, driver)
	},
	`mysql`: func(driver interface{}) error {
		if sqld, ok := driver.(*sql.DB); ok {
			var err error

			_, err = sqld.Exec(`DROP TABLE IF EXISTS ` + "`" + `birthdays` + "`" + ``)
			if err != nil {
				return err
			}
			_, err = sqld.Exec(`CREATE TABLE ` + "`" + `birthdays` + "`" + ` (
				id BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT, PRIMARY KEY(id),
				name VARCHAR(50),
				born DATE,
				born_ut BIGINT(20) SIGNED
			) CHARSET=utf8`)
			if err != nil {
				return err
			}

			_, err = sqld.Exec(`DROP TABLE IF EXISTS ` + "`" + `fibonacci` + "`" + ``)
			if err != nil {
				return err
			}
			_, err = sqld.Exec(`CREATE TABLE ` + "`" + `fibonacci` + "`" + ` (
				id BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT, PRIMARY KEY(id),
				input BIGINT(20) UNSIGNED NOT NULL,
				output BIGINT(20) UNSIGNED NOT NULL
			) CHARSET=utf8`)
			if err != nil {
				return err
			}

			_, err = sqld.Exec(`DROP TABLE IF EXISTS ` + "`" + `is_even` + "`" + ``)
			if err != nil {
				return err
			}
			_, err = sqld.Exec(`CREATE TABLE ` + "`" + `is_even` + "`" + ` (
				input BIGINT(20) UNSIGNED NOT NULL,
				is_even TINYINT(1)
			) CHARSET=utf8`)
			if err != nil {
				return err
			}

			_, err = sqld.Exec(`DROP TABLE IF EXISTS ` + "`" + `CaSe_TesT` + "`" + ``)
			if err != nil {
				return err
			}
			_, err = sqld.Exec(`CREATE TABLE ` + "`" + `CaSe_TesT` + "`" + ` (
				id BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT, PRIMARY KEY(id),
				case_test VARCHAR(60)
			) CHARSET=utf8`)
			if err != nil {
				return err
			}

			return nil
		}
		return fmt.Errorf("Expecting *sql.DB got %T (%#v).", driver, driver)
	},
	`mssql`: func(driver interface{}) error {
		if sqld, ok := driver.(*sql.DB); ok {
			var err error

			_, err = sqld.Exec(`DROP TABLE IF EXISTS [birthdays]`)
			if err != nil {
				return err
			}
			_, err = sqld.Exec(`CREATE TABLE [birthdays] (
				id BIGINT PRIMARY KEY NOT NULL IDENTITY(1,1),
				name NVARCHAR(50),
				born DATETIME,
				born_ut BIGINT
			)`)
			if err != nil {
				return err
			}

			_, err = sqld.Exec(`DROP TABLE IF EXISTS [fibonacci]`)
			if err != nil {
				return err
			}
			_, err = sqld.Exec(`CREATE TABLE [fibonacci] (
				id BIGINT PRIMARY KEY NOT NULL IDENTITY(1,1),
				input BIGINT NOT NULL,
				output BIGINT NOT NULL
			)`)
			if err != nil {
				return err
			}

			_, err = sqld.Exec(`DROP TABLE IF EXISTS [is_even]`)
			if err != nil {
				return err
			}
			_, err = sqld.Exec(`CREATE TABLE [is_even] (
				input BIGINT NOT NULL,
				is_even TINYINT
			)`)
			if err != nil {
				return err
			}

			_, err = sqld.Exec(`DROP TABLE IF EXISTS [CaSe_TesT]`)
			if err != nil {
				return err
			}
			_, err = sqld.Exec(`CREATE TABLE [CaSe_TesT] (
				id BIGINT PRIMARY KEY NOT NULL IDENTITY(1,1),
				case_test NVARCHAR(60)
			)`)
			if err != nil {
				return err
			}

			return nil
		}
		return fmt.Errorf("Expecting *sql.DB got %T (%#v).", driver, driver)
	},
	`sqlite`: func(driver interface{}) error {
		if sqld, ok := driver.(*sql.DB); ok {
			var err error

			_, err = sqld.Exec(`DROP TABLE IF EXISTS "birthdays"`)
			if err != nil {
				return err
			}
			_, err = sqld.Exec(`CREATE TABLE "birthdays" (
				"id" INTEGER PRIMARY KEY,
				"name" VARCHAR(50) DEFAULT NULL,
				"born" DATETIME DEFAULT NULL,
				"born_ut" INTEGER
			)`)
			if err != nil {
				return err
			}

			_, err = sqld.Exec(`DROP TABLE IF EXISTS "fibonacci"`)
			if err != nil {
				return err
			}
			_, err = sqld.Exec(`CREATE TABLE "fibonacci" (
				"id" INTEGER PRIMARY KEY,
				"input" INTEGER,
				"output" INTEGER
			)`)
			if err != nil {
				return err
			}

			_, err = sqld.Exec(`DROP TABLE IF EXISTS "is_even"`)
			if err != nil {
				return err
			}
			_, err = sqld.Exec(`CREATE TABLE "is_even" (
				"input" INTEGER,
				"is_even" INTEGER
			)`)
			if err != nil {
				return err
			}

			_, err = sqld.Exec(`DROP TABLE IF EXISTS "CaSe_TesT"`)
			if err != nil {
				return err
			}
			_, err = sqld.Exec(`CREATE TABLE "CaSe_TesT" (
				"id" INTEGER PRIMARY KEY,
				"case_test" VARCHAR
			)`)
			if err != nil {
				return err
			}

			return nil
		}
		return errDriverErr
	},
	`ql`: func(driver interface{}) error {
		if sqld, ok := driver.(*sql.DB); ok {
			var err error
			var tx *sql.Tx

			if tx, err = sqld.Begin(); err != nil {
				return err
			}

			_, err = tx.Exec(`DROP TABLE IF EXISTS birthdays`)
			if err != nil {
				return err
			}

			_, err = tx.Exec(`CREATE TABLE birthdays (
				name string,
				born time,
				born_ut int
			)`)
			if err != nil {
				return err
			}

			_, err = tx.Exec(`DROP TABLE IF EXISTS fibonacci`)
			if err != nil {
				return err
			}

			_, err = tx.Exec(`CREATE TABLE fibonacci (
				input int,
				output int
			)`)
			if err != nil {
				return err
			}

			_, err = tx.Exec(`DROP TABLE IF EXISTS is_even`)
			if err != nil {
				return err
			}

			_, err = tx.Exec(`CREATE TABLE is_even (
				input int,
				is_even bool
			)`)
			if err != nil {
				return err
			}

			_, err = tx.Exec(`DROP TABLE IF EXISTS CaSe_TesT`)
			if err != nil {
				return err
			}

			_, err = tx.Exec(`CREATE TABLE CaSe_TesT (
				case_test string
			)`)
			if err != nil {
				return err
			}

			if err = tx.Commit(); err != nil {
				return err
			}

			return nil
		}
		return errDriverErr
	},
}

type birthday struct {
	Name   string    `db:"name"`
	Born   time.Time `db:"born"`
	BornUT timeType  `db:"born_ut,omitempty"`
	OmitMe bool      `json:"omit_me" db:"-" bson:"-"`
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
type timeType struct {
	// Time is handled internally as time.Time but saved as an (integer) unix
	// timestamp.
	value time.Time
}

// time.Time -> unix timestamp
func (u timeType) MarshalDB() (interface{}, error) {
	return u.value.Unix(), nil
}

// unix timestamp -> time.Time
func (u *timeType) UnmarshalDB(v interface{}) error {
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
	*u = timeType{t}

	return nil
}

var (
	_ db.Marshaler   = timeType{}
	_ db.Unmarshaler = &timeType{}
)

func even(i int) bool {
	if i%2 == 0 {
		return true
	}
	return false
}

func fib(i uint64) uint64 {
	if i == 0 {
		return 0
	} else if i == 1 {
		return 1
	}
	return fib(i-1) + fib(i-2)
}

func TestOpen(t *testing.T) {
	var err error
	for _, wrapper := range wrappers {
		t.Logf("Testing wrapper: %q", wrapper)

		if settings[wrapper] == nil {
			t.Fatalf(`No such settings entry for wrapper %s.`, wrapper)
		} else {
			var sess db.Database
			sess, err = db.Open(wrapper, settings[wrapper])
			if err != nil {
				t.Fatalf(`Test for wrapper %s failed: %q`, wrapper, err)
			}
			err = sess.Close()
			if err != nil {
				t.Fatalf(`Test for wrapper %s failed: %q`, wrapper, err)
			}
		}
	}
}

func TestSetup(t *testing.T) {
	var err error
	for _, wrapper := range wrappers {
		t.Logf("Testing wrapper: %q", wrapper)

		if settings[wrapper] == nil {
			t.Fatalf(`No such settings entry for wrapper %s.`, wrapper)
		} else {
			var sess db.Database

			sess, err = db.Open(wrapper, settings[wrapper])
			if err != nil {
				t.Fatalf(`Test for wrapper %s failed: %q`, wrapper, err)
			}

			if setupFn[wrapper] == nil {
				t.Fatalf(`Missing setup function for wrapper %s.`, wrapper)
			} else {
				if err = setupFn[wrapper](sess.Driver()); err != nil {
					t.Fatalf(`Failed to setup wrapper %s: %q`, wrapper, err)
				}
			}

			err = sess.Close()
			if err != nil {
				t.Fatalf(`Could not close %s: %q`, wrapper, err)
			}

		}
	}
}

func TestSimpleCRUD(t *testing.T) {
	var err error

	var controlItem birthday

	for _, wrapper := range wrappers {
		if settings[wrapper] == nil {
			t.Fatalf(`No such settings entry for wrapper %s.`, wrapper)
		} else {

			t.Logf("Testing wrapper: %q", wrapper)

			var sess db.Database

			sess, err = db.Open(wrapper, settings[wrapper])
			if err != nil {
				t.Fatalf(`Test for wrapper %s failed: %q`, wrapper, err)
			}

			defer sess.Close()

			born := time.Date(1941, time.January, 5, 0, 0, 0, 0, time.UTC)

			controlItem = birthday{
				Name:   "Hayao Miyazaki",
				Born:   born,
				BornUT: timeType{born},
			}

			col := sess.Collection(`birthdays`)

			var id interface{}
			if id, err = col.Insert(controlItem); err != nil {
				t.Fatalf(`Could not append item with wrapper %s: %q`, wrapper, err)
			}

			var res db.Result
			switch wrapper {
			case `mongo`:
				res = col.Find(db.Cond{"_id": id.(bson.ObjectId)})
			case `ql`:
				res = col.Find(db.Cond{"id()": id})
			default:
				res = col.Find(db.Cond{"id": id})
			}

			var total uint64
			total, err = res.Count()

			if total != 1 {
				t.Fatalf("%s: Expecting one row.", wrapper)
			}

			// No support for Marshaler and Unmarshaler is implemeted for QL and
			// MongoDB.
			if wrapper == `ql` || wrapper == `mongo` {
				continue
			}

			var testItem birthday
			err = res.One(&testItem)
			if err != nil {
				t.Fatalf("%s One(): %s", wrapper, err)
			}

			if wrapper == `sqlite` {
				// SQLite does not save time zone info, so you have to do this by hand.
				testItem.Born = testItem.Born.In(time.UTC)
			}

			if reflect.DeepEqual(testItem, controlItem) == false {
				t.Errorf("%s: controlItem (inserted): %v (ts: %v)\n", wrapper, controlItem, controlItem.BornUT.value.Unix())
				t.Fatalf("%s: Structs are different", wrapper)
			}

			var testItems []birthday
			err = res.All(&testItems)
			if err != nil {
				t.Fatalf("%s All(): %s", wrapper, err)
			}

			if len(testItems) == 0 {
				t.Fatalf("%s All(): Expecting at least one row.", wrapper)
			}

			for _, testItem = range testItems {
				if wrapper == `sqlite` {
					// SQLite does not save time zone info, so you have to do this by hand.
					testItem.Born = testItem.Born.In(time.UTC)
				}
				if reflect.DeepEqual(testItem, controlItem) == false {
					t.Errorf("%s: testItem: %v\n", wrapper, testItem)
					t.Errorf("%s: controlItem: %v\n", wrapper, controlItem)
					t.Fatalf("%s: Structs are different", wrapper)
				}
			}

			controlItem.Name = `宮崎駿`
			err = res.Update(controlItem)

			if err != nil {
				t.Fatalf(`Could not update with wrapper %s: %q`, wrapper, err)
			}

			err = res.One(&testItem)
			if err != nil {
				t.Fatalf("%s One(): %s", wrapper, err)
			}

			if wrapper == `sqlite` {
				// SQLite does not save time zone info, so you have to do this by hand.
				testItem.Born = testItem.Born.In(time.UTC)
			}

			if reflect.DeepEqual(testItem, controlItem) == false {
				t.Fatalf("Struct is different with wrapper %s, got: %#v, expecting: %#v.", wrapper, testItem, controlItem)
			}

			err = res.Delete()

			if err != nil {
				t.Fatalf(`Could not remove with wrapper %s: %q`, wrapper, err)
			}

			total, err = res.Count()

			if total != 0 {
				t.Fatalf(`Expecting no items %s: %q`, wrapper, err)
			}

			err = res.Close()
			if err != nil {
				t.Errorf("Failed to close result %s: %q.", wrapper, err)
			}

			err = sess.Close()
			if err != nil {
				t.Errorf("Failed to close %s: %q.", wrapper, err)
			}

		}
	}
}

func TestFibonacci(t *testing.T) {
	var err error
	var res db.Result
	var total uint64

	for _, wrapper := range wrappers {
		t.Logf("Testing wrapper: %q", wrapper)

		if settings[wrapper] == nil {
			t.Fatalf(`No such settings entry for wrapper %s.`, wrapper)
		} else {

			var sess db.Database

			sess, err = db.Open(wrapper, settings[wrapper])
			if err != nil {
				t.Fatalf(`Test for wrapper %s failed: %q`, wrapper, err)
			}
			defer sess.Close()

			col := sess.Collection("fibonacci")

			// Adding some items.
			var i uint64
			for i = 0; i < 10; i++ {
				item := fibonacci{Input: i, Output: fib(i)}
				_, err = col.Insert(item)
				if err != nil {
					t.Fatalf(`Could not append item with wrapper %s: %q`, wrapper, err)
				}
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
			switch wrapper {
			case `postgresql`:
				res = res.OrderBy(db.Raw(`RANDOM()`))
			case `sqlite`:
				res = res.OrderBy(db.Raw(`RANDOM()`))
			case `mysql`:
				res = res.OrderBy(db.Raw(`RAND()`))
			case `sqlserver`:
				res = res.OrderBy(db.Raw(`NEWID()`))
			}

			total, err = res.Count()

			if err != nil {
				t.Fatalf(`%s: %q`, wrapper, err)
			}

			if total != 4 {
				t.Fatalf("%s: Expecting a count of 4, got %d.", wrapper, total)
			}

			// Find() with IN/$in
			res = col.Find(db.Cond{"input IN": []int{3, 5, 6, 7}}).OrderBy("input")

			total, err = res.Count()

			if err != nil {
				t.Fatalf(`%s: %q`, wrapper, err)
			}

			if total != 4 {
				t.Fatalf(`Expecting a count of 4.`)
			}

			res = res.Offset(1).Limit(2)

			var item fibonacci
			for res.Next(&item) {
				switch item.Input {
				case 5:
				case 6:
					if fib(item.Input) != item.Output {
						t.Fatalf(`Unexpected value in item with wrapper %s.`, wrapper)
					}
				default:
					t.Fatalf(`Unexpected item: %v with wrapper %s.`, item, wrapper)
				}
			}
			if err := res.Err(); err != nil {
				t.Fatalf(`%s: %q`, wrapper, err)
			}

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

			if total, err = res.Count(); err != nil {
				t.Fatalf(`%s: %q`, wrapper, err)
			}

			if total != 4 {
				t.Fatalf(`Expecting a count of 4.`)
			}

			// Skipping.
			res = res.Offset(1).Limit(2)

			var item2 fibonacci
			for res.Next(&item2) {
				switch item2.Input {
				case 5:
				case 6:
					if fib(item2.Input) != item2.Output {
						t.Fatalf(`Unexpected value in item2 with wrapper %s.`, wrapper)
					}
				default:
					t.Fatalf(`Unexpected item2: %v with wrapper %s.`, item2, wrapper)
				}
			}
			if err := res.Err(); err != nil {
				t.Fatalf(`%s: %q`, wrapper, err)
			}

			if err = res.Delete(); err != nil {
				t.Fatalf(`%s: %q`, wrapper, err)
			}

			if total, err = res.Count(); err != nil {
				t.Fatalf(`%s: %q`, wrapper, err)
			}

			if total != 0 {
				t.Fatalf(`%s: Unexpected count %d.`, wrapper, total)
			}

			// Find() with no arguments.
			res = col.Find()
			total, err = res.Count()

			if total != 6 {
				t.Fatalf(`%s: Unexpected count %d.`, wrapper, total)
			}

			// Skipping mongodb as the results of this are not defined there.
			if wrapper != `mongo` {

				// Find() with empty db.Cond.
				res1 := col.Find(db.Cond{})
				total, err = res1.Count()

				if total != 6 {
					t.Fatalf(`%s: Unexpected count %d.`, wrapper, total)
				}

				// Find() with empty expression
				res1b := col.Find(db.Or(db.And(db.Cond{}, db.Cond{}), db.Or(db.Cond{})))
				total, err = res1b.Count()

				if total != 6 {
					t.Fatalf(`%s: Unexpected count %d.`, wrapper, total)
				}

				// Find() with explicit IS NULL
				res2 := col.Find(db.Cond{"input IS": nil})
				total, err = res2.Count()

				if total != 0 {
					t.Fatalf(`%s: Unexpected count %d.`, wrapper, total)
				}

				// Find() with implicit IS NULL
				res2a := col.Find(db.Cond{"input": nil})
				total, err = res2a.Count()

				if total != 0 {
					t.Fatalf(`%s: Unexpected count %d.`, wrapper, total)
				}

				// Find() with explicit = NULL
				res2b := col.Find(db.Cond{"input =": nil})
				total, err = res2b.Count()

				if total != 0 {
					t.Fatalf(`%s: Unexpected count %d.`, wrapper, total)
				}

				// Find() with implicit IN
				res3 := col.Find(db.Cond{"input": []int{1, 2, 3, 4}})
				total, err = res3.Count()

				if total != 3 {
					t.Fatalf(`%s: Unexpected count %d.`, wrapper, total)
				}

				// Find() with implicit NOT IN
				res3a := col.Find(db.Cond{"input NOT IN": []int{1, 2, 3, 4}})
				total, err = res3a.Count()

				if total != 3 {
					t.Fatalf(`%s: Unexpected count %d.`, wrapper, total)
				}
			}

			var items []fibonacci
			err = res.All(&items)

			if err != nil {
				t.Fatalf(`%s: %q`, wrapper, err)
			}

			if len(items) != 6 {
				t.Fatalf(`Waiting for 6 items.`)
			}

			for _, item := range items {
				switch item.Input {
				case 0:
				case 1:
				case 2:
				case 4:
				case 8:
				case 9:
					if fib(item.Input) != item.Output {
						t.Fatalf(`Unexpected value in item with wrapper %s.`, wrapper)
					}
				default:
					t.Fatalf(`Unexpected item: %v with wrapper %s.`, item, wrapper)
				}
			}

			err = res.Close()
			if err != nil {
				t.Errorf("Failed to close result %s: %q.", wrapper, err)
			}

			err = sess.Close()
			if err != nil {
				t.Errorf("Failed to close %s: %q.", wrapper, err)
			}
		}
	}
}

func TestEven(t *testing.T) {
	var err error

	for _, wrapper := range wrappers {
		t.Logf("Testing wrapper: %q", wrapper)

		if settings[wrapper] == nil {
			t.Fatalf(`No such settings entry for wrapper %s.`, wrapper)
		} else {
			var sess db.Database

			sess, err = db.Open(wrapper, settings[wrapper])
			if err != nil {
				t.Fatalf(`Test for wrapper %s failed: %q`, wrapper, err)
			}
			defer sess.Close()

			col := sess.Collection("is_even")

			// Adding some items.
			var i int
			for i = 1; i < 100; i++ {
				item := oddEven{Input: i, IsEven: even(i)}
				_, err = col.Insert(item)
				if err != nil {
					t.Fatalf(`Could not append item with wrapper %s: %q`, wrapper, err)
				}
			}

			// Retrieving items
			res := col.Find(db.Cond{"is_even": true})

			var item oddEven
			for res.Next(&item) {
				if item.Input%2 != 0 {
					t.Fatalf("Expecting even numbers with wrapper %s. Got: %v\n", wrapper, item)
				}
			}
			if err := res.Err(); err != nil {
				t.Fatalf(`%s: %q`, wrapper, err)
			}
			if err = res.Delete(); err != nil {
				t.Fatalf(`Could not remove with wrapper %s: %q`, wrapper, err)
			}

			// Testing named inputs (using tags).
			res = col.Find()

			var item2 struct {
				Value uint `db:"input" bson:"input"` // The "bson" tag is required by mgo.
			}
			for res.Next(&item2) {
				if item2.Value%2 == 0 {
					t.Fatalf("Expecting odd numbers only with wrapper %s. Got: %v\n", wrapper, item2)
				}
			}
			if err := res.Err(); err != nil {
				t.Fatalf(`%s: %q`, wrapper, err)
			}

			// Testing inline tag.
			res = col.Find()

			var item3 struct {
				OddEven oddEven `db:",inline" bson:",inline"`
			}
			for res.Next(&item3) {
				if item3.OddEven.Input%2 == 0 {
					t.Fatalf("Expecting odd numbers only with wrapper %s. Got: %v\n", wrapper, item3)
				}
				if item3.OddEven.Input == 0 {
					t.Fatal("Expecting a number > 0")
				}
			}
			if err := res.Err(); err != nil {
				t.Fatalf(`%s: %q`, wrapper, err)
			}

			// Testing inline tag.
			type OddEven oddEven
			res = col.Find()

			var item31 struct {
				OddEven `db:",inline" bson:",inline"`
			}
			for res.Next(&item31) {
				if item31.Input%2 == 0 {
					t.Fatalf("Expecting odd numbers only with wrapper %s. Got: %v\n", wrapper, item31)
				}
				if item31.Input == 0 {
					t.Fatal("Expecting a number > 0")
				}
			}
			if err := res.Err(); err != nil {
				t.Fatalf(`%s: %q`, wrapper, err)
			}

			// Testing omision tag.
			res = col.Find()

			var item4 struct {
				Value uint `db:"-"`
			}
			for res.Next(&item4) {
				if item4.Value != 0 {
					t.Fatalf("Expecting no data with wrapper %s. Got: %v\n", wrapper, item4)
				}
			}
			if err := res.Err(); err != nil {
				t.Fatalf(`%s: %q`, wrapper, err)
			}
		}
	}

}

func TestExplicitAndDefaultMapping(t *testing.T) {
	var err error
	var sess db.Database
	var res db.Result

	var testE mapE
	var testN mapN

	for _, wrapper := range wrappers {
		t.Logf("Testing wrapper: %q", wrapper)

		if settings[wrapper] == nil {
			t.Fatalf(`No such settings entry for wrapper %s.`, wrapper)
		} else {

			if sess, err = db.Open(wrapper, settings[wrapper]); err != nil {
				t.Fatalf(`Test for wrapper %s failed: %q`, wrapper, err)
			}

			defer sess.Close()

			col := sess.Collection("CaSe_TesT")

			if err = col.Truncate(); err != nil {
				if wrapper == `mongo` {
					// Nothing, this is expected.
				} else {
					t.Fatal(err)
				}
			}

			// Testing explicit mapping.
			testE = mapE{
				CaseTest: "Hello!",
			}

			if _, err = col.Insert(testE); err != nil {
				t.Fatal(err)
			}

			res = col.Find(db.Cond{"case_test": "Hello!"})

			if wrapper == `ql` {
				res = res.Select(`id() as id`, `case_test`)
			}

			if err = res.One(&testE); err != nil {
				t.Fatal(err)
			}

			if wrapper == `mongo` {
				if testE.MongoID.Valid() == false {
					t.Fatalf("Expecting an ID.")
				}
			} else {
				if testE.ID == 0 {
					t.Fatalf("Expecting an ID.")
				}
			}

			// Testing default mapping.
			testN = mapN{
				Case_TEST: "World!",
			}

			if _, err = col.Insert(testN); err != nil {
				t.Fatal(err)
			}

			if wrapper == `mongo` {
				res = col.Find(db.Cond{"case_test": "World!"})
			} else {
				res = col.Find(db.Cond{"case_test": "World!"})
			}

			if wrapper == `ql` {
				res = res.Select(`id() as id`, `case_test`)
			}

			if err = res.One(&testN); err != nil {
				t.Fatal(err)
			}

			if wrapper == `mongo` {
				if testN.MongoID.Valid() == false {
					t.Fatalf("Expecting an ID.")
				}
			} else {
				if testN.ID == 0 {
					t.Fatalf("Expecting an ID.")
				}
			}
		}
	}
}

func TestComparisonOperators(t *testing.T) {
	var err error
	var sess db.Database

	for _, wrapper := range wrappers {
		t.Logf("Testing wrapper: %q", wrapper)

		if settings[wrapper] == nil {
			t.Fatalf("No such settings entry for wrapper %s.", wrapper)
		}

		if sess, err = db.Open(wrapper, settings[wrapper]); err != nil {
			t.Fatalf("Test for wrapper %s failed: %q", wrapper, err)
		}

		defer sess.Close()

		birthdays := sess.Collection("birthdays")
		err := birthdays.Truncate()
		assert.NoError(t, err)

		// Insert data for testing
		birthdaysDataset := []birthday{
			{
				Name: "Marie Smith",
				Born: time.Date(1956, time.August, 5, 0, 0, 0, 0, time.Local),
			},
			{
				Name: "Peter",
				Born: time.Date(1967, time.July, 23, 0, 0, 0, 0, time.Local),
			},
			{
				Name: "Eve Smith",
				Born: time.Date(1911, time.February, 8, 0, 0, 0, 0, time.Local),
			},
			{
				Name: "Alex López",
				Born: time.Date(2001, time.May, 5, 0, 0, 0, 0, time.Local),
			},
			{
				Name: "Rose Smith",
				Born: time.Date(1944, time.December, 9, 0, 0, 0, 0, time.Local),
			},
			{
				Name: "Daria López",
				Born: time.Date(1923, time.March, 23, 0, 0, 0, 0, time.Local),
			},
			{
				Name: "",
				Born: time.Date(1945, time.December, 1, 0, 0, 0, 0, time.Local),
			},
			{
				Name: "Colin",
				Born: time.Date(2010, time.May, 6, 0, 0, 0, 0, time.Local),
			},
		}
		for _, birthday := range birthdaysDataset {
			_, err := birthdays.Insert(birthday)
			assert.NoError(t, err)
		}

		// Test: equal
		{
			var item birthday
			err := birthdays.Find(db.Cond{
				"name": db.Eq("Colin"),
			}).One(&item)
			assert.NoError(t, err)
			assert.NotNil(t, item)

			assert.Equal(t, "Colin", item.Name)
		}

		// Test: not equal
		{
			var item birthday
			err := birthdays.Find(db.Cond{
				"name": db.NotEq("Colin"),
			}).One(&item)
			assert.NoError(t, err)
			assert.NotNil(t, item)

			assert.NotEqual(t, "Colin", item.Name)
		}

		// Test: greater than
		{
			var items []birthday
			ref := time.Date(1967, time.July, 23, 0, 0, 0, 0, time.Local)
			err := birthdays.Find(db.Cond{
				"born": db.Gt(ref),
			}).All(&items)
			assert.NoError(t, err)
			assert.NotZero(t, len(items))
			assert.Equal(t, 2, len(items))
			for _, item := range items {
				assert.True(t, item.Born.After(ref))
			}
		}

		// Test: less than
		{
			var items []birthday
			ref := time.Date(1967, time.July, 23, 0, 0, 0, 0, time.Local)
			err := birthdays.Find(db.Cond{
				"born": db.Lt(ref),
			}).All(&items)
			assert.NoError(t, err)
			assert.NotZero(t, len(items))
			assert.Equal(t, 5, len(items))
			for _, item := range items {
				assert.True(t, item.Born.Before(ref))
			}
		}

		// Test: greater than or equal to
		{
			var items []birthday
			ref := time.Date(1967, time.July, 23, 0, 0, 0, 0, time.Local)
			err := birthdays.Find(db.Cond{
				"born": db.Gte(ref),
			}).All(&items)
			assert.NoError(t, err)
			assert.NotZero(t, len(items))
			assert.Equal(t, 3, len(items))
			for _, item := range items {
				assert.True(t, item.Born.After(ref) || item.Born.Equal(ref))
			}
		}

		// Test: less than or equal to
		{
			var items []birthday
			ref := time.Date(1967, time.July, 23, 0, 0, 0, 0, time.Local)
			err := birthdays.Find(db.Cond{
				"born": db.Lte(ref),
			}).All(&items)
			assert.NoError(t, err)
			assert.NotZero(t, len(items))
			assert.Equal(t, 6, len(items))
			for _, item := range items {
				assert.True(t, item.Born.Before(ref) || item.Born.Equal(ref))
			}
		}

		// Test: between
		{
			var items []birthday
			dateA := time.Date(1911, time.February, 8, 0, 0, 0, 0, time.Local)
			dateB := time.Date(1967, time.July, 23, 0, 0, 0, 0, time.Local)
			err := birthdays.Find(db.Cond{
				"born": db.Between(dateA, dateB),
			}).All(&items)
			assert.NoError(t, err)
			assert.Equal(t, 6, len(items))
			for _, item := range items {
				assert.True(t, item.Born.After(dateA) || item.Born.Equal(dateA))
				assert.True(t, item.Born.Before(dateB) || item.Born.Equal(dateB))
			}
		}

		// Test: not between
		{
			var items []birthday
			dateA := time.Date(1911, time.February, 8, 0, 0, 0, 0, time.Local)
			dateB := time.Date(1967, time.July, 23, 0, 0, 0, 0, time.Local)
			err := birthdays.Find(db.Cond{
				"born": db.NotBetween(dateA, dateB),
			}).All(&items)
			assert.NoError(t, err)
			assert.Equal(t, 2, len(items))
			for _, item := range items {
				assert.False(t, item.Born.Before(dateA) || item.Born.Equal(dateA))
				assert.False(t, item.Born.Before(dateB) || item.Born.Equal(dateB))
			}
		}

		// Test: in
		{
			var items []birthday
			names := []string{"Peter", "Eve Smith", "Daria López", "Alex López"}
			err := birthdays.Find(db.Cond{
				"name": db.In(names),
			}).All(&items)
			assert.NoError(t, err)
			assert.Equal(t, 4, len(items))
			for _, item := range items {
				inArray := false
				for _, name := range names {
					if name == item.Name {
						inArray = true
					}
				}
				assert.True(t, inArray)
			}
		}

		// Test: not in
		{
			var items []birthday
			names := []string{"Peter", "Eve Smith", "Daria López", "Alex López"}
			err := birthdays.Find(db.Cond{
				"name": db.NotIn(names),
			}).All(&items)
			assert.NoError(t, err)
			assert.Equal(t, 4, len(items))
			for _, item := range items {
				inArray := false
				for _, name := range names {
					if name == item.Name {
						inArray = true
					}
				}
				assert.False(t, inArray)
			}
		}

		// Test: not in
		{
			var items []birthday
			names := []string{"Peter", "Eve Smith", "Daria López", "Alex López"}
			err := birthdays.Find(db.Cond{
				"name": db.NotIn(names),
			}).All(&items)
			assert.NoError(t, err)
			assert.Equal(t, 4, len(items))
			for _, item := range items {
				inArray := false
				for _, name := range names {
					if name == item.Name {
						inArray = true
					}
				}
				assert.False(t, inArray)
			}
		}

		// Test: is and is not
		{
			var items []birthday
			err := birthdays.Find(db.And(
				db.Cond{"name": db.Is(nil)},
				db.Cond{"name": db.IsNot(nil)},
			)).All(&items)
			assert.NoError(t, err)
			assert.Equal(t, 0, len(items))
		}

		// Test: is nil
		{
			var items []birthday
			err := birthdays.Find(db.And(
				db.Cond{"born_ut": db.IsNull()},
			)).All(&items)
			assert.NoError(t, err)
			assert.Equal(t, 8, len(items))
		}

		// Test: like and not like
		{
			var items []birthday
			var q db.Result

			switch wrapper {
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
			assert.NoError(t, err)
			assert.Equal(t, 1, len(items))

			assert.Equal(t, "Daria López", items[0].Name)
		}

		if wrapper != "sqlite" && wrapper != "mssql" {
			// Test: regexp
			{
				var items []birthday
				err := birthdays.Find(db.And(
					db.Cond{"name": db.RegExp("^[D|C|M]")},
				)).OrderBy("name").All(&items)
				assert.NoError(t, err)
				assert.Equal(t, 3, len(items))

				assert.Equal(t, "Colin", items[0].Name)
				assert.Equal(t, "Daria López", items[1].Name)
				assert.Equal(t, "Marie Smith", items[2].Name)
			}

			// Test: not regexp
			{
				var items []birthday
				names := []string{"Daria López", "Colin", "Marie Smith"}
				err := birthdays.Find(db.And(
					db.Cond{"name": db.NotRegExp("^[D|C|M]")},
				)).OrderBy("name").All(&items)
				assert.NoError(t, err)
				assert.Equal(t, 5, len(items))

				for _, item := range items {
					for _, name := range names {
						assert.NotEqual(t, item.Name, name)
					}
				}
			}
		}

		// Test: after
		{
			ref := time.Date(1944, time.December, 9, 0, 0, 0, 0, time.Local)
			var items []birthday
			err := birthdays.Find(db.Cond{
				"born": db.After(ref),
			}).All(&items)
			assert.NoError(t, err)
			assert.Equal(t, 5, len(items))
		}

		// Test: on or after
		{
			ref := time.Date(1944, time.December, 9, 0, 0, 0, 0, time.Local)
			var items []birthday
			err := birthdays.Find(db.Cond{
				"born": db.OnOrAfter(ref),
			}).All(&items)
			assert.NoError(t, err)
			assert.Equal(t, 6, len(items))
		}

		// Test: before
		{
			ref := time.Date(1944, time.December, 9, 0, 0, 0, 0, time.Local)
			var items []birthday
			err := birthdays.Find(db.Cond{
				"born": db.Before(ref),
			}).All(&items)
			assert.NoError(t, err)
			assert.Equal(t, 2, len(items))
		}

		// Test: on or before
		{
			ref := time.Date(1944, time.December, 9, 0, 0, 0, 0, time.Local)
			var items []birthday
			err := birthdays.Find(db.Cond{
				"born": db.OnOrBefore(ref),
			}).All(&items)
			assert.NoError(t, err)
			assert.Equal(t, 3, len(items))
		}
	}
}
