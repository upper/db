package db_test

import (
	"database/sql"
	"errors"
	"flag"
	"log"
	"reflect"
	"testing"
	"time"

	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"upper.io/db"
	_ "upper.io/db/mongo"
	_ "upper.io/db/mysql"
	_ "upper.io/db/postgresql"
	_ "upper.io/db/ql"
	_ "upper.io/db/sqlite"
)

var wrappers = []string{
	`sqlite`,
	`mysql`,
	`postgresql`,
	`mongo`,
	`ql`,
}

const (
	TestAllWrappers = `all`
)

var (
	errDriverErr = errors.New(`Driver error`)
)

var settings map[string]*db.Settings

func init() {

	// Getting host from the environment.
	host := flag.String("host", "testserver.local", "Testing server address.")
	wrapper := flag.String("wrapper", "all", "Wrappers to test.")

	flag.Parse()

	log.Printf("Running tests against host %s.\n", *host)

	settings = map[string]*db.Settings{
		`sqlite`: &db.Settings{
			Database: `upperio_tests.db`,
		},
		`mongo`: &db.Settings{
			Database: `upperio_tests`,
			Host:     *host,
			User:     `upperio`,
			Password: `upperio`,
		},
		`mysql`: &db.Settings{
			Database: `upperio_tests`,
			Host:     *host,
			User:     `upperio`,
			Password: `upperio`,
		},
		`postgresql`: &db.Settings{
			Database: `upperio_tests`,
			Host:     *host,
			User:     `upperio`,
			Password: `upperio`,
		},
		`ql`: &db.Settings{
			Database: `file://upperio_test.ql`,
		},
	}

	if *wrapper != TestAllWrappers {
		wrappers = []string{*wrapper}
		log.Printf("Testing wrapper %s.", *wrapper)
	}

}

var setupFn = map[string]func(driver interface{}) error{
	`mongo`: func(driver interface{}) error {
		if mgod, ok := driver.(*mgo.Session); ok == true {
			var col *mgo.Collection
			col = mgod.DB("upperio_tests").C("birthdays")
			col.DropCollection()

			col = mgod.DB("upperio_tests").C("fibonacci")
			col.DropCollection()

			col = mgod.DB("upperio_tests").C("is_even")
			col.DropCollection()
			return nil
		}
		return errDriverErr
	},
	`postgresql`: func(driver interface{}) error {
		if sqld, ok := driver.(*sql.DB); ok == true {
			var err error

			_, err = sqld.Exec(`DROP TABLE IF EXISTS birthdays`)
			if err != nil {
				return err
			}
			_, err = sqld.Exec(`CREATE TABLE "birthdays" (
					"id" serial,
					"name" CHARACTER VARYING(50),
					"born" TIMESTAMP
			)`)
			if err != nil {
				return err
			}

			_, err = sqld.Exec(`DROP TABLE IF EXISTS "fibonacci"`)
			if err != nil {
				return err
			}
			_, err = sqld.Exec(`CREATE TABLE "fibonacci" (
					"id" serial,
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
					"is_even" INT
			)`)
			if err != nil {
				return err
			}

			return nil
		}
		return errDriverErr
	},
	`mysql`: func(driver interface{}) error {
		if sqld, ok := driver.(*sql.DB); ok == true {
			var err error
			_, err = sqld.Exec(`DROP TABLE IF EXISTS birthdays`)
			if err != nil {
				return err
			}
			_, err = sqld.Exec(`CREATE TABLE birthdays (
				id BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT, PRIMARY KEY(id),
				name VARCHAR(50),
				born DATE
			) CHARSET=utf8`)
			if err != nil {
				return err
			}
			_, err = sqld.Exec(`DROP TABLE IF EXISTS fibonacci`)
			if err != nil {
				return err
			}
			_, err = sqld.Exec(`CREATE TABLE fibonacci (
				id BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT, PRIMARY KEY(id),
				input BIGINT(20) UNSIGNED NOT NULL,
				output BIGINT(20) UNSIGNED NOT NULL
			) CHARSET=utf8`)
			if err != nil {
				return err
			}
			_, err = sqld.Exec(`DROP TABLE IF EXISTS is_even`)
			if err != nil {
				return err
			}
			_, err = sqld.Exec(`CREATE TABLE is_even (
				input BIGINT(20) UNSIGNED NOT NULL,
				is_even TINYINT(1)
			) CHARSET=utf8`)
			if err != nil {
				return err
			}
			return nil
		}
		return errDriverErr
	},
	`sqlite`: func(driver interface{}) error {
		if sqld, ok := driver.(*sql.DB); ok == true {
			var err error
			_, err = sqld.Exec(`DROP TABLE IF EXISTS "birthdays"`)
			if err != nil {
				return err
			}
			_, err = sqld.Exec(`CREATE TABLE "birthdays" (
				"id" INTEGER PRIMARY KEY,
				"name" VARCHAR(50) DEFAULT NULL,
				"born" VARCHAR(12) DEFAULT NULL
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
			return nil
		}
		return errDriverErr
	},
	`ql`: func(driver interface{}) error {
		if sqld, ok := driver.(*sql.DB); ok == true {
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
				born time
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

			if err = tx.Commit(); err != nil {
				return err
			}

			return nil
		}
		return errDriverErr
	},
}

type Birthday struct {
	Name   string    // `db:"name"`	// Must match by name.
	Born   time.Time // `db:"born"` // Must match by name.
	OmitMe bool      `db:"-" bson:"-"`
}

type Fibonacci struct {
	Input  uint64 `db:"input"`
	Output uint64 `db:"output"`
	OmitMe bool   `db:"omit_me,omitempty" bson:"omit_me,omitempty"`
}

type OddEven struct {
	Input  int  `db:"input"`
	IsEven bool `db:"is_even" bson:"is_even"` // The "bson" tag is required by mgo.
	OmitMe bool `db:"-,omitempty" bson:"-,omitempty"`
}

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
		if settings[wrapper] == nil {
			t.Fatalf(`No such settings entry for wrapper %s.`, wrapper)
		} else {
			var sess db.Database
			sess, err = db.Open(wrapper, *settings[wrapper])
			if err != nil {
				t.Fatalf(`Test for wrapper %s failed: %s`, wrapper, err.Error())
			}
			err = sess.Close()
			if err != nil {
				t.Fatalf(`Test for wrapper %s failed: %s`, wrapper, err.Error())
			}
		}
	}
}

func TestSetup(t *testing.T) {
	var err error
	for _, wrapper := range wrappers {
		if settings[wrapper] == nil {
			t.Fatalf(`No such settings entry for wrapper %s.`, wrapper)
		} else {
			var sess db.Database

			sess, err = db.Open(wrapper, *settings[wrapper])
			if err != nil {
				t.Fatalf(`Test for wrapper %s failed: %s`, wrapper, err.Error())
			}

			if setupFn[wrapper] == nil {
				t.Fatalf(`Missing setup function for wrapper %s.`, wrapper)
			} else {
				err = setupFn[wrapper](sess.Driver())
				if err != nil {
					t.Fatalf(`Failed to setup wrapper %s: %s`, wrapper, err.Error())
				}
			}

			err = sess.Close()
			if err != nil {
				t.Fatalf(`Could not close %s: %s`, wrapper, err.Error())
			}

		}
	}
}

func TestSimpleCRUD(t *testing.T) {
	var err error

	var controlItem Birthday

	for _, wrapper := range wrappers {
		if settings[wrapper] == nil {
			t.Fatalf(`No such settings entry for wrapper %s.`, wrapper)
		} else {
			var sess db.Database

			sess, err = db.Open(wrapper, *settings[wrapper])
			if err != nil {
				t.Fatalf(`Test for wrapper %s failed: %s`, wrapper, err.Error())
			}

			defer sess.Close()

			controlItem = Birthday{
				Name: "Hayao Miyazaki",
				Born: time.Date(1941, time.January, 5, 0, 0, 0, 0, time.Local),
			}

			col, err := sess.Collection(`birthdays`)

			if err != nil {
				if wrapper == `mongo` && err == db.ErrCollectionDoesNotExist {
					// Expected error with mongodb.
				} else {
					t.Fatalf(`Could not use collection with wrapper %s: %s`, wrapper, err.Error())
				}
			}

			var id interface{}

			if id, err = col.Append(controlItem); err != nil {
				t.Fatalf(`Could not append item with wrapper %s: %s`, wrapper, err.Error())
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

			var testItem Birthday
			err = res.One(&testItem)
			if err != nil {
				t.Fatalf("%s One(): %s", wrapper, err)
			}

			if reflect.DeepEqual(testItem, controlItem) == false {
				t.Errorf("%s: testItem: %v\n", wrapper, testItem)
				t.Errorf("%s: controlItem: %v\n", wrapper, controlItem)
				t.Fatalf("%s: Structs are different", wrapper)
			}

			var testItems []Birthday
			err = res.All(&testItems)
			if err != nil {
				t.Fatalf("%s All(): %s", wrapper, err)
			}

			if len(testItems) == 0 {
				t.Fatalf("%s All(): Expecting at least one row.", wrapper)
			}

			for _, testItem = range testItems {
				if reflect.DeepEqual(testItem, controlItem) == false {
					t.Errorf("%s: testItem: %v\n", wrapper, testItem)
					t.Errorf("%s: controlItem: %v\n", wrapper, controlItem)
					t.Fatalf("%s: Structs are different", wrapper)
				}
			}

			controlItem.Name = `宮崎駿`
			err = res.Update(controlItem)

			if err != nil {
				t.Fatalf(`Could not update with wrapper %s: %s`, wrapper, err.Error())
			}

			res.One(&testItem)

			if reflect.DeepEqual(testItem, controlItem) == false {
				t.Fatalf("Struct is different with wrapper %s.", wrapper)
			}

			err = res.Remove()

			if err != nil {
				t.Fatalf(`Could not remove with wrapper %s: %s`, wrapper, err.Error())
			}

			total, err = res.Count()

			if total != 0 {
				t.Fatalf(`Expecting no items %s: %s`, wrapper, err.Error())
			}

			err = res.Close()
			if err != nil {
				t.Errorf("Failed to close result %s: %s.", wrapper, err.Error())
			}

			err = sess.Close()
			if err != nil {
				t.Errorf("Failed to close %s: %s.", wrapper, err.Error())
			}

		}
	}
}

func TestFibonacci(t *testing.T) {
	var err error

	for _, wrapper := range wrappers {
		if settings[wrapper] == nil {
			t.Fatalf(`No such settings entry for wrapper %s.`, wrapper)
		} else {
			var sess db.Database

			sess, err = db.Open(wrapper, *settings[wrapper])
			if err != nil {
				t.Fatalf(`Test for wrapper %s failed: %s`, wrapper, err.Error())
			}
			defer sess.Close()

			var col db.Collection
			col, err = sess.Collection("fibonacci")

			if err != nil {
				if wrapper == `mongo` && err == db.ErrCollectionDoesNotExist {
					// Expected error with mongodb.
				} else {
					t.Fatalf(`Could not use collection with wrapper %s: %s`, wrapper, err.Error())
				}
			}

			// Adding some items.
			var i uint64
			for i = 0; i < 10; i++ {
				item := Fibonacci{Input: i, Output: fib(i)}
				_, err = col.Append(item)
				if err != nil {
					t.Fatalf(`Could not append item with wrapper %s: %s`, wrapper, err.Error())
				}
			}

			// Find() with IN/$in
			var res db.Result
			var total uint64
			var whereIn db.Cond

			switch wrapper {
			case `mongo`:
				whereIn = db.Cond{"input": db.Func{"$in", []int{3, 5, 6, 7}}}
			default:
				whereIn = db.Cond{"input": db.Func{"IN", []int{3, 5, 6, 7}}}
			}

			res = col.Find(whereIn).Sort("input")

			total, err = res.Count()

			if err != nil {
				t.Fatalf(`%s: %s`, wrapper, err.Error())
			}

			if total != 4 {
				t.Fatalf(`Expecting a count of 4.`)
			}

			res = res.Skip(1).Limit(2)

			for {
				var item Fibonacci
				err = res.Next(&item)
				if err == nil {
					switch item.Input {
					case 5:
					case 6:
						if fib(item.Input) != item.Output {
							t.Fatalf(`Unexpected value in item with wrapper %s.`, wrapper)
						}
					default:
						t.Fatalf(`Unexpected item: %v with wrapper %s.`, item, wrapper)
					}
				} else if err == db.ErrNoMoreRows {
					break
				} else {
					t.Fatalf(`%s: %s`, wrapper, err.Error())
				}
			}

			// Find() with range
			res = col.Find(
				// 5, 6, 7, 3
				db.Or{
					db.And{
						db.Cond{"input >=": 5},
						db.Cond{"input <=": 7},
					},
					db.Cond{"input": 3},
				},
			).Sort("-input")

			if total, err = res.Count(); err != nil {
				t.Fatalf(`%s: %s`, wrapper, err.Error())
			}

			if total != 4 {
				t.Fatalf(`Expecting a count of 4.`)
			}

			// Skipping.
			res = res.Skip(1).Limit(2)

			for {
				var item Fibonacci
				err = res.Next(&item)
				if err == nil {
					switch item.Input {
					case 5:
					case 6:
						if fib(item.Input) != item.Output {
							t.Fatalf(`Unexpected value in item with wrapper %s.`, wrapper)
						}
					default:
						t.Fatalf(`Unexpected item: %v with wrapper %s.`, item, wrapper)
					}
				} else if err == db.ErrNoMoreRows {
					break
				} else {
					t.Fatalf(`%s: %s`, wrapper, err.Error())
				}
			}

			err = res.Remove()

			if err != nil {
				t.Fatalf(`%s: %s`, wrapper, err.Error())
			}

			if total, err = res.Count(); err != nil {
				t.Fatalf(`%s: %s`, wrapper, err.Error())
			}

			if total != 0 {
				t.Fatalf(`%s: Unexpected count %d.`, wrapper, total)
			}

			res = col.Find()

			total, err = res.Count()

			if total != 6 {
				t.Fatalf(`%s: Unexpected count %d.`, wrapper, total)
			}

			var items []Fibonacci
			err = res.All(&items)

			if err != nil {
				t.Fatalf(`%s: %s`, wrapper, err.Error())
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
				t.Errorf("Failed to close result %s: %s.", wrapper, err.Error())
			}

			err = sess.Close()
			if err != nil {
				t.Errorf("Failed to close %s: %s.", wrapper, err.Error())
			}

		}
	}
}

func TestEven(t *testing.T) {
	var err error

	for _, wrapper := range wrappers {
		if settings[wrapper] == nil {
			t.Fatalf(`No such settings entry for wrapper %s.`, wrapper)
		} else {
			var sess db.Database

			sess, err = db.Open(wrapper, *settings[wrapper])
			if err != nil {
				t.Fatalf(`Test for wrapper %s failed: %s`, wrapper, err.Error())
			}
			defer sess.Close()

			var col db.Collection
			col, err = sess.Collection("is_even")

			if err != nil {
				if wrapper == `mongo` && err == db.ErrCollectionDoesNotExist {
					// Expected error with mongodb.
				} else {
					t.Fatalf(`Could not use collection with wrapper %s: %s`, wrapper, err.Error())
				}
			}

			// Adding some items.
			var i int
			for i = 1; i < 100; i++ {
				item := OddEven{Input: i, IsEven: even(i)}
				_, err = col.Append(item)
				if err != nil {
					t.Fatalf(`Could not append item with wrapper %s: %s`, wrapper, err.Error())
				}
			}

			// Retrieving items
			res := col.Find(db.Cond{"is_even": true})

			for {
				var item OddEven
				err = res.Next(&item)
				if err != nil {
					if err == db.ErrNoMoreRows {
						break
					} else {
						t.Fatalf(`%s: %v`, wrapper, err)
					}
				}
				if item.Input%2 != 0 {
					t.Fatalf("Expecting even numbers with wrapper %s. Got: %v\n", wrapper, item)
				}
			}

			if err = res.Remove(); err != nil {
				t.Fatalf(`Could not remove with wrapper %s: %s`, wrapper, err.Error())
			}

			res = col.Find()

			for {
				// Testing named inputs (using tags).
				var item struct {
					Value uint `db:"input" bson:"input"` // The "bson" tag is required by mgo.
				}
				err = res.Next(&item)
				if err != nil {
					if err == db.ErrNoMoreRows {
						break
					} else {
						t.Fatalf(`%s: %v`, wrapper, err)
					}
				}
				if item.Value%2 == 0 {
					t.Fatalf("Expecting odd numbers only with wrapper %s. Got: %v\n", wrapper, item)
				}
			}

			for {
				// Testing inline tag.
				var item struct {
					OddEven `db:",inline" bson:",inline"`
				}
				err = res.Next(&item)
				if err != nil {
					if err == db.ErrNoMoreRows {
						break
					} else {
						t.Fatalf(`%s: %v`, wrapper, err)
					}
				}
				if item.Input%2 == 0 {
					t.Fatalf("Expecting odd numbers only with wrapper %s. Got: %v\n", wrapper, item)
				}
			}

			// Testing omision tag.
			for {
				var item struct {
					Value uint `db:"-"`
				}
				err = res.Next(&item)
				if err != nil {
					if err == db.ErrNoMoreRows {
						break
					} else {
						t.Fatalf(`%s: %v`, wrapper, err)
					}
				}
				if item.Value != 0 {
					t.Fatalf("Expecting no data with wrapper %s. Got: %v\n", wrapper, item)
				}
			}

			// Testing (deprecated) "field" tag.
			for {
				// Testing named inputs (using tags).
				var item struct {
					Value uint `field:"input"`
				}
				err = res.Next(&item)
				if err != nil {
					if err == db.ErrNoMoreRows {
						break
					} else {
						t.Fatalf(`%s: %v`, wrapper, err)
					}
				}
				if item.Value%2 == 0 {
					t.Fatalf("Expecting no data with wrapper %s. Got: %v\n", wrapper, item)
				}
			}

		}
	}

}
