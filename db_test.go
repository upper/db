package db_test

import (
	"database/sql"
	"errors"
	"labix.org/v2/mgo"
	"reflect"
	"testing"
	"time"
	"upper.io/db"
	_ "upper.io/db/mongo"
	_ "upper.io/db/mysql"
	_ "upper.io/db/postgresql"
	_ "upper.io/db/sqlite"
)

var wrappers = []string{`sqlite`, `mysql`, `postgresql`, `mongo`}

var (
	errDriverErr = errors.New(`Driver error`)
)

var settings = map[string]*db.Settings{
	`sqlite`: &db.Settings{
		Database: `upperio_tests.db`,
	},
	`mongo`: &db.Settings{
		Database: `upperio_tests`,
		Host:     `127.0.0.1`,
		User:     `upperio`,
		Password: `upperio`,
	},
	`mysql`: &db.Settings{
		Database: `upperio_tests`,
		Socket:   `/var/run/mysqld/mysqld.sock`,
		User:     `upperio`,
		Password: `upperio`,
	},
	`postgresql`: &db.Settings{
		Database: `upperio_tests`,
		Socket:   `/var/run/postgresql/`,
		User:     `upperio`,
		Password: `upperio`,
	},
}

var setupFn = map[string]func(driver interface{}) error{
	`mongo`: func(driver interface{}) error {
		if mgod, ok := driver.(*mgo.Session); ok == true {
			var col *mgo.Collection
			col = mgod.DB("upperio_tests").C("birthdays")
			col.DropCollection()

			col = mgod.DB("upperio_tests").C("fibonacci")
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
			_, err = sqld.Exec(`DROP TABLE IF EXISTS fibonacci`)
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
			return nil
		}
		return errDriverErr
	},
}

type Birthday struct {
	Name string    `field:"name"`
	Born time.Time `field:"born"`
}

type Fibonacci struct {
	Input  uint64 `field:"input"`
	Output uint64 `field:"output"`
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

			controlItem = Birthday{
				Name: "Hayao Miyazaki",
				Born: time.Date(1941, time.January, 5, 0, 0, 0, 0, time.Local),
			}

			col, err := sess.Collection(`birthdays`)

			if err != nil {
				if wrapper == `mongo` && err == db.ErrCollectionDoesNotExists {
					// Expected error with mongodb.
				} else {
					t.Fatalf(`Could not use collection with wrapper %s: %s`, wrapper, err.Error())
				}
			}

			var id interface{}

			id, err = col.Append(controlItem)

			if err != nil {
				t.Fatalf(`Could not append item with wrapper %s: %s`, wrapper, err.Error())
			}

			var res db.Result
			if wrapper == `mongo` {
				res = col.Find(db.Cond{"_id": id})
			} else {
				res = col.Find(db.Cond{"id": id})
			}

			var testItem Birthday

			res.One(&testItem)

			if reflect.DeepEqual(testItem, controlItem) == false {
				t.Errorf("Struct is different with wrapper %s.", wrapper)
			}

			controlItem.Name = `宮崎駿`
			err = res.Update(controlItem)

			if err != nil {
				t.Fatalf(`Could not update with wrapper %s: %s`, wrapper, err.Error())
			}

			res.One(&testItem)

			if reflect.DeepEqual(testItem, controlItem) == false {
				t.Errorf("Struct is different with wrapper %s.", wrapper)
			}

			err = res.Remove()

			if err != nil {
				t.Fatalf(`Could not update with wrapper %s: %s`, wrapper, err.Error())
			}

			var total uint64
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

func TestFinds(t *testing.T) {
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

			var col db.Collection
			col, err = sess.Collection("fibonacci")

			if err != nil {
				if wrapper == `mongo` && err == db.ErrCollectionDoesNotExists {
					// Expected error with mongodb.
				} else {
					t.Fatalf(`Could not use collection with wrapper %s: %s`, wrapper, err.Error())
				}
			}

			// Adding some items.
			var i uint64
			for i = 0; i < 10; i++ {
				item := Fibonacci{i, fib(i)}
				_, err = col.Append(item)
				if err != nil {
					t.Fatalf(`Could not append item with wrapper %s: %s`, wrapper, err.Error())
				}
			}

			// Querying range.
			res := col.Find(
				// 5, 6, 7, 3
				db.Or{
					db.And{
						db.Cond{"input >=": 5},
						db.Cond{"input <=": 7},
					},
					db.Cond{"input": 3},
				},
			).Skip(1).Limit(2).Sort("-input")

			var total uint64
			total, err = res.Count()

			if err != nil {
				t.Fatalf(`%s: %s`, wrapper, err.Error())
			}

			if total != 4 {
				t.Fatalf(`Expecting a count of 4.`)
			}

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

			total, err = res.Count()

			if total != 0 {
				t.Fatalf(`Unexpected count %s.`, wrapper)
			}

			res = col.Find()

			total, err = res.Count()

			if total != 6 {
				t.Fatalf(`Unexpected count %s.`, wrapper)
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
