package db_test

import (
	"database/sql"
	"errors"
	"labix.org/v2/mgo"
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
		Database: `example.db`,
	},
	`mongo`: &db.Settings{
		Database: `upperio_tests`,
		Host:     `127.0.0.1`,
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
			col := mgod.DB("upperio_tests").C("birthdays")
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
				id BIGINT(20) UNSIGNED NOT NULL, PRIMARY KEY(id),
				name VARCHAR(50),
				born DATE
			)`)
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
			return nil
		}
		return errDriverErr
	},
}

type Birthday struct {
	Name string    `field:"name"`
	Born time.Time `field:"born"`
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
			defer sess.Close()

			if setupFn[wrapper] == nil {
				t.Fatalf(`Missing setup function for wrapper %s.`, wrapper)
			} else {
				err = setupFn[wrapper](sess.Driver())
				if err != nil {
					t.Fatalf(`Failed to setup wrapper %s: %s`, wrapper, err.Error())
				}
			}

		}
	}
}

func TestAppend(t *testing.T) {
	var err error

	var testItem = Birthday{
		Name: "Hayao Miyazaki",
		Born: time.Date(1941, time.January, 5, 0, 0, 0, 0, time.UTC),
	}

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

			col, err := sess.Collection(`birthdays`)

			if err != nil {
				if wrapper == `mongo` && err == db.ErrCollectionDoesNotExists {
					// Expected error with mongodb.
				} else {
					t.Fatalf(`Could not use collection with wrapper %s: %s`, wrapper, err.Error())
				}
			}

			_, err = col.Append(testItem)

			if err != nil {
				t.Fatalf(`Could not append item with wrapper %s: %s`, wrapper, err.Error())
			}

		}
	}
}
