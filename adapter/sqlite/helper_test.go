// Copyright (c) 2012-today The upper.io/db authors. All rights reserved.
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

package sqlite

import (
	"database/sql"
	"os"

	db "github.com/upper/db/v4"
	"github.com/upper/db/v4/internal/testsuite"
)

var settings = ConnectionURL{
	Database: os.Getenv("DB_NAME"),
}

type Helper struct {
	sess db.Session
}

func (h *Helper) Session() db.Session {
	return h.sess
}

func (h *Helper) Adapter() string {
	return "sqlite"
}

func (h *Helper) TearDown() error {
	return h.sess.Close()
}

func (h *Helper) TearUp() error {
	var err error

	h.sess, err = Open(settings)
	if err != nil {
		return err
	}

	batch := []string{
		`PRAGMA foreign_keys=OFF`,

		`BEGIN TRANSACTION`,

		`DROP TABLE IF EXISTS artist`,
		`CREATE TABLE artist (
			id integer primary key,
			name varchar(60)
		)`,

		`DROP TABLE IF EXISTS publication`,
		`CREATE TABLE publication (
			id integer primary key,
			title varchar(80),
			author_id integer
		)`,

		`DROP TABLE IF EXISTS review`,
		`CREATE TABLE review (
			id integer primary key,
			publication_id integer,
			name varchar(80),
			comments text,
			created datetime
		)`,

		`DROP TABLE IF EXISTS data_types`,
		`CREATE TABLE data_types (
			id integer primary key,
		 _uint integer,
		 _uintptr integer,
		 _uint8 integer,
		 _uint16 int,
		 _uint32 int,
		 _uint64 int,
		 _int integer,
		 _int8 integer,
		 _int16 integer,
		 _int32 integer,
		 _int64 integer,
		 _float32 real,
		 _float64 real,
		 _byte integer,
		 _rune integer,
		 _bool integer,
		 _string text,
		 _blob blob,
		 _date datetime,
		 _nildate datetime,
		 _ptrdate datetime,
		 _defaultdate datetime default current_timestamp,
		 _time text
		)`,

		`DROP TABLE IF EXISTS stats_test`,
		`CREATE TABLE stats_test (
			id integer primary key,
			numeric integer,
			value integer
		)`,

		`DROP TABLE IF EXISTS composite_keys`,
		`CREATE TABLE composite_keys (
			code VARCHAR(255) default '',
			user_id VARCHAR(255) default '',
			some_val VARCHAR(255) default '',
			primary key (code, user_id)
		)`,

		`DROP TABLE IF EXISTS "birthdays"`,
		`CREATE TABLE "birthdays" (
				"id" INTEGER PRIMARY KEY,
				"name" VARCHAR(50) DEFAULT NULL,
				"born" DATETIME DEFAULT NULL,
				"born_ut" INTEGER
			)`,

		`DROP TABLE IF EXISTS "fibonacci"`,
		`CREATE TABLE "fibonacci" (
				"id" INTEGER PRIMARY KEY,
				"input" INTEGER,
				"output" INTEGER
			)`,

		`DROP TABLE IF EXISTS "is_even"`,
		`CREATE TABLE "is_even" (
				"input" INTEGER,
				"is_even" INTEGER
			)`,

		`DROP TABLE IF EXISTS "CaSe_TesT"`,
		`CREATE TABLE "CaSe_TesT" (
				"id" INTEGER PRIMARY KEY,
				"case_test" VARCHAR
			)`,

		`DROP TABLE IF EXISTS accounts`,
		`CREATE TABLE accounts (
			id integer primary key,
			name varchar,
			disabled integer,
			created_at datetime default current_timestamp
		)`,

		`DROP TABLE IF EXISTS users`,
		`CREATE TABLE users (
			id integer primary key,
			account_id integer,
			username varchar UNIQUE
		)`,

		`DROP TABLE IF EXISTS logs`,
		`CREATE TABLE logs (
			id integer primary key,
			message VARCHAR
		)`,

		`COMMIT`,
	}

	for _, query := range batch {
		driver := h.sess.Driver().(*sql.DB)
		if _, err := driver.Exec(query); err != nil {
			return err
		}
	}

	return nil
}

var _ testsuite.Helper = &Helper{}
