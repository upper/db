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

package postgresql

import (
	"database/sql"
	"os"
)

const (
	testTimeZone = "Canada/Eastern"
)

var settings = ConnectionURL{
	Database: "upperio_tests",
	User:     "upperio_tests",
	Password: "upperio_secret",
	Host:     "localhost",
	Options: map[string]string{
		"timezone": testTimeZone,
	},
}

func config() {
	if host := os.Getenv("TEST_HOST"); host != "" {
		settings.Host = host
	}
}

func tearUp() error {
	sess := mustOpen()
	defer sess.Close()

	batch := []string{
		`DROP TABLE IF EXISTS artist`,

		`CREATE TABLE artist (
			id serial primary key,
			name varchar(60)
		)`,

		`DROP TABLE IF EXISTS publication`,

		`CREATE TABLE publication (
			id serial primary key,
			title varchar(80),
			author_id integer
		)`,

		`DROP TABLE IF EXISTS review`,

		`CREATE TABLE review (
			id serial primary key,
			publication_id integer,
			name varchar(80),
			comments text,
			created timestamp without time zone
		)`,

		`DROP TABLE IF EXISTS data_types`,

		`CREATE TABLE data_types (
			id serial primary key,
			_uint integer,
			_uint8 integer,
			_uint16 integer,
			_uint32 integer,
			_uint64 integer,
			_int integer,
			_int8 integer,
			_int16 integer,
			_int32 integer,
			_int64 integer,
			_float32 numeric(10,6),
			_float64 numeric(10,6),
			_bool boolean,
			_string text,
			_date timestamp with time zone,
			_nildate timestamp without time zone null,
			_ptrdate timestamp without time zone,
			_defaultdate timestamp without time zone DEFAULT now(),
			_time bigint
		)`,

		`DROP TABLE IF EXISTS stats_test`,

		`CREATE TABLE stats_test (
			id serial primary key,
			numeric integer,
			value integer
		)`,

		`DROP TABLE IF EXISTS composite_keys`,

		`CREATE TABLE composite_keys (
			code varchar(255) default '',
			user_id varchar(255) default '',
			some_val varchar(255) default '',
			primary key (code, user_id)
		)`,

		`DROP TABLE IF EXISTS option_types`,

		`CREATE TABLE option_types (
			id serial primary key,
			name varchar(255) default '',
			tags varchar(64)[],
			settings jsonb
		)`,
	}

	for _, s := range batch {
		driver := sess.Driver().(*sql.DB)
		if _, err := driver.Exec(s); err != nil {
			return err
		}
	}

	return nil
}

//go:generate bash -c "sed s/ADAPTER/postgresql/g ../internal/testing/adapter.go > generated_test.go"
