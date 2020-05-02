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
	"fmt"
	"os"

	db "github.com/upper/db"
	"github.com/upper/db/internal/sqladapter"
	"github.com/upper/db/internal/testsuite"
	"github.com/upper/db/sqlbuilder"
)

var settings = ConnectionURL{
	Database: os.Getenv("DB_NAME"),
	User:     os.Getenv("DB_USERNAME"),
	Password: os.Getenv("DB_PASSWORD"),
	Host:     os.Getenv("DB_HOST") + ":" + os.Getenv("DB_PORT"),
	Options: map[string]string{
		"timezone": testsuite.TimeZone,
	},
}

type Helper struct {
	sess sqlbuilder.Database
}

func cleanUp(sess sqlbuilder.Database) error {
	if activeStatements := sqladapter.NumActiveStatements(); activeStatements > 128 {
		return fmt.Errorf("Expecting active statements to be less than 128, got %d", activeStatements)
	}

	sess.ClearCache()

	stats, err := getStats(sess)
	if err != nil {
		return err
	}

	if stats["pg_prepared_statements_count"] != 0 {
		return fmt.Errorf(`Expecting "Prepared_stmt_count" to be 0, got %d`, stats["Prepared_stmt_count"])
	}

	return nil
}

func getStats(sess sqlbuilder.Database) (map[string]int, error) {
	stats := make(map[string]int)

	row := sess.Driver().(*sql.DB).QueryRow(`SELECT count(1) AS value FROM pg_prepared_statements`)

	var value int
	err := row.Scan(&value)
	if err != nil {
		return nil, err
	}

	stats["pg_prepared_statements_count"] = value

	return stats, nil
}

func (h *Helper) Session() db.Database {
	return h.sess
}

func (h *Helper) SQLBuilder() sqlbuilder.Database {
	return h.sess
}

func (h *Helper) Adapter() string {
	return "postgresql"
}

func (h *Helper) TearDown() error {
	if err := cleanUp(h.sess); err != nil {
		return err
	}

	return h.sess.Close()
}

func (h *Helper) TearUp() error {
	var err error

	h.sess, err = Open(settings)
	if err != nil {
		return err
	}

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
			_blob bytea,
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

		`DROP TABLE IF EXISTS test_schema.test`,
		`DROP SCHEMA IF EXISTS test_schema`,

		`CREATE SCHEMA test_schema`,
		`CREATE TABLE test_schema.test (id integer)`,

		`DROP TABLE IF EXISTS pg_types`,
		`CREATE TABLE pg_types (id serial primary key
			, uint8_value smallint
			, uint8_value_array smallint[]

			, int64_value smallint
			, int64_value_array smallint[]

			, integer_array integer[]
			, string_array text[]
			, jsonb_map jsonb

			, integer_array_ptr integer[]
			, string_array_ptr text[]
			, jsonb_map_ptr jsonb

			, auto_integer_array integer[]
			, auto_string_array text[]
			, auto_jsonb_map jsonb
			, auto_jsonb_map_string jsonb
			, auto_jsonb_map_integer jsonb

			, jsonb_object jsonb
			, jsonb_array jsonb

			, custom_jsonb_object jsonb
			, auto_custom_jsonb_object jsonb

			, custom_jsonb_object_ptr jsonb
			, auto_custom_jsonb_object_ptr jsonb

			, custom_jsonb_object_array jsonb
			, auto_custom_jsonb_object_array jsonb
			, auto_custom_jsonb_object_map jsonb

			, string_value varchar(255)
			, integer_value int
			, varchar_value varchar(64)
			, decimal_value decimal

			, integer_compat_value int
			, uinteger_compat_value int
			, string_compat_value text

			, integer_compat_value_jsonb_array jsonb
			, string_compat_value_jsonb_array jsonb
			, uinteger_compat_value_jsonb_array jsonb

			, string_value_ptr varchar(255)
			, integer_value_ptr int
			, varchar_value_ptr varchar(64)
			, decimal_value_ptr decimal

		)`,

		`DROP TABLE IF EXISTS issue_370`,
		`CREATE TABLE issue_370 (
			id UUID PRIMARY KEY,
			name VARCHAR(25)
		)`,

		`DROP TABLE IF EXISTS issue_370_2`,
		`CREATE TABLE issue_370_2 (
			id INTEGER[3] PRIMARY KEY,
			name VARCHAR(25)
		)`,

		`DROP TABLE IF EXISTS varchar_primary_key`,
		`CREATE TABLE varchar_primary_key (
			address VARCHAR(42) PRIMARY KEY NOT NULL,
			name VARCHAR(25)
		)`,

		`DROP TABLE IF EXISTS "birthdays"`,
		`CREATE TABLE "birthdays" (
		"id" serial primary key,
		"name" CHARACTER VARYING(50),
		"born" TIMESTAMP WITH TIME ZONE,
		"born_ut" INT
	)`,

		`DROP TABLE IF EXISTS "fibonacci"`,
		`CREATE TABLE "fibonacci" (
		"id" serial primary key,
		"input" NUMERIC,
		"output" NUMERIC
	)`,

		`DROP TABLE IF EXISTS "is_even"`,
		`CREATE TABLE "is_even" (
		"input" NUMERIC,
		"is_even" BOOL
	)`,

		`DROP TABLE IF EXISTS "CaSe_TesT"`,
		`CREATE TABLE "CaSe_TesT" (
		"id" SERIAL PRIMARY KEY,
		"case_test" VARCHAR(60)
	)`,
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
