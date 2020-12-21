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

package ql

import (
	"database/sql"
	"os"

	db "github.com/upper/db/v4"
	"github.com/upper/db/v4/internal/testsuite"
)

var (
	settings ConnectionURL
)

func init() {
	settings, _ = ParseURL(os.Getenv("DB_NAME"))
}

type Helper struct {
	sess db.Session
}

func (h *Helper) Session() db.Session {
	return h.sess
}

func (h *Helper) Adapter() string {
	return "ql"
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
		`DROP TABLE IF EXISTS artist`,
		`CREATE TABLE artist (
			name string
		)`,

		`DROP TABLE IF EXISTS publication`,
		`CREATE TABLE publication (
			title string,
			author_id int
		)`,

		`DROP TABLE IF EXISTS review`,
		`CREATE TABLE review (
			publication_id int,
			name string,
			comments string,
			created time
		)`,

		`DROP TABLE IF EXISTS data_types`,
		`CREATE TABLE data_types (
			_uint uint,
			_uint8 uint8,
			_uint16 uint16,
			_uint32 uint32,
			_uint64 uint64,
			_int int,
			_int8 int8,
			_int16 int16,
			_int32 int32,
			_int64 int64,
			_float32 float32,
			_float64 float64,
			_bool bool,
			_string string,
			_blob string,
			_date time,
			_nildate time,
			_ptrdate time,
			_defaultdate time,
			_time time
		)`,

		`DROP TABLE IF EXISTS stats_test`,
		`CREATE TABLE stats_test (
			id uint,
			numeric int64,
			value int64
		)`,

		`DROP TABLE IF EXISTS composite_keys`,
		`-- Composite keys are currently not supported in QL.
		CREATE TABLE composite_keys (
		-- code string,
		-- user_id string,
			some_val string,
		-- primary key (code, user_id)
		)`,

		`DROP TABLE IF EXISTS birthdays`,
		`CREATE TABLE birthdays (
				name string,
				born time,
				born_ut int
		)`,

		`DROP TABLE IF EXISTS fibonacci`,
		`CREATE TABLE fibonacci (
				input int,
				output int
		)`,

		`DROP TABLE IF EXISTS is_even`,
		`CREATE TABLE is_even (
				input int,
				is_even bool
		)`,

		`DROP TABLE IF EXISTS CaSe_TesT`,
		`CREATE TABLE CaSe_TesT (
				case_test string
		)`,

		/*
			`DROP TABLE IF EXISTS accounts`,
			`CREATE TABLE accounts (
				name string,
				disabled bool,
				created_at time
			)`,

			`DROP TABLE IF EXISTS users`,
			`CREATE TABLE users (
				account_id int,
				username string
			)`,
			`CREATE UNIQUE INDEX users_username on users (username)`,

			`DROP TABLE IF EXISTS logs`,
			`CREATE TABLE logs (
				message string
			)`,
		*/
	}

	for _, query := range batch {
		driver := h.sess.Driver().(*sql.DB)
		tx, err := driver.Begin()
		if err != nil {
			return err
		}
		if _, err := tx.Exec(query); err != nil {
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
	}

	return nil
}

var _ testsuite.Helper = &Helper{}
