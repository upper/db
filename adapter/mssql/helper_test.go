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

package mssql

import (
	"database/sql"
	"os"

	db "github.com/upper/db/v4"
	"github.com/upper/db/v4/internal/testsuite"
)

var settings = ConnectionURL{
	Database: os.Getenv("DB_NAME"),
	User:     os.Getenv("DB_USERNAME"),
	Password: os.Getenv("DB_PASSWORD"),
	Host:     os.Getenv("DB_HOST") + ":" + os.Getenv("DB_PORT"),
	Options:  map[string]string{},
}

type Helper struct {
	sess db.Session
}

func (h *Helper) Session() db.Session {
	return h.sess
}

func (h *Helper) Adapter() string {
	return "mssql"
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
			id BIGINT PRIMARY KEY NOT NULL IDENTITY(1,1),
			name VARCHAR(60)
		)`,

		`DROP TABLE IF EXISTS publication`,

		`CREATE TABLE publication (
			id BIGINT PRIMARY KEY NOT NULL IDENTITY(1,1),
			title VARCHAR(80),
			author_id BIGINT
		)`,

		`DROP TABLE IF EXISTS review`,
		`CREATE TABLE review (
			id BIGINT PRIMARY KEY NOT NULL IDENTITY(1,1),
			publication_id BIGINT,
			name VARCHAR(80),
			comments TEXT,
			created DATETIME NOT NULL
		)`,

		`DROP TABLE IF EXISTS data_types`,
		`CREATE TABLE data_types (
			id BIGINT PRIMARY KEY NOT NULL IDENTITY(1,1),
			_uint INT DEFAULT 0,
			_uint8 INT DEFAULT 0,
			_uint16 INT DEFAULT 0,
			_uint32 INT DEFAULT 0,
			_uint64 INT DEFAULT 0,
			_int INT DEFAULT 0,
			_int8 INT DEFAULT 0,
			_int16 INT DEFAULT 0,
			_int32 INT DEFAULT 0,
			_int64 INT DEFAULT 0,
			_float32 DECIMAL(10,6),
			_float64 DECIMAL(10,6),
			_bool TINYINT,
			_string TEXT,
			_blob BINARY(12),
			_date DATETIMEOFFSET(4) NULL,
			_nildate DATETIME NULL,
			_ptrdate DATETIME NULL,
			_defaultdate DATETIME NOT NULL DEFAULT(GETDATE()),
			_time BIGINT NOT NULL DEFAULT 0
		)`,

		`DROP TABLE IF EXISTS stats_test`,
		`CREATE TABLE stats_test (
			id BIGINT PRIMARY KEY NOT NULL IDENTITY(1,1),
			[numeric] INT,
			[value] INT
		)`,

		`DROP TABLE IF EXISTS composite_keys`,
		`CREATE TABLE composite_keys (
			code VARCHAR(255) default '',
			user_id VARCHAR(255) default '',
			some_val VARCHAR(255) default '',
			PRIMARY KEY (code, user_id)
		)`,

		`DROP TABLE IF EXISTS [birthdays]`,
		`CREATE TABLE [birthdays] (
			id BIGINT IDENTITY(1, 1) PRIMARY KEY NOT NULL,
			name NVARCHAR(50),
			born DATETIMEOFFSET,
			born_ut BIGINT
		)`,

		`DROP TABLE IF EXISTS [fibonacci]`,
		`CREATE TABLE [fibonacci] (
			id BIGINT PRIMARY KEY NOT NULL IDENTITY(1,1),
			input BIGINT NOT NULL,
			output BIGINT NOT NULL
		)`,

		`DROP TABLE IF EXISTS [is_even]`,
		`CREATE TABLE [is_even] (
			input BIGINT NOT NULL,
			is_even TINYINT
		)`,

		`DROP TABLE IF EXISTS [CaSe_TesT]`,
		`CREATE TABLE [CaSe_TesT] (
			id BIGINT PRIMARY KEY NOT NULL IDENTITY(1,1),
			case_test NVARCHAR(60)
		)`,

		`DROP TABLE IF EXISTS [accounts]`,
		`CREATE TABLE [accounts] (
			id BIGINT PRIMARY KEY NOT NULL IDENTITY(1,1),
			name nvarchar(255),
			disabled tinyint,
			created_at DATETIME NOT NULL DEFAULT(GETDATE())
		)`,

		`DROP TABLE IF EXISTS [users]`,
		`CREATE TABLE [users] (
			id BIGINT PRIMARY KEY NOT NULL IDENTITY(1,1),
			account_id BIGINT,
			username nvarchar(255) UNIQUE
		)`,

		`DROP TABLE IF EXISTS [logs]`,
		`CREATE TABLE [logs] (
			id	BIGINT PRIMARY KEY NOT NULL IDENTITY(1,1),
			message NVARCHAR(255)
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
