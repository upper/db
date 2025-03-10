package mssql

import (
	"database/sql"
	"fmt"

	db "github.com/upper/db/v4"
	"github.com/upper/db/v4/adapter/mssql"
)

type Helper struct {
	sess db.Session

	connURL mssql.ConnectionURL
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

func (h *Helper) SetUp() error {
	var err error

	h.sess, err = mssql.Open(h.connURL)
	if err != nil {
		return fmt.Errorf("error opening session: %v", err)
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

func NewHelper(connURL mssql.ConnectionURL) *Helper {
	return &Helper{
		connURL: connURL,
	}
}
