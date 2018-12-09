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

//go:generate bash -c "sed s/ADAPTER/mysql/g ../internal/sqladapter/testing/adapter.go.tpl > generated_test.go"
package mysql

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"upper.io/db.v3/internal/sqladapter"
	"upper.io/db.v3/lib/sqlbuilder"
)

const (
	testTimeZone = "Canada/Eastern"
)

var settings = ConnectionURL{
	Database: os.Getenv("DB_NAME"),
	User:     os.Getenv("DB_USERNAME"),
	Password: os.Getenv("DB_PASSWORD"),
	Host:     os.Getenv("DB_HOST") + ":" + os.Getenv("DB_PORT"),
	Options: map[string]string{
		// See https://github.com/go-sql-driver/mysql/issues/9
		"parseTime": "true",
		// Might require you to use mysql_tzinfo_to_sql /usr/share/zoneinfo | mysql -u root -p mysql
		"time_zone": fmt.Sprintf(`"%s"`, testTimeZone),
	},
}

func tearUp() error {
	sess := mustOpen()
	defer sess.Close()

	batch := []string{
		`DROP TABLE IF EXISTS artist`,

		`CREATE TABLE artist (
			id BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT,
			PRIMARY KEY(id),
			name VARCHAR(60)
		)`,

		`DROP TABLE IF EXISTS publication`,

		`CREATE TABLE publication (
			id BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT,
			PRIMARY KEY(id),
			title VARCHAR(80),
			author_id BIGINT(20)
		)`,

		`DROP TABLE IF EXISTS review`,

		`CREATE TABLE review (
			id BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT,
			PRIMARY KEY(id),
			publication_id BIGINT(20),
			name VARCHAR(80),
			comments TEXT,
			created DATETIME NOT NULL
		)`,

		`DROP TABLE IF EXISTS data_types`,

		`CREATE TABLE data_types (
			id BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT,
			PRIMARY KEY(id),
			_uint INT(10) UNSIGNED DEFAULT 0,
			_uint8 INT(10) UNSIGNED DEFAULT 0,
			_uint16 INT(10) UNSIGNED DEFAULT 0,
			_uint32 INT(10) UNSIGNED DEFAULT 0,
			_uint64 INT(10) UNSIGNED DEFAULT 0,
			_int INT(10) DEFAULT 0,
			_int8 INT(10) DEFAULT 0,
			_int16 INT(10) DEFAULT 0,
			_int32 INT(10) DEFAULT 0,
			_int64 INT(10) DEFAULT 0,
			_float32 DECIMAL(10,6),
			_float64 DECIMAL(10,6),
			_bool TINYINT(1),
			_string text,
			_blob blob,
			_date TIMESTAMP NULL,
			_nildate DATETIME NULL,
			_ptrdate DATETIME NULL,
			_defaultdate TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			_time BIGINT UNSIGNED NOT NULL DEFAULT 0
		)`,

		`DROP TABLE IF EXISTS stats_test`,

		`CREATE TABLE stats_test (
			id BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT, PRIMARY KEY(id),
			` + "`numeric`" + ` INT(10),
			` + "`value`" + ` INT(10)
		)`,

		`DROP TABLE IF EXISTS composite_keys`,

		`CREATE TABLE composite_keys (
			code VARCHAR(255) default '',
			user_id VARCHAR(255) default '',
			some_val VARCHAR(255) default '',
			primary key (code, user_id)
		)`,

		`CREATE TABLE admin (
			ID int(11) NOT NULL AUTO_INCREMENT,
			Accounts varchar(255) DEFAULT '',
			LoginPassWord varchar(255) DEFAULT '',
			Date TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
			PRIMARY KEY (ID,Date)
		) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8`,

		`CREATE TABLE my_types (id int(11) NOT NULL AUTO_INCREMENT, PRIMARY KEY(id)
			, json_map JSON
			, json_map_ptr JSON

			, auto_json_map JSON
			, auto_json_map_string JSON
			, auto_json_map_integer JSON

			, json_object JSON
			, json_array JSON

			, custom_json_object JSON
			, auto_custom_json_object JSON

			, custom_json_object_ptr JSON
			, auto_custom_json_object_ptr JSON

			, custom_json_object_array JSON
			, auto_custom_json_object_array JSON
			, auto_custom_json_object_map JSON

			, integer_compat_value_json_array JSON
			, string_compat_value_json_array JSON
			, uinteger_compat_value_json_array JSON

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

func getStats(sess sqlbuilder.Database) (map[string]int, error) {
	stats := make(map[string]int)

	res, err := sess.Driver().(*sql.DB).Query(`SHOW GLOBAL STATUS LIKE '%stmt%'`)
	if err != nil {
		return nil, err
	}
	var result struct {
		VariableName string `db:"Variable_name"`
		Value        int    `db:"Value"`
	}

	iter := sqlbuilder.NewIterator(res)
	for iter.Next(&result) {
		stats[result.VariableName] = result.Value
	}

	return stats, nil
}

func TestInsertReturningCompositeKey_Issue383(t *testing.T) {
	sess := mustOpen()
	defer sess.Close()

	type Admin struct {
		ID            int       `db:"ID,omitempty"`
		Accounts      string    `db:"Accounts"`
		LoginPassWord string    `db:"LoginPassWord"`
		Date          time.Time `db:"Date"`
	}

	dateNow := time.Now()

	a := Admin{
		Accounts:      "admin",
		LoginPassWord: "E10ADC3949BA59ABBE56E057F20F883E",
		Date:          dateNow,
	}

	adminCollection := sess.Collection("admin")
	err := adminCollection.InsertReturning(&a)
	assert.NoError(t, err)

	assert.NotZero(t, a.ID)
	assert.NotZero(t, a.Date)
	assert.Equal(t, "admin", a.Accounts)
	assert.Equal(t, "E10ADC3949BA59ABBE56E057F20F883E", a.LoginPassWord)

	b := Admin{
		Accounts:      "admin2",
		LoginPassWord: "E10ADC3949BA59ABBE56E057F20F883E",
		Date:          dateNow,
	}

	err = adminCollection.InsertReturning(&b)
	assert.NoError(t, err)

	assert.NotZero(t, b.ID)
	assert.NotZero(t, b.Date)
	assert.Equal(t, "admin2", b.Accounts)
	assert.Equal(t, "E10ADC3949BA59ABBE56E057F20F883E", a.LoginPassWord)
}

func TestIssue469_BadConnection(t *testing.T) {
	var err error

	sess := mustOpen()
	defer sess.Close()

	// Ask the MySQL server to disconnect sessions that remain inactive for more
	// than 1 second.
	_, err = sess.Exec(`SET SESSION wait_timeout=1`)
	assert.NoError(t, err)

	// Remain inactive for 2 seconds.
	time.Sleep(time.Second * 2)

	// A query should start a new connection, even if the server disconnected us.
	_, err = sess.Collection("artist").Find().Count()
	assert.NoError(t, err)

	// This is a new session, ask the MySQL server to disconnect sessions that
	// remain inactive for more than 1 second.
	_, err = sess.Exec(`SET SESSION wait_timeout=1`)
	assert.NoError(t, err)

	// Remain inactive for 2 seconds.
	time.Sleep(time.Second * 2)

	// At this point the server should have disconnected us. Let's try to create
	// a transaction anyway.
	err = sess.Tx(nil, func(sess sqlbuilder.Tx) error {
		var err error

		_, err = sess.Collection("artist").Find().Count()
		if err != nil {
			return err
		}
		return nil
	})
	assert.NoError(t, err)

	// This is a new session, ask the MySQL server to disconnect sessions that
	// remain inactive for more than 1 second.
	_, err = sess.Exec(`SET SESSION wait_timeout=1`)
	assert.NoError(t, err)

	err = sess.Tx(nil, func(sess sqlbuilder.Tx) error {
		var err error

		// This query should succeed.
		_, err = sess.Collection("artist").Find().Count()
		if err != nil {
			panic(err.Error())
		}

		// Remain inactive for 2 seconds.
		time.Sleep(time.Second * 2)

		// This query should fail because the server disconnected us in the middle
		// of a transaction.
		_, err = sess.Collection("artist").Find().Count()
		if err != nil {
			return err
		}

		return nil
	})

	assert.Error(t, err, "Expecting an error (can't recover from this)")
}

func TestMySQLTypes(t *testing.T) {
	sess := mustOpen()
	defer sess.Close()

	type MyTypeInline struct {
		JSONMapPtr *JSONMap `db:"json_map_ptr,omitempty"`
	}

	type MyTypeAutoInline struct {
		AutoJSONMap        map[string]interface{} `db:"auto_json_map"`
		AutoJSONMapString  map[string]string      `db:"auto_json_map_string"`
		AutoJSONMapInteger map[string]int64       `db:"auto_json_map_integer"`
	}

	type MyType struct {
		ID int64 `db:"id,omitempty"`

		JSONMap JSONMap `db:"json_map"`

		JSONObject JSON      `db:"json_object"`
		JSONArray  JSONArray `db:"json_array"`

		CustomJSONObject     customJSON     `db:"custom_json_object"`
		AutoCustomJSONObject autoCustomJSON `db:"auto_custom_json_object"`

		CustomJSONObjectPtr     *customJSON     `db:"custom_json_object_ptr,omitempty"`
		AutoCustomJSONObjectPtr *autoCustomJSON `db:"auto_custom_json_object_ptr,omitempty"`

		AutoCustomJSONObjectArray []autoCustomJSON          `db:"auto_custom_json_object_array"`
		AutoCustomJSONObjectMap   map[string]autoCustomJSON `db:"auto_custom_json_object_map"`

		Int64CompatValueJSONArray  []int64Compat   `db:"integer_compat_value_json_array"`
		UIntCompatValueJSONArray   uintCompatArray `db:"uinteger_compat_value_json_array"`
		StringCompatValueJSONArray []stringCompat  `db:"string_compat_value_json_array"`
	}

	origMyTypeTests := []MyType{
		MyType{
			Int64CompatValueJSONArray:  []int64Compat{1, -2, 3, -4},
			UIntCompatValueJSONArray:   []uintCompat{1, 2, 3, 4},
			StringCompatValueJSONArray: []stringCompat{"a", "b", "", "c"},
		},
		MyType{
			Int64CompatValueJSONArray:  []int64Compat(nil),
			UIntCompatValueJSONArray:   []uintCompat(nil),
			StringCompatValueJSONArray: []stringCompat(nil),
		},
		MyType{
			AutoCustomJSONObjectArray: []autoCustomJSON{
				autoCustomJSON{
					N: "Hello",
				},
				autoCustomJSON{
					N: "World",
				},
			},
			AutoCustomJSONObjectMap: map[string]autoCustomJSON{
				"a": autoCustomJSON{
					N: "Hello",
				},
				"b": autoCustomJSON{
					N: "World",
				},
			},
			JSONArray: JSONArray{float64(1), float64(2), float64(3), float64(4)},
		},
		MyType{
			JSONArray: JSONArray{},
		},
		MyType{
			JSONArray: JSONArray(nil),
		},
		MyType{},
		MyType{
			CustomJSONObject: customJSON{
				N: "Hello",
			},
			AutoCustomJSONObject: autoCustomJSON{
				N: "World",
			},
		},
		MyType{
			CustomJSONObject:     customJSON{},
			AutoCustomJSONObject: autoCustomJSON{},
		},
		MyType{
			CustomJSONObject: customJSON{
				N: "Hello 1",
			},
			AutoCustomJSONObject: autoCustomJSON{
				N: "World 2",
			},
		},
		MyType{
			CustomJSONObjectPtr:     nil,
			AutoCustomJSONObjectPtr: nil,
		},
		MyType{
			CustomJSONObjectPtr:     &customJSON{},
			AutoCustomJSONObjectPtr: &autoCustomJSON{},
		},
		MyType{
			CustomJSONObjectPtr: &customJSON{
				N: "Hello 3",
			},
			AutoCustomJSONObjectPtr: &autoCustomJSON{
				N: "World 4",
			},
		},
		MyType{
			CustomJSONObject: customJSON{
				V: 4.4,
			},
		},
		MyType{
			CustomJSONObject: customJSON{},
		},
		MyType{
			CustomJSONObject: customJSON{
				N: "Peter",
				V: 5.56,
			},
		},
	}

	for i := 0; i < 100; i++ {

		myTypeTests := make([]MyType, len(origMyTypeTests))
		perm := rand.Perm(len(origMyTypeTests))
		for i, v := range perm {
			myTypeTests[v] = origMyTypeTests[i]
		}

		for i := range myTypeTests {
			id, err := sess.Collection("my_types").Insert(myTypeTests[i])
			assert.NoError(t, err)

			var actual MyType
			err = sess.Collection("my_types").Find(id).One(&actual)
			assert.NoError(t, err)

			expected := myTypeTests[i]
			expected.ID = id.(int64)
			assert.Equal(t, expected, actual)
		}

		for i := range myTypeTests {
			res, err := sess.InsertInto("my_types").Values(myTypeTests[i]).Exec()
			assert.NoError(t, err)

			id, err := res.LastInsertId()
			assert.NoError(t, err)
			assert.NotEqual(t, 0, id)

			var actual MyType
			err = sess.Collection("my_types").Find(id).One(&actual)
			assert.NoError(t, err)

			expected := myTypeTests[i]
			expected.ID = id

			assert.Equal(t, expected, actual)

			var actual2 MyType
			err = sess.SelectFrom("my_types").Where("id = ?", id).One(&actual2)
			assert.NoError(t, err)
			assert.Equal(t, expected, actual2)
		}

		inserter := sess.InsertInto("my_types")
		for i := range myTypeTests {
			inserter = inserter.Values(myTypeTests[i])
		}
		_, err := inserter.Exec()
		assert.NoError(t, err)

		err = sess.Collection("my_types").Truncate()
		assert.NoError(t, err)

		batch := sess.InsertInto("my_types").Batch(50)
		go func() {
			defer batch.Done()
			for i := range myTypeTests {
				batch.Values(myTypeTests[i])
			}
		}()

		err = batch.Wait()
		assert.NoError(t, err)

		var values []MyType
		err = sess.SelectFrom("my_types").All(&values)
		assert.NoError(t, err)

		for i := range values {
			expected := myTypeTests[i]
			expected.ID = values[i].ID
			assert.Equal(t, expected, values[i])
		}
	}
}

func cleanUpCheck(sess sqlbuilder.Database) (err error) {
	var stats map[string]int

	stats, err = getStats(sess)
	if err != nil {
		return err
	}

	if activeStatements := sqladapter.NumActiveStatements(); activeStatements > 128 {
		return fmt.Errorf("Expecting active statements to be at most 128, got %d", activeStatements)
	}

	sess.ClearCache()

	if activeStatements := sqladapter.NumActiveStatements(); activeStatements != 0 {
		return fmt.Errorf("Expecting active statements to be 0, got %d", activeStatements)
	}

	for i := 0; i < 10; i++ {
		stats, err = getStats(sess)
		if err != nil {
			return err
		}

		if stats["Prepared_stmt_count"] != 0 {
			time.Sleep(time.Millisecond * 200) // Sometimes it takes a bit to clean prepared statements
			err = fmt.Errorf(`Expecting "Prepared_stmt_count" to be 0, got %d`, stats["Prepared_stmt_count"])
			continue
		}
		break
	}

	return err
}

type int64Compat int64

type uintCompat uint

type stringCompat string

type uint8Compat uint8

type int64CompatArray []int64Compat

type uint8CompatArray []uint8Compat

type uintCompatArray []uintCompat

func (u *uint8Compat) Scan(src interface{}) error {
	if src != nil {
		switch v := src.(type) {
		case int64:
			*u = uint8Compat((src).(int64))
		case []byte:
			i, err := strconv.ParseInt(string(v), 10, 64)
			if err != nil {
				return err
			}
			*u = uint8Compat(i)
		default:
			panic(fmt.Sprintf("expected type %T", src))
		}
	}
	return nil
}

func (u *int64Compat) Scan(src interface{}) error {
	if src != nil {
		switch v := src.(type) {
		case int64:
			*u = int64Compat((src).(int64))
		case []byte:
			i, err := strconv.ParseInt(string(v), 10, 64)
			if err != nil {
				return err
			}
			*u = int64Compat(i)
		default:
			panic(fmt.Sprintf("expected type %T", src))
		}
	}
	return nil
}

type customJSON struct {
	N string  `json:"name"`
	V float64 `json:"value"`
}

func (c customJSON) Value() (driver.Value, error) {
	return JSONValue(c)
}

func (c *customJSON) Scan(src interface{}) error {
	return ScanJSON(c, src)
}

type autoCustomJSON struct {
	N string  `json:"name"`
	V float64 `json:"value"`

	*JSONConverter
}

var (
	_ = driver.Valuer(&customJSON{})
	_ = sql.Scanner(&customJSON{})
)
