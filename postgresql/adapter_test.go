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

//go:generate bash -c "sed s/ADAPTER/postgresql/g ../internal/sqladapter/testing/adapter.go.tpl > generated_test.go"
package postgresql

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
	"upper.io/db.v3"
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
		"timezone": testTimeZone,
	},
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
	}

	for _, s := range batch {
		driver := sess.Driver().(*sql.DB)
		if _, err := driver.Exec(s); err != nil {
			return err
		}
	}

	return nil
}

type customJSONB struct {
	N string  `json:"name"`
	V float64 `json:"value"`
}

func (c customJSONB) Value() (driver.Value, error) {
	return JSONBValue(c)
}

func (c *customJSONB) Scan(src interface{}) error {
	return ScanJSONB(c, src)
}

type autoCustomJSONB struct {
	N string  `json:"name"`
	V float64 `json:"value"`

	*JSONBConverter
}

var (
	_ = driver.Valuer(&customJSONB{})
	_ = sql.Scanner(&customJSONB{})
)

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

func (u uint8CompatArray) WrapValue(src interface{}) interface{} {
	return Array(src)
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

func (u int64CompatArray) WrapValue(src interface{}) interface{} {
	return Array(src)
}

func TestIssue469_BadConnection(t *testing.T) {
	var err error

	sess := mustOpen()
	defer sess.Close()

	// Ask the PostgreSQL server to disconnect sessions that remain inactive for more
	// than 1 second.
	_, err = sess.Exec(`SET SESSION idle_in_transaction_session_timeout=1000`)
	assert.NoError(t, err)

	// Remain inactive for 2 seconds.
	time.Sleep(time.Second * 2)

	// A query should start a new connection, even if the server disconnected us.
	_, err = sess.Collection("artist").Find().Count()
	assert.NoError(t, err)

	// This is a new session, ask the PostgreSQL server to disconnect sessions that
	// remain inactive for more than 1 second.
	_, err = sess.Exec(`SET SESSION idle_in_transaction_session_timeout=1000`)
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

	// This is a new session, ask the PostgreSQL server to disconnect sessions that
	// remain inactive for more than 1 second.
	_, err = sess.Exec(`SET SESSION idle_in_transaction_session_timeout=1000`)
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

func testPostgreSQLTypes(t *testing.T, sess sqlbuilder.Database) {

	type PGTypeInline struct {
		IntegerArrayPtr *Int64Array  `db:"integer_array_ptr,omitempty"`
		StringArrayPtr  *StringArray `db:"string_array_ptr,omitempty"`
		JSONBMapPtr     *JSONBMap    `db:"jsonb_map_ptr,omitempty"`
	}

	type PGTypeAutoInline struct {
		AutoIntegerArray    []int64                `db:"auto_integer_array"`
		AutoStringArray     []string               `db:"auto_string_array"`
		AutoJSONBMap        map[string]interface{} `db:"auto_jsonb_map"`
		AutoJSONBMapString  map[string]string      `db:"auto_jsonb_map_string"`
		AutoJSONBMapInteger map[string]int64       `db:"auto_jsonb_map_integer"`
	}

	type PGType struct {
		ID int64 `db:"id,omitempty"`

		UInt8Value      uint8Compat      `db:"uint8_value"`
		UInt8ValueArray uint8CompatArray `db:"uint8_value_array"`

		Int64Value      int64Compat      `db:"int64_value"`
		Int64ValueArray int64CompatArray `db:"int64_value_array"`

		IntegerArray Int64Array  `db:"integer_array"`
		StringArray  StringArray `db:"string_array,stringarray"`
		JSONBMap     JSONBMap    `db:"jsonb_map"`

		PGTypeInline `db:",inline"`

		PGTypeAutoInline `db:",inline"`

		JSONBObject JSONB      `db:"jsonb_object"`
		JSONBArray  JSONBArray `db:"jsonb_array"`

		CustomJSONBObject     customJSONB     `db:"custom_jsonb_object"`
		AutoCustomJSONBObject autoCustomJSONB `db:"auto_custom_jsonb_object"`

		CustomJSONBObjectPtr     *customJSONB     `db:"custom_jsonb_object_ptr,omitempty"`
		AutoCustomJSONBObjectPtr *autoCustomJSONB `db:"auto_custom_jsonb_object_ptr,omitempty"`

		AutoCustomJSONBObjectArray []autoCustomJSONB          `db:"auto_custom_jsonb_object_array"`
		AutoCustomJSONBObjectMap   map[string]autoCustomJSONB `db:"auto_custom_jsonb_object_map"`

		StringValue  string  `db:"string_value"`
		IntegerValue int64   `db:"integer_value"`
		VarcharValue string  `db:"varchar_value"`
		DecimalValue float64 `db:"decimal_value"`

		Int64CompatValue  int64Compat  `db:"integer_compat_value"`
		UIntCompatValue   uintCompat   `db:"uinteger_compat_value"`
		StringCompatValue stringCompat `db:"string_compat_value"`

		Int64CompatValueJSONBArray  []int64Compat   `db:"integer_compat_value_jsonb_array"`
		UIntCompatValueJSONBArray   uintCompatArray `db:"uinteger_compat_value_jsonb_array"`
		StringCompatValueJSONBArray []stringCompat  `db:"string_compat_value_jsonb_array"`

		StringValuePtr  *string  `db:"string_value_ptr,omitempty"`
		IntegerValuePtr *int64   `db:"integer_value_ptr,omitempty"`
		VarcharValuePtr *string  `db:"varchar_value_ptr,omitempty"`
		DecimalValuePtr *float64 `db:"decimal_value_ptr,omitempty"`
	}

	integerValue := int64(10)
	stringValue := string("ten")
	decimalValue := float64(10.0)

	integerArrayValue := Int64Array{1, 2, 3, 4}
	stringArrayValue := StringArray{"a", "b", "c"}
	jsonbMapValue := JSONBMap{"Hello": "World"}

	testValue := "Hello world!"

	origPgTypeTests := []PGType{
		PGType{
			UInt8Value:      7,
			UInt8ValueArray: uint8CompatArray{1, 2, 3, 4, 5, 6},
		},
		PGType{
			Int64Value:      -1,
			Int64ValueArray: int64CompatArray{1, 2, 3, -4, 5, 6},
		},
		PGType{
			UInt8Value:      1,
			UInt8ValueArray: uint8CompatArray{1, 2, 3, 4, 5, 6},
		},
		PGType{
			Int64Value:      1,
			Int64ValueArray: int64CompatArray{7, 7, 7},
		},
		PGType{
			Int64Value:      1,
			Int64ValueArray: int64CompatArray{},
		},
		PGType{
			Int64Value:      99,
			Int64ValueArray: nil,
		},
		PGType{
			Int64CompatValue:  -5,
			UIntCompatValue:   3,
			StringCompatValue: "abc",
		},
		PGType{
			Int64CompatValueJSONBArray:  []int64Compat{1, -2, 3, -4},
			UIntCompatValueJSONBArray:   []uintCompat{1, 2, 3, 4},
			StringCompatValueJSONBArray: []stringCompat{"a", "b", "", "c"},
		},
		PGType{
			Int64CompatValueJSONBArray:  []int64Compat(nil),
			UIntCompatValueJSONBArray:   []uintCompat(nil),
			StringCompatValueJSONBArray: []stringCompat(nil),
		},
		PGType{
			IntegerValuePtr: &integerValue,
			StringValuePtr:  &stringValue,
			DecimalValuePtr: &decimalValue,
			PGTypeAutoInline: PGTypeAutoInline{
				AutoJSONBMapString:  map[string]string{"a": "x", "b": "67"},
				AutoJSONBMapInteger: map[string]int64{"a": 12, "b": 13},
			},
		},
		PGType{
			IntegerValue: integerValue,
			StringValue:  stringValue,
			DecimalValue: decimalValue,
		},
		PGType{
			IntegerArray: []int64{1, 2, 3, 4},
		},
		PGType{
			PGTypeAutoInline: PGTypeAutoInline{
				AutoIntegerArray: Int64Array{1, 2, 3, 4},
				AutoStringArray:  nil,
			},
		},
		PGType{
			AutoCustomJSONBObjectArray: []autoCustomJSONB{
				autoCustomJSONB{
					N: "Hello",
				},
				autoCustomJSONB{
					N: "World",
				},
			},
			AutoCustomJSONBObjectMap: map[string]autoCustomJSONB{
				"a": autoCustomJSONB{
					N: "Hello",
				},
				"b": autoCustomJSONB{
					N: "World",
				},
			},
			PGTypeAutoInline: PGTypeAutoInline{
				AutoJSONBMap: map[string]interface{}{
					"Hello": "world",
					"Roses": "red",
				},
			},
			JSONBArray: JSONBArray{float64(1), float64(2), float64(3), float64(4)},
		},
		PGType{
			PGTypeAutoInline: PGTypeAutoInline{
				AutoIntegerArray: nil,
			},
		},
		PGType{
			PGTypeAutoInline: PGTypeAutoInline{
				AutoJSONBMap: map[string]interface{}{},
			},
			JSONBArray: JSONBArray{},
		},
		PGType{
			PGTypeAutoInline: PGTypeAutoInline{
				AutoJSONBMap: map[string]interface{}(nil),
			},
			JSONBArray: JSONBArray(nil),
		},
		PGType{
			PGTypeAutoInline: PGTypeAutoInline{
				AutoStringArray: []string{"aaa", "bbb", "ccc"},
			},
		},
		PGType{
			PGTypeAutoInline: PGTypeAutoInline{
				AutoStringArray: nil,
			},
		},
		PGType{
			PGTypeAutoInline: PGTypeAutoInline{
				AutoJSONBMap: map[string]interface{}{"hello": "world!"},
			},
		},
		PGType{
			IntegerArray: []int64{1, 2, 3, 4},
			StringArray:  []string{"a", "boo", "bar"},
		},
		PGType{
			StringValue:  stringValue,
			DecimalValue: decimalValue,
		},
		PGType{
			IntegerArray: []int64{},
		},
		PGType{
			StringArray: []string{},
		},
		PGType{
			IntegerArray: []int64{},
			StringArray:  []string{},
		},
		PGType{},
		PGType{
			IntegerArray: []int64{1},
			StringArray:  []string{"a"},
		},
		PGType{
			PGTypeInline: PGTypeInline{
				IntegerArrayPtr: &integerArrayValue,
				StringArrayPtr:  &stringArrayValue,
				JSONBMapPtr:     &jsonbMapValue,
			},
		},
		PGType{
			IntegerArray: []int64{0, 0, 0, 0},
			StringValue:  testValue,
			CustomJSONBObject: customJSONB{
				N: "Hello",
			},
			AutoCustomJSONBObject: autoCustomJSONB{
				N: "World",
			},
			StringArray: []string{"", "", "", ``, `""`},
		},
		PGType{
			CustomJSONBObject:     customJSONB{},
			AutoCustomJSONBObject: autoCustomJSONB{},
		},
		PGType{
			CustomJSONBObject: customJSONB{
				N: "Hello 1",
			},
			AutoCustomJSONBObject: autoCustomJSONB{
				N: "World 2",
			},
		},
		PGType{
			CustomJSONBObjectPtr:     nil,
			AutoCustomJSONBObjectPtr: nil,
		},
		PGType{
			CustomJSONBObjectPtr:     &customJSONB{},
			AutoCustomJSONBObjectPtr: &autoCustomJSONB{},
		},
		PGType{
			CustomJSONBObjectPtr: &customJSONB{
				N: "Hello 3",
			},
			AutoCustomJSONBObjectPtr: &autoCustomJSONB{
				N: "World 4",
			},
		},
		PGType{
			StringValue: testValue,
		},
		PGType{
			IntegerValue:    integerValue,
			IntegerValuePtr: &integerValue,
			CustomJSONBObject: customJSONB{
				V: 4.4,
			},
		},
		PGType{
			StringArray: []string{"a", "boo", "bar"},
		},
		PGType{
			StringArray:       []string{"a", "boo", "bar", `""`},
			CustomJSONBObject: customJSONB{},
		},
		PGType{
			IntegerArray: []int64{0},
			StringArray:  []string{""},
		},
		PGType{
			CustomJSONBObject: customJSONB{
				N: "Peter",
				V: 5.56,
			},
		},
	}

	for i := 0; i < 100; i++ {

		pgTypeTests := make([]PGType, len(origPgTypeTests))
		perm := rand.Perm(len(origPgTypeTests))
		for i, v := range perm {
			pgTypeTests[v] = origPgTypeTests[i]
		}

		for i := range pgTypeTests {
			id, err := sess.Collection("pg_types").Insert(pgTypeTests[i])
			assert.NoError(t, err)

			var actual PGType
			err = sess.Collection("pg_types").Find(id).One(&actual)
			assert.NoError(t, err)

			expected := pgTypeTests[i]
			expected.ID = id.(int64)
			assert.Equal(t, expected, actual)
		}

		for i := range pgTypeTests {
			row, err := sess.InsertInto("pg_types").Values(pgTypeTests[i]).Returning("id").QueryRow()
			assert.NoError(t, err)

			var id int64
			err = row.Scan(&id)
			assert.NoError(t, err)

			var actual PGType
			err = sess.Collection("pg_types").Find(id).One(&actual)
			assert.NoError(t, err)

			expected := pgTypeTests[i]
			expected.ID = id

			assert.Equal(t, expected, actual)

			var actual2 PGType
			err = sess.SelectFrom("pg_types").Where("id = ?", id).One(&actual2)
			assert.NoError(t, err)
			assert.Equal(t, expected, actual2)
		}

		inserter := sess.InsertInto("pg_types")
		for i := range pgTypeTests {
			inserter = inserter.Values(pgTypeTests[i])
		}
		_, err := inserter.Exec()
		assert.NoError(t, err)

		err = sess.Collection("pg_types").Truncate()
		assert.NoError(t, err)

		batch := sess.InsertInto("pg_types").Batch(50)
		go func() {
			defer batch.Done()
			for i := range pgTypeTests {
				batch.Values(pgTypeTests[i])
			}
		}()

		err = batch.Wait()
		assert.NoError(t, err)

		var values []PGType
		err = sess.SelectFrom("pg_types").All(&values)
		assert.NoError(t, err)

		for i := range values {
			expected := pgTypeTests[i]
			expected.ID = values[i].ID
			assert.Equal(t, expected, values[i])
		}
	}
}

func TestOptionTypes(t *testing.T) {
	sess := mustOpen()
	defer sess.Close()

	optionTypes := sess.Collection("option_types")
	err := optionTypes.Truncate()
	assert.NoError(t, err)

	// TODO: lets do some benchmarking on these auto-wrapped option types..

	// TODO: add nullable jsonb field mapped to a []string

	// A struct with wrapped option types defined in the struct tags
	// for postgres string array and jsonb types
	type optionType struct {
		ID       int64                  `db:"id,omitempty"`
		Name     string                 `db:"name"`
		Tags     []string               `db:"tags"`
		Settings map[string]interface{} `db:"settings"`
	}

	// Item 1
	item1 := optionType{
		Name:     "Food",
		Tags:     []string{"toronto", "pizza"},
		Settings: map[string]interface{}{"a": 1, "b": 2},
	}

	id, err := optionTypes.Insert(item1)
	assert.NoError(t, err)

	if pk, ok := id.(int64); !ok || pk == 0 {
		t.Fatalf("Expecting an ID.")
	}

	var item1Chk optionType
	err = optionTypes.Find(db.Cond{"id": id}).One(&item1Chk)
	assert.NoError(t, err)

	assert.Equal(t, float64(1), item1Chk.Settings["a"])
	assert.Equal(t, "toronto", item1Chk.Tags[0])

	// Item 1 B
	item1b := &optionType{
		Name:     "Golang",
		Tags:     []string{"love", "it"},
		Settings: map[string]interface{}{"go": 1, "lang": 2},
	}

	id, err = optionTypes.Insert(item1b)
	assert.NoError(t, err)

	if pk, ok := id.(int64); !ok || pk == 0 {
		t.Fatalf("Expecting an ID.")
	}

	var item1bChk optionType
	err = optionTypes.Find(db.Cond{"id": id}).One(&item1bChk)
	assert.NoError(t, err)

	assert.Equal(t, float64(1), item1bChk.Settings["go"])
	assert.Equal(t, "love", item1bChk.Tags[0])

	// Item 1 C
	item1c := &optionType{
		Name: "Sup", Tags: []string{}, Settings: map[string]interface{}{},
	}

	id, err = optionTypes.Insert(item1c)
	assert.NoError(t, err)

	if pk, ok := id.(int64); !ok || pk == 0 {
		t.Fatalf("Expecting an ID.")
	}

	var item1cChk optionType
	err = optionTypes.Find(db.Cond{"id": id}).One(&item1cChk)
	assert.NoError(t, err)

	assert.Zero(t, len(item1cChk.Tags))
	assert.Zero(t, len(item1cChk.Settings))

	// An option type to pointer jsonb field
	type optionType2 struct {
		ID       int64       `db:"id,omitempty"`
		Name     string      `db:"name"`
		Tags     StringArray `db:"tags"`
		Settings *JSONBMap   `db:"settings"`
	}

	item2 := optionType2{
		Name: "JS", Tags: []string{"hi", "bye"}, Settings: nil,
	}

	id, err = optionTypes.Insert(item2)
	assert.NoError(t, err)

	if pk, ok := id.(int64); !ok || pk == 0 {
		t.Fatalf("Expecting an ID.")
	}

	var item2Chk optionType2
	res := optionTypes.Find(db.Cond{"id": id})
	err = res.One(&item2Chk)
	assert.NoError(t, err)

	assert.Equal(t, id.(int64), item2Chk.ID)

	assert.Equal(t, item2Chk.Name, item2.Name)

	assert.Equal(t, item2Chk.Tags[0], item2.Tags[0])
	assert.Equal(t, len(item2Chk.Tags), len(item2.Tags))

	// Update the value
	m := JSONBMap{}
	m["lang"] = "javascript"
	m["num"] = 31337
	item2.Settings = &m
	err = res.Update(item2)
	assert.NoError(t, err)

	err = res.One(&item2Chk)
	assert.NoError(t, err)

	assert.Equal(t, float64(31337), (*item2Chk.Settings)["num"].(float64))

	assert.Equal(t, "javascript", (*item2Chk.Settings)["lang"])

	// An option type to pointer string array field
	type optionType3 struct {
		ID       int64        `db:"id,omitempty"`
		Name     string       `db:"name"`
		Tags     *StringArray `db:"tags"`
		Settings JSONBMap     `db:"settings"`
	}

	item3 := optionType3{
		Name:     "Julia",
		Tags:     nil,
		Settings: JSONBMap{"girl": true, "lang": true},
	}

	id, err = optionTypes.Insert(item3)
	assert.NoError(t, err)

	if pk, ok := id.(int64); !ok || pk == 0 {
		t.Fatalf("Expecting an ID.")
	}

	var item3Chk optionType2
	err = optionTypes.Find(db.Cond{"id": id}).One(&item3Chk)
	assert.NoError(t, err)
}

type Settings struct {
	Name string `json:"name"`
	Num  int64  `json:"num"`
}

func (s *Settings) Scan(src interface{}) error {
	return ScanJSONB(s, src)
}
func (s Settings) Value() (driver.Value, error) {
	return JSONBValue(s)
}

func TestOptionTypeJsonbStruct(t *testing.T) {
	sess := mustOpen()
	defer sess.Close()

	optionTypes := sess.Collection("option_types")

	err := optionTypes.Truncate()
	assert.NoError(t, err)

	type OptionType struct {
		ID       int64       `db:"id,omitempty"`
		Name     string      `db:"name"`
		Tags     StringArray `db:"tags"`
		Settings Settings    `db:"settings"`
	}

	item1 := &OptionType{
		Name:     "Hi",
		Tags:     []string{"aah", "ok"},
		Settings: Settings{Name: "a", Num: 123},
	}

	id, err := optionTypes.Insert(item1)
	assert.NoError(t, err)

	if pk, ok := id.(int64); !ok || pk == 0 {
		t.Fatalf("Expecting an ID.")
	}

	var item1Chk OptionType
	err = optionTypes.Find(db.Cond{"id": id}).One(&item1Chk)
	assert.NoError(t, err)

	assert.Equal(t, 2, len(item1Chk.Tags))
	assert.Equal(t, "aah", item1Chk.Tags[0])
	assert.Equal(t, "a", item1Chk.Settings.Name)
	assert.Equal(t, int64(123), item1Chk.Settings.Num)
}

func TestSchemaCollection(t *testing.T) {
	sess := mustOpen()
	defer sess.Close()

	col := sess.Collection("test_schema.test")
	_, err := col.Insert(map[string]int{"id": 9})
	assert.Equal(t, nil, err)

	var dump []map[string]int
	err = col.Find().All(&dump)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(dump))
	assert.Equal(t, 9, dump[0]["id"])
}

func TestMaxOpenConns_Issue340(t *testing.T) {
	sess := mustOpen()
	defer sess.Close()

	sess.SetMaxOpenConns(5)

	var wg sync.WaitGroup
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			_, err := sess.Exec(fmt.Sprintf(`SELECT pg_sleep(1.%d)`, i))
			if err != nil {
				t.Fatal(err)
			}
		}(i)
	}

	wg.Wait()

	sess.SetMaxOpenConns(0)
}

func TestUUIDInsert_Issue370(t *testing.T) {
	sess := mustOpen()
	defer sess.Close()

	{
		type itemT struct {
			ID   *uuid.UUID `db:"id"`
			Name string     `db:"name"`
		}

		newUUID := uuid.Must(uuid.NewV4())

		item1 := itemT{
			ID:   &newUUID,
			Name: "Jonny",
		}

		col := sess.Collection("issue_370")
		err := col.Truncate()
		assert.NoError(t, err)

		err = col.InsertReturning(&item1)
		assert.NoError(t, err)

		var item2 itemT
		err = col.Find(item1.ID).One(&item2)
		assert.NoError(t, err)
		assert.Equal(t, item1.Name, item2.Name)

		var item3 itemT
		err = col.Find(db.Cond{"id": item1.ID}).One(&item3)
		assert.NoError(t, err)
		assert.Equal(t, item1.Name, item3.Name)
	}

	{
		type itemT struct {
			ID   uuid.UUID `db:"id"`
			Name string    `db:"name"`
		}

		item1 := itemT{
			ID:   uuid.Must(uuid.NewV4()),
			Name: "Jonny",
		}

		col := sess.Collection("issue_370")
		err := col.Truncate()
		assert.NoError(t, err)

		err = col.InsertReturning(&item1)
		assert.NoError(t, err)

		var item2 itemT
		err = col.Find(item1.ID).One(&item2)
		assert.NoError(t, err)
		assert.Equal(t, item1.Name, item2.Name)

		var item3 itemT
		err = col.Find(db.Cond{"id": item1.ID}).One(&item3)
		assert.NoError(t, err)
		assert.Equal(t, item1.Name, item3.Name)
	}

	{
		type itemT struct {
			ID   Int64Array `db:"id"`
			Name string     `db:"name"`
		}

		item1 := itemT{
			ID:   Int64Array{1, 2, 3},
			Name: "Vojtech",
		}

		col := sess.Collection("issue_370_2")
		err := col.Truncate()
		assert.NoError(t, err)

		err = col.InsertReturning(&item1)
		assert.NoError(t, err)

		var item2 itemT
		err = col.Find(item1.ID).One(&item2)
		assert.NoError(t, err)
		assert.Equal(t, item1.Name, item2.Name)

		var item3 itemT
		err = col.Find(db.Cond{"id": item1.ID}).One(&item3)
		assert.NoError(t, err)
		assert.Equal(t, item1.Name, item3.Name)
	}
}

func TestInsertVarcharPrimaryKey(t *testing.T) {
	sess := mustOpen()
	defer sess.Close()

	{
		type itemT struct {
			Address string `db:"address"`
			Name    string `db:"name"`
		}

		item1 := itemT{
			Address: "1234",
			Name:    "Jonny",
		}

		col := sess.Collection("varchar_primary_key")
		err := col.Truncate()
		assert.NoError(t, err)

		err = col.InsertReturning(&item1)
		assert.NoError(t, err)

		var item2 itemT
		err = col.Find(db.Cond{"address": item1.Address}).One(&item2)
		assert.NoError(t, err)
		assert.Equal(t, item1.Name, item2.Name)

		var item3 itemT
		err = col.Find(db.Cond{"address": item1.Address}).One(&item3)
		assert.NoError(t, err)
		assert.Equal(t, item1.Name, item3.Name)
	}
}

func TestTxOptions_Issue409(t *testing.T) {
	sess := mustOpen()
	defer sess.Close()

	sess.SetTxOptions(sql.TxOptions{
		ReadOnly: true,
	})

	{
		col := sess.Collection("publication")

		row := map[string]interface{}{
			"title":     "foo",
			"author_id": 1,
		}
		err := col.InsertReturning(&row)
		assert.Error(t, err)

		assert.True(t, strings.Contains(err.Error(), "read-only transaction"))
	}
}

func TestEscapeQuestionMark(t *testing.T) {
	sess := mustOpen()
	defer sess.Close()

	var val bool

	{
		res, err := sess.QueryRow(`SELECT '{"mykey":["val1", "val2"]}'::jsonb->'mykey' ?? ?`, "val2")
		assert.NoError(t, err)

		err = res.Scan(&val)
		assert.NoError(t, err)
		assert.Equal(t, true, val)
	}

	{
		res, err := sess.QueryRow(`SELECT ?::jsonb->'mykey' ?? ?`, `{"mykey":["val1", "val2"]}`, `val2`)
		assert.NoError(t, err)

		err = res.Scan(&val)
		assert.NoError(t, err)
		assert.Equal(t, true, val)
	}

	{
		res, err := sess.QueryRow(`SELECT ?::jsonb->? ?? ?`, `{"mykey":["val1", "val2"]}`, `mykey`, `val2`)
		assert.NoError(t, err)

		err = res.Scan(&val)
		assert.NoError(t, err)
		assert.Equal(t, true, val)
	}
}

func TestTextMode_Issue391(t *testing.T) {
	sess := mustOpen()
	defer sess.Close()

	testPostgreSQLTypes(t, sess)
}

func TestBinaryMode_Issue391(t *testing.T) {
	settingsWithBinaryMode := settings
	settingsWithBinaryMode.Options["binary_parameters"] = "yes"

	sess, err := Open(settingsWithBinaryMode)
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close()

	testPostgreSQLTypes(t, sess)
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

	stats, err = getStats(sess)
	if err != nil {
		return err
	}

	if stats["pg_prepared_statements_count"] != 0 {
		return fmt.Errorf(`Expecting "Prepared_stmt_count" to be 0, got %d`, stats["Prepared_stmt_count"])
	}
	return nil
}
