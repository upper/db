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
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"upper.io/db.v2"
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
		Tags     []string               `db:"tags,stringarray"`
		Settings map[string]interface{} `db:"settings,jsonb"`
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
		ID       int64                   `db:"id,omitempty"`
		Name     string                  `db:"name"`
		Tags     []string                `db:"tags,stringarray"`
		Settings *map[string]interface{} `db:"settings,jsonb"`
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
	m := map[string]interface{}{}
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
		ID       int64                  `db:"id,omitempty"`
		Name     string                 `db:"name"`
		Tags     *[]string              `db:"tags,stringarray"`
		Settings map[string]interface{} `db:"settings,jsonb"`
	}

	item3 := optionType3{
		Name:     "Julia",
		Tags:     nil,
		Settings: map[string]interface{}{"girl": true, "lang": true},
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

func TestOptionTypeJsonbStruct(t *testing.T) {
	sess := mustOpen()
	defer sess.Close()

	optionTypes := sess.Collection("option_types")

	err := optionTypes.Truncate()
	assert.NoError(t, err)

	// A struct with wrapped option types defined in the struct tags
	// for postgres string array and jsonb types
	type Settings struct {
		Name string `json:"name"`
		Num  int64  `json:"num"`
	}

	type OptionType struct {
		ID       int64    `db:"id,omitempty"`
		Name     string   `db:"name"`
		Tags     []string `db:"tags,stringarray"`
		Settings Settings `db:"settings,jsonb"`
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
