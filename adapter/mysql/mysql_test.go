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

package mysql

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"github.com/upper/db/v4"
	"github.com/upper/db/v4/internal/testsuite"
)

type int64Compat int64

type uintCompat uint

type stringCompat string

type uintCompatArray []uintCompat

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

type AdapterTests struct {
	testsuite.Suite
}

func (s *AdapterTests) SetupSuite() {
	s.Helper = &Helper{}
}

func (s *AdapterTests) TestInsertReturningCompositeKey_Issue383() {
	sess := s.Session()

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
	s.NoError(err)

	s.NotZero(a.ID)
	s.NotZero(a.Date)
	s.Equal("admin", a.Accounts)
	s.Equal("E10ADC3949BA59ABBE56E057F20F883E", a.LoginPassWord)

	b := Admin{
		Accounts:      "admin2",
		LoginPassWord: "E10ADC3949BA59ABBE56E057F20F883E",
		Date:          dateNow,
	}

	err = adminCollection.InsertReturning(&b)
	s.NoError(err)

	s.NotZero(b.ID)
	s.NotZero(b.Date)
	s.Equal("admin2", b.Accounts)
	s.Equal("E10ADC3949BA59ABBE56E057F20F883E", a.LoginPassWord)
}

func (s *AdapterTests) TestIssue469_BadConnection() {
	var err error
	sess := s.Session()

	// Ask the MySQL server to disconnect sessions that remain inactive for more
	// than 1 second.
	_, err = sess.SQL().Exec(`SET SESSION wait_timeout=1`)
	s.NoError(err)

	// Remain inactive for 2 seconds.
	time.Sleep(time.Second * 2)

	// A query should start a new connection, even if the server disconnected us.
	_, err = sess.Collection("artist").Find().Count()
	s.NoError(err)

	// This is a new session, ask the MySQL server to disconnect sessions that
	// remain inactive for more than 1 second.
	_, err = sess.SQL().Exec(`SET SESSION wait_timeout=1`)
	s.NoError(err)

	// Remain inactive for 2 seconds.
	time.Sleep(time.Second * 2)

	// At this point the server should have disconnected us. Let's try to create
	// a transaction anyway.
	err = sess.Tx(func(sess db.Session) error {
		var err error

		_, err = sess.Collection("artist").Find().Count()
		if err != nil {
			return err
		}
		return nil
	})
	s.NoError(err)

	// This is a new session, ask the MySQL server to disconnect sessions that
	// remain inactive for more than 1 second.
	_, err = sess.SQL().Exec(`SET SESSION wait_timeout=1`)
	s.NoError(err)

	err = sess.Tx(func(sess db.Session) error {
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

	s.Error(err, "Expecting an error (can't recover from this)")
}

func (s *AdapterTests) TestMySQLTypes() {
	sess := s.Session()

	type MyType struct {
		ID int64 `db:"id,omitempty"`

		JSONMap JSONMap `db:"json_map"`

		JSONObject JSONMap   `db:"json_object"`
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
		MyType{},
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
			result, err := sess.Collection("my_types").Insert(myTypeTests[i])
			s.NoError(err)

			var actual MyType
			err = sess.Collection("my_types").Find(result).One(&actual)
			s.NoError(err)

			expected := myTypeTests[i]
			expected.ID = result.ID().(int64)
			s.Equal(expected, actual)
		}

		for i := range myTypeTests {
			res, err := sess.SQL().InsertInto("my_types").Values(myTypeTests[i]).Exec()
			s.NoError(err)

			id, err := res.LastInsertId()
			s.NoError(err)
			s.NotEqual(0, id)

			var actual MyType
			err = sess.Collection("my_types").Find(id).One(&actual)
			s.NoError(err)

			expected := myTypeTests[i]
			expected.ID = id

			s.Equal(expected, actual)

			var actual2 MyType
			err = sess.SQL().SelectFrom("my_types").Where("id = ?", id).One(&actual2)
			s.NoError(err)
			s.Equal(expected, actual2)
		}

		inserter := sess.SQL().InsertInto("my_types")
		for i := range myTypeTests {
			inserter = inserter.Values(myTypeTests[i])
		}
		_, err := inserter.Exec()
		s.NoError(err)

		err = sess.Collection("my_types").Truncate()
		s.NoError(err)

		batch := sess.SQL().InsertInto("my_types").Batch(50)
		go func() {
			defer batch.Done()
			for i := range myTypeTests {
				batch.Values(myTypeTests[i])
			}
		}()

		err = batch.Wait()
		s.NoError(err)

		var values []MyType
		err = sess.SQL().SelectFrom("my_types").All(&values)
		s.NoError(err)

		for i := range values {
			expected := myTypeTests[i]
			expected.ID = values[i].ID
			s.Equal(expected, values[i])
		}
	}
}

func TestAdapter(t *testing.T) {
	suite.Run(t, &AdapterTests{})
}
