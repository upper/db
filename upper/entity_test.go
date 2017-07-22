package repo

import (
	"fmt"
	"reflect"
	"testing"

	"upper.io/db.v3"
)

type testStruct struct {
	ID          int     `db:"id,omitempty"`
	StringValue string  `db:"string_value"`
	IntValue    int     `db:"int_value"`
	BoolValue   bool    `db:"bool_value"`
	FloatValue  float64 `db:"float_value"`

	PointerToFloatValue  *float64 `db:"ptr_float_value"`
	PointerToStringValue *string  `db:"ptr_string_value"`

	Entity
}

var (
	stringValue = "hello world!"
	floatValue  = 5.555
)

var testCases = []struct {
	in  Mapper
	fn  func(interface{})
	out db.Changeset
}{
	{
		&testStruct{
			ID:          1,
			StringValue: "five",
			BoolValue:   false,
			IntValue:    4,
		},
		func(update interface{}) {
			u := update.(*testStruct)
			u.ID = 2
			u.BoolValue = false
		},
		db.Changeset{
			"id": 2,
		},
	},
	{
		&testStruct{
			ID:          1,
			StringValue: "five",
			BoolValue:   false,
			IntValue:    4,
		},
		func(update interface{}) {
			u := update.(*testStruct)
			u.ID = 2
			u.StringValue = "four"
			u.FloatValue = 0
			u.BoolValue = false
		},
		db.Changeset{
			"id":           2,
			"string_value": "four",
		},
	},
	{
		&testStruct{
			ID:          1,
			StringValue: "five",
			BoolValue:   false,
			IntValue:    4,
		},
		func(update interface{}) {
			u := update.(*testStruct)
			u.ID = 2
			u.StringValue = "four"
			u.FloatValue = 1.23
			u.BoolValue = false
		},
		db.Changeset{
			"id":           2,
			"string_value": "four",
			"float_value":  1.23,
		},
	},
	{
		&testStruct{},
		func(update interface{}) {
			u := update.(*testStruct)
			u.PointerToStringValue = &stringValue
		},
		db.Changeset{
			"ptr_string_value": &stringValue,
		},
	},
	{
		&testStruct{
			PointerToStringValue: &stringValue,
		},
		func(update interface{}) {
			u := update.(*testStruct)
			u.PointerToStringValue = &stringValue
		},
		nil,
	},
	{
		&testStruct{
			FloatValue:          2.123,
			PointerToFloatValue: &floatValue,
		},
		func(update interface{}) {
			u := update.(*testStruct)
			u.ID = 9
		},
		db.Changeset{
			"id": 9,
		},
	},
	{
		&testStruct{},
		nil,
		nil,
	},
}

func TestChangeset(t *testing.T) {
	for i := range testCases {

		s := testCases[i].in
		if err := s.Store(s); err != nil {
			t.Fatal(err)
		}

		if fn := testCases[i].fn; fn != nil {
			fn(s)
		}

		values, err := s.Changeset()
		if err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(testCases[i].out, values) {
			t.Fatal(fmt.Sprintf("test: %v, expected: %#v, got: %#v", i, testCases[i].out, values))
		}
	}
}
