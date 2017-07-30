package sqlbuilder

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"upper.io/db.v3"
)

func TestMapper(t *testing.T) {

	type exhibitA struct {
		ID          int     `db:"id,omitempty"`
		StringValue string  `db:"string_value"`
		IntValue    int     `db:"int_value"`
		BoolValue   bool    `db:"bool_value"`
		FloatValue  float64 `db:"float_value"`

		PointerToFloatValue  *float64 `db:"ptr_float_value"`
		PointerToStringValue *string  `db:"ptr_string_value"`
	}

	type exhibitB struct {
		ID          int     `db:"id,omitempty"`
		StringValue string  `db:"string_value"`
		IntValue    int     `db:"int_value"`
		BoolValue   bool    `db:"bool_value"`
		FloatValue  float64 `db:"float_value"`

		Entity
	}

	testCases := []struct {
		in interface{}

		prepareFn func(interface{}) error

		outFields []string
		outValues []interface{}
		outErr    error
	}{
		{
			in: &exhibitA{ID: 5},

			outFields: []string{
				"bool_value",
				"float_value",
				"id",
				"int_value",
				"ptr_float_value",
				"ptr_string_value",
				"string_value",
			},
			outValues: []interface{}{
				false,
				float64(0),
				5,
				0,
				nil,
				nil,
				"",
			},
		},
		{
			in: exhibitA{
				BoolValue:   true,
				FloatValue:  1.2,
				StringValue: "hurray",
			},

			outFields: []string{
				"bool_value",
				"float_value",
				"int_value",
				"ptr_float_value",
				"ptr_string_value",
				"string_value",
			},
			outValues: []interface{}{
				true,
				float64(1.2),
				0,
				nil,
				nil,
				"hurray",
			},
		},
		{
			in: map[string]string{"foo": "bar"},

			outFields: []string{"foo"},
			outValues: []interface{}{"bar"},
		},
		{
			in: &map[string]string{"foo": "bar"},

			outFields: []string{"foo"},
			outValues: []interface{}{"bar"},
		},
		{
			in: nil,

			outFields: []string(nil),
			outValues: []interface{}(nil),
		},
		{
			in: db.Changeset{"foo": "bar"},

			outFields: []string{"foo"},
			outValues: []interface{}{"bar"},
		},
		{
			in: &db.Changeset{"foo": "bar"},

			prepareFn: func(in interface{}) error {
				changeset := in.(*db.Changeset)
				(*changeset)["foo"] = "baz"
				return nil
			},

			outFields: []string{"foo"},
			outValues: []interface{}{"baz"},
		},
		{
			in: exhibitB{
				StringValue: "Hello",
			},

			outFields: []string{
				"bool_value",
				"float_value",
				"int_value",
				"string_value",
			},
			outValues: []interface{}{
				false,
				float64(0),
				0,
				"Hello",
			},
		},
		{
			in: &exhibitB{
				StringValue: "Hello",
			},

			prepareFn: func(in interface{}) error {
				data := in.(*exhibitB)
				if err := data.Store(data); err != nil {
					return err
				}
				data.BoolValue = true
				data.StringValue = "World"
				return nil
			},

			outFields: []string{
				"bool_value",
				"string_value",
			},
			outValues: []interface{}{
				true,
				"World",
			},
		},
		{
			in: &exhibitB{
				StringValue: "Hello",
			},

			prepareFn: func(in interface{}) error {
				data := in.(*exhibitB)
				if err := data.Store(data); err != nil {
					return err
				}
				data.BoolValue = true
				data.StringValue = "World"
				if err := data.Store(data); err != nil {
					return err
				}
				return nil
			},

			outFields: []string{},
			outValues: []interface{}{},
		},
	}

	for _, test := range testCases {
		if fn := test.prepareFn; fn != nil {
			err := fn(test.in)
			assert.NoError(t, err)
		}

		fields, values, err := Map(test.in, nil)

		assert.Equal(t, test.outFields, fields)
		assert.Equal(t, test.outValues, values)

		if test.outErr != nil {
			assert.Error(t, test.outErr, err)
		} else {
			assert.NoError(t, err)
		}
	}
}
