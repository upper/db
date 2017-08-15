// Copyright (c) 2012-present The upper.io/db authors. All rights reserved.
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
	"database/sql/driver"
	"encoding/json"
	"errors"
	"reflect"

	"github.com/lib/pq"
)

// Type JSONB represents a PostgreSQL's JSONB value.
type JSONB struct {
	V interface{}
}

// MarshalJSON implements json.Marshaler
func (j JSONB) MarshalJSON() ([]byte, error) {
	return json.Marshal(j.V)
}

// UnmarshalJSON implements json.Unmarshaler
func (j *JSONB) UnmarshalJSON(b []byte) error {
	var v interface{}
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	j.V = v
	return nil
}

// Scan implements the sql.Scanner interface.
func (j *JSONB) Scan(src interface{}) error {
	if src == nil {
		j.V = nil
		return nil
	}

	b, ok := src.([]byte)
	if !ok {
		return errors.New("Scan source was not []bytes")
	}

	if err := json.Unmarshal(b, &j.V); err != nil {
		return err
	}
	return nil
}

// Value implements the driver.Valuer interface.
func (j JSONB) Value() (driver.Value, error) {
	// See https://github.com/lib/pq/issues/528#issuecomment-257197239 on why are
	// we returning string instead of []byte.
	if j.V == nil {
		return nil, nil
	}
	if v, ok := j.V.(json.RawMessage); ok {
		return string(v), nil
	}
	b, err := json.Marshal(j.V)
	if err != nil {
		return nil, err
	}
	return string(b), nil
}

// Type StringArray is an alias for pq.StringArray
type StringArray pq.StringArray

// Value implements the driver.Valuer interface.
func (a StringArray) Value() (driver.Value, error) {
	return pq.StringArray(a).Value()
}

// Scan implements the sql.Scanner interface.
func (a *StringArray) Scan(src interface{}) error {
	s := pq.StringArray(*a)
	if err := s.Scan(src); err != nil {
		return err
	}
	*a = StringArray(s)
	return nil
}

// Type Int64Array is an alias for pq.Int64Array
type Int64Array pq.Int64Array

// Value implements the driver.Valuer interface.
func (i Int64Array) Value() (driver.Value, error) {
	return pq.Int64Array(i).Value()
}

// Scan implements the sql.Scanner interface.
func (i *Int64Array) Scan(src interface{}) error {
	s := pq.Int64Array(*i)
	if err := s.Scan(src); err != nil {
		return err
	}
	*i = Int64Array(s)
	return nil
}

// Type Float64Array is an alias for pq.Float64Array
type Float64Array pq.Float64Array

// Value implements the driver.Valuer interface.
func (f Float64Array) Value() (driver.Value, error) {
	return pq.Float64Array(f).Value()
}

// Scan implements the sql.Scanner interface.
func (f *Float64Array) Scan(src interface{}) error {
	s := pq.Float64Array(*f)
	if err := s.Scan(src); err != nil {
		return err
	}
	*f = Float64Array(s)
	return nil
}

// Type BoolArray is an alias for pq.BoolArray
type BoolArray pq.BoolArray

// Value implements the driver.Valuer interface.
func (b BoolArray) Value() (driver.Value, error) {
	return pq.BoolArray(b).Value()
}

// Scan implements the sql.Scanner interface.
func (b *BoolArray) Scan(src interface{}) error {
	s := pq.BoolArray(*b)
	if err := s.Scan(src); err != nil {
		return err
	}
	*b = BoolArray(s)
	return nil
}

// Type GenericArray is an alias for pq.GenericArray
type GenericArray pq.GenericArray

// Value implements the driver.Valuer interface.
func (g GenericArray) Value() (driver.Value, error) {
	return pq.GenericArray(g).Value()
}

// Scan implements the sql.Scanner interface.
func (g *GenericArray) Scan(src interface{}) error {
	s := pq.GenericArray(*g)
	if err := s.Scan(src); err != nil {
		return err
	}
	*g = GenericArray(s)
	return nil
}

type JSONBMap map[string]interface{}

func (m JSONBMap) Value() (driver.Value, error) {
	return ToJSONBValue(m)
}

func (m *JSONBMap) Scan(src interface{}) error {
	*m = map[string]interface{}(nil)
	return FromJSONBValue(m, src)
}

type JSONBArray []interface{}

func (a JSONBArray) Value() (driver.Value, error) {
	return ToJSONBValue(a)
}

func (a *JSONBArray) Scan(src interface{}) error {
	return FromJSONBValue(a, src)
}

// ToJSONBValue takes an interface and provides a driver.Value that can be
// stored as a JSONB column.
func ToJSONBValue(i interface{}) (driver.Value, error) {
	v := JSONB{i}
	return v.Value()
}

// FromJSONBValue decodes a JSON byte stream into the passed dst value.
func FromJSONBValue(dst interface{}, src interface{}) error {
	v := JSONB{dst}
	return v.Scan(src)
}

type valueWrapper interface {
	WrapValue(interface{}) interface{}
}

type JSONBConverter struct {
}

func (obj *JSONBConverter) WrapValue(src interface{}) interface{} {
	return &JSONB{src}
}

type scannerValuer interface {
	driver.Valuer
	sql.Scanner
}

var (
	driverValuerType = reflect.TypeOf((*driver.Valuer)(nil)).Elem()
	sqlScannerType   = reflect.TypeOf((*sql.Scanner)(nil)).Elem()
)

func autoWrap(elem reflect.Value, v interface{}) interface{} {
	kind := elem.Kind()

	if kind == reflect.Invalid {
		return v
	}

	if elem.Type().Implements(sqlScannerType) {
		return v
	}

	if elem.Type().Implements(driverValuerType) {
		return v
	}

	switch kind {
	case reflect.Ptr:
		return autoWrap(elem.Elem(), v)
	case reflect.Slice:
		return &JSONB{v}
	case reflect.Map:
		if reflect.TypeOf(v).Kind() == reflect.Ptr {
			w := reflect.ValueOf(v)
			z := reflect.New(w.Elem().Type())
			w.Elem().Set(z.Elem())
		}
		return &JSONB{v}
	}

	return v
}

var (
	_ valueWrapper  = &JSONBConverter{}
	_ scannerValuer = &StringArray{}
	_ scannerValuer = &Int64Array{}
	_ scannerValuer = &Float64Array{}
	_ scannerValuer = &BoolArray{}
	_ scannerValuer = &GenericArray{}
	_ scannerValuer = &JSONBMap{}
	_ scannerValuer = &JSONBArray{}
)
