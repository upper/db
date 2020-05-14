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

package db

import (
	"reflect"
	"time"
)

// Comparison defines methods to represent comparison operators.
type Comparison interface {
	Operator() ComparisonOperator

	Value() interface{}
}

// ComparisonOperator is the base type for comparison operators.
type ComparisonOperator uint8

// Comparison operators
const (
	ComparisonOperatorNone ComparisonOperator = iota
	ComparisonOperatorCustom

	ComparisonOperatorEqual
	ComparisonOperatorNotEqual

	ComparisonOperatorLessThan
	ComparisonOperatorGreaterThan

	ComparisonOperatorLessThanOrEqualTo
	ComparisonOperatorGreaterThanOrEqualTo

	ComparisonOperatorBetween
	ComparisonOperatorNotBetween

	ComparisonOperatorIn
	ComparisonOperatorNotIn

	ComparisonOperatorIs
	ComparisonOperatorIsNot

	ComparisonOperatorLike
	ComparisonOperatorNotLike

	ComparisonOperatorRegExp
	ComparisonOperatorNotRegExp

	ComparisonOperatorAfter
	ComparisonOperatorBefore

	ComparisonOperatorOnOrAfter
	ComparisonOperatorOnOrBefore
)

type comparisonOperator struct {
	t  ComparisonOperator
	op string
	v  interface{}
}

func (c *comparisonOperator) CustomOperator() string {
	return c.op
}

func (c *comparisonOperator) Operator() ComparisonOperator {
	return c.t
}

func (c *comparisonOperator) Value() interface{} {
	return c.v
}

// Gte returns a comparison that means: is greater than or equal to value.
func Gte(value interface{}) Comparison {
	return &comparisonOperator{
		t: ComparisonOperatorGreaterThanOrEqualTo,
		v: value,
	}
}

// Lte returns a comparison that means: is less than or equal to value.
func Lte(value interface{}) Comparison {
	return &comparisonOperator{
		t: ComparisonOperatorLessThanOrEqualTo,
		v: value,
	}
}

// Eq returns a comparison that means: is equal to value.
func Eq(value interface{}) Comparison {
	return &comparisonOperator{
		t: ComparisonOperatorEqual,
		v: value,
	}
}

// NotEq returns a comparison that means: is not equal to value.
func NotEq(value interface{}) Comparison {
	return &comparisonOperator{
		t: ComparisonOperatorNotEqual,
		v: value,
	}
}

// Gt returns a comparison that means: is greater than value.
func Gt(value interface{}) Comparison {
	return &comparisonOperator{
		t: ComparisonOperatorGreaterThan,
		v: value,
	}
}

// Lt returns a comparison that means: is less than value.
func Lt(value interface{}) Comparison {
	return &comparisonOperator{
		t: ComparisonOperatorLessThan,
		v: value,
	}
}

// In returns a comparison that means: is any of the values.
func In(value interface{}) Comparison {
	return &comparisonOperator{
		t: ComparisonOperatorIn,
		v: toInterfaceArray(value),
	}
}

// NotIn returns a comparison that means: is none of the values.
func NotIn(value interface{}) Comparison {
	return &comparisonOperator{
		t: ComparisonOperatorNotIn,
		v: toInterfaceArray(value),
	}
}

// After returns a comparison that means: is after the (time.Time) value.
func After(value time.Time) Comparison {
	return &comparisonOperator{
		t: ComparisonOperatorGreaterThan,
		v: value,
	}
}

// Before returns a comparison that means: is before the (time.Time) value.
func Before(t time.Time) Comparison {
	return &comparisonOperator{
		t: ComparisonOperatorLessThan,
		v: t,
	}
}

// OnOrAfter returns a comparison that means: is on or after the (time.Time)
// value.
func OnOrAfter(t time.Time) Comparison {
	return &comparisonOperator{
		t: ComparisonOperatorGreaterThanOrEqualTo,
		v: t,
	}
}

// OnOrBefore returns a comparison that means: is on or before the (time.Time)
// value.
func OnOrBefore(t time.Time) Comparison {
	return &comparisonOperator{
		t: ComparisonOperatorLessThanOrEqualTo,
		v: t,
	}
}

// Between returns a comparison that means: is between valueA and valueB.
func Between(valueA interface{}, valueB interface{}) Comparison {
	return &comparisonOperator{
		t: ComparisonOperatorBetween,
		v: []interface{}{valueA, valueB},
	}
}

// NotBetween returns a comparison that means: is not between valueA and
// valueB.
func NotBetween(valueA interface{}, valueB interface{}) Comparison {
	return &comparisonOperator{
		t: ComparisonOperatorNotBetween,
		v: []interface{}{valueA, valueB},
	}
}

// Is returns a comparison that means: is equivalent to nil, true or false.
func Is(value interface{}) Comparison {
	return &comparisonOperator{
		t: ComparisonOperatorIs,
		v: value,
	}
}

// IsNot returns a comparison that means: is not equivalent to nil, true nor
// false.
func IsNot(value interface{}) Comparison {
	return &comparisonOperator{
		t: ComparisonOperatorIsNot,
		v: value,
	}
}

// IsNull returns a comparison that means: is equivalent to nil.
func IsNull() Comparison {
	return Is(nil)
}

// IsNotNull returns a comparison that means: is not equivalent to nil.
func IsNotNull() Comparison {
	return IsNot(nil)
}

// Like returns a comparison that checks whether the reference matches the
// wildcard value.
func Like(value string) Comparison {
	return &comparisonOperator{
		t: ComparisonOperatorLike,
		v: value,
	}
}

// NotLike returns a comparison that checks whether the reference does not
// match the wildcard value.
func NotLike(value string) Comparison {
	return &comparisonOperator{
		t: ComparisonOperatorNotLike,
		v: value,
	}
}

// RegExp returns a comparison that checks whether the reference matches the
// regular expression.
func RegExp(value string) Comparison {
	return &comparisonOperator{
		t: ComparisonOperatorRegExp,
		v: value,
	}
}

// NotRegExp returns a comparison that checks whether the reference does not
// match the regular expression.
func NotRegExp(value string) Comparison {
	return &comparisonOperator{
		t: ComparisonOperatorNotRegExp,
		v: value,
	}
}

// Op returns a custom comparison operator.
func Op(customOperator string, value interface{}) Comparison {
	return &comparisonOperator{
		op: customOperator,
		t:  ComparisonOperatorCustom,
		v:  value,
	}
}

func toInterfaceArray(value interface{}) []interface{} {
	rv := reflect.ValueOf(value)
	switch rv.Type().Kind() {
	case reflect.Ptr:
		return toInterfaceArray(rv.Elem().Interface())
	case reflect.Slice:
		elems := rv.Len()
		args := make([]interface{}, elems)
		for i := 0; i < elems; i++ {
			args[i] = rv.Index(i).Interface()
		}
		return args
	}
	return []interface{}{value}
}

var _ = Comparison(&comparisonOperator{})
