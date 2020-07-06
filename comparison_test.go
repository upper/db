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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/upper/db/v4/internal/adapter"
)

func TestComparison(t *testing.T) {
	testTimeVal := time.Now()

	testCases := []struct {
		expects *adapter.Comparison
		result  *Comparison
	}{
		{
			adapter.NewComparisonOperator(adapter.ComparisonOperatorGreaterThanOrEqualTo, 1),
			Gte(1),
		},
		{
			adapter.NewComparisonOperator(adapter.ComparisonOperatorLessThanOrEqualTo, 22),
			Lte(22),
		},
		{
			adapter.NewComparisonOperator(adapter.ComparisonOperatorEqual, 6),
			Eq(6),
		},
		{
			adapter.NewComparisonOperator(adapter.ComparisonOperatorNotEqual, 67),
			NotEq(67),
		},
		{
			adapter.NewComparisonOperator(adapter.ComparisonOperatorGreaterThan, 4),
			Gt(4),
		},
		{
			adapter.NewComparisonOperator(adapter.ComparisonOperatorLessThan, 47),
			Lt(47),
		},
		{
			adapter.NewComparisonOperator(adapter.ComparisonOperatorIn, []interface{}{1, 22, 34}),
			In(1, 22, 34),
		},
		{
			adapter.NewComparisonOperator(adapter.ComparisonOperatorGreaterThan, testTimeVal),
			After(testTimeVal),
		},
		{
			adapter.NewComparisonOperator(adapter.ComparisonOperatorLessThan, testTimeVal),
			Before(testTimeVal),
		},
		{
			adapter.NewComparisonOperator(adapter.ComparisonOperatorGreaterThanOrEqualTo, testTimeVal),
			OnOrAfter(testTimeVal),
		},
		{
			adapter.NewComparisonOperator(adapter.ComparisonOperatorLessThanOrEqualTo, testTimeVal),
			OnOrBefore(testTimeVal),
		},
		{
			adapter.NewComparisonOperator(adapter.ComparisonOperatorBetween, []interface{}{11, 35}),
			Between(11, 35),
		},
		{
			adapter.NewComparisonOperator(adapter.ComparisonOperatorNotBetween, []interface{}{11, 35}),
			NotBetween(11, 35),
		},
		{
			adapter.NewComparisonOperator(adapter.ComparisonOperatorIs, 178),
			Is(178),
		},
		{
			adapter.NewComparisonOperator(adapter.ComparisonOperatorIsNot, 32),
			IsNot(32),
		},
		{
			adapter.NewComparisonOperator(adapter.ComparisonOperatorIs, nil),
			IsNull(),
		},
		{
			adapter.NewComparisonOperator(adapter.ComparisonOperatorIsNot, nil),
			IsNotNull(),
		},
		{
			adapter.NewComparisonOperator(adapter.ComparisonOperatorLike, "%a%"),
			Like("%a%"),
		},
		{
			adapter.NewComparisonOperator(adapter.ComparisonOperatorNotLike, "%z%"),
			NotLike("%z%"),
		},
		{
			adapter.NewComparisonOperator(adapter.ComparisonOperatorRegExp, ".*"),
			RegExp(".*"),
		},
		{
			adapter.NewComparisonOperator(adapter.ComparisonOperatorNotRegExp, ".*"),
			NotRegExp(".*"),
		},
		{
			adapter.NewCustomComparisonOperator("~", 56),
			Op("~", 56),
		},
	}

	for i := range testCases {
		assert.Equal(t, testCases[i].expects, testCases[i].result.Comparison)
	}
}
