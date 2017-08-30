package sqlbuilder

import (
	"strings"

	"upper.io/db.v3"
)

var comparisonOperators = map[db.ComparisonOperator]string{
	db.ComparisonOperatorEqual:                "=",
	db.ComparisonOperatorNotEqual:             "!=",
	db.ComparisonOperatorGreaterThanOrEqualTo: ">=",
	db.ComparisonOperatorLessThanOrEqualTo:    "<=",
	db.ComparisonOperatorLessThan:             "<",
	db.ComparisonOperatorGreaterThan:          ">",
	db.ComparisonOperatorBetween:              "BETWEEN",
	db.ComparisonOperatorIs:                   "IS",
	db.ComparisonOperatorIsNot:                "IS NOT",
	db.ComparisonOperatorIn:                   "IN",
	db.ComparisonOperatorNotIn:                "NOT IN",
	db.ComparisonOperatorIsDistinctFrom:       "IS DISTINCT FROM",
	db.ComparisonOperatorIsNotDistinctFrom:    "IS NOT DISTINCT FROM",
}

type hasCustomOperator interface {
	CustomOperator() string
}

type operatorWrapper struct {
	tu       *templateWithUtils
	op       db.Comparison
	customOp string
	v        interface{}
}

func (ow *operatorWrapper) cmp() db.Comparison {
	if ow.op != nil {
		return ow.op
	}

	if ow.customOp != "" {
		return db.Op(ow.customOp, ow.v)
	}

	if ow.v == nil {
		return db.Is(nil)
	}

	args, isSlice := toInterfaceArguments(ow.v)
	if isSlice {
		return db.In(args)
	}

	return db.Eq(ow.v)
}

func (ow *operatorWrapper) build() (string, string, []interface{}) {
	cmp := ow.cmp()

	op := ow.tu.comparisonOperatorMapper(cmp.Operator())

	switch cmp.Operator() {
	case db.ComparisonOperatorNone:
		if c, ok := cmp.(hasCustomOperator); ok {
			op = c.CustomOperator()
		} else {
			panic("no operator given")
		}
	case db.ComparisonOperatorIn, db.ComparisonOperatorNotIn:
		values := cmp.Value().([]interface{})
		if len(values) < 1 {
			return op, "(NULL)", nil
		}
		if len(values) > 0 {
			format := "(?" + strings.Repeat(", ?", len(values)-1) + ")"
			return op, format, values
		}
		return op, "(NULL)", nil
	case db.ComparisonOperatorIs, db.ComparisonOperatorIsNot:
		switch cmp.Value() {
		case nil:
			return op, "NULL", nil
		case false:
			return op, "FALSE", nil
		case true:
			return op, "TRUE", nil
		}
	case db.ComparisonOperatorBetween:
		values := cmp.Value()
		return op, "? AND ?", values.([]interface{})
	}

	v := cmp.Value()
	return op, "?", []interface{}{v}
}
