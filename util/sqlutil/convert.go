package sqlutil

import (
	"fmt"
	"reflect"
	"strings"
	"upper.io/db"
	"upper.io/db/util/sqlgen"
)

var (
	sqlPlaceholder     = sqlgen.RawValue(`?`)
	sqlNull            = sqlgen.RawValue(`NULL`)
	sqlDefaultOperator = "="
)

// ToWhereWithArguments converts the given db.Cond parameters into a sqlgen.Where
// value.
func ToWhereWithArguments(term interface{}) (where sqlgen.Where, args []interface{}) {
	args = []interface{}{}

	switch t := term.(type) {
	case []interface{}:
		for i := range t {
			w, v := ToWhereWithArguments(t[i])
			args = append(args, v...)
			where.Conditions = append(where.Conditions, w.Conditions...)
		}
		return
	case db.And:
		var op sqlgen.And
		for i := range t {
			k, v := ToWhereWithArguments(t[i])
			args = append(args, v...)
			op.Conditions = append(op.Conditions, k.Conditions...)
		}
		where.Conditions = append(where.Conditions, &op)
		return
	case db.Or:
		var op sqlgen.Or
		for i := range t {
			w, v := ToWhereWithArguments(t[i])
			args = append(args, v...)
			op.Conditions = append(op.Conditions, w.Conditions...)
		}
		where.Conditions = append(where.Conditions, &op)
		return
	case db.Raw:
		if s, ok := t.Value.(string); ok {
			where.Conditions = append(where.Conditions, sqlgen.RawValue(s))
		}
		return
	case db.Cond:
		cv, v := ToColumnValues(t)
		args = append(args, v...)
		for i := range cv.ColumnValues {
			where.Conditions = append(where.Conditions, cv.ColumnValues[i])
		}
		return
	case db.Constrainer:
		cv, v := ToColumnValues(t.Constraint())
		args = append(args, v...)
		for i := range cv.ColumnValues {
			where.Conditions = append(where.Conditions, cv.ColumnValues[i])
		}
		return
	}

	panic(fmt.Sprintf(db.ErrUnknownConditionType.Error(), term))
}

// ToInterfaceArguments converts the given value into an array of interfaces.
func ToInterfaceArguments(value interface{}) (args []interface{}) {
	if value == nil {
		return nil
	}

	v := reflect.ValueOf(value)

	switch v.Type().Kind() {
	case reflect.Slice:
		var i, total int

		total = v.Len()
		if total > 0 {
			args = make([]interface{}, total)

			for i = 0; i < total; i++ {
				args[i] = v.Index(i).Interface()
			}

			return args
		}
		return nil
	default:
		args = []interface{}{value}
	}

	return args
}

// ToColumnValues converts the given db.Cond into a sqlgen.ColumnValues struct.
func ToColumnValues(cond db.Cond) (ToColumnValues sqlgen.ColumnValues, args []interface{}) {

	args = []interface{}{}

	for column, value := range cond {
		columnValue := sqlgen.ColumnValue{}

		// Guessing operator from input, or using a default one.
		column := strings.TrimSpace(column)
		chunks := strings.SplitN(column, ` `, 2)

		columnValue.Column = sqlgen.ColumnWithName(chunks[0])

		if len(chunks) > 1 {
			columnValue.Operator = chunks[1]
		} else {
			columnValue.Operator = sqlDefaultOperator
		}

		switch value := value.(type) {
		case db.Func:
			v := ToInterfaceArguments(value.Args)
			columnValue.Operator = value.Name

			if v == nil {
				// A function with no arguments.
				columnValue.Value = sqlgen.RawValue(`()`)
			} else {
				// A function with one or more arguments.
				columnValue.Value = sqlgen.RawValue(fmt.Sprintf(`(?%s)`, strings.Repeat(`, ?`, len(v)-1)))
			}

			args = append(args, v...)
		default:
			v := ToInterfaceArguments(value)

			l := len(v)
			if v == nil || l == 0 {
				// Nil value given.
				columnValue.Value = sqlNull
			} else {
				if l > 1 {
					// Array value given.
					columnValue.Value = sqlgen.RawValue(fmt.Sprintf(`(?%s)`, strings.Repeat(`, ?`, len(v)-1)))
				} else {
					// Single value given.
					columnValue.Value = sqlPlaceholder
				}
				args = append(args, v...)
			}
		}

		ToColumnValues.ColumnValues = append(ToColumnValues.ColumnValues, &columnValue)
	}

	return ToColumnValues, args
}

// ToColumnsValuesAndArguments maps the given columnNames and columnValues into
// sqlgen's Columns and Values, it also extracts and returns query arguments.
func ToColumnsValuesAndArguments(columnNames []string, columnValues []interface{}) (*sqlgen.Columns, *sqlgen.Values, []interface{}, error) {
	var arguments []interface{}

	columns := new(sqlgen.Columns)

	columns.Columns = make([]sqlgen.Fragment, 0, len(columnNames))
	for i := range columnNames {
		columns.Columns = append(columns.Columns, sqlgen.ColumnWithName(columnNames[i]))
	}

	values := new(sqlgen.Values)

	arguments = make([]interface{}, 0, len(columnValues))
	values.Values = make([]sqlgen.Fragment, 0, len(columnValues))

	for i := range columnValues {
		switch v := columnValues[i].(type) {
		case *sqlgen.Value:
			// Adding value.
			values.Values = append(values.Values, v)
		case sqlgen.Value:
			// Adding value.
			values.Values = append(values.Values, &v)
		default:
			// Adding both value and placeholder.
			values.Values = append(values.Values, sqlPlaceholder)
			arguments = append(arguments, v)
		}
	}

	return columns, values, arguments, nil
}
