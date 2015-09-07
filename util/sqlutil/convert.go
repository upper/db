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
	sqlIsOperator      = `IS`
	sqlInOperator      = `IN`
	sqlDefaultOperator = `=`
)

type TemplateWithUtils struct {
	*sqlgen.Template
}

func NewTemplateWithUtils(template *sqlgen.Template) *TemplateWithUtils {
	return &TemplateWithUtils{template}
}

// ToWhereWithArguments converts the given db.Cond parameters into a sqlgen.Where
// value.
func (tu *TemplateWithUtils) ToWhereWithArguments(term interface{}) (where sqlgen.Where, args []interface{}) {
	args = []interface{}{}

	switch t := term.(type) {
	case []interface{}:
		if len(t) > 0 {
			if s, ok := t[0].(string); ok {
				if strings.ContainsAny(s, "?") || len(t) == 1 {
					where.Conditions = []sqlgen.Fragment{sqlgen.RawValue(s)}
					args = t[1:]
				} else {
					cond := db.Cond{}

					if len(t) > 2 {
						cond[s] = t[1:]
					} else {
						cond[s] = t[1]
					}

					cv, v := tu.ToColumnValues(cond)

					args = append(args, v...)
					for i := range cv.ColumnValues {
						where.Conditions = append(where.Conditions, cv.ColumnValues[i])
					}
				}
				return
			}
		}
		for i := range t {
			w, v := tu.ToWhereWithArguments(t[i])
			if len(w.Conditions) == 0 {
				continue
			}
			args = append(args, v...)
			where.Conditions = append(where.Conditions, w.Conditions...)
		}
		return
	case db.And:
		var op sqlgen.And
		for i := range t {
			w, v := tu.ToWhereWithArguments(t[i])
			if len(w.Conditions) == 0 {
				continue
			}
			args = append(args, v...)
			op.Conditions = append(op.Conditions, w.Conditions...)
		}
		if len(op.Conditions) > 0 {
			where.Conditions = append(where.Conditions, &op)
		}
		return
	case db.Or:
		var op sqlgen.Or
		for i := range t {
			w, v := tu.ToWhereWithArguments(t[i])
			if len(w.Conditions) == 0 {
				continue
			}
			args = append(args, v...)
			op.Conditions = append(op.Conditions, w.Conditions...)
		}
		if len(op.Conditions) > 0 {
			where.Conditions = append(where.Conditions, &op)
		}
		return
	case db.Raw:
		if s, ok := t.Value.(string); ok {
			where.Conditions = append(where.Conditions, sqlgen.RawValue(s))
		}
		return
	case db.Cond:
		cv, v := tu.ToColumnValues(t)
		args = append(args, v...)
		for i := range cv.ColumnValues {
			where.Conditions = append(where.Conditions, cv.ColumnValues[i])
		}
		return
	case db.Constrainer:
		cv, v := tu.ToColumnValues(t.Constraint())
		args = append(args, v...)
		for i := range cv.ColumnValues {
			where.Conditions = append(where.Conditions, cv.ColumnValues[i])
		}
		return
	}

	panic(fmt.Sprintf(db.ErrUnknownConditionType.Error(), term))
}

// ToInterfaceArguments converts the given value into an array of interfaces.
func (tu *TemplateWithUtils) ToInterfaceArguments(value interface{}) (args []interface{}) {
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
func (tu *TemplateWithUtils) ToColumnValues(term interface{}) (cv sqlgen.ColumnValues, args []interface{}) {
	args = []interface{}{}

	switch t := term.(type) {
	case []interface{}:
		l := len(t)
		for i := 0; i < l; i++ {
			column := t[i].(string)

			if !strings.ContainsAny(column, "=") {
				column = fmt.Sprintf("%s = ?", column)
			}

			chunks := strings.SplitN(column, "=", 2)

			column = chunks[0]
			format := strings.TrimSpace(chunks[1])

			columnValue := sqlgen.ColumnValue{
				Column:   sqlgen.ColumnWithName(column),
				Operator: "=",
				Value:    sqlgen.RawValue(format),
			}

			ps := strings.Count(format, "?")
			if i+ps < l {
				for j := 0; j < ps; j++ {
					args = append(args, t[i+j+1])
				}
				i = i + ps
			} else {
				panic(fmt.Sprintf("Format string %q has more placeholders than given arguments.", format))
			}

			cv.ColumnValues = append(cv.ColumnValues, &columnValue)
		}
		return cv, args
		// Return error.
	case db.Cond:
		for column, value := range t {
			columnValue := sqlgen.ColumnValue{}

			// Guessing operator from input, or using a default one.
			column := strings.TrimSpace(column)
			chunks := strings.SplitN(column, ` `, 2)

			columnValue.Column = sqlgen.ColumnWithName(chunks[0])

			if len(chunks) > 1 {
				columnValue.Operator = chunks[1]
			}

			switch value := value.(type) {
			case db.Func:
				v := tu.ToInterfaceArguments(value.Args)
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
				v := tu.ToInterfaceArguments(value)

				if v == nil {
					// Nil value given.
					columnValue.Value = sqlNull
					if columnValue.Operator == "" {
						columnValue.Operator = sqlIsOperator
					}
				} else {
					if len(v) > 1 {
						// Array value given.
						columnValue.Value = sqlgen.RawValue(fmt.Sprintf(`(?%s)`, strings.Repeat(`, ?`, len(v)-1)))
						if columnValue.Operator == "" {
							columnValue.Operator = sqlInOperator
						}
					} else {
						// Single value given.
						columnValue.Value = sqlPlaceholder
					}
					args = append(args, v...)
				}
			}

			// Using guessed operator if no operator was given.
			if columnValue.Operator == "" {
				if tu.DefaultOperator != "" {
					columnValue.Operator = tu.DefaultOperator
				} else {
					columnValue.Operator = sqlDefaultOperator
				}
			}

			cv.ColumnValues = append(cv.ColumnValues, &columnValue)
		}
		return cv, args
	}

	panic("Unknown map type.")
}

// ToColumnsValuesAndArguments maps the given columnNames and columnValues into
// sqlgen's Columns and Values, it also extracts and returns query arguments.
func (tu *TemplateWithUtils) ToColumnsValuesAndArguments(columnNames []string, columnValues []interface{}) (*sqlgen.Columns, *sqlgen.Values, []interface{}, error) {
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
