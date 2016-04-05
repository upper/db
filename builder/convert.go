package builder

import (
	"fmt"
	"reflect"
	"strings"

	"upper.io/db.v2/builder/exql"
)

var (
	sqlNull            = exql.RawValue(`NULL`)
	sqlIsOperator      = `IS`
	sqlInOperator      = `IN`
	sqlDefaultOperator = `=`
)

type templateWithUtils struct {
	*exql.Template
}

func newTemplateWithUtils(template *exql.Template) *templateWithUtils {
	return &templateWithUtils{template}
}

// ToWhereWithArguments converts the given parameters into a exql.Where
// value.
func (tu *templateWithUtils) ToWhereWithArguments(term interface{}) (where exql.Where, args []interface{}) {
	args = []interface{}{}

	switch t := term.(type) {
	case []interface{}:
		if len(t) > 0 {
			if s, ok := t[0].(string); ok {
				if strings.ContainsAny(s, "?") || len(t) == 1 {
					var j int

					vv := t[1:]

					for i := 0; i < len(s); i++ {
						if s[i] == '?' {
							if len(vv) > j {
								u := tu.ToInterfaceArguments(vv[j])
								args = append(args, u...)
								j = j + 1
								if len(u) > 1 {
									k := "(?" + strings.Repeat(", ?", len(u)-1) + ")"
									s = s[:i] + k + s[i+1:]
									i = i - 1 + len(k)
								}
							}
						}
					}

					where.Conditions = []exql.Fragment{exql.RawValue(s)}
				} else {
					var val interface{}
					key := s

					if len(t) > 2 {
						val = t[1:]
					} else {
						val = t[1]
					}

					cv, v := tu.ToColumnValues(NewConstraint(key, val))

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
	case Constraints:
		for _, c := range t.Constraints() {
			w, v := tu.ToWhereWithArguments(c)
			if len(w.Conditions) == 0 {
				continue
			}
			args = append(args, v...)
			where.Conditions = append(where.Conditions, w.Conditions...)
		}
		return
	case Compound:
		var cond exql.Where

		for _, c := range t.Sentences() {
			w, v := tu.ToWhereWithArguments(c)
			if len(w.Conditions) == 0 {
				continue
			}
			args = append(args, v...)
			cond.Conditions = append(cond.Conditions, w.Conditions...)
		}

		if len(cond.Conditions) > 0 {
			var frag exql.Fragment
			switch t.Operator() {
			case OperatorNone, OperatorAnd:
				q := exql.And(cond)
				frag = &q
			case OperatorOr:
				q := exql.Or(cond)
				frag = &q
			default:
				panic(fmt.Sprintf("Unknown type %T", t))
			}
			where.Conditions = append(where.Conditions, frag)
		}

		return
	case Constraint:
		cv, v := tu.ToColumnValues(t)
		args = append(args, v...)
		where.Conditions = append(where.Conditions, cv.ColumnValues...)
		return where, args
	}

	panic(fmt.Sprintf("Unknown condition type %T", term))
}

// ToInterfaceArguments converts the given value into an array of interfaces.
func (tu *templateWithUtils) ToInterfaceArguments(value interface{}) (args []interface{}) {
	if value == nil {
		return nil
	}

	v := reflect.ValueOf(value)

	switch v.Type().Kind() {
	case reflect.Slice:
		var i, total int

		if v.Type().Elem().Kind() == reflect.Uint8 {
			return []interface{}{string(value.([]byte))}
		}

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

// ToColumnValues converts the given conditions into a exql.ColumnValues struct.
func (tu *templateWithUtils) ToColumnValues(term interface{}) (cv exql.ColumnValues, args []interface{}) {
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

			columnValue := exql.ColumnValue{
				Column:   exql.ColumnWithName(column),
				Operator: "=",
				Value:    exql.RawValue(format),
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
	case Constraint:
		columnValue := exql.ColumnValue{}

		// Guessing operator from input, or using a default one.
		column := strings.TrimSpace(t.Key())
		chunks := strings.SplitN(column, ` `, 2)

		columnValue.Column = exql.ColumnWithName(chunks[0])

		if len(chunks) > 1 {
			columnValue.Operator = chunks[1]
		}

		switch value := t.Value().(type) {
		case Function:
			v := tu.ToInterfaceArguments(value.Arguments())

			if v == nil {
				// A function with no arguments.
				columnValue.Value = exql.RawValue(fmt.Sprintf(`%s()`, value.Name()))
			} else {
				// A function with one or more arguments.
				columnValue.Value = exql.RawValue(fmt.Sprintf(`%s(?%s)`, value.Name(), strings.Repeat(`, ?`, len(v)-1)))
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
				if len(v) > 1 || reflect.TypeOf(value).Kind() == reflect.Slice {
					// Array value given.
					columnValue.Value = exql.RawValue(fmt.Sprintf(`(?%s)`, strings.Repeat(`, ?`, len(v)-1)))
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

		return cv, args
	case Constraints:
		for _, c := range t.Constraints() {
			p, q := tu.ToColumnValues(c)
			cv.ColumnValues = append(cv.ColumnValues, p.ColumnValues...)
			args = append(args, q...)
		}
		return cv, args
	}

	panic(fmt.Sprintf("Unknown term type %T.", term))
}

// ToColumnsValuesAndArguments maps the given columnNames and columnValues into
// expr's Columns and Values, it also extracts and returns query arguments.
func (tu *templateWithUtils) ToColumnsValuesAndArguments(columnNames []string, columnValues []interface{}) (*exql.Columns, *exql.Values, []interface{}, error) {
	var arguments []interface{}

	columns := new(exql.Columns)

	columns.Columns = make([]exql.Fragment, 0, len(columnNames))
	for i := range columnNames {
		columns.Columns = append(columns.Columns, exql.ColumnWithName(columnNames[i]))
	}

	values := new(exql.Values)

	arguments = make([]interface{}, 0, len(columnValues))
	values.Values = make([]exql.Fragment, 0, len(columnValues))

	for i := range columnValues {
		switch v := columnValues[i].(type) {
		case *exql.Value:
			// Adding value.
			values.Values = append(values.Values, v)
		case exql.Value:
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
