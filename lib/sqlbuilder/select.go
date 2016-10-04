package sqlbuilder

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"upper.io/db.v2"
	"upper.io/db.v2/internal/sqladapter/exql"
)

type selectMode uint8

const (
	selectModeAll selectMode = iota
	selectModeDistinct
)

type selector struct {
	*stringer
	mode      selectMode
	builder   *sqlBuilder
	table     *exql.Columns
	as        string
	where     *exql.Where
	groupBy   *exql.GroupBy
	orderBy   exql.OrderBy
	limit     exql.Limit
	offset    exql.Offset
	columns   *exql.Columns
	joins     []*exql.Join
	arguments []interface{}
	err       error
}

func (qs *selector) From(tables ...interface{}) Selector {
	f, args, err := columnFragments(qs.builder.t, tables)
	if err != nil {
		qs.err = err
		return qs
	}
	c := exql.JoinColumns(f...)
	qs.table = c

	qs.arguments = append(qs.arguments, args...)
	return qs
}

func (qs *selector) Columns(columns ...interface{}) Selector {
	f, args, err := columnFragments(qs.builder.t, columns)
	if err != nil {
		qs.err = err
		return qs
	}

	c := exql.JoinColumns(f...)
	if qs.columns != nil {
		qs.columns.Append(c)
	} else {
		qs.columns = c
	}

	qs.arguments = append(qs.arguments, args...)
	return qs
}

func (qs *selector) Distinct() Selector {
	qs.mode = selectModeDistinct
	return qs
}

func (qs *selector) Where(terms ...interface{}) Selector {
	if qs.where != nil {
		return qs.And(terms...)
	}
	where, arguments := qs.builder.t.ToWhereWithArguments(terms)
	qs.where = &where
	qs.arguments = append(qs.arguments, arguments...)
	return qs
}

func (qs *selector) And(terms ...interface{}) Selector {
	where, arguments := qs.builder.t.ToWhereWithArguments(terms)
	if qs.where == nil {
		qs.where = &exql.Where{}
	}
	qs.where.Append(&where)
	qs.arguments = append(qs.arguments, arguments...)
	return qs
}

func (qs *selector) Arguments() []interface{} {
	return qs.arguments
}

func (qs *selector) GroupBy(columns ...interface{}) Selector {
	fragments, args, err := columnFragments(qs.builder.t, columns)
	if err != nil {
		qs.err = err
		return qs
	}
	if fragments != nil {
		qs.groupBy = exql.GroupByColumns(fragments...)
	}
	qs.arguments = append(qs.arguments, args...)
	return qs
}

func (qs *selector) OrderBy(columns ...interface{}) Selector {
	var sortColumns exql.SortColumns

	for i := range columns {
		var sort *exql.SortColumn

		switch value := columns[i].(type) {
		case db.RawValue:
			col, args := expandPlaceholders(value.Raw(), value.Arguments()...)
			sort = &exql.SortColumn{
				Column: exql.RawValue(col),
			}
			qs.arguments = append(qs.arguments, args...)
		case db.Function:
			fnName, fnArgs := value.Name(), value.Arguments()
			if len(fnArgs) == 0 {
				fnName = fnName + "()"
			} else {
				fnName = fnName + "(?" + strings.Repeat("?, ", len(fnArgs)-1) + ")"
			}
			expanded, fnArgs := expandPlaceholders(fnName, fnArgs...)
			sort = &exql.SortColumn{
				Column: exql.RawValue(expanded),
			}
			qs.arguments = append(qs.arguments, fnArgs...)
		case string:
			if strings.HasPrefix(value, "-") {
				sort = &exql.SortColumn{
					Column: exql.ColumnWithName(value[1:]),
					Order:  exql.Descendent,
				}
			} else {
				chunks := strings.SplitN(value, " ", 2)

				order := exql.Ascendent
				if len(chunks) > 1 && strings.ToUpper(chunks[1]) == "DESC" {
					order = exql.Descendent
				}

				sort = &exql.SortColumn{
					Column: exql.ColumnWithName(chunks[0]),
					Order:  order,
				}
			}
		default:
			qs.err = fmt.Errorf("Can't sort by type %T", value)
			return qs
		}
		sortColumns.Columns = append(sortColumns.Columns, sort)
	}

	qs.orderBy.SortColumns = &sortColumns

	return qs
}

func (qs *selector) Using(columns ...interface{}) Selector {
	if len(qs.joins) == 0 {
		qs.err = errors.New(`Cannot use Using() without a preceding Join() expression.`)
		return qs
	}

	lastJoin := qs.joins[len(qs.joins)-1]

	if lastJoin.On != nil {
		qs.err = errors.New(`Cannot use Using() and On() with the same Join() expression.`)
		return qs
	}

	fragments, args, err := columnFragments(qs.builder.t, columns)
	if err != nil {
		qs.err = err
		return qs
	}
	qs.arguments = append(qs.arguments, args...)

	lastJoin.Using = exql.UsingColumns(fragments...)
	return qs
}

func (qs *selector) pushJoin(t string, tables []interface{}) Selector {
	if qs.joins == nil {
		qs.joins = []*exql.Join{}
	}

	tableNames := make([]string, len(tables))
	for i := range tables {
		tableNames[i] = fmt.Sprintf("%s", tables[i])
	}

	qs.joins = append(qs.joins,
		&exql.Join{
			Type:  t,
			Table: exql.TableWithName(strings.Join(tableNames, ", ")),
		},
	)

	return qs
}

func (qs *selector) FullJoin(tables ...interface{}) Selector {
	return qs.pushJoin("FULL", tables)
}

func (qs *selector) CrossJoin(tables ...interface{}) Selector {
	return qs.pushJoin("CROSS", tables)
}

func (qs *selector) RightJoin(tables ...interface{}) Selector {
	return qs.pushJoin("RIGHT", tables)
}

func (qs *selector) LeftJoin(tables ...interface{}) Selector {
	return qs.pushJoin("LEFT", tables)
}

func (qs *selector) Join(tables ...interface{}) Selector {
	return qs.pushJoin("", tables)
}

func (qs *selector) On(terms ...interface{}) Selector {
	if len(qs.joins) == 0 {
		qs.err = errors.New(`Cannot use On() without a preceding Join() expression.`)
		return qs
	}

	lastJoin := qs.joins[len(qs.joins)-1]

	if lastJoin.On != nil {
		qs.err = errors.New(`Cannot use Using() and On() with the same Join() expression.`)
		return qs
	}

	w, a := qs.builder.t.ToWhereWithArguments(terms)
	o := exql.On(w)
	lastJoin.On = &o

	qs.arguments = append(qs.arguments, a...)
	return qs
}

func (qs *selector) Limit(n int) Selector {
	qs.limit = exql.Limit(n)
	return qs
}

func (qs *selector) Offset(n int) Selector {
	qs.offset = exql.Offset(n)
	return qs
}

func (qs *selector) statement() *exql.Statement {
	return &exql.Statement{
		Type:    exql.Select,
		Table:   qs.table,
		Columns: qs.columns,
		Limit:   qs.limit,
		Offset:  qs.offset,
		Joins:   exql.JoinConditions(qs.joins...),
		Where:   qs.where,
		OrderBy: &qs.orderBy,
		GroupBy: qs.groupBy,
	}
}

func (qs *selector) Query() (*sql.Rows, error) {
	return qs.builder.sess.StatementQuery(qs.statement(), qs.arguments...)
}

func (qs *selector) As(alias string) Selector {
	if qs.table == nil {
		qs.err = errors.New("Cannot use As() without a preceding From() expression")
		return qs
	}
	last := len(qs.table.Columns) - 1
	if raw, ok := qs.table.Columns[last].(*exql.Raw); ok {
		qs.table.Columns[last] = exql.RawValue("(" + raw.Value + ") AS " + exql.ColumnWithName(alias).Compile(qs.stringer.t))
	}
	return qs
}

func (qs *selector) QueryRow() (*sql.Row, error) {
	return qs.builder.sess.StatementQueryRow(qs.statement(), qs.arguments...)
}

func (qs *selector) Iterator() Iterator {
	rows, err := qs.builder.sess.StatementQuery(qs.statement(), qs.arguments...)
	return &iterator{rows, err}
}

func (qs *selector) All(destSlice interface{}) error {
	return qs.Iterator().All(destSlice)
}

func (qs *selector) One(dest interface{}) error {
	return qs.Iterator().One(dest)
}
