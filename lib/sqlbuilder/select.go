package sqlbuilder

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"

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

	mode    selectMode
	builder *sqlBuilder

	table     *exql.Columns
	tableArgs []interface{}

	as string

	where     *exql.Where
	whereArgs []interface{}

	groupBy     *exql.GroupBy
	groupByArgs []interface{}

	orderBy     *exql.OrderBy
	orderByArgs []interface{}

	limit  exql.Limit
	offset exql.Offset

	columns     *exql.Columns
	columnsArgs []interface{}

	joins     []*exql.Join
	joinsArgs []interface{}

	mu sync.Mutex

	err error
}

func (qs *selector) From(tables ...interface{}) Selector {
	f, args, err := columnFragments(qs.builder.t, tables)
	if err != nil {
		qs.setErr(err)
		return qs
	}
	c := exql.JoinColumns(f...)

	qs.mu.Lock()
	qs.table = c
	qs.tableArgs = args
	qs.mu.Unlock()

	return qs
}

func (qs *selector) Columns(columns ...interface{}) Selector {
	f, args, err := columnFragments(qs.builder.t, columns)
	if err != nil {
		qs.setErr(err)
		return qs
	}

	c := exql.JoinColumns(f...)

	qs.mu.Lock()
	if qs.columns != nil {
		qs.columns.Append(c)
	} else {
		qs.columns = c
	}
	qs.columnsArgs = append(qs.columnsArgs, args...)
	qs.mu.Unlock()

	return qs
}

func (qs *selector) Distinct() Selector {
	qs.mu.Lock()
	qs.mode = selectModeDistinct
	qs.mu.Unlock()
	return qs
}

func (qs *selector) Where(terms ...interface{}) Selector {
	qs.mu.Lock()
	qs.where, qs.whereArgs = &exql.Where{}, []interface{}{}
	qs.mu.Unlock()
	return qs.And(terms...)
}

func (qs *selector) And(terms ...interface{}) Selector {
	where, whereArgs := qs.builder.t.ToWhereWithArguments(terms)

	qs.mu.Lock()
	if qs.where == nil {
		qs.where, qs.whereArgs = &exql.Where{}, []interface{}{}
	}
	qs.where.Append(&where)
	qs.whereArgs = append(qs.whereArgs, whereArgs...)
	qs.mu.Unlock()

	return qs
}

func (qs *selector) Arguments() []interface{} {
	qs.mu.Lock()
	defer qs.mu.Unlock()

	return joinArguments(
		qs.tableArgs,
		qs.columnsArgs,
		qs.joinsArgs,
		qs.whereArgs,
		qs.groupByArgs,
		qs.orderByArgs,
	)
}

func (qs *selector) GroupBy(columns ...interface{}) Selector {
	fragments, args, err := columnFragments(qs.builder.t, columns)
	if err != nil {
		qs.setErr(err)
		return qs
	}

	qs.mu.Lock()
	if fragments != nil {
		qs.groupBy = exql.GroupByColumns(fragments...)
	}
	qs.groupByArgs = args
	qs.mu.Unlock()

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
			qs.mu.Lock()
			qs.orderByArgs = append(qs.orderByArgs, args...)
			qs.mu.Unlock()
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
			qs.mu.Lock()
			qs.orderByArgs = append(qs.orderByArgs, fnArgs...)
			qs.mu.Unlock()
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
			qs.setErr(fmt.Errorf("Can't sort by type %T", value))
			return qs
		}
		sortColumns.Columns = append(sortColumns.Columns, sort)
	}

	qs.mu.Lock()
	qs.orderBy = &exql.OrderBy{
		SortColumns: &sortColumns,
	}
	qs.mu.Unlock()

	return qs
}

func (qs *selector) Using(columns ...interface{}) Selector {
	qs.mu.Lock()
	joins := len(qs.joins)
	qs.mu.Unlock()

	if joins == 0 {
		qs.setErr(errors.New(`Cannot use Using() without a preceding Join() expression.`))
		return qs
	}

	lastJoin := qs.joins[joins-1]
	if lastJoin.On != nil {
		qs.setErr(errors.New(`Cannot use Using() and On() with the same Join() expression.`))
		return qs
	}

	fragments, args, err := columnFragments(qs.builder.t, columns)
	if err != nil {
		qs.setErr(err)
		return qs
	}

	qs.mu.Lock()
	qs.joinsArgs = append(qs.joinsArgs, args...)
	lastJoin.Using = exql.UsingColumns(fragments...)
	qs.mu.Unlock()

	return qs
}

func (qs *selector) pushJoin(t string, tables []interface{}) Selector {
	tableNames := make([]string, len(tables))
	for i := range tables {
		tableNames[i] = fmt.Sprintf("%s", tables[i])
	}

	qs.mu.Lock()
	if qs.joins == nil {
		qs.joins = []*exql.Join{}
	}
	qs.joins = append(qs.joins,
		&exql.Join{
			Type:  t,
			Table: exql.TableWithName(strings.Join(tableNames, ", ")),
		},
	)
	qs.mu.Unlock()

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
	qs.mu.Lock()
	joins := len(qs.joins)
	qs.mu.Unlock()

	if joins == 0 {
		qs.setErr(errors.New(`Cannot use On() without a preceding Join() expression.`))
		return qs
	}

	lastJoin := qs.joins[joins-1]
	if lastJoin.On != nil {
		qs.setErr(errors.New(`Cannot use Using() and On() with the same Join() expression.`))
		return qs
	}

	w, a := qs.builder.t.ToWhereWithArguments(terms)
	o := exql.On(w)

	lastJoin.On = &o

	qs.mu.Lock()
	qs.joinsArgs = append(qs.joinsArgs, a...)
	qs.mu.Unlock()

	return qs
}

func (qs *selector) Limit(n int) Selector {
	qs.mu.Lock()
	qs.limit = exql.Limit(n)
	qs.mu.Unlock()
	return qs
}

func (qs *selector) Offset(n int) Selector {
	qs.mu.Lock()
	qs.offset = exql.Offset(n)
	qs.mu.Unlock()
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
		OrderBy: qs.orderBy,
		GroupBy: qs.groupBy,
	}
}

func (qs *selector) Query() (*sql.Rows, error) {
	return qs.builder.sess.StatementQuery(qs.statement(), qs.Arguments()...)
}

func (qs *selector) As(alias string) Selector {
	if qs.table == nil {
		qs.setErr(errors.New("Cannot use As() without a preceding From() expression"))
		return qs
	}
	last := len(qs.table.Columns) - 1
	if raw, ok := qs.table.Columns[last].(*exql.Raw); ok {
		qs.table.Columns[last] = exql.RawValue("(" + raw.Value + ") AS " + exql.ColumnWithName(alias).Compile(qs.stringer.t))
	}
	return qs
}

func (qs *selector) QueryRow() (*sql.Row, error) {
	return qs.builder.sess.StatementQueryRow(qs.statement(), qs.Arguments()...)
}

func (qs *selector) Iterator() Iterator {
	rows, err := qs.builder.sess.StatementQuery(qs.statement(), qs.Arguments()...)
	return &iterator{rows, err}
}

func (qs *selector) All(destSlice interface{}) error {
	return qs.Iterator().All(destSlice)
}

func (qs *selector) One(dest interface{}) error {
	return qs.Iterator().One(dest)
}

func (qs *selector) setErr(err error) {
	qs.mu.Lock()
	qs.err = err
	qs.mu.Unlock()
}
