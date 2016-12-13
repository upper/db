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

type selectorQuery struct {
	mode selectMode

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
}

func (sq *selectorQuery) and(terms ...interface{}) error {
	where, whereArgs := toWhereWithArguments(terms)

	if sq.where == nil {
		sq.where, sq.whereArgs = &exql.Where{}, []interface{}{}
	}
	sq.where.Append(&where)
	sq.whereArgs = append(sq.whereArgs, whereArgs...)

	return nil
}

func (sq *selectorQuery) arguments() []interface{} {
	return joinArguments(
		sq.tableArgs,
		sq.columnsArgs,
		sq.joinsArgs,
		sq.whereArgs,
		sq.groupByArgs,
		sq.orderByArgs,
	)
}

func (sq *selectorQuery) statement() *exql.Statement {
	stmt := &exql.Statement{
		Type:    exql.Select,
		Table:   sq.table,
		Columns: sq.columns,
		Limit:   sq.limit,
		Offset:  sq.offset,
		Where:   sq.where,
		OrderBy: sq.orderBy,
		GroupBy: sq.groupBy,
	}

	if len(sq.joins) > 0 {
		stmt.Joins = exql.JoinConditions(sq.joins...)
	}

	return stmt
}

func (sq *selectorQuery) pushJoin(t string, tables []interface{}) error {
	tableNames := make([]string, len(tables))
	for i := range tables {
		tableNames[i] = fmt.Sprintf("%s", tables[i])
	}

	if sq.joins == nil {
		sq.joins = []*exql.Join{}
	}
	sq.joins = append(sq.joins,
		&exql.Join{
			Type:  t,
			Table: exql.TableWithName(strings.Join(tableNames, ", ")),
		},
	)

	return nil
}

type selector struct {
	builder *sqlBuilder
	*stringer

	fn   func(*selectorQuery) error
	prev *selector
}

func (sel *selector) Builder() *sqlBuilder {
	p := &sel
	for {
		if (*p).builder != nil {
			return (*p).builder
		}
		if (*p).prev == nil {
			return nil
		}
		p = &(*p).prev
	}
}

func (sel *selector) Stringer() *stringer {
	p := &sel
	for {
		if (*p).stringer != nil {
			return (*p).stringer
		}
		if (*p).prev == nil {
			return nil
		}
		p = &(*p).prev
	}
}

func (sel *selector) String() string {
	query, err := sel.build()
	if err != nil {
		return ""
	}
	q := sel.Stringer().compileAndReplacePlaceholders(query.statement())
	q = reInvisibleChars.ReplaceAllString(q, ` `)
	return strings.TrimSpace(q)
}

func (sel *selector) frame(fn func(*selectorQuery) error) *selector {
	return &selector{prev: sel, fn: fn}
}

func (sel *selector) From(tables ...interface{}) Selector {
	return sel.frame(
		func(sq *selectorQuery) error {
			f, args, err := columnFragments(tables)
			if err != nil {
				return err
			}
			sq.table = exql.JoinColumns(f...)
			sq.tableArgs = args
			return nil
		},
	)
}

func (sel *selector) Columns(columns ...interface{}) Selector {
	return sel.frame(
		func(sq *selectorQuery) error {
			f, args, err := columnFragments(columns)
			if err != nil {
				return err
			}

			c := exql.JoinColumns(f...)

			if sq.columns != nil {
				sq.columns.Append(c)
			} else {
				sq.columns = c
			}

			sq.columnsArgs = append(sq.columnsArgs, args...)
			return nil
		},
	)
}

func (sel *selector) Distinct() Selector {
	return sel.frame(func(sq *selectorQuery) error {
		sq.mode = selectModeDistinct
		return nil
	})
}

func (sel *selector) Where(terms ...interface{}) Selector {
	return sel.frame(func(sq *selectorQuery) error {
		sq.where, sq.whereArgs = &exql.Where{}, []interface{}{}
		return sq.and(terms...)
	})
}

func (sel *selector) And(terms ...interface{}) Selector {
	return sel.frame(func(sq *selectorQuery) error {
		sq.and(terms...)
		return nil
	})
}

func (sel *selector) Arguments() []interface{} {
	sq, err := sel.build()
	if err != nil {
		return nil
	}
	return sq.arguments()
}

func (sel *selector) GroupBy(columns ...interface{}) Selector {
	return sel.frame(func(sq *selectorQuery) error {
		fragments, args, err := columnFragments(columns)
		if err != nil {
			return err
		}

		if fragments != nil {
			sq.groupBy = exql.GroupByColumns(fragments...)
		}
		sq.groupByArgs = args

		return nil
	})
}

func (sel *selector) OrderBy(columns ...interface{}) Selector {
	return sel.frame(func(sq *selectorQuery) error {
		var sortColumns exql.SortColumns

		for i := range columns {
			var sort *exql.SortColumn

			switch value := columns[i].(type) {
			case db.RawValue:
				col, args := expandPlaceholders(value.Raw(), value.Arguments())
				sort = &exql.SortColumn{
					Column: exql.RawValue(col),
				}
				sq.orderByArgs = append(sq.orderByArgs, args...)
			case db.Function:
				fnName, fnArgs := value.Name(), value.Arguments()
				if len(fnArgs) == 0 {
					fnName = fnName + "()"
				} else {
					fnName = fnName + "(?" + strings.Repeat("?, ", len(fnArgs)-1) + ")"
				}
				expanded, fnArgs := expandPlaceholders(fnName, fnArgs)
				sort = &exql.SortColumn{
					Column: exql.RawValue(expanded),
				}
				sq.orderByArgs = append(sq.orderByArgs, fnArgs...)
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
				return fmt.Errorf("Can't sort by type %T", value)
			}
			sortColumns.Columns = append(sortColumns.Columns, sort)
		}

		sq.orderBy = &exql.OrderBy{
			SortColumns: &sortColumns,
		}
		return nil
	})
}

func (sel *selector) Using(columns ...interface{}) Selector {
	return sel.frame(func(sq *selectorQuery) error {

		joins := len(sq.joins)

		if joins == 0 {
			return errors.New(`Cannot use Using() without a preceding Join() expression.`)
		}

		lastJoin := sq.joins[joins-1]
		if lastJoin.On != nil {
			return errors.New(`Cannot use Using() and On() with the same Join() expression.`)
		}

		fragments, args, err := columnFragments(columns)
		if err != nil {
			return err
		}

		sq.joinsArgs = append(sq.joinsArgs, args...)
		lastJoin.Using = exql.UsingColumns(fragments...)

		return nil
	})
}

func (sel *selector) FullJoin(tables ...interface{}) Selector {
	return sel.frame(func(sq *selectorQuery) error {
		return sq.pushJoin("FULL", tables)
	})
}

func (sel *selector) CrossJoin(tables ...interface{}) Selector {
	return sel.frame(func(sq *selectorQuery) error {
		return sq.pushJoin("CROSS", tables)
	})
}

func (sel *selector) RightJoin(tables ...interface{}) Selector {
	return sel.frame(func(sq *selectorQuery) error {
		return sq.pushJoin("RIGHT", tables)
	})
}

func (sel *selector) LeftJoin(tables ...interface{}) Selector {
	return sel.frame(func(sq *selectorQuery) error {
		return sq.pushJoin("LEFT", tables)
	})
}

func (sel *selector) Join(tables ...interface{}) Selector {
	return sel.frame(func(sq *selectorQuery) error {
		return sq.pushJoin("", tables)
	})
}

func (sel *selector) On(terms ...interface{}) Selector {
	return sel.frame(func(sq *selectorQuery) error {
		joins := len(sq.joins)

		if joins == 0 {
			return errors.New(`Cannot use On() without a preceding Join() expression.`)
		}

		lastJoin := sq.joins[joins-1]
		if lastJoin.On != nil {
			return errors.New(`Cannot use Using() and On() with the same Join() expression.`)
		}

		w, a := toWhereWithArguments(terms)
		o := exql.On(w)

		lastJoin.On = &o

		sq.joinsArgs = append(sq.joinsArgs, a...)

		return nil
	})
}

func (sel *selector) Limit(n int) Selector {
	return sel.frame(func(sq *selectorQuery) error {
		sq.limit = exql.Limit(n)
		return nil
	})
}

func (sel *selector) Offset(n int) Selector {
	return sel.frame(func(sq *selectorQuery) error {
		sq.offset = exql.Offset(n)
		return nil
	})
}

func (sel *selector) As(alias string) Selector {
	return sel.frame(func(sq *selectorQuery) error {
		if sq.table == nil {
			return errors.New("Cannot use As() without a preceding From() expression")
		}
		last := len(sq.table.Columns) - 1
		if raw, ok := sq.table.Columns[last].(*exql.Raw); ok {
			sq.table.Columns[last] = exql.RawValue("(" + raw.Value + ") AS " + exql.ColumnWithName(alias).Compile(sel.Stringer().t))
		}
		return nil
	})
}

func (sel *selector) statement() *exql.Statement {
	sq, _ := sel.build()
	return sq.statement()
}

func (sel *selector) QueryRow() (*sql.Row, error) {
	sq, err := sel.build()
	if err != nil {
		return nil, err
	}

	return sel.Builder().sess.StatementQueryRow(sq.statement(), sq.arguments()...)
}

func (sel *selector) Query() (*sql.Rows, error) {
	sq, err := sel.build()
	if err != nil {
		return nil, err
	}
	return sel.Builder().sess.StatementQuery(sq.statement(), sq.arguments()...)
}

func (sel *selector) Iterator() Iterator {
	sq, err := sel.build()
	if err != nil {
		return &iterator{nil, err}
	}

	rows, err := sel.Builder().sess.StatementQuery(sq.statement(), sq.arguments()...)
	return &iterator{rows, err}
}

func (sel *selector) All(destSlice interface{}) error {
	return sel.Iterator().All(destSlice)
}

func (sel *selector) One(dest interface{}) error {
	return sel.Iterator().One(dest)
}

func (sel *selector) build() (*selectorQuery, error) {
	sq, err := selectorFastForward(&selectorQuery{}, sel)
	if err != nil {
		return nil, err
	}
	return sq, nil
}

func (sel *selector) Compile() string {
	return sel.statement().Compile(sel.Stringer().t)
}

func selectorFastForward(in *selectorQuery, curr *selector) (*selectorQuery, error) {
	if curr == nil || curr.fn == nil {
		return in, nil
	}
	in, err := selectorFastForward(in, curr.prev)
	if err != nil {
		return nil, err
	}
	err = curr.fn(in)
	return in, err
}
