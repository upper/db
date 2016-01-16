package sqlbuilder

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"upper.io/db.v2/builder"
	"upper.io/db.v2/builder/sqlgen"
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
	table     string
	where     *sqlgen.Where
	groupBy   *sqlgen.GroupBy
	orderBy   sqlgen.OrderBy
	limit     sqlgen.Limit
	offset    sqlgen.Offset
	columns   *sqlgen.Columns
	joins     []*sqlgen.Join
	arguments []interface{}
	err       error
}

func (qs *selector) From(tables ...string) builder.Selector {
	qs.table = strings.Join(tables, ",")
	return qs
}

func (qs *selector) Columns(columns ...interface{}) builder.Selector {
	f, err := columnFragments(qs.builder.t, columns)
	if err != nil {
		qs.err = err
		return qs
	}
	qs.columns = sqlgen.JoinColumns(f...)
	return qs
}

func (qs *selector) Distinct() builder.Selector {
	qs.mode = selectModeDistinct
	return qs
}

func (qs *selector) Where(terms ...interface{}) builder.Selector {
	where, arguments := qs.builder.t.ToWhereWithArguments(terms)
	qs.where = &where
	qs.arguments = append(qs.arguments, arguments...)
	return qs
}

func (qs *selector) GroupBy(columns ...interface{}) builder.Selector {
	var fragments []sqlgen.Fragment
	fragments, qs.err = columnFragments(qs.builder.t, columns)
	if fragments != nil {
		qs.groupBy = sqlgen.GroupByColumns(fragments...)
	}
	return qs
}

func (qs *selector) OrderBy(columns ...interface{}) builder.Selector {
	var sortColumns sqlgen.SortColumns

	for i := range columns {
		var sort *sqlgen.SortColumn

		switch value := columns[i].(type) {
		case builder.RawValue:
			sort = &sqlgen.SortColumn{
				Column: sqlgen.RawValue(value.String()),
			}
		case string:
			if strings.HasPrefix(value, "-") {
				sort = &sqlgen.SortColumn{
					Column: sqlgen.ColumnWithName(value[1:]),
					Order:  sqlgen.Descendent,
				}
			} else {
				chunks := strings.SplitN(value, " ", 2)

				order := sqlgen.Ascendent
				if len(chunks) > 1 && strings.ToUpper(chunks[1]) == "DESC" {
					order = sqlgen.Descendent
				}

				sort = &sqlgen.SortColumn{
					Column: sqlgen.ColumnWithName(chunks[0]),
					Order:  order,
				}
			}
		}
		sortColumns.Columns = append(sortColumns.Columns, sort)
	}

	qs.orderBy.SortColumns = &sortColumns

	return qs
}

func (qs *selector) Using(columns ...interface{}) builder.Selector {
	if len(qs.joins) == 0 {
		qs.err = errors.New(`Cannot use Using() without a preceding Join() expression.`)
		return qs
	}

	lastJoin := qs.joins[len(qs.joins)-1]

	if lastJoin.On != nil {
		qs.err = errors.New(`Cannot use Using() and On() with the same Join() expression.`)
		return qs
	}

	fragments, err := columnFragments(qs.builder.t, columns)
	if err != nil {
		qs.err = err
		return qs
	}

	lastJoin.Using = sqlgen.UsingColumns(fragments...)
	return qs
}

func (qs *selector) pushJoin(t string, tables []interface{}) builder.Selector {
	if qs.joins == nil {
		qs.joins = []*sqlgen.Join{}
	}

	tableNames := make([]string, len(tables))
	for i := range tables {
		tableNames[i] = fmt.Sprintf("%s", tables[i])
	}

	qs.joins = append(qs.joins,
		&sqlgen.Join{
			Type:  t,
			Table: sqlgen.TableWithName(strings.Join(tableNames, ", ")),
		},
	)

	return qs
}

func (qs *selector) FullJoin(tables ...interface{}) builder.Selector {
	return qs.pushJoin("FULL", tables)
}

func (qs *selector) CrossJoin(tables ...interface{}) builder.Selector {
	return qs.pushJoin("CROSS", tables)
}

func (qs *selector) RightJoin(tables ...interface{}) builder.Selector {
	return qs.pushJoin("RIGHT", tables)
}

func (qs *selector) LeftJoin(tables ...interface{}) builder.Selector {
	return qs.pushJoin("LEFT", tables)
}

func (qs *selector) Join(tables ...interface{}) builder.Selector {
	return qs.pushJoin("", tables)
}

func (qs *selector) On(terms ...interface{}) builder.Selector {
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
	o := sqlgen.On(w)
	lastJoin.On = &o

	qs.arguments = append(qs.arguments, a...)
	return qs
}

func (qs *selector) Limit(n int) builder.Selector {
	qs.limit = sqlgen.Limit(n)
	return qs
}

func (qs *selector) Offset(n int) builder.Selector {
	qs.offset = sqlgen.Offset(n)
	return qs
}

func (qs *selector) statement() *sqlgen.Statement {
	return &sqlgen.Statement{
		Type:    sqlgen.Select,
		Table:   sqlgen.TableWithName(qs.table),
		Columns: qs.columns,
		Limit:   qs.limit,
		Offset:  qs.offset,
		Joins:   sqlgen.JoinConditions(qs.joins...),
		Where:   qs.where,
		OrderBy: &qs.orderBy,
		GroupBy: qs.groupBy,
	}
}

func (qs *selector) Query() (*sql.Rows, error) {
	return qs.builder.sess.Query(qs.statement(), qs.arguments...)
}

func (qs *selector) QueryRow() (*sql.Row, error) {
	return qs.builder.sess.QueryRow(qs.statement(), qs.arguments...)
}

func (qs *selector) Iterator() builder.Iterator {
	rows, err := qs.builder.sess.Query(qs.statement(), qs.arguments...)
	return &iterator{rows, err}
}
