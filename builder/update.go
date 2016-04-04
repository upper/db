package builder

import (
	"database/sql"

	"upper.io/db.v2/builder/expr"
)

type updater struct {
	*stringer
	builder      *sqlBuilder
	table        string
	columnValues *expr.ColumnValues
	limit        int
	where        *expr.Where
	arguments    []interface{}
}

func (qu *updater) Set(terms ...interface{}) Updater {
	if len(terms) == 1 {
		ff, vv, _ := Map(terms[0])

		cvs := make([]expr.Fragment, len(ff))

		for i := range ff {
			cvs[i] = &expr.ColumnValue{
				Column:   expr.ColumnWithName(ff[i]),
				Operator: qu.builder.t.AssignmentOperator,
				Value:    sqlPlaceholder,
			}
		}
		qu.columnValues.Insert(cvs...)
		qu.arguments = append(qu.arguments, vv...)
	} else if len(terms) > 1 {
		cv, arguments := qu.builder.t.ToColumnValues(terms)
		qu.columnValues.Insert(cv.ColumnValues...)
		qu.arguments = append(qu.arguments, arguments...)
	}

	return qu
}

func (qu *updater) Where(terms ...interface{}) Updater {
	where, arguments := qu.builder.t.ToWhereWithArguments(terms)
	qu.where = &where
	qu.arguments = append(qu.arguments, arguments...)
	return qu
}

func (qu *updater) Exec() (sql.Result, error) {
	return qu.builder.sess.Exec(qu.statement(), qu.arguments...)
}

func (qu *updater) Limit(limit int) Updater {
	qu.limit = limit
	return qu
}

func (qu *updater) statement() *expr.Statement {
	stmt := &expr.Statement{
		Type:         expr.Update,
		Table:        expr.TableWithName(qu.table),
		ColumnValues: qu.columnValues,
	}

	if qu.Where != nil {
		stmt.Where = qu.where
	}

	if qu.limit != 0 {
		stmt.Limit = expr.Limit(qu.limit)
	}

	return stmt
}
