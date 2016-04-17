package builder

import (
	"database/sql"

	"upper.io/db.v2/builder/exql"
)

type updater struct {
	*stringer
	builder      *sqlBuilder
	table        string
	columnValues *exql.ColumnValues
	limit        int
	where        *exql.Where
	arguments    []interface{}
}

func (qu *updater) Set(terms ...interface{}) Updater {
	if len(terms) == 1 {
		ff, vv, _ := Map(terms[0])

		cvs := make([]exql.Fragment, len(ff))

		for i := range ff {
			cvs[i] = &exql.ColumnValue{
				Column:   exql.ColumnWithName(ff[i]),
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
	return qu.builder.sess.ExecStatement(qu.statement(), qu.arguments...)
}

func (qu *updater) Limit(limit int) Updater {
	qu.limit = limit
	return qu
}

func (qu *updater) statement() *exql.Statement {
	stmt := &exql.Statement{
		Type:         exql.Update,
		Table:        exql.TableWithName(qu.table),
		ColumnValues: qu.columnValues,
	}

	if qu.Where != nil {
		stmt.Where = qu.where
	}

	if qu.limit != 0 {
		stmt.Limit = exql.Limit(qu.limit)
	}

	return stmt
}
