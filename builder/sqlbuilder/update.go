package sqlbuilder

import (
	"database/sql"

	"upper.io/db.v2/builder"
	"upper.io/db.v2/builder/sqlgen"
)

type updater struct {
	*stringer
	builder      *sqlBuilder
	table        string
	columnValues *sqlgen.ColumnValues
	limit        int
	where        *sqlgen.Where
	arguments    []interface{}
}

func (qu *updater) Set(terms ...interface{}) builder.Updater {
	if len(terms) == 1 {
		ff, vv, _ := Map(terms[0])

		cvs := make([]sqlgen.Fragment, len(ff))

		for i := range ff {
			cvs[i] = &sqlgen.ColumnValue{
				Column:   sqlgen.ColumnWithName(ff[i]),
				Operator: qu.builder.t.AssignmentOperator,
				Value:    sqlPlaceholder,
			}
		}
		qu.columnValues.Append(cvs...)
		qu.arguments = append(qu.arguments, vv...)
	} else if len(terms) > 1 {
		cv, arguments := qu.builder.t.ToColumnValues(terms)
		qu.columnValues.Append(cv.ColumnValues...)
		qu.arguments = append(qu.arguments, arguments...)
	}

	return qu
}

func (qu *updater) Where(terms ...interface{}) builder.Updater {
	where, arguments := qu.builder.t.ToWhereWithArguments(terms)
	qu.where = &where
	qu.arguments = append(qu.arguments, arguments...)
	return qu
}

func (qu *updater) Exec() (sql.Result, error) {
	return qu.builder.sess.Exec(qu.statement(), qu.arguments...)
}

func (qu *updater) Limit(limit int) builder.Updater {
	qu.limit = limit
	return qu
}

func (qu *updater) statement() *sqlgen.Statement {
	stmt := &sqlgen.Statement{
		Type:         sqlgen.Update,
		Table:        sqlgen.TableWithName(qu.table),
		ColumnValues: qu.columnValues,
	}

	if qu.Where != nil {
		stmt.Where = qu.where
	}

	if qu.limit != 0 {
		stmt.Limit = sqlgen.Limit(qu.limit)
	}

	return stmt
}
