package sqlbuilder

import (
	"database/sql"
	"sync"

	"upper.io/db.v2/internal/sqladapter/exql"
)

type updater struct {
	*stringer
	builder *sqlBuilder
	table   string

	columnValues     *exql.ColumnValues
	columnValuesArgs []interface{}

	limit int

	where     *exql.Where
	whereArgs []interface{}

	mu sync.Mutex
}

func (qu *updater) Set(terms ...interface{}) Updater {
	if len(terms) == 1 {
		ff, vv, _ := Map(terms[0], nil)

		cvs := make([]exql.Fragment, 0, len(ff))
		args := make([]interface{}, 0, len(vv))

		for i := range ff {
			cv := &exql.ColumnValue{
				Column:   exql.ColumnWithName(ff[i]),
				Operator: qu.builder.t.AssignmentOperator,
			}

			var localArgs []interface{}
			cv.Value, localArgs = qu.builder.t.PlaceholderValue(vv[i])

			args = append(args, localArgs...)
			cvs = append(cvs, cv)
		}

		qu.columnValues.Insert(cvs...)
		qu.columnValuesArgs = append(qu.columnValuesArgs, args...)
	} else if len(terms) > 1 {
		cv, arguments := qu.builder.t.ToColumnValues(terms)
		qu.columnValues.Insert(cv.ColumnValues...)
		qu.columnValuesArgs = append(qu.columnValuesArgs, arguments...)
	}

	return qu
}

func (qu *updater) Arguments() []interface{} {
	qu.mu.Lock()
	defer qu.mu.Unlock()

	return joinArguments(
		qu.columnValuesArgs,
		qu.whereArgs,
	)
}

func (qu *updater) Where(terms ...interface{}) Updater {
	where, arguments := qu.builder.t.ToWhereWithArguments(terms)
	qu.where = &where
	qu.whereArgs = append(qu.whereArgs, arguments...)
	return qu
}

func (qu *updater) Exec() (sql.Result, error) {
	return qu.builder.sess.StatementExec(qu.statement(), qu.Arguments()...)
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
