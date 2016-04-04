package builder

import (
	"database/sql"

	"upper.io/db.v2/builder/expr"
)

type deleter struct {
	*stringer
	builder   *sqlBuilder
	table     string
	limit     int
	where     *expr.Where
	arguments []interface{}
}

func (qd *deleter) Where(terms ...interface{}) Deleter {
	where, arguments := qd.builder.t.ToWhereWithArguments(terms)
	qd.where = &where
	qd.arguments = append(qd.arguments, arguments...)
	return qd
}

func (qd *deleter) Limit(limit int) Deleter {
	qd.limit = limit
	return qd
}

func (qd *deleter) Exec() (sql.Result, error) {
	return qd.builder.sess.Exec(qd.statement(), qd.arguments...)
}

func (qd *deleter) statement() *expr.Statement {
	stmt := &expr.Statement{
		Type:  expr.Delete,
		Table: expr.TableWithName(qd.table),
	}

	if qd.Where != nil {
		stmt.Where = qd.where
	}

	if qd.limit != 0 {
		stmt.Limit = expr.Limit(qd.limit)
	}

	return stmt
}
