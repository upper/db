package sqlbuilder

import (
	"database/sql"

	"upper.io/db.v2/internal/sqladapter/exql"
)

type deleter struct {
	*stringer
	builder   *sqlBuilder
	table     string
	limit     int
	where     *exql.Where
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

func (qd *deleter) Arguments() []interface{} {
	return qd.arguments
}

func (qd *deleter) Exec() (sql.Result, error) {
	return qd.builder.sess.StatementExec(qd.statement(), qd.arguments...)
}

func (qd *deleter) statement() *exql.Statement {
	stmt := &exql.Statement{
		Type:  exql.Delete,
		Table: exql.TableWithName(qd.table),
	}

	if qd.Where != nil {
		stmt.Where = qd.where
	}

	if qd.limit != 0 {
		stmt.Limit = exql.Limit(qd.limit)
	}

	return stmt
}
