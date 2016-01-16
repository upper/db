package sqlbuilder

import (
	"database/sql"

	"upper.io/db.v2/builder"
	"upper.io/db.v2/builder/sqlgen"
)

type deleter struct {
	*stringer
	builder   *sqlBuilder
	table     string
	limit     int
	where     *sqlgen.Where
	arguments []interface{}
}

func (qd *deleter) Where(terms ...interface{}) builder.Deleter {
	where, arguments := qd.builder.t.ToWhereWithArguments(terms)
	qd.where = &where
	qd.arguments = append(qd.arguments, arguments...)
	return qd
}

func (qd *deleter) Limit(limit int) builder.Deleter {
	qd.limit = limit
	return qd
}

func (qd *deleter) Exec() (sql.Result, error) {
	return qd.builder.sess.Exec(qd.statement(), qd.arguments...)
}

func (qd *deleter) statement() *sqlgen.Statement {
	stmt := &sqlgen.Statement{
		Type:  sqlgen.Delete,
		Table: sqlgen.TableWithName(qd.table),
	}

	if qd.Where != nil {
		stmt.Where = qd.where
	}

	if qd.limit != 0 {
		stmt.Limit = sqlgen.Limit(qd.limit)
	}

	return stmt
}
