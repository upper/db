package sqlbuilder

import (
	"database/sql"
	"sync"

	"upper.io/db.v2/internal/sqladapter/exql"
)

type deleter struct {
	*stringer
	builder *sqlBuilder
	table   string
	limit   int

	where     *exql.Where
	whereArgs []interface{}

	amendFn func(string) string
	mu      sync.Mutex
}

func (qd *deleter) Where(terms ...interface{}) Deleter {
	qd.mu.Lock()
	qd.where, qd.whereArgs = &exql.Where{}, []interface{}{}
	qd.mu.Unlock()
	return qd.And(terms...)
}

func (qd *deleter) And(terms ...interface{}) Deleter {
	where, whereArgs := qd.builder.t.ToWhereWithArguments(terms)

	qd.mu.Lock()
	if qd.where == nil {
		qd.where, qd.whereArgs = &exql.Where{}, []interface{}{}
	}
	qd.where.Append(&where)
	qd.whereArgs = append(qd.whereArgs, whereArgs...)
	qd.mu.Unlock()

	return qd
}

func (qd *deleter) Limit(limit int) Deleter {
	qd.limit = limit
	return qd
}

func (qd *deleter) Amend(fn func(string) string) Deleter {
	qd.amendFn = fn
	return qd
}

func (qd *deleter) Arguments() []interface{} {
	return qd.whereArgs
}

func (qd *deleter) Exec() (sql.Result, error) {
	return qd.builder.sess.StatementExec(qd.statement(), qd.whereArgs...)
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

	stmt.SetAmendment(qd.amendFn)

	return stmt
}
