package sqlbuilder

import (
	"context"
	"database/sql"

	"upper.io/db.v3/internal/immutable"
	"upper.io/db.v3/internal/sqladapter/exql"
)

type deleterQuery struct {
	table     string
	limit     int
	where     *exql.Where
	arguments []interface{}
	amendFn   func(string) string
}

func (dq *deleterQuery) statement() *exql.Statement {
	stmt := &exql.Statement{
		Type:  exql.Delete,
		Table: exql.TableWithName(dq.table),
	}

	if dq.where != nil {
		stmt.Where = dq.where
	}

	if dq.limit != 0 {
		stmt.Limit = exql.Limit(dq.limit)
	}

	stmt.SetAmendment(dq.amendFn)

	return stmt
}

type deleter struct {
	builder *sqlBuilder

	fn   func(*deleterQuery) error
	prev *deleter
}

var _ = immutable.Immutable(&deleter{})

func (del *deleter) SQLBuilder() *sqlBuilder {
	if del.prev == nil {
		return del.builder
	}
	return del.prev.SQLBuilder()
}

func (del *deleter) template() *exql.Template {
	return del.SQLBuilder().t.Template
}

func (del *deleter) String() string {
	s, err := del.Compile()
	if err != nil {
		panic(err.Error())
	}
	return prepareQueryForDisplay(s)
}

func (del *deleter) setTable(table string) *deleter {
	return del.frame(func(uq *deleterQuery) error {
		uq.table = table
		return nil
	})
}

func (del *deleter) frame(fn func(*deleterQuery) error) *deleter {
	return &deleter{prev: del, fn: fn}
}

func (del *deleter) Where(terms ...interface{}) Deleter {
	return del.frame(func(dq *deleterQuery) error {
		where, arguments := del.SQLBuilder().t.toWhereWithArguments(terms)
		dq.where = &where
		dq.arguments = append(dq.arguments, arguments...)
		return nil
	})
}

func (del *deleter) Limit(limit int) Deleter {
	return del.frame(func(dq *deleterQuery) error {
		dq.limit = limit
		return nil
	})
}

func (del *deleter) Amend(fn func(string) string) Deleter {
	return del.frame(func(dq *deleterQuery) error {
		dq.amendFn = fn
		return nil
	})
}

func (del *deleter) Arguments() []interface{} {
	dq, err := del.build()
	if err != nil {
		return nil
	}
	return dq.arguments
}

func (del *deleter) Exec() (sql.Result, error) {
	return del.ExecContext(del.SQLBuilder().sess.Context())
}

func (del *deleter) ExecContext(ctx context.Context) (sql.Result, error) {
	dq, err := del.build()
	if err != nil {
		return nil, err
	}
	return del.SQLBuilder().sess.StatementExec(ctx, dq.statement(), dq.arguments...)
}

func (del *deleter) statement() (*exql.Statement, error) {
	iq, err := del.build()
	if err != nil {
		return nil, err
	}
	return iq.statement(), nil
}

func (del *deleter) build() (*deleterQuery, error) {
	dq, err := immutable.FastForward(del)
	if err != nil {
		return nil, err
	}
	return dq.(*deleterQuery), nil
}

func (del *deleter) Compile() (string, error) {
	s, err := del.statement()
	if err != nil {
		return "", err
	}
	return s.Compile(del.template())
}

func (del *deleter) Prev() immutable.Immutable {
	if del == nil {
		return nil
	}
	return del.prev
}

func (del *deleter) Fn(in interface{}) error {
	if del.fn == nil {
		return nil
	}
	return del.fn(in.(*deleterQuery))
}

func (del *deleter) Base() interface{} {
	return &deleterQuery{}
}
