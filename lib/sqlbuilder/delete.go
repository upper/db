package sqlbuilder

import (
	"database/sql"

	"upper.io/db.v3/internal/immutable"
	"upper.io/db.v3/internal/sqladapter/exql"
)

type deleterQuery struct {
	table     string
	limit     int
	where     *exql.Where
	arguments []interface{}
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

	return stmt
}

type deleter struct {
	builder *sqlBuilder

	fn   func(*deleterQuery) error
	prev *deleter
}

var _ = immutable.Immutable(&deleter{})

func (del *deleter) Builder() *sqlBuilder {
	if del.prev == nil {
		return del.builder
	}
	return del.prev.Builder()
}

func (del *deleter) template() *exql.Template {
	return del.Builder().t.Template
}

func (del *deleter) String() string {
	return prepareQueryForDisplay(del.Compile())
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
		where, arguments := toWhereWithArguments(terms)
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

func (del *deleter) Arguments() []interface{} {
	dq, err := del.build()
	if err != nil {
		return nil
	}
	return dq.arguments
}

func (del *deleter) Exec() (sql.Result, error) {
	dq, err := del.build()
	if err != nil {
		return nil, err
	}
	return del.Builder().sess.StatementExec(dq.statement(), dq.arguments...)
}

func (del *deleter) statement() *exql.Statement {
	iq, _ := del.build()
	return iq.statement()
}

func (del *deleter) build() (*deleterQuery, error) {
	dq, err := immutable.FastForward(del)
	if err != nil {
		return nil, err
	}
	return dq.(*deleterQuery), nil
}

func (del *deleter) Compile() string {
	return del.statement().Compile(del.template())
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
