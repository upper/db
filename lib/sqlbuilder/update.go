package sqlbuilder

import (
	"context"
	"database/sql"

	"upper.io/db.v3/internal/immutable"
	"upper.io/db.v3/internal/sqladapter/exql"
)

type updaterQuery struct {
	table string

	columnValues     *exql.ColumnValues
	columnValuesArgs []interface{}

	limit int

	where     *exql.Where
	whereArgs []interface{}
}

func (uq *updaterQuery) statement() *exql.Statement {
	stmt := &exql.Statement{
		Type:         exql.Update,
		Table:        exql.TableWithName(uq.table),
		ColumnValues: uq.columnValues,
	}

	if uq.where != nil {
		stmt.Where = uq.where
	}

	if uq.limit != 0 {
		stmt.Limit = exql.Limit(uq.limit)
	}

	return stmt
}

func (uq *updaterQuery) arguments() []interface{} {
	return joinArguments(
		uq.columnValuesArgs,
		uq.whereArgs,
	)
}

type updater struct {
	builder *sqlBuilder

	fn   func(*updaterQuery) error
	prev *updater
}

var _ = immutable.Immutable(&updater{})

func (upd *updater) SQLBuilder() *sqlBuilder {
	if upd.prev == nil {
		return upd.builder
	}
	return upd.prev.SQLBuilder()
}

func (upd *updater) template() *exql.Template {
	return upd.SQLBuilder().t.Template
}

func (upd *updater) String() string {
	return prepareQueryForDisplay(upd.Compile())
}

func (upd *updater) setTable(table string) *updater {
	return upd.frame(func(uq *updaterQuery) error {
		uq.table = table
		return nil
	})
}

func (upd *updater) frame(fn func(*updaterQuery) error) *updater {
	return &updater{prev: upd, fn: fn}
}

func (upd *updater) Set(terms ...interface{}) Updater {
	return upd.frame(func(uq *updaterQuery) error {
		if uq.columnValues == nil {
			uq.columnValues = &exql.ColumnValues{}
		}

		if len(terms) == 1 {
			ff, vv, _ := Map(terms[0], nil)

			cvs := make([]exql.Fragment, 0, len(ff))
			args := make([]interface{}, 0, len(vv))

			for i := range ff {
				cv := &exql.ColumnValue{
					Column:   exql.ColumnWithName(ff[i]),
					Operator: upd.SQLBuilder().t.AssignmentOperator,
				}

				var localArgs []interface{}
				cv.Value, localArgs = upd.SQLBuilder().t.PlaceholderValue(vv[i])

				args = append(args, localArgs...)
				cvs = append(cvs, cv)
			}

			uq.columnValues.Insert(cvs...)
			uq.columnValuesArgs = append(uq.columnValuesArgs, args...)
		} else if len(terms) > 1 {
			cv, arguments := toColumnValues(terms)
			uq.columnValues.Insert(cv.ColumnValues...)
			uq.columnValuesArgs = append(uq.columnValuesArgs, arguments...)
		}

		return nil
	})
}

func (upd *updater) Arguments() []interface{} {
	uq, err := upd.build()
	if err != nil {
		return nil
	}
	return uq.arguments()
}

func (upd *updater) Where(terms ...interface{}) Updater {
	return upd.frame(func(uq *updaterQuery) error {
		where, arguments := toWhereWithArguments(terms)
		uq.where = &where
		uq.whereArgs = append(uq.whereArgs, arguments...)
		return nil
	})
}

func (upd *updater) Exec() (sql.Result, error) {
	return upd.ExecContext(upd.SQLBuilder().sess.Context())
}

func (upd *updater) ExecContext(ctx context.Context) (sql.Result, error) {
	uq, err := upd.build()
	if err != nil {
		return nil, err
	}
	return upd.SQLBuilder().sess.StatementExec(ctx, uq.statement(), uq.arguments()...)
}

func (upd *updater) Limit(limit int) Updater {
	return upd.frame(func(uq *updaterQuery) error {
		uq.limit = limit
		return nil
	})
}

func (upd *updater) statement() *exql.Statement {
	iq, _ := upd.build()
	return iq.statement()
}

func (upd *updater) build() (*updaterQuery, error) {
	uq, err := immutable.FastForward(upd)
	if err != nil {
		return nil, err
	}
	return uq.(*updaterQuery), nil
}

func (upd *updater) Compile() string {
	return upd.statement().Compile(upd.template())
}

func (upd *updater) Prev() immutable.Immutable {
	if upd == nil {
		return nil
	}
	return upd.prev
}

func (upd *updater) Fn(in interface{}) error {
	if upd.fn == nil {
		return nil
	}
	return upd.fn(in.(*updaterQuery))
}

func (upd *updater) Base() interface{} {
	return &updaterQuery{}
}
