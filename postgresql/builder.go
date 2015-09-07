package postgresql

import (
	"database/sql"
	"fmt"
	"upper.io/db"
	"upper.io/db/util/sqlgen"
)

type Builder struct {
	sess *database
}

func (b *Builder) Select(fields ...interface{}) db.QuerySelector {
	return &QuerySelector{
		builder: b,
		fields:  fields,
	}
}

func (b *Builder) InsertInto(table string) db.QueryInserter {
	return &QueryInserter{
		builder: b,
		table:   table,
	}
}

func (b *Builder) DeleteFrom(table string) db.QueryDeleter {
	return &QueryDeleter{
		builder: b,
		table:   table,
	}
}

func (b *Builder) Update(table string) db.QueryUpdater {
	return &QueryUpdater{
		builder: b,
		table:   table,
	}
}

type QuerySelector struct {
	builder *Builder
	fields  []interface{}
}

func (qs *QuerySelector) From(table ...string) db.Result {
	return qs.builder.sess.C(table...).Find().Select(qs.fields...)
}

type QueryInserter struct {
	builder *Builder
	table   string
	values  []*sqlgen.Values
	columns []sqlgen.Fragment
}

func (qi *QueryInserter) Exec() (sql.Result, error) {
	stmt := &sqlgen.Statement{
		Type:  sqlgen.Insert,
		Table: sqlgen.TableWithName(qi.table),
	}

	if len(qi.values) > 0 {
		stmt.Values = sqlgen.JoinValueGroups(qi.values...)
	}
	if len(qi.columns) > 0 {
		stmt.Columns = sqlgen.JoinColumns(qi.columns...)
	}

	return qi.builder.sess.Exec(stmt)
}

func (qi *QueryInserter) Columns(columns ...string) db.QueryInserter {
	l := len(columns)
	f := make([]sqlgen.Fragment, l)
	for i := 0; i < l; i++ {
		f[i] = sqlgen.ColumnWithName(columns[i])
	}
	qi.columns = append(qi.columns, f...)
	return qi
}

func (qi *QueryInserter) Values(values ...interface{}) db.QueryInserter {
	l := len(values)
	f := make([]sqlgen.Fragment, l)
	for i := 0; i < l; i++ {
		if _, ok := values[i].(db.Raw); ok {
			f[i] = sqlgen.NewValue(sqlgen.RawValue(fmt.Sprintf("%v", values[i])))
		} else {
			f[i] = sqlgen.NewValue(values[i])
		}
	}
	qi.values = append(qi.values, sqlgen.NewValueGroup(f...))
	return qi
}

type QueryDeleter struct {
	builder *Builder
	table   string
	limit   int
	where   *sqlgen.Where
	args    []interface{}
}

func (qd *QueryDeleter) Where(terms ...interface{}) db.QueryDeleter {
	where, arguments := template.ToWhereWithArguments(terms)
	qd.where = &where
	qd.args = append(qd.args, arguments...)
	return qd
}

func (qd *QueryDeleter) Limit(limit int) db.QueryDeleter {
	qd.limit = limit
	return qd
}

func (qd *QueryDeleter) Exec() (sql.Result, error) {
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

	return qd.builder.sess.Exec(stmt, qd.args...)
}

type QueryUpdater struct {
	builder      *Builder
	table        string
	columnValues *sqlgen.ColumnValues
	limit        int
	where        *sqlgen.Where
	args         []interface{}
}

func (qu *QueryUpdater) Set(terms ...interface{}) db.QueryUpdater {
	cv, args := template.ToColumnValues(terms)
	qu.columnValues = &cv
	qu.args = append(qu.args, args...)
	return qu
}

func (qu *QueryUpdater) Where(terms ...interface{}) db.QueryUpdater {
	where, arguments := template.ToWhereWithArguments(terms)
	qu.where = &where
	qu.args = append(qu.args, arguments...)
	return qu
}

func (qu *QueryUpdater) Exec() (sql.Result, error) {
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

	return qu.builder.sess.Exec(stmt, qu.args...)
}

func (qu *QueryUpdater) Limit(limit int) db.QueryUpdater {
	qu.limit = limit
	return qu
}
