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
