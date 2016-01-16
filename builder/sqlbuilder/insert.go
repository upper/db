package sqlbuilder

import (
	"database/sql"

	"upper.io/db.v2/builder"
	"upper.io/db.v2/builder/sqlgen"
)

type inserter struct {
	*stringer
	builder   *sqlBuilder
	table     string
	values    []*sqlgen.Values
	returning []sqlgen.Fragment
	columns   []sqlgen.Fragment
	arguments []interface{}
	extra     string
}

func (qi *inserter) columnsToFragments(dst *[]sqlgen.Fragment, columns []string) error {
	l := len(columns)
	f := make([]sqlgen.Fragment, l)
	for i := 0; i < l; i++ {
		f[i] = sqlgen.ColumnWithName(columns[i])
	}
	*dst = append(*dst, f...)
	return nil
}

func (qi *inserter) Returning(columns ...string) builder.Inserter {
	qi.columnsToFragments(&qi.returning, columns)
	return qi
}

func (qi *inserter) Exec() (sql.Result, error) {
	return qi.builder.sess.Exec(qi.statement(), qi.arguments...)
}

func (qi *inserter) Query() (*sql.Rows, error) {
	return qi.builder.sess.Query(qi.statement(), qi.arguments...)
}

func (qi *inserter) QueryRow() (*sql.Row, error) {
	return qi.builder.sess.QueryRow(qi.statement(), qi.arguments...)
}

func (qi *inserter) Iterator() builder.Iterator {
	rows, err := qi.builder.sess.Query(qi.statement(), qi.arguments...)
	return &iterator{rows, err}
}

func (qi *inserter) Columns(columns ...string) builder.Inserter {
	qi.columnsToFragments(&qi.columns, columns)
	return qi
}

func (qi *inserter) Values(values ...interface{}) builder.Inserter {
	if len(qi.columns) == 0 && len(values) == 1 {
		ff, vv, _ := Map(values[0])

		columns, vals, arguments, _ := qi.builder.t.ToColumnsValuesAndArguments(ff, vv)

		qi.arguments = append(qi.arguments, arguments...)
		qi.values = append(qi.values, vals)

		for _, c := range columns.Columns {
			qi.columns = append(qi.columns, c)
		}
	} else if len(qi.columns) == 0 || len(values) == len(qi.columns) {
		qi.arguments = append(qi.arguments, values...)

		l := len(values)
		placeholders := make([]sqlgen.Fragment, l)
		for i := 0; i < l; i++ {
			placeholders[i] = sqlgen.RawValue(`?`)
		}
		qi.values = append(qi.values, sqlgen.NewValueGroup(placeholders...))
	}

	return qi
}

func (qi *inserter) statement() *sqlgen.Statement {
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

	if len(qi.returning) > 0 {
		stmt.Returning = sqlgen.ReturningColumns(qi.returning...)
	}

	return stmt
}
