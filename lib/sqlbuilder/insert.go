package sqlbuilder

import (
	"database/sql"

	"upper.io/db.v2/internal/sqladapter/exql"
)

type inserter struct {
	*stringer
	builder   *sqlBuilder
	table     string
	values    []*exql.Values
	returning []exql.Fragment
	columns   []exql.Fragment
	arguments []interface{}
	extra     string
}

func (qi *inserter) clone() *inserter {
	clone := &inserter{}
	*clone = *qi
	return clone
}

func (qi *inserter) Batch(n int) *BatchInserter {
	return newBatchInserter(qi.clone(), n)
}

func (qi *inserter) Arguments() []interface{} {
	return qi.arguments
}

func (qi *inserter) columnsToFragments(dst *[]exql.Fragment, columns []string) error {
	l := len(columns)
	f := make([]exql.Fragment, l)
	for i := 0; i < l; i++ {
		f[i] = exql.ColumnWithName(columns[i])
	}
	*dst = append(*dst, f...)
	return nil
}

func (qi *inserter) Returning(columns ...string) Inserter {
	qi.columnsToFragments(&qi.returning, columns)
	return qi
}

func (qi *inserter) Exec() (sql.Result, error) {
	return qi.builder.sess.StatementExec(qi.statement(), qi.arguments...)
}

func (qi *inserter) Query() (*sql.Rows, error) {
	return qi.builder.sess.StatementQuery(qi.statement(), qi.arguments...)
}

func (qi *inserter) QueryRow() (*sql.Row, error) {
	return qi.builder.sess.StatementQueryRow(qi.statement(), qi.arguments...)
}

func (qi *inserter) Iterator() Iterator {
	rows, err := qi.builder.sess.StatementQuery(qi.statement(), qi.arguments...)
	return &iterator{rows, err}
}

func (qi *inserter) Columns(columns ...string) Inserter {
	qi.columnsToFragments(&qi.columns, columns)
	return qi
}

func (qi *inserter) Values(values ...interface{}) Inserter {
	if len(values) == 1 {
		ff, vv, err := Map(values[0], &MapOptions{IncludeZeroed: true, IncludeNil: true})
		if err == nil {
			columns, vals, arguments, _ := qi.builder.t.ToColumnsValuesAndArguments(ff, vv)

			qi.arguments = append(qi.arguments, arguments...)
			qi.values = append(qi.values, vals)
			if len(qi.columns) == 0 {
				for _, c := range columns.Columns {
					qi.columns = append(qi.columns, c)
				}
			}
			return qi
		}
	}

	if len(qi.columns) == 0 || len(values) == len(qi.columns) {
		qi.arguments = append(qi.arguments, values...)

		l := len(values)
		placeholders := make([]exql.Fragment, l)
		for i := 0; i < l; i++ {
			placeholders[i] = exql.RawValue(`?`)
		}
		qi.values = append(qi.values, exql.NewValueGroup(placeholders...))
	}

	return qi
}

func (qi *inserter) statement() *exql.Statement {
	stmt := &exql.Statement{
		Type:  exql.Insert,
		Table: exql.TableWithName(qi.table),
	}

	if len(qi.values) > 0 {
		stmt.Values = exql.JoinValueGroups(qi.values...)
	}

	if len(qi.columns) > 0 {
		stmt.Columns = exql.JoinColumns(qi.columns...)
	}

	if len(qi.returning) > 0 {
		stmt.Returning = exql.ReturningColumns(qi.returning...)
	}

	return stmt
}
