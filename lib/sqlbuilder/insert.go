package sqlbuilder

import (
	"database/sql"
	"sync"

	"upper.io/db.v2/internal/sqladapter/exql"
)

type inserter struct {
	*stringer
	builder *sqlBuilder
	table   string

	enqueuedValues [][]interface{}
	mu             sync.Mutex

	returning []exql.Fragment
	columns   []exql.Fragment
	arguments []interface{}

	amendFn func(string) string
	extra   string
}

func (qi *inserter) clone() *inserter {
	clone := &inserter{}
	*clone = *qi
	return clone
}

func (qi *inserter) Batch(n int) *BatchInserter {
	return newBatchInserter(qi.clone(), n)
}

func (qi *inserter) Amend(fn func(string) string) Inserter {
	qi.amendFn = fn
	return qi
}

func (qi *inserter) Arguments() []interface{} {
	_ = qi.statement()
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
	qi.mu.Lock()
	defer qi.mu.Unlock()

	if qi.enqueuedValues == nil {
		qi.enqueuedValues = [][]interface{}{}
	}
	qi.enqueuedValues = append(qi.enqueuedValues, values)
	return qi
}

func (qi *inserter) processValues() (values []*exql.Values, arguments []interface{}) {
	// TODO: simplify with immutable queries
	var insertNils bool

	for _, enqueuedValue := range qi.enqueuedValues {
		if len(enqueuedValue) == 1 {
			ff, vv, err := Map(enqueuedValue[0], nil)
			if err == nil {
				columns, vals, args, _ := qi.builder.t.ToColumnsValuesAndArguments(ff, vv)

				values, arguments = append(values, vals), append(arguments, args...)

				if len(qi.columns) == 0 {
					for _, c := range columns.Columns {
						qi.columns = append(qi.columns, c)
					}
				} else {
					if len(qi.columns) != len(columns.Columns) {
						insertNils = true
						break
					}
				}
				continue
			}
		}

		if len(qi.columns) == 0 || len(enqueuedValue) == len(qi.columns) {
			arguments = append(arguments, enqueuedValue...)

			l := len(enqueuedValue)
			placeholders := make([]exql.Fragment, l)
			for i := 0; i < l; i++ {
				placeholders[i] = exql.RawValue(`?`)
			}
			values = append(values, exql.NewValueGroup(placeholders...))
		}
	}

	if insertNils {
		values, arguments = values[0:0], arguments[0:0]

		for _, enqueuedValue := range qi.enqueuedValues {
			if len(enqueuedValue) == 1 {
				ff, vv, err := Map(enqueuedValue[0], &MapOptions{IncludeZeroed: true, IncludeNil: true})
				if err == nil {
					columns, vals, args, _ := qi.builder.t.ToColumnsValuesAndArguments(ff, vv)
					values, arguments = append(values, vals), append(arguments, args...)

					if len(qi.columns) != len(columns.Columns) {
						qi.columns = qi.columns[0:0]
						for _, c := range columns.Columns {
							qi.columns = append(qi.columns, c)
						}
					}
				}
				continue
			}
		}
	}

	return
}

func (qi *inserter) statement() *exql.Statement {
	stmt := &exql.Statement{
		Type:  exql.Insert,
		Table: exql.TableWithName(qi.table),
	}

	values, arguments := qi.processValues()

	qi.arguments = arguments

	if len(qi.columns) > 0 {
		stmt.Columns = exql.JoinColumns(qi.columns...)
	}

	if len(values) > 0 {
		stmt.Values = exql.JoinValueGroups(values...)
	}

	if len(qi.returning) > 0 {
		stmt.Returning = exql.ReturningColumns(qi.returning...)
	}

	stmt.SetAmendment(qi.amendFn)

	return stmt
}
