package sqlbuilder

import (
	"database/sql"
	"strings"

	"upper.io/db.v2/internal/sqladapter/exql"
)

type inserterQuery struct {
	table          string
	enqueuedValues [][]interface{}
	returning      []exql.Fragment
	columns        []exql.Fragment
	values         []*exql.Values
	arguments      []interface{}
	extra          string
}

func (iq *inserterQuery) processValues() (values []*exql.Values, arguments []interface{}) {
	var insertNils bool

	for _, enqueuedValue := range iq.enqueuedValues {
		if len(enqueuedValue) == 1 {
			ff, vv, err := Map(enqueuedValue[0], nil)
			if err == nil {
				columns, vals, args, _ := toColumnsValuesAndArguments(ff, vv)

				values, arguments = append(values, vals), append(arguments, args...)

				if len(iq.columns) == 0 {
					for _, c := range columns.Columns {
						iq.columns = append(iq.columns, c)
					}
				} else {
					if len(iq.columns) != len(columns.Columns) {
						insertNils = true
						break
					}
				}
				continue
			}
		}

		if len(iq.columns) == 0 || len(enqueuedValue) == len(iq.columns) {
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

		for _, enqueuedValue := range iq.enqueuedValues {
			if len(enqueuedValue) == 1 {
				ff, vv, err := Map(enqueuedValue[0], &MapOptions{IncludeZeroed: true, IncludeNil: true})
				if err == nil {
					columns, vals, args, _ := toColumnsValuesAndArguments(ff, vv)
					values, arguments = append(values, vals), append(arguments, args...)

					if len(iq.columns) != len(columns.Columns) {
						iq.columns = iq.columns[0:0]
						for _, c := range columns.Columns {
							iq.columns = append(iq.columns, c)
						}
					}
				}
				continue
			}
		}
	}
	return
}

func (iq *inserterQuery) statement() *exql.Statement {
	stmt := &exql.Statement{
		Type:  exql.Insert,
		Table: exql.TableWithName(iq.table),
	}

	if len(iq.values) > 0 {
		stmt.Values = exql.JoinValueGroups(iq.values...)
	}

	if len(iq.columns) > 0 {
		stmt.Columns = exql.JoinColumns(iq.columns...)
	}

	if len(iq.returning) > 0 {
		stmt.Returning = exql.ReturningColumns(iq.returning...)
	}

	return stmt
}

func columnsToFragments(dst *[]exql.Fragment, columns []string) error {
	l := len(columns)
	f := make([]exql.Fragment, l)
	for i := 0; i < l; i++ {
		f[i] = exql.ColumnWithName(columns[i])
	}
	*dst = append(*dst, f...)
	return nil
}

type inserter struct {
	builder *sqlBuilder
	*stringer

	fn   func(*inserterQuery) error
	prev *inserter
}

func (ins *inserter) Builder() *sqlBuilder {
	if ins.prev == nil {
		return ins.builder
	}
	return ins.prev.Builder()
}

func (ins *inserter) Stringer() *stringer {
	if ins.prev == nil {
		return ins.stringer
	}
	return ins.prev.Stringer()
}

func (ins *inserter) String() string {
	query, err := ins.build()
	if err != nil {
		return ""
	}
	q := ins.Stringer().compileAndReplacePlaceholders(query.statement())
	q = reInvisibleChars.ReplaceAllString(q, ` `)
	return strings.TrimSpace(q)
}

func (ins *inserter) frame(fn func(*inserterQuery) error) *inserter {
	return &inserter{prev: ins, fn: fn}
}

func (ins *inserter) clone() *inserter {
	clone := &inserter{}
	*clone = *ins
	return clone
}

func (ins *inserter) Batch(n int) *BatchInserter {
	return newBatchInserter(ins.clone(), n)
}

func (ins *inserter) Arguments() []interface{} {
	iq, err := ins.build()
	if err != nil {
		return nil
	}
	return iq.arguments
}

func (ins *inserter) Returning(columns ...string) Inserter {
	return ins.frame(func(iq *inserterQuery) error {
		columnsToFragments(&iq.returning, columns)
		return nil
	})
}

func (ins *inserter) Exec() (sql.Result, error) {
	iq, err := ins.build()
	if err != nil {
		return nil, err
	}
	return ins.Builder().sess.StatementExec(iq.statement(), iq.arguments...)
}

func (ins *inserter) Query() (*sql.Rows, error) {
	iq, err := ins.build()
	if err != nil {
		return nil, err
	}
	return ins.Builder().sess.StatementQuery(iq.statement(), iq.arguments...)
}

func (ins *inserter) QueryRow() (*sql.Row, error) {
	iq, err := ins.build()
	if err != nil {
		return nil, err
	}
	return ins.Builder().sess.StatementQueryRow(iq.statement(), iq.arguments...)
}

func (ins *inserter) Iterator() Iterator {
	rows, err := ins.Query()
	return &iterator{rows, err}
}

func (ins *inserter) Into(table string) Inserter {
	return ins.frame(func(iq *inserterQuery) error {
		iq.table = table
		return nil
	})
}

func (ins *inserter) Columns(columns ...string) Inserter {
	return ins.frame(func(iq *inserterQuery) error {
		columnsToFragments(&iq.columns, columns)
		return nil
	})
}

func (ins *inserter) Values(values ...interface{}) Inserter {
	return ins.frame(func(iq *inserterQuery) error {
		iq.enqueuedValues = append(iq.enqueuedValues, values)
		return nil
	})
}

func (ins *inserter) statement() *exql.Statement {
	iq, _ := ins.build()
	return iq.statement()
}

func (ins *inserter) build() (*inserterQuery, error) {
	iq, err := inserterFastForward(&inserterQuery{}, ins)
	if err != nil {
		return nil, err
	}
	iq.values, iq.arguments = iq.processValues()
	return iq, nil
}

func (ins *inserter) Compile() string {
	return ins.statement().Compile(ins.Stringer().t)
}

func inserterFastForward(in *inserterQuery, curr *inserter) (*inserterQuery, error) {
	if curr == nil || curr.fn == nil {
		return in, nil
	}
	in, err := inserterFastForward(in, curr.prev)
	if err != nil {
		return nil, err
	}
	err = curr.fn(in)
	return in, err
}
