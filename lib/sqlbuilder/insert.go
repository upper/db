package sqlbuilder

import (
	"database/sql"

	"upper.io/db.v3/internal/immutable"
	"upper.io/db.v3/internal/sqladapter/exql"
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

type inserter struct {
	builder *sqlBuilder

	fn   func(*inserterQuery) error
	prev *inserter
}

var _ = immutable.Immutable(&inserter{})

func (ins *inserter) Builder() *sqlBuilder {
	if ins.prev == nil {
		return ins.builder
	}
	return ins.prev.Builder()
}

func (ins *inserter) template() *exql.Template {
	return ins.Builder().t.Template
}

func (ins *inserter) String() string {
	return prepareQueryForDisplay(ins.Compile())
}

func (ins *inserter) frame(fn func(*inserterQuery) error) *inserter {
	return &inserter{prev: ins, fn: fn}
}

func (ins *inserter) Batch(n int) *BatchInserter {
	return newBatchInserter(ins, n)
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
	iq, err := immutable.FastForward(ins)
	if err != nil {
		return nil, err
	}
	ret := iq.(*inserterQuery)
	ret.values, ret.arguments = ret.processValues()
	return ret, nil
}

func (ins *inserter) Compile() string {
	return ins.statement().Compile(ins.template())
}

func (ins *inserter) Prev() immutable.Immutable {
	if ins == nil {
		return nil
	}
	return ins.prev
}

func (ins *inserter) Fn(in interface{}) error {
	if ins.fn == nil {
		return nil
	}
	return ins.fn(in.(*inserterQuery))
}

func (ins *inserter) Base() interface{} {
	return &inserterQuery{}
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
