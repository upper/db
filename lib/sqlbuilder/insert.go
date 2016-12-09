package sqlbuilder

import (
	"database/sql"
	"strings"

	"upper.io/db.v2/internal/sqladapter/exql"
)

type inserterQuery struct {
	table     string
	values    []*exql.Values
	returning []exql.Fragment
	columns   []exql.Fragment
	arguments []interface{}
	extra     string
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

func (ins *inserter) Stringer() *stringer {
	p := &ins
	for {
		if (*p).stringer != nil {
			return (*p).stringer
		}
		if (*p).prev == nil {
			return nil
		}
		p = &(*p).prev
	}
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
	return ins.builder.sess.StatementExec(iq.statement(), iq.arguments...)
}

func (ins *inserter) Query() (*sql.Rows, error) {
	iq, err := ins.build()
	if err != nil {
		return nil, err
	}
	return ins.builder.sess.StatementQuery(iq.statement(), iq.arguments...)
}

func (ins *inserter) QueryRow() (*sql.Row, error) {
	iq, err := ins.build()
	if err != nil {
		return nil, err
	}
	return ins.builder.sess.StatementQueryRow(iq.statement(), iq.arguments...)
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
		if len(values) == 1 {
			ff, vv, err := Map(values[0], &MapOptions{IncludeZeroed: true, IncludeNil: true})
			if err == nil {
				columns, vals, arguments, _ := toColumnsValuesAndArguments(ff, vv)

				iq.arguments = append(iq.arguments, arguments...)
				iq.values = append(iq.values, vals)
				if len(iq.columns) == 0 {
					for _, c := range columns.Columns {
						iq.columns = append(iq.columns, c)
					}
				}
				return nil
			}
		}

		if len(iq.columns) == 0 || len(values) == len(iq.columns) {
			iq.arguments = append(iq.arguments, values...)

			l := len(values)
			placeholders := make([]exql.Fragment, l)
			for i := 0; i < l; i++ {
				placeholders[i] = exql.RawValue(`?`)
			}
			iq.values = append(iq.values, exql.NewValueGroup(placeholders...))
		}

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
