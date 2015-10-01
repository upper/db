package postgresql

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/jmoiron/sqlx"
	"regexp"
	"strings"
	"upper.io/db"
	"upper.io/db/util/sqlgen"
	"upper.io/db/util/sqlutil"
)

type SelectMode uint8

var (
	reInvisibleChars = regexp.MustCompile(`[\s\r\n\t]+`)
)

const (
	selectModeAll SelectMode = iota
	selectModeDistinct
)

type Builder struct {
	sess *database
}

func (b *Builder) SelectAllFrom(table string) db.QuerySelector {
	qs := &QuerySelector{
		builder: b,
		table:   table,
	}

	qs.stringer = &stringer{qs}
	return qs
}

func (b *Builder) Select(columns ...interface{}) db.QuerySelector {
	f, err := columnFragments(columns)

	qs := &QuerySelector{
		builder: b,
		columns: sqlgen.JoinColumns(f...),
		err:     err,
	}

	qs.stringer = &stringer{qs}
	return qs
}

func (b *Builder) InsertInto(table string) db.QueryInserter {
	qi := &QueryInserter{
		builder: b,
		table:   table,
	}

	qi.stringer = &stringer{qi}
	return qi
}

func (b *Builder) DeleteFrom(table string) db.QueryDeleter {
	qd := &QueryDeleter{
		builder: b,
		table:   table,
	}

	qd.stringer = &stringer{qd}
	return qd
}

func (b *Builder) Update(table string) db.QueryUpdater {
	qu := &QueryUpdater{
		builder: b,
		table:   table,
	}

	qu.stringer = &stringer{qu}
	return qu
}

type QueryInserter struct {
	*stringer
	builder   *Builder
	table     string
	values    []*sqlgen.Values
	columns   []sqlgen.Fragment
	arguments []interface{}
}

func (qi *QueryInserter) Exec() (sql.Result, error) {
	return qi.builder.sess.Exec(qi.statement())
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
	if len(qi.columns) == 0 || len(values) == len(qi.columns) {
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

func (qi *QueryInserter) statement() *sqlgen.Statement {
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
	return stmt
}

type QueryDeleter struct {
	*stringer
	builder   *Builder
	table     string
	limit     int
	where     *sqlgen.Where
	arguments []interface{}
}

func (qd *QueryDeleter) Where(terms ...interface{}) db.QueryDeleter {
	where, arguments := template.ToWhereWithArguments(terms)
	qd.where = &where
	qd.arguments = append(qd.arguments, arguments...)
	return qd
}

func (qd *QueryDeleter) Limit(limit int) db.QueryDeleter {
	qd.limit = limit
	return qd
}

func (qd *QueryDeleter) Exec() (sql.Result, error) {
	return qd.builder.sess.Exec(qd.statement(), qd.arguments...)
}

func (qd *QueryDeleter) statement() *sqlgen.Statement {
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

	return stmt
}

type QueryUpdater struct {
	*stringer
	builder      *Builder
	table        string
	columnValues *sqlgen.ColumnValues
	limit        int
	where        *sqlgen.Where
	arguments    []interface{}
}

func (qu *QueryUpdater) Set(terms ...interface{}) db.QueryUpdater {
	cv, arguments := template.ToColumnValues(terms)
	qu.columnValues = &cv
	qu.arguments = append(qu.arguments, arguments...)
	return qu
}

func (qu *QueryUpdater) Where(terms ...interface{}) db.QueryUpdater {
	where, arguments := template.ToWhereWithArguments(terms)
	qu.where = &where
	qu.arguments = append(qu.arguments, arguments...)
	return qu
}

func (qu *QueryUpdater) Exec() (sql.Result, error) {
	return qu.builder.sess.Exec(qu.statement(), qu.arguments...)
}

func (qu *QueryUpdater) Limit(limit int) db.QueryUpdater {
	qu.limit = limit
	return qu
}

func (qu *QueryUpdater) statement() *sqlgen.Statement {
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

	return stmt
}

type QuerySelector struct {
	*stringer
	mode      SelectMode
	cursor    *sqlx.Rows // This is the main query cursor. It starts as a nil value.
	builder   *Builder
	table     string
	where     *sqlgen.Where
	groupBy   *sqlgen.GroupBy
	orderBy   sqlgen.OrderBy
	limit     sqlgen.Limit
	offset    sqlgen.Offset
	columns   *sqlgen.Columns
	joins     []*sqlgen.Join
	arguments []interface{}
	err       error
}

func (qs *QuerySelector) From(tables ...string) db.QuerySelector {
	qs.table = strings.Join(tables, ",")
	return qs
}

func (qs *QuerySelector) Distinct() db.QuerySelector {
	qs.mode = selectModeDistinct
	return qs
}

func (qs *QuerySelector) Where(terms ...interface{}) db.QuerySelector {
	where, arguments := template.ToWhereWithArguments(terms)
	qs.where = &where
	qs.arguments = append(qs.arguments, arguments...)
	return qs
}

func (qs *QuerySelector) GroupBy(columns ...interface{}) db.QuerySelector {
	var fragments []sqlgen.Fragment
	fragments, qs.err = columnFragments(columns)
	if fragments != nil {
		qs.groupBy = sqlgen.GroupByColumns(fragments...)
	}
	return qs
}

func (qs *QuerySelector) OrderBy(columns ...interface{}) db.QuerySelector {
	var sortColumns sqlgen.SortColumns

	for i := range columns {
		var sort *sqlgen.SortColumn

		switch value := columns[i].(type) {
		case db.Raw:
			sort = &sqlgen.SortColumn{
				Column: sqlgen.RawValue(fmt.Sprintf(`%v`, value.Value)),
			}
		case string:
			if strings.HasPrefix(value, `-`) {
				sort = &sqlgen.SortColumn{
					Column: sqlgen.ColumnWithName(value[1:]),
					Order:  sqlgen.Descendent,
				}
			} else {
				sort = &sqlgen.SortColumn{
					Column: sqlgen.ColumnWithName(value),
					Order:  sqlgen.Ascendent,
				}
			}
		}
		sortColumns.Columns = append(sortColumns.Columns, sort)
	}

	qs.orderBy.SortColumns = &sortColumns

	return qs
}

func (qs *QuerySelector) Using(columns ...interface{}) db.QuerySelector {
	if len(qs.joins) == 0 {
		qs.err = errors.New(`Cannot use Using() without a preceding Join() expression.`)
		return qs
	}

	lastJoin := qs.joins[len(qs.joins)-1]

	if lastJoin.On != nil {
		qs.err = errors.New(`Cannot use Using() and On() with the same Join() expression.`)
		return qs
	}

	fragments, err := columnFragments(columns)
	if err != nil {
		qs.err = err
		return qs
	}

	lastJoin.Using = sqlgen.UsingColumns(fragments...)
	return qs
}

func (qs *QuerySelector) pushJoin(t string, tables []interface{}) db.QuerySelector {
	if qs.joins == nil {
		qs.joins = []*sqlgen.Join{}
	}

	tableNames := make([]string, len(tables))
	for i := range tables {
		tableNames[i] = fmt.Sprintf("%s", tables[i])
	}

	qs.joins = append(qs.joins,
		&sqlgen.Join{
			Type:  t,
			Table: sqlgen.TableWithName(strings.Join(tableNames, ", ")),
		},
	)

	return qs
}

func (qs *QuerySelector) FullJoin(tables ...interface{}) db.QuerySelector {
	return qs.pushJoin("FULL", tables)
}

func (qs *QuerySelector) CrossJoin(tables ...interface{}) db.QuerySelector {
	return qs.pushJoin("CROSS", tables)
}

func (qs *QuerySelector) RightJoin(tables ...interface{}) db.QuerySelector {
	return qs.pushJoin("RIGHT", tables)
}

func (qs *QuerySelector) LeftJoin(tables ...interface{}) db.QuerySelector {
	return qs.pushJoin("LEFT", tables)
}

func (qs *QuerySelector) Join(tables ...interface{}) db.QuerySelector {
	return qs.pushJoin("", tables)
}

func (qs *QuerySelector) On(terms ...interface{}) db.QuerySelector {
	if len(qs.joins) == 0 {
		qs.err = errors.New(`Cannot use On() without a preceding Join() expression.`)
		return qs
	}

	lastJoin := qs.joins[len(qs.joins)-1]

	if lastJoin.On != nil {
		qs.err = errors.New(`Cannot use Using() and On() with the same Join() expression.`)
		return qs
	}

	w, a := template.ToWhereWithArguments(terms)
	o := sqlgen.On(w)
	lastJoin.On = &o

	qs.arguments = append(qs.arguments, a...)
	return qs
}

func (qs *QuerySelector) Limit(n int) db.QuerySelector {
	qs.limit = sqlgen.Limit(n)
	return qs
}

func (qs *QuerySelector) Offset(n int) db.QuerySelector {
	qs.offset = sqlgen.Offset(n)
	return qs
}

func (qs *QuerySelector) statement() *sqlgen.Statement {
	return &sqlgen.Statement{
		Type:    sqlgen.Select,
		Table:   sqlgen.TableWithName(qs.table),
		Columns: qs.columns,
		Limit:   qs.limit,
		Offset:  qs.offset,
		Joins:   sqlgen.JoinConditions(qs.joins...),
		Where:   qs.where,
		OrderBy: &qs.orderBy,
		GroupBy: qs.groupBy,
	}
}

func (qs *QuerySelector) Query() (*sqlx.Rows, error) {
	return qs.builder.sess.Query(qs.statement(), qs.arguments...)
}

func (qs *QuerySelector) QueryRow() (*sqlx.Row, error) {
	return qs.builder.sess.QueryRow(qs.statement(), qs.arguments...)
}

func (qs *QuerySelector) Close() (err error) {
	if qs.cursor != nil {
		err = qs.cursor.Close()
		qs.cursor = nil
	}
	return err
}

func (qs *QuerySelector) setCursor() (err error) {
	if qs.cursor == nil {
		qs.cursor, err = qs.builder.sess.Query(qs.statement(), qs.arguments...)
	}
	return err
}

func (qs *QuerySelector) One(dst interface{}) error {
	if qs.err != nil {
		return qs.err
	}

	if qs.cursor != nil {
		return db.ErrQueryIsPending
	}

	defer qs.Close()

	if !qs.Next(dst) {
		return qs.Err()
	}

	return nil
}

func (qs *QuerySelector) All(dst interface{}) error {
	var err error

	if qs.err != nil {
		return qs.err
	}

	if qs.cursor != nil {
		return db.ErrQueryIsPending
	}

	err = qs.setCursor()

	if err != nil {
		return err
	}

	defer qs.Close()

	// Fetching all results within the cursor.
	err = sqlutil.FetchRows(qs.cursor, dst)

	return err
}

func (qs *QuerySelector) Err() (err error) {
	return qs.err
}

func (qs *QuerySelector) Next(dst interface{}) bool {
	var err error

	if qs.err != nil {
		return false
	}

	if err = qs.setCursor(); err != nil {
		qs.err = err
		qs.Close()
		return false
	}

	if err = sqlutil.FetchRow(qs.cursor, dst); err != nil {
		qs.err = err
		qs.Close()
		return false
	}

	return true
}

func columnFragments(columns []interface{}) ([]sqlgen.Fragment, error) {
	l := len(columns)
	f := make([]sqlgen.Fragment, l)

	for i := 0; i < l; i++ {
		switch v := columns[i].(type) {
		case db.Raw:
			f[i] = sqlgen.RawValue(fmt.Sprintf("%v", v.Value))
		case sqlgen.Fragment:
			f[i] = v
		case string:
			f[i] = sqlgen.ColumnWithName(v)
		case interface{}:
			f[i] = sqlgen.ColumnWithName(fmt.Sprintf("%v", v))
		default:
			return nil, fmt.Errorf("Unexpected argument type %T for Select() argument.", v)
		}
	}

	return f, nil
}

type hasStatement interface {
	statement() *sqlgen.Statement
}

type stringer struct {
	i hasStatement
}

func (s *stringer) String() string {
	if s != nil && s.i != nil {
		q := compileAndReplacePlaceholders(s.i.statement())
		q = reInvisibleChars.ReplaceAllString(q, ` `)
		return strings.TrimSpace(q)
	}
	return ""
}
