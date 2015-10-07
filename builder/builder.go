package builder

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/jmoiron/sqlx/reflectx"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"upper.io/db"
	"upper.io/db/util/sqlgen"
	"upper.io/db/util/sqlutil"
)

type SelectMode uint8

var mapper = reflectx.NewMapper("db")

type fieldValue struct {
	fields []string
	values []interface{}
}

func (fv *fieldValue) Len() int {
	return len(fv.fields)
}

func (fv *fieldValue) Swap(i, j int) {
	fv.fields[i], fv.fields[j] = fv.fields[j], fv.fields[i]
	fv.values[i], fv.values[j] = fv.values[j], fv.values[i]
}

func (fv *fieldValue) Less(i, j int) bool {
	return fv.fields[i] < fv.fields[j]
}

var (
	reInvisibleChars       = regexp.MustCompile(`[\s\r\n\t]+`)
	reColumnCompareExclude = regexp.MustCompile(`[^a-zA-Z0-9]`)
)

var (
	sqlPlaceholder = sqlgen.RawValue(`?`)
)

const (
	selectModeAll SelectMode = iota
	selectModeDistinct
)

type sqlDatabase interface {
	Query(stmt *sqlgen.Statement, args ...interface{}) (*sqlx.Rows, error)
	QueryRow(stmt *sqlgen.Statement, args ...interface{}) (*sqlx.Row, error)
	Exec(stmt *sqlgen.Statement, args ...interface{}) (sql.Result, error)
}

type Builder struct {
	sess sqlDatabase
	t    *sqlutil.TemplateWithUtils
}

func (b *Builder) Exec(query interface{}, args ...interface{}) (sql.Result, error) {
	switch q := query.(type) {
	case *sqlgen.Statement:
		return b.sess.Exec(q, args...)
	default:
		return nil, errors.New("Unsupported query type.")
	}
}

func (b *Builder) TruncateTable(table string) db.QueryTruncater {
	qs := &QueryTruncater{
		builder: b,
		table:   table,
	}

	qs.stringer = &stringer{qs, b.t.Template}
	return qs
}

func (b *Builder) SelectAllFrom(table string) db.QuerySelector {
	qs := &QuerySelector{
		builder: b,
		table:   table,
	}

	qs.stringer = &stringer{qs, b.t.Template}
	return qs
}

func (b *Builder) Select(columns ...interface{}) db.QuerySelector {
	qs := &QuerySelector{
		builder: b,
	}

	qs.stringer = &stringer{qs, b.t.Template}
	return qs.Columns(columns...)
}

func (b *Builder) InsertInto(table string) db.QueryInserter {
	qi := &QueryInserter{
		builder: b,
		table:   table,
	}

	qi.stringer = &stringer{qi, b.t.Template}
	return qi
}

func (b *Builder) DeleteFrom(table string) db.QueryDeleter {
	qd := &QueryDeleter{
		builder: b,
		table:   table,
	}

	qd.stringer = &stringer{qd, b.t.Template}
	return qd
}

func (b *Builder) Update(table string) db.QueryUpdater {
	qu := &QueryUpdater{
		builder:      b,
		table:        table,
		columnValues: &sqlgen.ColumnValues{},
	}

	qu.stringer = &stringer{qu, b.t.Template}
	return qu
}

type QueryInserter struct {
	*stringer
	builder   *Builder
	table     string
	values    []*sqlgen.Values
	columns   []sqlgen.Fragment
	arguments []interface{}
	extra     string
}

func (qi *QueryInserter) Extra(s string) db.QueryInserter {
	qi.extra = s
	return qi
}

func (qi *QueryInserter) Exec() (sql.Result, error) {
	return qi.builder.sess.Exec(qi.statement())
}

func (qi *QueryInserter) Query() (*sqlx.Rows, error) {
	return qi.builder.sess.Query(qi.statement(), qi.arguments...)
}

func (qi *QueryInserter) QueryRow() (*sqlx.Row, error) {
	return qi.builder.sess.QueryRow(qi.statement(), qi.arguments...)
}

func (qi *QueryInserter) Iterator() db.Iterator {
	rows, err := qi.builder.sess.Query(qi.statement(), qi.arguments...)
	return &iterator{rows, err}
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

func (qi *QueryInserter) statement() *sqlgen.Statement {
	stmt := &sqlgen.Statement{
		Type:  sqlgen.Insert,
		Table: sqlgen.TableWithName(qi.table),
		Extra: sqlgen.Extra(qi.extra),
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
	where, arguments := qd.builder.t.ToWhereWithArguments(terms)
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
	if len(terms) == 1 {
		ff, vv, _ := Map(terms[0])

		cvs := make([]sqlgen.Fragment, len(ff))

		for i := range ff {
			cvs[i] = &sqlgen.ColumnValue{
				Column:   sqlgen.ColumnWithName(ff[i]),
				Operator: qu.builder.t.AssignmentOperator,
				Value:    sqlPlaceholder,
			}
		}
		qu.columnValues.Append(cvs...)
		qu.arguments = append(qu.arguments, vv...)
	} else if len(terms) > 1 {
		cv, arguments := qu.builder.t.ToColumnValues(terms)
		qu.columnValues.Append(cv.ColumnValues...)
		qu.arguments = append(qu.arguments, arguments...)
	}

	return qu
}

func (qu *QueryUpdater) Where(terms ...interface{}) db.QueryUpdater {
	where, arguments := qu.builder.t.ToWhereWithArguments(terms)
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

type iterator struct {
	cursor *sqlx.Rows // This is the main query cursor. It starts as a nil value.
	err    error
}

type QuerySelector struct {
	*stringer
	mode      SelectMode
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

func (qs *QuerySelector) Columns(columns ...interface{}) db.QuerySelector {
	f, err := columnFragments(qs.builder.t, columns)
	if err != nil {
		qs.err = err
		return qs
	}
	qs.columns = sqlgen.JoinColumns(f...)
	return qs
}

func (qs *QuerySelector) Distinct() db.QuerySelector {
	qs.mode = selectModeDistinct
	return qs
}

func (qs *QuerySelector) Where(terms ...interface{}) db.QuerySelector {
	where, arguments := qs.builder.t.ToWhereWithArguments(terms)
	qs.where = &where
	qs.arguments = append(qs.arguments, arguments...)
	return qs
}

func (qs *QuerySelector) GroupBy(columns ...interface{}) db.QuerySelector {
	var fragments []sqlgen.Fragment
	fragments, qs.err = columnFragments(qs.builder.t, columns)
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

	fragments, err := columnFragments(qs.builder.t, columns)
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

	w, a := qs.builder.t.ToWhereWithArguments(terms)
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

func (qs *QuerySelector) Iterator() db.Iterator {
	rows, err := qs.builder.sess.Query(qs.statement(), qs.arguments...)
	return &iterator{rows, err}
}

type QueryTruncater struct {
	*stringer
	builder *Builder
	table   string
	extra   string
	err     error
}

func (qt *QueryTruncater) Extra(extra string) db.QueryTruncater {
	qt.extra = extra
	return qt
}

func (qt *QueryTruncater) statement() *sqlgen.Statement {

	stmt := &sqlgen.Statement{
		Type:  sqlgen.Truncate,
		Table: sqlgen.TableWithName(qt.table),
		Extra: sqlgen.Extra(qt.extra),
	}

	return stmt
}

func columnFragments(template *sqlutil.TemplateWithUtils, columns []interface{}) ([]sqlgen.Fragment, error) {
	l := len(columns)
	f := make([]sqlgen.Fragment, l)

	for i := 0; i < l; i++ {
		switch v := columns[i].(type) {
		case db.Func:
			var s string
			a := template.ToInterfaceArguments(v.Args)
			if len(a) == 0 {
				s = fmt.Sprintf(`%s()`, v.Name)
			} else {
				ss := make([]string, 0, len(a))
				for j := range a {
					ss = append(ss, fmt.Sprintf(`%v`, a[j]))
				}
				s = fmt.Sprintf(`%s(%s)`, v.Name, strings.Join(ss, `, `))
			}
			f[i] = sqlgen.RawValue(s)
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
	t *sqlgen.Template
}

func (s *stringer) String() string {
	if s != nil && s.i != nil {
		q := s.compileAndReplacePlaceholders(s.i.statement())
		q = reInvisibleChars.ReplaceAllString(q, ` `)
		return strings.TrimSpace(q)
	}
	return ""
}

func (s *stringer) compileAndReplacePlaceholders(stmt *sqlgen.Statement) (query string) {
	buf := stmt.Compile(s.t)

	j := 1
	for i := range buf {
		if buf[i] == '?' {
			query = query + "$" + strconv.Itoa(j)
			j++
		} else {
			query = query + string(buf[i])
		}
	}

	return query
}

func NewBuilder(sess sqlDatabase, t *sqlgen.Template) *Builder {
	return &Builder{
		sess: sess,
		t:    sqlutil.NewTemplateWithUtils(t),
	}
}

func (iter *iterator) Scan(dst ...interface{}) error {
	if iter.err != nil {
		return iter.err
	}
	return iter.cursor.Scan(dst...)
}

func (iter *iterator) One(dst interface{}) error {
	if iter.err != nil {
		return iter.err
	}

	defer iter.Close()

	if !iter.Next(dst) {
		return iter.Err()
	}

	return nil
}

func (iter *iterator) All(dst interface{}) error {
	var err error

	if iter.err != nil {
		return iter.err
	}

	defer iter.Close()

	// Fetching all results within the cursor.
	err = sqlutil.FetchRows(iter.cursor, dst)

	return err
}

func (iter *iterator) Err() (err error) {
	return iter.err
}

func (iter *iterator) Next(dst ...interface{}) bool {
	var err error

	if iter.err != nil {
		return false
	}

	switch len(dst) {
	case 0:
		if ok := iter.cursor.Next(); !ok {
			iter.err = iter.cursor.Err()
			iter.Close()
			return false
		}
		return true
	case 1:
		if err = sqlutil.FetchRow(iter.cursor, dst[0]); err != nil {
			iter.err = err
			iter.Close()
			return false
		}
		return true
	}

	iter.err = db.ErrUnsupported
	return false
}

func (iter *iterator) Close() (err error) {
	if iter.cursor != nil {
		err = iter.cursor.Close()
		iter.cursor = nil
	}
	return err
}

func Map(item interface{}) ([]string, []interface{}, error) {
	var fv fieldValue

	itemV := reflect.ValueOf(item)
	itemT := itemV.Type()

	if itemT.Kind() == reflect.Ptr {
		// Single derefence. Just in case user passed a pointer to struct instead of a struct.
		item = itemV.Elem().Interface()
		itemV = reflect.ValueOf(item)
		itemT = itemV.Type()
	}

	switch itemT.Kind() {

	case reflect.Struct:

		fieldMap := mapper.TypeMap(itemT).Names
		nfields := len(fieldMap)

		fv.values = make([]interface{}, 0, nfields)
		fv.fields = make([]string, 0, nfields)

		for _, fi := range fieldMap {
			// log.Println("=>", fi.Name, fi.Options)

			fld := reflectx.FieldByIndexesReadOnly(itemV, fi.Index)
			if fld.Kind() == reflect.Ptr && fld.IsNil() {
				continue
			}

			var value interface{}
			if _, ok := fi.Options["stringarray"]; ok {
				value = sqlutil.StringArray(fld.Interface().([]string))
			} else if _, ok := fi.Options["int64array"]; ok {
				value = sqlutil.Int64Array(fld.Interface().([]int64))
			} else if _, ok := fi.Options["jsonb"]; ok {
				value = sqlutil.JsonbType{fld.Interface()}
			} else {
				value = fld.Interface()
			}

			if _, ok := fi.Options["omitempty"]; ok {
				if value == fi.Zero.Interface() {
					continue
				}
			}

			fv.fields = append(fv.fields, fi.Name)
			v, err := marshal(value)
			if err != nil {
				return nil, nil, err
			}
			fv.values = append(fv.values, v)
		}

	case reflect.Map:
		nfields := itemV.Len()
		fv.values = make([]interface{}, nfields)
		fv.fields = make([]string, nfields)
		mkeys := itemV.MapKeys()

		for i, keyV := range mkeys {
			valv := itemV.MapIndex(keyV)
			fv.fields[i] = fmt.Sprintf("%v", keyV.Interface())

			v, err := marshal(valv.Interface())
			if err != nil {
				return nil, nil, err
			}

			fv.values[i] = v
		}
	default:
		return nil, nil, db.ErrExpectingMapOrStruct
	}

	sort.Sort(&fv)

	return fv.fields, fv.values, nil
}

func marshal(v interface{}) (interface{}, error) {
	if m, isMarshaler := v.(db.Marshaler); isMarshaler {
		var err error
		if v, err = m.MarshalDB(); err != nil {
			return nil, err
		}
	}
	return v, nil
}
