package sqlbuilder

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"upper.io/db.v2"
	"upper.io/db.v2/internal/sqladapter/exql"
	"upper.io/db.v2/lib/reflectx"
)

// MapOptions represents options for the mapper.
type MapOptions struct {
	IncludeZeroed bool
	IncludeNil    bool
}

var defaultMapOptions = MapOptions{
	IncludeZeroed: false,
	IncludeNil:    false,
}

type hasIsZero interface {
	IsZero() bool
}

type hasArguments interface {
	Arguments() []interface{}
}

type hasStatement interface {
	statement() *exql.Statement
}

type stringer struct {
	i hasStatement
	t *exql.Template
}

type iterator struct {
	cursor *sql.Rows // This is the main query cursor. It starts as a nil value.
	err    error
}

type fieldValue struct {
	fields []string
	values []interface{}
}

var (
	reInvisibleChars       = regexp.MustCompile(`[\s\r\n\t]+`)
	reColumnCompareExclude = regexp.MustCompile(`[^a-zA-Z0-9]`)
)

var (
	sqlPlaceholder = exql.RawValue(`?`)
)

type exprDB interface {
	StatementQuery(stmt *exql.Statement, args ...interface{}) (*sql.Rows, error)
	StatementQueryRow(stmt *exql.Statement, args ...interface{}) (*sql.Row, error)
	StatementExec(stmt *exql.Statement, args ...interface{}) (sql.Result, error)
}

type sqlBuilder struct {
	sess exprDB
	t    *templateWithUtils
}

// WithSession returns a query builder that is bound to the given database session.
func WithSession(sess interface{}, t *exql.Template) (Builder, error) {
	switch v := sess.(type) {
	case *sql.DB:
		sess = newSqlgenProxy(v, t)
	case exprDB:
		// OK!
	default:
		// There should be no way this error is ignored.
		panic(fmt.Sprintf("Unkown source type: %T", sess))
	}
	return &sqlBuilder{
		sess: sess.(exprDB),
		t:    newTemplateWithUtils(t),
	}, nil
}

// WithTemplate returns a builder that is based on the given template.
func WithTemplate(t *exql.Template) Builder {
	return &sqlBuilder{
		t: newTemplateWithUtils(t),
	}
}

// NewIterator creates an iterator using the given *sql.Rows.
func NewIterator(rows *sql.Rows) Iterator {
	return &iterator{rows, nil}
}

func (b *sqlBuilder) Iterator(query interface{}, args ...interface{}) Iterator {
	rows, err := b.Query(query, args...)
	return &iterator{rows, err}
}

func (b *sqlBuilder) Exec(query interface{}, args ...interface{}) (sql.Result, error) {
	switch q := query.(type) {
	case *exql.Statement:
		return b.sess.StatementExec(q, args...)
	case string:
		return b.sess.StatementExec(exql.RawSQL(q), args...)
	default:
		return nil, fmt.Errorf("Unsupported query type %T.", query)
	}
}

func (b *sqlBuilder) Query(query interface{}, args ...interface{}) (*sql.Rows, error) {
	switch q := query.(type) {
	case *exql.Statement:
		return b.sess.StatementQuery(q, args...)
	case string:
		return b.sess.StatementQuery(exql.RawSQL(q), args...)
	default:
		return nil, fmt.Errorf("Unsupported query type %T.", query)
	}
}

func (b *sqlBuilder) QueryRow(query interface{}, args ...interface{}) (*sql.Row, error) {
	switch q := query.(type) {
	case *exql.Statement:
		return b.sess.StatementQueryRow(q, args...)
	case string:
		return b.sess.StatementQueryRow(exql.RawSQL(q), args...)
	default:
		return nil, fmt.Errorf("Unsupported query type %T.", query)
	}
}

func (b *sqlBuilder) SelectFrom(table ...interface{}) Selector {
	qs := &selector{
		builder: b,
	}
	qs.stringer = &stringer{qs, b.t.Template}
	return qs.From(table...)
}

func (b *sqlBuilder) Select(columns ...interface{}) Selector {
	qs := &selector{
		builder: b,
	}

	qs.stringer = &stringer{qs, b.t.Template}
	return qs.Columns(columns...)
}

func (b *sqlBuilder) InsertInto(table string) Inserter {
	qi := &inserter{
		builder: b,
		table:   table,
	}

	qi.stringer = &stringer{qi, b.t.Template}
	return qi
}

func (b *sqlBuilder) DeleteFrom(table string) Deleter {
	qd := &deleter{
		builder: b,
		table:   table,
	}

	qd.stringer = &stringer{qd, b.t.Template}
	return qd
}

func (b *sqlBuilder) Update(table string) Updater {
	qu := &updater{
		builder:      b,
		table:        table,
		columnValues: &exql.ColumnValues{},
	}

	qu.stringer = &stringer{qu, b.t.Template}
	return qu
}

// Map receives a pointer to map or struct and maps it to columns and values.
func Map(item interface{}, options *MapOptions) ([]string, []interface{}, error) {
	var fv fieldValue

	if options == nil {
		options = &defaultMapOptions
	}

	itemV := reflect.ValueOf(item)
	itemT := itemV.Type()

	if itemT.Kind() == reflect.Ptr {
		// Single dereference. Just in case the user passes a pointer to struct
		// instead of a struct.
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

			// Field options
			_, tagOmitEmpty := fi.Options["omitempty"]
			_, tagStringArray := fi.Options["stringarray"]
			_, tagInt64Array := fi.Options["int64array"]
			_, tagJSONB := fi.Options["jsonb"]

			fld := reflectx.FieldByIndexesReadOnly(itemV, fi.Index)
			if fld.Kind() == reflect.Ptr && fld.IsNil() {
				if options.IncludeNil || !tagOmitEmpty {
					fv.fields = append(fv.fields, fi.Name)
					fv.values = append(fv.values, fld.Interface())
				}
				continue
			}

			var value interface{}
			switch {
			case tagStringArray:
				v, ok := fld.Interface().([]string)
				if !ok {
					return nil, nil, fmt.Errorf(`Expecting field %q to be []string (using "stringarray" tag)`, fi.Name)
				}
				value = stringArray(v)
			case tagInt64Array:
				v, ok := fld.Interface().([]int64)
				if !ok {
					return nil, nil, fmt.Errorf(`Expecting field %q to be []int64 (using "int64array" tag)`, fi.Name)
				}
				value = int64Array(v)
			case tagJSONB:
				value = jsonbType{fld.Interface()}
			default:
				value = fld.Interface()
			}

			if !options.IncludeZeroed {
				if tagOmitEmpty {
					if t, ok := fld.Interface().(hasIsZero); ok {
						if t.IsZero() {
							continue
						}
					} else if value == fi.Zero.Interface() {
						continue
					}
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
		return nil, nil, ErrExpectingPointerToEitherMapOrStruct
	}

	sort.Sort(&fv)

	return fv.fields, fv.values, nil
}

func extractArguments(fragments []interface{}) []interface{} {
	args := []interface{}{}
	l := len(fragments)
	for i := 0; i < l; i++ {
		switch v := fragments[i].(type) {
		case hasArguments: // TODO: use this on other places where we want to extract arguments.
			args = append(args, v.Arguments()...)
		}
	}
	return args
}

func columnFragments(template *templateWithUtils, columns []interface{}) ([]exql.Fragment, []interface{}, error) {
	l := len(columns)
	f := make([]exql.Fragment, l)
	args := []interface{}{}

	for i := 0; i < l; i++ {
		switch v := columns[i].(type) {
		case *selector:
			expanded, rawArgs := expandPlaceholders(v.statement().Compile(v.stringer.t), v.Arguments()...)
			f[i] = exql.RawValue(expanded)
			args = append(args, rawArgs...)
		case db.Function:
			fnName, fnArgs := v.Name(), v.Arguments()
			if len(fnArgs) == 0 {
				fnName = fnName + "()"
			} else {
				fnName = fnName + "(?" + strings.Repeat("?, ", len(fnArgs)-1) + ")"
			}
			expanded, fnArgs := expandPlaceholders(fnName, fnArgs...)
			f[i] = exql.RawValue(expanded)
			args = append(args, fnArgs...)
		case db.RawValue:
			expanded, rawArgs := expandPlaceholders(v.Raw(), v.Arguments()...)
			f[i] = exql.RawValue(expanded)
			args = append(args, rawArgs...)
		case exql.Fragment:
			f[i] = v
		case string:
			f[i] = exql.ColumnWithName(v)
		case interface{}:
			f[i] = exql.ColumnWithName(fmt.Sprintf("%v", v))
		default:
			return nil, nil, fmt.Errorf("Unexpected argument type %T for Select() argument.", v)
		}
	}
	return f, args, nil
}

func (s *stringer) String() string {
	if s != nil && s.i != nil {
		q := s.compileAndReplacePlaceholders(s.i.statement())
		q = reInvisibleChars.ReplaceAllString(q, ` `)
		return strings.TrimSpace(q)
	}
	return ""
}

func (s *stringer) compileAndReplacePlaceholders(stmt *exql.Statement) (query string) {
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

func (iter *iterator) NextScan(dst ...interface{}) error {
	if ok := iter.Next(); ok {
		return iter.Scan(dst...)
	}
	if err := iter.Err(); err != nil {
		return err
	}
	return db.ErrNoMoreRows
}

func (iter *iterator) ScanOne(dst ...interface{}) error {
	defer iter.Close()
	return iter.NextScan(dst...)
}

func (iter *iterator) Scan(dst ...interface{}) error {
	if err := iter.Err(); err != nil {
		return err
	}
	return iter.cursor.Scan(dst...)
}

func (iter *iterator) setErr(err error) error {
	iter.err = err
	return iter.err
}

func (iter *iterator) One(dst interface{}) error {
	if err := iter.Err(); err != nil {
		return err
	}
	defer iter.Close()
	return iter.setErr(iter.next(dst))
}

func (iter *iterator) All(dst interface{}) error {
	if err := iter.Err(); err != nil {
		return err
	}
	defer iter.Close()

	// Fetching all results within the cursor.
	if err := fetchRows(iter.cursor, dst); err != nil {
		return iter.setErr(err)
	}

	return nil
}

func (iter *iterator) Err() (err error) {
	return iter.err
}

func (iter *iterator) Next(dst ...interface{}) bool {
	if err := iter.Err(); err != nil {
		return false
	}

	if err := iter.next(dst...); err != nil {
		// ignore db.ErrNoMoreRows, just break.
		if err != db.ErrNoMoreRows {
			iter.setErr(err)
		}
		return false
	}

	return true
}

func (iter *iterator) next(dst ...interface{}) error {
	if iter.cursor == nil {
		return iter.setErr(db.ErrNoMoreRows)
	}

	switch len(dst) {
	case 0:
		if ok := iter.cursor.Next(); !ok {
			defer iter.Close()
			err := iter.cursor.Err()
			if err == nil {
				err = db.ErrNoMoreRows
			}
			return err
		}
		return nil
	case 1:
		if err := fetchRow(iter.cursor, dst[0]); err != nil {
			defer iter.Close()
			return err
		}
		return nil
	}

	return errors.New("Next does not currently supports more than one parameters")
}

func (iter *iterator) Close() (err error) {
	if iter.cursor != nil {
		err = iter.cursor.Close()
		iter.cursor = nil
	}
	return err
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

type exprProxy struct {
	db *sql.DB
	t  *exql.Template
}

func newSqlgenProxy(db *sql.DB, t *exql.Template) *exprProxy {
	return &exprProxy{db: db, t: t}
}

func (p *exprProxy) StatementExec(stmt *exql.Statement, args ...interface{}) (sql.Result, error) {
	s := stmt.Compile(p.t)
	return p.db.Exec(s, args...)
}

func (p *exprProxy) StatementQuery(stmt *exql.Statement, args ...interface{}) (*sql.Rows, error) {
	s := stmt.Compile(p.t)
	return p.db.Query(s, args...)
}

func (p *exprProxy) StatementQueryRow(stmt *exql.Statement, args ...interface{}) (*sql.Row, error) {
	s := stmt.Compile(p.t)
	return p.db.QueryRow(s, args...), nil
}

var (
	_ = Builder(&sqlBuilder{})
	_ = exprDB(&exprProxy{})
)

func joinArguments(args ...[]interface{}) []interface{} {
	total := 0
	for i := range args {
		total += len(args[i])
	}
	if total == 0 {
		return nil
	}

	flatten := make([]interface{}, 0, total)
	for i := range args {
		flatten = append(flatten, args[i]...)
	}
	return flatten
}
