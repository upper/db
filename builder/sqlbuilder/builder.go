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

	"github.com/jmoiron/sqlx/reflectx"
	"upper.io/db.v2/builder"
	"upper.io/db.v2/builder/internal/util"
	"upper.io/db.v2/builder/sqlgen"
)

type hasStatement interface {
	statement() *sqlgen.Statement
}

type stringer struct {
	i hasStatement
	t *sqlgen.Template
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
	sqlPlaceholder = sqlgen.RawValue(`?`)
)

type sqlgenDB interface {
	Query(stmt *sqlgen.Statement, args ...interface{}) (*sql.Rows, error)
	QueryRow(stmt *sqlgen.Statement, args ...interface{}) (*sql.Row, error)
	Exec(stmt *sqlgen.Statement, args ...interface{}) (sql.Result, error)
}

type sqlBuilder struct {
	sess sqlgenDB
	t    *util.TemplateWithUtils
}

// New returns a query builder that is bound to the given database session.
func New(sess interface{}, t func() *sqlgen.Template) (builder.Builder, error) {
	tt := t()
	switch v := sess.(type) {
	case *sql.DB:
		sess = newSqlgenProxy(v, tt)
	case sqlgenDB:
		// OK!
	default:
		// There should be no way this error is ignored.
		panic(fmt.Sprintf("Unkown source type: %T", sess))
	}
	return &sqlBuilder{
		sess: sess.(sqlgenDB),
		t:    util.NewTemplateWithUtils(tt),
	}, nil
}

func NewBuilderWithTemplate(t *sqlgen.Template) builder.Builder {
	return &sqlBuilder{
		t: util.NewTemplateWithUtils(t),
	}
}

// func (b *Builder) Iterator(query interface{}, args ...interface{}) builder.Iterator {
// 	rows, err := b.Query(query, args...)
// 	return &iterator{rows, err}
// }

// NewIterator creates an iterator using the given *sql.Rows.

func NewIterator(rows *sql.Rows) builder.Iterator {
	return &iterator{rows, nil}
}

func (b *sqlBuilder) Iterator(query interface{}, args ...interface{}) builder.Iterator {
	rows, err := b.Query(query, args...)
	return &iterator{rows, err}
}

func (b *sqlBuilder) Exec(query interface{}, args ...interface{}) (sql.Result, error) {
	switch q := query.(type) {
	case *sqlgen.Statement:
		return b.sess.Exec(q, args...)
	case string:
		return b.sess.Exec(sqlgen.RawSQL(q), args...)
	default:
		return nil, fmt.Errorf("Unsupported query type %T.", query)
	}
}

func (b *sqlBuilder) Query(query interface{}, args ...interface{}) (*sql.Rows, error) {
	switch q := query.(type) {
	case *sqlgen.Statement:
		return b.sess.Query(q, args...)
	case string:
		return b.sess.Query(sqlgen.RawSQL(q), args...)
	default:
		return nil, fmt.Errorf("Unsupported query type %T.", query)
	}
}

func (b *sqlBuilder) QueryRow(query interface{}, args ...interface{}) (*sql.Row, error) {
	switch q := query.(type) {
	case *sqlgen.Statement:
		return b.sess.QueryRow(q, args...)
	case string:
		return b.sess.QueryRow(sqlgen.RawSQL(q), args...)
	default:
		return nil, fmt.Errorf("Unsupported query type %T.", query)
	}
}

func (b *sqlBuilder) SelectAllFrom(table string) builder.Selector {
	qs := &selector{
		builder: b,
		table:   table,
	}

	qs.stringer = &stringer{qs, b.t.Template}
	return qs
}

func (b *sqlBuilder) Select(columns ...interface{}) builder.Selector {
	qs := &selector{
		builder: b,
	}

	qs.stringer = &stringer{qs, b.t.Template}
	return qs.Columns(columns...)
}

func (b *sqlBuilder) InsertInto(table string) builder.Inserter {
	qi := &inserter{
		builder: b,
		table:   table,
	}

	qi.stringer = &stringer{qi, b.t.Template}
	return qi
}

func (b *sqlBuilder) DeleteFrom(table string) builder.Deleter {
	qd := &deleter{
		builder: b,
		table:   table,
	}

	qd.stringer = &stringer{qd, b.t.Template}
	return qd
}

func (b *sqlBuilder) Update(table string) builder.Updater {
	qu := &updater{
		builder:      b,
		table:        table,
		columnValues: &sqlgen.ColumnValues{},
	}

	qu.stringer = &stringer{qu, b.t.Template}
	return qu
}

// Map receives a pointer to map or sturct and maps it to columns and values.
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

		fieldMap := util.Mapper.TypeMap(itemT).Names
		nfields := len(fieldMap)

		fv.values = make([]interface{}, 0, nfields)
		fv.fields = make([]string, 0, nfields)

		for _, fi := range fieldMap {
			fld := reflectx.FieldByIndexesReadOnly(itemV, fi.Index)
			if fld.Kind() == reflect.Ptr && fld.IsNil() {
				continue
			}

			var value interface{}
			if _, ok := fi.Options["stringarray"]; ok {
				value = util.StringArray(fld.Interface().([]string))
			} else if _, ok := fi.Options["int64array"]; ok {
				value = util.Int64Array(fld.Interface().([]int64))
			} else if _, ok := fi.Options["jsonb"]; ok {
				value = util.JsonbType{fld.Interface()}
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
		return nil, nil, ErrExpectingPointerToEitherMapOrStruct
	}

	sort.Sort(&fv)

	return fv.fields, fv.values, nil
}

func columnFragments(template *util.TemplateWithUtils, columns []interface{}) ([]sqlgen.Fragment, error) {
	l := len(columns)
	f := make([]sqlgen.Fragment, l)

	for i := 0; i < l; i++ {
		switch v := columns[i].(type) {
		case builder.Function:
			var s string
			a := template.ToInterfaceArguments(v.Arguments())
			if len(a) == 0 {
				s = fmt.Sprintf(`%s()`, v.Name())
			} else {
				ss := make([]string, 0, len(a))
				for j := range a {
					ss = append(ss, fmt.Sprintf(`%v`, a[j]))
				}
				s = fmt.Sprintf(`%s(%s)`, v.Name(), strings.Join(ss, `, `))
			}
			f[i] = sqlgen.RawValue(s)
		case builder.RawValue:
			f[i] = sqlgen.RawValue(v.String())
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
	err = util.FetchRows(iter.cursor, dst)

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
		if err = util.FetchRow(iter.cursor, dst[0]); err != nil {
			iter.err = err
			iter.Close()
			return false
		}
		return true
	}

	iter.err = errors.New("Next does not currently supports more than one parameters")
	return false
}

func (iter *iterator) Close() (err error) {
	if iter.cursor != nil {
		err = iter.cursor.Close()
		iter.cursor = nil
	}
	return err
}

func marshal(v interface{}) (interface{}, error) {
	if m, isMarshaler := v.(builder.Marshaler); isMarshaler {
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

type sqlgenProxy struct {
	db *sql.DB
	t  *sqlgen.Template
}

func newSqlgenProxy(db *sql.DB, t *sqlgen.Template) *sqlgenProxy {
	return &sqlgenProxy{db: db, t: t}
}

func (p *sqlgenProxy) Exec(stmt *sqlgen.Statement, args ...interface{}) (sql.Result, error) {
	s := stmt.Compile(p.t)
	return p.db.Exec(s, args...)
}

func (p *sqlgenProxy) Query(stmt *sqlgen.Statement, args ...interface{}) (*sql.Rows, error) {
	s := stmt.Compile(p.t)
	return p.db.Query(s, args...)
}

func (p *sqlgenProxy) QueryRow(stmt *sqlgen.Statement, args ...interface{}) (*sql.Row, error) {
	s := stmt.Compile(p.t)
	return p.db.QueryRow(s, args...), nil
}

var (
	_ = builder.Builder(&sqlBuilder{})
	_ = sqlgenDB(&sqlgenProxy{})
)
