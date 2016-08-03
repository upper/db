// Copyright (c) 2012-present The upper.io/db authors. All rights reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining
// a copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to
// permit persons to whom the Software is furnished to do so, subject to
// the following conditions:
//
// The above copyright notice and this permission notice shall be
// included in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
// LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
// OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
// WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

package sqladapter

import (
	"sync"

	"upper.io/db.v2"
	"upper.io/db.v2/lib/sqlbuilder"
)

// Result represents a delimited set of items bound by a condition.
type Result struct {
	b       sqlbuilder.Builder
	table   string
	iter    sqlbuilder.Iterator
	limit   int
	offset  int
	fields  []interface{}
	columns []interface{}
	orderBy []interface{}
	groupBy []interface{}
	conds   []interface{}
	err     error
	errMu   sync.RWMutex
	iterMu  sync.Mutex
}

func filter(conds []interface{}) []interface{} {
	return conds
}

// NewResult creates and Results a new Result set on the given table, this set
// is limited by the given exql.Where conditions.
func NewResult(b sqlbuilder.Builder, table string, conds []interface{}) *Result {
	return &Result{
		b:     b,
		table: table,
		conds: conds,
	}
}

func (r *Result) setErr(err error) error {
	if err == nil {
		return nil
	}

	r.errMu.Lock()
	defer r.errMu.Unlock()

	r.err = err
	return err
}

// Err returns the last error that has happened with the result set,
// nil otherwise
func (r *Result) Err() error {
	r.errMu.RLock()
	defer r.errMu.RUnlock()
	return r.err
}

// Where sets conditions for the result set.
func (r *Result) Where(conds ...interface{}) db.Result {
	r.conds = conds
	return r
}

// Limit determines the maximum limit of Results to be returned.
func (r *Result) Limit(n int) db.Result {
	r.limit = n
	return r
}

// Offset determines how many documents will be skipped before starting to grab
// Results.
func (r *Result) Offset(n int) db.Result {
	r.offset = n
	return r
}

// Group is used to group Results that have the same value in the same column
// or columns.
func (r *Result) Group(fields ...interface{}) db.Result {
	r.groupBy = fields
	return r
}

// OrderBy determines sorting of Results according to the provided names. Fields
// may be prefixed by - (minus) which means descending order, ascending order
// would be used otherwise.
func (r *Result) OrderBy(fields ...interface{}) db.Result {
	r.orderBy = fields
	return r
}

// Select determines which fields to return.
func (r *Result) Select(fields ...interface{}) db.Result {
	r.fields = fields
	return r
}

// String satisfies fmt.Stringer
func (r *Result) String() string {
	return r.buildSelect().String()
}

// All dumps all Results into a pointer to an slice of structs or maps.
func (r *Result) All(dst interface{}) error {
	err := r.buildSelect().Iterator().All(dst)
	return r.setErr(err)
}

// One fetches only one Result from the set.
func (r *Result) One(dst interface{}) error {
	err := r.buildSelect().Iterator().One(dst)
	return r.setErr(err)
}

// Next fetches the next Result from the set.
func (r *Result) Next(dst interface{}) bool {
	r.iterMu.Lock()
	defer r.iterMu.Unlock()

	if r.iter == nil {
		r.iter = r.buildSelect().Iterator()
	}
	if r.iter.Next(dst) {
		return true
	}
	if err := r.iter.Err(); err != db.ErrNoMoreRows {
		r.setErr(err)
	}
	return false
}

// Delete deletes all matching items from the collection.
func (r *Result) Delete() error {
	q := r.b.DeleteFrom(r.table).
		Where(filter(r.conds)...).
		Limit(r.limit)

	_, err := q.Exec()
	return r.setErr(err)
}

// Close closes the Result set.
func (r *Result) Close() error {
	if r.iter != nil {
		return r.setErr(r.iter.Close())
	}
	return nil
}

// Update updates matching items from the collection with values of the given
// map or struct.
func (r *Result) Update(values interface{}) error {
	q := r.b.Update(r.table).
		Set(values).
		Where(filter(r.conds)...).
		Limit(r.limit)

	_, err := q.Exec()
	return r.setErr(err)
}

// Count counts the elements on the set.
func (r *Result) Count() (uint64, error) {
	counter := struct {
		Count uint64 `db:"_t"`
	}{}

	q := r.b.Select(db.Raw("count(1) AS _t")).
		From(r.table).
		Where(filter(r.conds)...).
		GroupBy(r.groupBy...).
		Limit(1)

	if err := q.Iterator().One(&counter); err != nil {
		if err == db.ErrNoMoreRows {
			return 0, nil
		}
		return 0, r.setErr(err)
	}

	return counter.Count, nil
}

func (r *Result) buildSelect() sqlbuilder.Selector {
	q := r.b.Select(r.fields...)

	q.From(r.table)
	q.Where(filter(r.conds)...)
	q.Limit(r.limit)
	q.Offset(r.offset)

	q.GroupBy(r.groupBy...)
	q.OrderBy(r.orderBy...)

	return q
}
