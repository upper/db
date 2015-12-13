// Copyright (c) 2012-2015 The upper.io/db authors. All rights reserved.
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

package result

import (
	"upper.io/builder"
	"upper.io/db.v2"
)

type Result struct {
	b       builder.Builder
	table   string
	iter    builder.Iterator
	limit   int
	offset  int
	fields  []interface{}
	columns []interface{}
	orderBy []interface{}
	groupBy []interface{}
	conds   []interface{}
}

func filter(conds []interface{}) []interface{} {
	for i := range conds {
		switch v := conds[i].(type) {
		case db.Constrainer:
			conds[i] = v.Constraints()
		}
	}
	return conds
}

// NewResult creates and results a new result set on the given table, this set
// is limited by the given sqlgen.Where conditions.
func NewResult(b builder.Builder, table string, conds []interface{}) *Result {
	return &Result{
		b:     b,
		table: table,
		conds: conds,
	}
}

// Sets conditions for reducing the working set.
func (r *Result) Where(conds ...interface{}) db.Result {
	r.conds = conds
	return r
}

// Determines the maximum limit of results to be returned.
func (r *Result) Limit(n uint) db.Result {
	r.limit = int(n)
	return r
}

// Determines how many documents will be skipped before starting to grab
// results.
func (r *Result) Skip(n uint) db.Result {
	r.offset = int(n)
	return r
}

// Used to group results that have the same value in the same column or
// columns.
func (r *Result) Group(fields ...interface{}) db.Result {
	r.groupBy = fields
	return r
}

// Determines sorting of results according to the provided names. Fields may be
// prefixed by - (minus) which means descending order, ascending order would be
// used otherwise.
func (r *Result) Sort(fields ...interface{}) db.Result {
	r.orderBy = fields
	return r
}

// Retrieves only the given fields.
func (r *Result) Select(fields ...interface{}) db.Result {
	r.fields = fields
	return r
}

// Dumps all results into a pointer to an slice of structs or maps.
func (r *Result) All(dst interface{}) error {
	return r.buildSelect().Iterator().All(dst)
}

// Fetches only one result from the resultset.
func (r *Result) One(dst interface{}) error {
	return r.buildSelect().Iterator().One(dst)
}

// Fetches the next result from the resultset.
func (r *Result) Next(dst interface{}) (err error) {
	if r.iter == nil {
		r.iter = r.buildSelect().Iterator()
	}
	if !r.iter.Next(dst) {
		return r.iter.Err()
	}
	return nil
}

// Removes the matching items from the collection.
func (r *Result) Remove() error {
	q := r.b.DeleteFrom(r.table).
		Where(filter(r.conds)...).
		Limit(r.limit)

	_, err := q.Exec()
	return err
}

// Closes the result set.
func (r *Result) Close() error {
	if r.iter != nil {
		return r.iter.Close()
	}
	return nil
}

// Updates matching items from the collection with values of the given map or
// struct.
func (r *Result) Update(values interface{}) error {
	q := r.b.Update(r.table).
		Set(values).
		Where(filter(r.conds)...).
		Limit(r.limit)

	_, err := q.Exec()
	return err
}

// Counts the elements within the main conditions of the set.
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
		return 0, err
	}

	return counter.Count, nil
}

func (r *Result) buildSelect() builder.Selector {
	q := r.b.Select(r.fields...)

	q.From(r.table)
	q.Where(filter(r.conds)...)
	q.Limit(r.limit)
	q.Offset(r.offset)

	q.GroupBy(r.groupBy...)
	q.OrderBy(r.orderBy...)

	return q
}
