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

package mongo

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"

	db "github.com/upper/db/v4"
	"github.com/upper/db/v4/internal/adapter"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// Collection represents a mongodb collection.
type Collection struct {
	parent     *Source
	collection *mongo.Collection
}

var (
	// idCache should be a struct if we're going to cache more than just
	// _id field here
	idCache      = make(map[reflect.Type]string)
	idCacheMutex sync.RWMutex
)

// Find creates a result set with the given conditions.
func (col *Collection) Find(terms ...interface{}) db.Result {
	fields := []string{"*"}

	conditions := col.compileQuery(terms...)

	res := &result{}
	res = res.frame(func(r *resultQuery) error {
		r.c = col
		r.conditions = conditions
		r.fields = fields
		return nil
	})

	return res
}

var comparisonOperators = map[adapter.ComparisonOperator]string{
	adapter.ComparisonOperatorEqual:    "$eq",
	adapter.ComparisonOperatorNotEqual: "$ne",

	adapter.ComparisonOperatorLessThan:    "$lt",
	adapter.ComparisonOperatorGreaterThan: "$gt",

	adapter.ComparisonOperatorLessThanOrEqualTo:    "$lte",
	adapter.ComparisonOperatorGreaterThanOrEqualTo: "$gte",

	adapter.ComparisonOperatorIn:    "$in",
	adapter.ComparisonOperatorNotIn: "$nin",
}

func compare(field string, cmp *adapter.Comparison) (string, interface{}) {
	op := cmp.Operator()
	value := cmp.Value()

	switch op {
	case adapter.ComparisonOperatorEqual:
		return field, value
	case adapter.ComparisonOperatorBetween:
		values := value.([]interface{})
		return field, bson.M{
			"$gte": values[0],
			"$lte": values[1],
		}
	case adapter.ComparisonOperatorNotBetween:
		values := value.([]interface{})
		return "$or", []bson.M{
			{field: bson.M{"$gt": values[1]}},
			{field: bson.M{"$lt": values[0]}},
		}
	case adapter.ComparisonOperatorIs:
		if value == nil {
			return field, bson.M{"$exists": false}
		}
		return field, bson.M{"$eq": value}
	case adapter.ComparisonOperatorIsNot:
		if value == nil {
			return field, bson.M{"$exists": true}
		}
		return field, bson.M{"$ne": value}
	case adapter.ComparisonOperatorRegExp, adapter.ComparisonOperatorLike:
		return field, bson.M{
			"$regex": value.(string),
		}
	case adapter.ComparisonOperatorNotRegExp, adapter.ComparisonOperatorNotLike:
		return field, bson.M{
			"$not": bson.M{
				"$regex": value.(string),
			},
		}
	}

	if cmpOp, ok := comparisonOperators[op]; ok {
		return field, bson.M{
			cmpOp: value,
		}
	}

	panic(fmt.Sprintf("Unsupported operator %v", op))
}

// compileStatement transforms conditions into something *mgo.Session can
// understand.
func compileStatement(cond db.Cond) bson.M {
	conds := bson.M{}

	// Walking over conditions
	for fieldI, value := range cond {
		field := strings.TrimSpace(fmt.Sprintf("%v", fieldI))

		if cmp, ok := value.(*db.Comparison); ok {
			k, v := compare(field, cmp.Comparison)
			conds[k] = v
			continue
		}

		var op string
		chunks := strings.SplitN(field, ` `, 2)

		if len(chunks) > 1 {
			switch chunks[1] {
			case `IN`:
				op = `$in`
			case `NOT IN`:
				op = `$nin`
			case `>`:
				op = `$gt`
			case `<`:
				op = `$lt`
			case `<=`:
				op = `$lte`
			case `>=`:
				op = `$gte`
			default:
				op = chunks[1]
			}
		}
		field = chunks[0]

		if op == "" {
			conds[field] = value
		} else {
			conds[field] = bson.M{op: value}
		}
	}

	return conds
}

// compileConditions compiles terms into something *mgo.Session can
// understand.
func (col *Collection) compileConditions(term interface{}) interface{} {

	switch t := term.(type) {
	case []interface{}:
		values := []interface{}{}
		for i := range t {
			value := col.compileConditions(t[i])
			if value != nil {
				values = append(values, value)
			}
		}
		if len(values) > 0 {
			return values
		}
	case db.Cond:
		return compileStatement(t)
	case adapter.LogicalExpr:
		values := []interface{}{}

		for _, s := range t.Expressions() {
			values = append(values, col.compileConditions(s))
		}

		var op string
		switch t.Operator() {
		case adapter.LogicalOperatorOr:
			op = `$or`
		default:
			op = `$and`
		}

		return bson.M{op: values}
	}
	return nil
}

// compileQuery compiles terms into something that *mgo.Session can
// understand.
func (col *Collection) compileQuery(terms ...interface{}) interface{} {
	compiled := col.compileConditions(terms)
	if compiled == nil {
		return nil
	}

	conditions := compiled.([]interface{})
	if len(conditions) == 1 {
		return conditions[0]
	}
	// this should be correct.
	// query = map[string]interface{}{"$and": conditions}

	// attempt to workaround https://jira.mongodb.org/browse/SERVER-4572
	mapped := map[string]interface{}{}
	for _, v := range conditions {
		for kk := range v.(map[string]interface{}) {
			mapped[kk] = v.(map[string]interface{})[kk]
		}
	}

	return mapped
}

// Name returns the name of the table or tables that form the collection.
func (col *Collection) Name() string {
	return col.collection.Name()
}

// Truncate deletes all rows from the table.
func (col *Collection) Truncate() error {
	err := col.collection.Drop(context.Background())
	if err != nil {
		return err
	}

	return nil
}

func (col *Collection) Session() db.Session {
	return col.parent
}

func (col *Collection) Count() (uint64, error) {
	return col.Find().Count()
}

func (col *Collection) InsertReturning(item interface{}) error {
	return db.ErrUnsupported
}

func (col *Collection) UpdateReturning(item interface{}) error {
	return db.ErrUnsupported
}

// Insert inserts a record (map or struct) into the collection.
func (col *Collection) Insert(item interface{}) (db.InsertResult, error) {
	ctx := context.Background()

	res, err := col.collection.InsertOne(ctx, item)
	if err != nil {
		return nil, err
	}

	return db.NewInsertResult(res.InsertedID), nil
}

// Exists returns true if the collection exists.
func (col *Collection) Exists() (bool, error) {
	ctx := context.Background()
	mcol := col.parent.database.Collection("system.namespaces")

	mcur, err := mcol.Find(ctx, bson.M{
		"name": fmt.Sprintf("%s.%s", col.parent.database.Name, col.collection.Name),
	})
	if err != nil {
		return false, err
	}
	defer mcur.Close(ctx)

	hasNext := mcur.Next(ctx)
	if err := mcur.Err(); err != nil {
		return false, err
	}

	return hasNext, nil
}
