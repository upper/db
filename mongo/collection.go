/*
  Copyright (c) 2012-2013 José Carlos Nieto, https://menteslibres.net/xiam

  Permission is hereby granted, free of charge, to any person obtaining
  a copy of this software and associated documentation files (the
  "Software"), to deal in the Software without restriction, including
  without limitation the rights to use, copy, modify, merge, publish,
  distribute, sublicense, and/or sell copies of the Software, and to
  permit persons to whom the Software is furnished to do so, subject to
  the following conditions:

  The above copyright notice and this permission notice shall be
  included in all copies or substantial portions of the Software.

  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
  NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
  LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
  OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
  WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

package mongo

import (
	"fmt"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"strings"
	"upper.io/db"
	"upper.io/db/util"
)

// Mongodb Collection
type Collection struct {
	name       string
	parent     *Source
	collection *mgo.Collection
	util.C
}

type chunks struct {
	Fields     []string
	Limit      int
	Offset     int
	Sort       []string
	Conditions interface{}
}

func (self *Collection) Find(terms ...interface{}) db.Result {

	queryChunks := &chunks{}

	// No specific fields given.
	if len(queryChunks.Fields) == 0 {
		queryChunks.Fields = []string{"*"}
	}

	queryChunks.Conditions = self.compileQuery(terms...)

	// Actually executing query.
	result := &Result{
		self,
		queryChunks,
		nil,
	}

	return result
}

// Transforms conditions into something *mgo.Session can understand.
func compileStatement(where db.Cond) bson.M {
	conds := bson.M{}

	for key, val := range where {
		key = strings.Trim(key, ` `)
		chunks := strings.SplitN(key, ` `, 2)

		if len(chunks) > 1 {
			op := ""
			switch chunks[1] {
			case `>`:
				op = `$gt`
			case `<`:
				op = `$gt`
			case `<=`:
				op = `$lte`
			case `>=`:
				op = `$gte`
			default:
				op = chunks[1]
			}
			//conds[chunks[0]] = bson.M{op: toInternal(val)}
			conds[chunks[0]] = bson.M{op: val}
		} else {
			//conds[key] = toInternal(val)
			conds[key] = val
		}

	}

	return conds
}

// Compiles terms into something *mgo.Session can understand.
func (self *Collection) compileConditions(term interface{}) interface{} {

	switch t := term.(type) {
	case []interface{}:
		values := []interface{}{}
		for i, _ := range t {
			value := self.compileConditions(t[i])
			if value != nil {
				values = append(values, value)
			}
		}
		if len(values) > 0 {
			return values
		}
	case db.Or:
		values := []interface{}{}
		for i, _ := range t {
			values = append(values, self.compileConditions(t[i]))
		}
		condition := bson.M{`$or`: values}
		return condition
	case db.And:
		values := []interface{}{}
		for i, _ := range t {
			values = append(values, self.compileConditions(t[i]))
		}
		condition := bson.M{`$and`: values}
		return condition
	case db.Cond:
		return compileStatement(t)
	}
	return nil
}

// Compiles terms into something that *mgo.Session can understand.
func (self *Collection) compileQuery(terms ...interface{}) interface{} {
	var query interface{}

	compiled := self.compileConditions(terms)

	if compiled != nil {
		conditions := compiled.([]interface{})
		if len(conditions) == 1 {
			query = conditions[0]
		} else {
			// this should be correct.
			// query = map[string]interface{}{"$and": conditions}

			// trying to workaround https://jira.mongodb.org/browse/SERVER-4572
			mapped := map[string]interface{}{}
			for _, v := range conditions {
				for kk, _ := range v.(map[string]interface{}) {
					mapped[kk] = v.(map[string]interface{})[kk]
				}
			}

			query = mapped
		}
	} else {
		query = map[string]interface{}{}
	}

	return query
}

// Deletes all the rows within the collection.
func (self *Collection) Truncate() error {
	err := self.collection.DropCollection()

	if err != nil {
		return err
	}

	return nil
}

// Appends an item (map or struct) into the collection.
func (self *Collection) Append(item interface{}) (interface{}, error) {
	var id bson.ObjectId

	// Dirty trick to return the Id with ease.
	res, err := self.collection.UpsertId(nil, item)

	if err != nil {
		return nil, err
	}

	if res.UpsertedId != nil {
		id = res.UpsertedId.(bson.ObjectId)
	}

	return id, nil
}

// Returns true if the collection exists.
func (self *Collection) Exists() bool {
	query := self.parent.database.C(`system.namespaces`).Find(map[string]string{`name`: fmt.Sprintf(`%s.%s`, self.parent.Name(), self.Name())})
	count, _ := query.Count()
	if count > 0 {
		return true
	}
	return false
}

// Transforms data from db.Item format into mgo format.
func toInternal(val interface{}) interface{} {

	// TODO: use reflection to target kinds and not just types.
	switch t := val.(type) {
	case db.Cond:
		for k, _ := range t {
			t[k] = toInternal(t[k])
		}
	case map[string]interface{}:
		for k, _ := range t {
			t[k] = toInternal(t[k])
		}
	}

	return val
}

// Transforms data from mgo format into db.Item format.
func toNative(val interface{}) interface{} {

	// TODO: use reflection to target kinds and not just types.

	switch t := val.(type) {
	case bson.M:
		v := map[string]interface{}{}
		for i, _ := range t {
			v[i] = toNative(t[i])
		}
		return v
	}

	return val

}
