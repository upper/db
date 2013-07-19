/*
  Copyright (c) 2012-2013 José Carlos Nieto, http://xiam.menteslibres.org/

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
	"menteslibres.net/gosexy/db"
	"menteslibres.net/gosexy/db/util"
	"menteslibres.net/gosexy/to"
	"strings"
)

// Mongodb Collection
type SourceCollection struct {
	name       string
	parent     *Source
	collection *mgo.Collection
	util.C
}

func (self *SourceCollection) Query(terms ...interface{}) (db.Result, error) {

	queryChunks := struct {
		Fields     []string
		Limit      int
		Offset     int
		Sort       *db.Sort
		Relate     db.Relate
		RelateAll  db.RelateAll
		Relations  []db.Relation
		Conditions interface{}
	}{}

	queryChunks.Relate = make(db.Relate)
	queryChunks.RelateAll = make(db.RelateAll)

	// Analyzing given terms.
	for _, term := range terms {

		switch v := term.(type) {
		case db.Limit:
			queryChunks.Limit = int(v)
		case db.Sort:
			queryChunks.Sort = &v
		case db.Offset:
			queryChunks.Offset = int(v)
		case db.Fields:
			queryChunks.Fields = append(queryChunks.Fields, v...)
		case db.Relate:
			for name, terms := range v {
				queryChunks.Relations = append(queryChunks.Relations, db.Relation{All: false, Name: name, Collection: nil, On: terms})
			}
		case db.RelateAll:
			for name, terms := range v {
				queryChunks.Relations = append(queryChunks.Relations, db.Relation{All: true, Name: name, Collection: nil, On: terms})
			}
		}
	}

	// No specific fields given.
	if len(queryChunks.Fields) == 0 {
		queryChunks.Fields = []string{"*"}
	}

	// Actually executing query.
	q := self.buildQuery(terms...)

	result := &Result{
		query:      q,
		collection: &self.C,
		relations:  queryChunks.Relations,
		iter:       q.Iter(),
	}

	return result, nil
}

// Transforms conditions into something *mgo.Session can understand.
func compileStatement(where db.Cond) bson.M {
	conds := bson.M{}

	for key, val := range where {
		key = strings.Trim(key, " ")
		chunks := strings.SplitN(key, " ", 2)

		if len(chunks) > 1 {
			op := ""
			switch chunks[1] {
			case ">":
				op = "$gt"
			case "<":
				op = "$gt"
			case "<=":
				op = "$lte"
			case ">=":
				op = "$gte"
			default:
				op = chunks[1]
			}
			conds[chunks[0]] = bson.M{op: toInternal(val)}
		} else {
			conds[key] = toInternal(val)
		}

	}

	return conds
}

/*
	Deletes the whole collection.
*/
func (self *SourceCollection) Truncate() error {
	err := self.collection.DropCollection()

	if err != nil {
		return err
	}

	return nil
}

/*
	Returns true if the collection exists.
*/
func (self *SourceCollection) Exists() bool {
	query := self.parent.database.C("system.namespaces").Find(db.Item{"name": fmt.Sprintf("%s.%s", self.parent.Name(), self.Name())})
	count, _ := query.Count()
	if count > 0 {
		return true
	}
	return false
}

/*
	Appends items to the collection. An item could be either a map or a struct.
*/
func (self *SourceCollection) Append(items ...interface{}) ([]db.Id, error) {
	var id db.Id
	ids := make([]db.Id, len(items))
	for i, item := range items {
		id = ""
		// Dirty trick to return the Id with ease.
		res, err := self.collection.Upsert(bson.M{"_id": nil}, toInternal(item))
		if err != nil {
			return ids, err
		}
		if res.UpsertedId != nil {
			id = db.Id(res.UpsertedId.(bson.ObjectId).Hex())
		}
		ids[i] = id
	}
	return ids, nil
}

// Compiles terms into something *mgo.Session can understand.
func (self *SourceCollection) compileConditions(term interface{}) interface{} {

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
		condition := bson.M{"$or": values}
		return condition
	case db.And:
		values := []interface{}{}
		for i, _ := range t {
			values = append(values, self.compileConditions(t[i]))
		}
		condition := bson.M{"$and": values}
		return condition
	case db.Cond:
		return compileStatement(t)
	}
	return nil
}

// Compiles terms into something that *mgo.Session can understand.
func (self *SourceCollection) compileQuery(terms ...interface{}) interface{} {
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

// Removes all the items that match the given conditions.
func (self *SourceCollection) Remove(terms ...interface{}) error {

	query := self.compileQuery(terms...)

	_, err := self.collection.RemoveAll(query)

	return err
}

// Updates all the items that match the given conditions.
func (self *SourceCollection) Update(selector interface{}, update interface{}) error {
	var err error
	query := self.compileQuery(selector)

	_, err = self.collection.UpdateAll(query, bson.M{"$set": update})
	return err
}

// Returns the number of items that match the given conditions.
func (self *SourceCollection) Count(terms ...interface{}) (int, error) {
	q := self.buildQuery(terms...)

	count, err := q.Count()

	return count, err
}

// Returns the first db.Item that matches the given conditions.
func (self *SourceCollection) Find(terms ...interface{}) (db.Item, error) {
	terms = append(terms, db.Limit(1))

	result, err := self.FindAll(terms...)

	if len(result) > 0 {
		return result[0], nil
	}

	return nil, err
}

// Returns a *mgo.Query based on the given terms.
func (self *SourceCollection) buildQuery(terms ...interface{}) *mgo.Query {

	var delim = struct {
		Limit  int
		Offset int
		Fields *db.Fields
		Sort   *db.Sort
	}{
		-1,
		-1,
		nil,
		nil,
	}

	// Conditions
	query := self.compileQuery(terms...)

	for i, _ := range terms {
		switch t := terms[i].(type) {
		case db.Fields:
			delim.Fields = &t
		case db.Limit:
			delim.Limit = int(t)
		case db.Offset:
			delim.Offset = int(t)
		case db.Sort:
			delim.Sort = &t
		}
	}

	// Actually executing query, returning a pointer.
	res := self.collection.Find(query)

	// Applying limits and offsets.
	if delim.Offset > -1 {
		res = res.Skip(delim.Offset)
	}

	if delim.Limit > -1 {
		res = res.Limit(delim.Limit)
	}

	// Delimiting fields.
	if delim.Fields != nil {
		sel := bson.M{}
		for _, field := range *delim.Fields {
			sel[field] = true
		}
		res = res.Select(sel)
	}

	// Sorting result.
	if delim.Sort != nil {
		for key, val := range *delim.Sort {
			sval := to.String(val)
			if sval == "-1" || sval == "DESC" {
				res = res.Sort("-" + key)
			} else if sval == "1" || sval == "ASC" {
				res = res.Sort(key)
			} else {
				panic(fmt.Sprintf(`Unknown sort value "%s".`, sval))
			}
		}
	}

	return res
}

// Transforms data from db.Item format into mgo format.
func toInternal(val interface{}) interface{} {

	// TODO: use reflection to target kinds and not just types.

	switch t := val.(type) {
	case []db.Id:
		ids := make([]bson.ObjectId, len(t))
		for i, _ := range t {
			ids[i] = bson.ObjectIdHex(string(t[i]))
		}
		return ids
	case db.Id:
		return bson.ObjectIdHex(string(t))
	case db.Item:
		for k, _ := range t {
			t[k] = toInternal(t[k])
		}
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
	case bson.ObjectId:
		return db.Id(t.Hex())
	}

	return val

}

// Returns all the items that match the given conditions. See Find().
func (self *SourceCollection) FindAll(terms ...interface{}) ([]db.Item, error) {
	var err error
	results := []db.Item{}
	q, err := self.Query(terms...)
	if err != nil {
		return nil, err
	}
	err = q.All(&results)
	return results, err
}
