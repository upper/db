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
	"errors"
	"fmt"
	"github.com/gosexy/db"
	"github.com/gosexy/to"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"reflect"
	"regexp"
	"strings"
)

// Mongodb Collection
type SourceCollection struct {
	name       string
	parent     *Source
	collection *mgo.Collection
}

var extRelationPattern = regexp.MustCompile(`\{(.+)\}`)
var columnComparePattern = regexp.MustCompile(`[^a-zA-Z0-9]`)

/*
	Returns true if a table column looks like a struct field.
*/
func compareColumnToField(s, c string) bool {
	s = columnComparePattern.ReplaceAllString(s, "")
	c = columnComparePattern.ReplaceAllString(c, "")
	return strings.ToLower(s) == strings.ToLower(c)
}

/*
	Returns the collection name as a string.
*/
func (self *SourceCollection) Name() string {
	return self.name
}

/*
	Fetches a result delimited by terms into a pointer to map or struct given by
	dst.
*/
func (self *SourceCollection) Fetch(dst interface{}, terms ...interface{}) error {
	/*
		At this moment it is not possible to create a slice of a given element
		type: https://code.google.com/p/go/issues/detail?id=2339

		When it gets available this function should change, it must rely on
		FetchAll() the same way Find() relies on FindAll().
	*/

	found := self.Find(terms...)

	dstv := reflect.ValueOf(dst)

	if dstv.Kind() != reflect.Ptr || dstv.IsNil() {
		return fmt.Errorf("Fetch() expects a pointer.")
	}

	itemv := dstv.Elem().Type()

	switch itemv.Kind() {
	case reflect.Struct:
		for column, _ := range found {
			f := func(s string) bool {
				return compareColumnToField(s, column)
			}
			v := dstv.Elem().FieldByNameFunc(f)
			if v.IsValid() {
				v.Set(reflect.ValueOf(found[column]))
			}
		}
	case reflect.Map:
		dstv.Elem().Set(reflect.ValueOf(found))
	default:
		return fmt.Errorf("Expecting a pointer to map or struct, got %s.", itemv.Kind())
	}

	return nil
}

/*
	Fetches results delimited by terms into an slice of maps or structs given by
	the pointer dst.
*/
func (self *SourceCollection) FetchAll(dst interface{}, terms ...interface{}) error {

	var err error

	var dstv reflect.Value
	var itemv reflect.Value
	var itemk reflect.Kind

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

	// Checking input
	dstv = reflect.ValueOf(dst)

	if dstv.Kind() != reflect.Ptr || dstv.IsNil() || dstv.Elem().Kind() != reflect.Slice {
		return errors.New("FetchAll() expects a pointer to slice.")
	}

	itemv = dstv.Elem()
	itemk = itemv.Type().Elem().Kind()

	if itemk != reflect.Struct && itemk != reflect.Map {
		return errors.New("FetchAll() expects a pointer to slice of maps or structs.")
	}

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

	// Fetching rows.
	err = q.All(dst)

	if err != nil {
		return err
	}

	fmt.Printf("DEST %v\n", dst)

	if len(queryChunks.Relations) > 0 {

		// Iterate over results.
		for i := 0; i < dstv.Elem().Len(); i++ {

			item := itemv.Index(i)

			for _, relation := range queryChunks.Relations {

				terms := make([]interface{}, len(relation.On))

				for j, term := range relation.On {
					switch t := term.(type) {
					// Just waiting for db.Cond statements.
					case db.Cond:
						for k, v := range t {
							switch s := v.(type) {
							case string:
								matches := extRelationPattern.FindStringSubmatch(s)
								if len(matches) > 1 {
									extkey := matches[1]
									var val reflect.Value
									switch itemk {
									case reflect.Struct:
										f := func(s string) bool {
											return compareColumnToField(s, extkey)
										}
										val = item.FieldByNameFunc(f)
									case reflect.Map:
										val = item.MapIndex(reflect.ValueOf(extkey))
									}
									if val.IsValid() {
										fmt.Printf("CONST: %v --> %v\n", term, val.Interface())
										term = db.Cond{k: toInternal(val.Interface())}
									}
								}
							}
						}
					case db.Collection:
						relation.Collection = t
					}
					terms[j] = term
				}

				if relation.Collection == nil {
					relation.Collection, err = self.parent.Collection(relation.Name)
					if err != nil {
						return fmt.Errorf("Could not relate to collection %s: %s", relation.Name, err.Error())
					}
				}

				keyv := reflect.ValueOf(relation.Name)

				switch itemk {
				case reflect.Struct:
					f := func(s string) bool {
						return compareColumnToField(s, relation.Name)
					}

					val := item.FieldByNameFunc(f)

					if val.IsValid() {
						p := reflect.New(val.Type())
						q := p.Interface()
						if relation.All == true {
							err = relation.Collection.FetchAll(q, terms...)
						} else {
							err = relation.Collection.Fetch(q, terms...)
						}
						if err != nil {
							return err
						}
						val.Set(reflect.Indirect(p))
					}
				case reflect.Map:
					// Executing external query.
					if relation.All == true {
						item.SetMapIndex(keyv, reflect.ValueOf(relation.Collection.FindAll(terms...)))
					} else {
						item.SetMapIndex(keyv, reflect.ValueOf(relation.Collection.Find(terms...)))
					}
				}

			}
		}
	}

	return nil
}

// Transforms conditions into something *mgo.Session can understand.
func compileStatement(where db.Cond) bson.M {
	conds := bson.M{}

	for key, val := range where {
		key = strings.Trim(key, " ")
		chunks := strings.Split(key, " ")

		if len(chunks) >= 2 {
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
	ids := []db.Id{}
	for _, item := range items {
		// Dirty trick to return the Id with ease.
		res, err := self.collection.Upsert(bson.M{"_id": nil}, item)
		if err != nil {
			return ids, err
		}
		var id db.Id
		if res.UpsertedId != nil {
			id = db.Id(res.UpsertedId.(bson.ObjectId).Hex())
		}
		ids = append(ids, id)
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
func (self *SourceCollection) Update(terms ...interface{}) error {
	var err error

	var action = struct {
		Set    *db.Set
		Upsert *db.Upsert
		Modify *db.Modify
	}{}

	query := self.compileQuery(terms...)

	for i, _ := range terms {
		switch t := terms[i].(type) {
		case db.Set:
			action.Set = &t
		case db.Upsert:
			action.Upsert = &t
		case db.Modify:
			action.Modify = &t
		}
	}

	if action.Set != nil {
		_, err = self.collection.UpdateAll(query, db.Item{"$set": action.Set})
		return err
	}

	if action.Modify != nil {
		_, err = self.collection.UpdateAll(query, action.Modify)
		return err
	}

	if action.Upsert != nil {
		_, err = self.collection.Upsert(query, action.Upsert)
		return err
	}

	return nil
}

// Calls a SourceCollection function by name.
func (self *SourceCollection) invoke(fn string, terms []interface{}) []reflect.Value {

	reflected := reflect.TypeOf(self)

	method, _ := reflected.MethodByName(fn)

	args := make([]reflect.Value, 1+len(terms))

	args[0] = reflect.ValueOf(self)

	itop := len(terms)
	for i := 0; i < itop; i++ {
		args[i+1] = reflect.ValueOf(terms[i])
	}

	exec := method.Func.Call(args)

	return exec
}

// Returns the number of items that match the given conditions.
func (self *SourceCollection) Count(terms ...interface{}) (int, error) {

	q := self.buildQuery(terms...)

	count, err := q.Count()

	return count, err
}

// Returns the first db.Item that matches the given conditions.
func (self *SourceCollection) Find(terms ...interface{}) db.Item {
	terms = append(terms, db.Limit(1))

	result := self.FindAll(terms...)

	if len(result) > 0 {
		return result[0]
	}

	return nil
}

// Returns a mgo.Query based on the given terms.
func (self *SourceCollection) buildQuery(terms ...interface{}) *mgo.Query {

	var delim = struct {
		Limit  int
		Offset int
		Sort   *db.Sort
	}{
		-1,
		-1,
		nil,
	}

	// Conditions
	query := self.compileQuery(terms...)

	for i, _ := range terms {
		switch t := terms[i].(type) {
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

	// Sorting result
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
func (self *SourceCollection) FindAll(terms ...interface{}) []db.Item {
	results := []db.Item{}
	err := self.FetchAll(&results, terms...)
	if err != nil {
		panic(err)
	}
	return results
}
