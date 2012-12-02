/*
  Copyright (c) 2012 José Carlos Nieto, http://xiam.menteslibres.org/

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
	"github.com/gosexy/db"
	"github.com/gosexy/sugar"
	"github.com/gosexy/to"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"net/url"
	"reflect"
	"regexp"
	"strings"
	"time"
)

func init() {
	db.Register("mongo", &MongoDataSource{})
}

// Session
type MongoDataSource struct {
	name string
	config   db.DataSource
	session  *mgo.Session
	database *mgo.Database
}

// Collection
type MongoDataSourceCollection struct {
	name string
	parent     *MongoDataSource
	collection *mgo.Collection
}

func (self *MongoDataSource) Name() string {
	return self.name
}

func (self *MongoDataSourceCollection) Name() string {
	return self.name
}

// Transforms conditions into something *mgo.Session can understand.
func (c *MongoDataSourceCollection) marshal(where db.Cond) map[string]interface{} {
	conds := make(map[string]interface{})

	for key, val := range where {
		chunks := strings.Split(strings.Trim(key, " "), " ")

		if len(chunks) >= 2 {
			conds[chunks[0]] = map[string]interface{}{chunks[1]: toInternal(val)}
		} else {
			conds[key] = toInternal(val)
		}

	}

	return conds
}

// Deletes the collection (there is no truncate).
func (c *MongoDataSourceCollection) Truncate() error {
	err := c.collection.DropCollection()

	if err != nil {
		return err
	}

	return nil
}

func (c *MongoDataSourceCollection) Exists() bool {
	query := c.parent.database.C("system.namespaces").Find(db.Item{"name": fmt.Sprintf("%s.%s", c.parent.Name(), c.Name())})
	count, _ := query.Count()
	if count > 0 {
		return true
	}
	return false
}

// Appends an item to the collection.
func (c *MongoDataSourceCollection) Append(items ...interface{}) ([]db.Id, error) {

	var err error

	ids := []db.Id{}

	parent := reflect.TypeOf(c.collection)
	method, _ := parent.MethodByName("Insert")

	args := make([]reflect.Value, 1+len(items))
	args[0] = reflect.ValueOf(c.collection)

	itop := len(items)

	for i := 0; i < itop; i++ {
		id := db.Id(bson.NewObjectId().Hex())

		switch items[i].(type) {
		case map[string]interface{}:
			if items[i].(map[string]interface{})["_id"] == nil {
				items[i].(map[string]interface{})["_id"] = id
			}
		case db.Item:
			if items[i].(db.Item)["_id"] == nil {
				items[i].(db.Item)["_id"] = id
			}
		}

		args[i+1] = reflect.ValueOf(toInternal(items[i]))
		ids = append(ids, id)
	}

	exec := method.Func.Call(args)

	if exec[0].Interface() != nil {
		err = exec[0].Interface().(error)
	}

	return ids, err
}

// Compiles terms into something *mgo.Session can understand.
func (c *MongoDataSourceCollection) compileConditions(term interface{}) interface{} {
	switch term.(type) {
	case []interface{}:
		values := []interface{}{}
		itop := len(term.([]interface{}))
		for i := 0; i < itop; i++ {
			value := c.compileConditions(term.([]interface{})[i])
			if value != nil {
				values = append(values, value)
			}
		}
		if len(values) > 0 {
			return values
		}
	case db.Or:
		values := []interface{}{}
		itop := len(term.(db.Or))
		for i := 0; i < itop; i++ {
			values = append(values, c.compileConditions(term.(db.Or)[i]))
		}
		condition := map[string]interface{}{"$or": values}
		return condition
	case db.And:
		values := []interface{}{}
		itop := len(term.(db.And))
		for i := 0; i < itop; i++ {
			values = append(values, c.compileConditions(term.(db.And)[i]))
		}
		condition := map[string]interface{}{"$and": values}
		return condition
	case db.Cond:
		return c.marshal(term.(db.Cond))
	}
	return nil
}

// Compiles terms into something that *mgo.Session can understand.
func (c *MongoDataSourceCollection) compileQuery(terms []interface{}) interface{} {
	var query interface{}

	compiled := c.compileConditions(terms)

	if compiled != nil {
		conditions := compiled.([]interface{})
		if len(conditions) == 1 {
			query = conditions[0]
		} else {
			query = map[string]interface{}{"$and": conditions}
		}
	} else {
		query = map[string]interface{}{}
	}

	return query
}

// Removes all the items that match the given conditions.
func (c *MongoDataSourceCollection) Remove(terms ...interface{}) error {

	query := c.compileQuery(terms)

	_, err := c.collection.RemoveAll(query)

	return err
}

// Updates all the items that match the given conditions.
func (c *MongoDataSourceCollection) Update(terms ...interface{}) error {

	var set interface{}
	var upsert interface{}
	var modify interface{}

	set = nil
	upsert = nil
	modify = nil

	query := c.compileQuery(terms)

	itop := len(terms)

	for i := 0; i < itop; i++ {
		term := terms[i]

		switch term.(type) {
		case db.Set:
			set = term.(db.Set)
		case db.Upsert:
			upsert = term.(db.Upsert)
		case db.Modify:
			modify = term.(db.Modify)
		}
	}

	var err error

	if set != nil {
		_, err = c.collection.UpdateAll(query, db.Item{"$set": set})
		return err
	}

	if modify != nil {
		_, err = c.collection.UpdateAll(query, modify)
		return err
	}

	if upsert != nil {
		_, err = c.collection.Upsert(query, upsert)
		return err
	}

	return nil
}

// Calls a MongoDataSourceCollection function by name.
func (c *MongoDataSourceCollection) invoke(fn string, terms []interface{}) []reflect.Value {

	self := reflect.TypeOf(c)
	method, _ := self.MethodByName(fn)

	args := make([]reflect.Value, 1+len(terms))

	args[0] = reflect.ValueOf(c)

	itop := len(terms)
	for i := 0; i < itop; i++ {
		args[i+1] = reflect.ValueOf(terms[i])
	}

	exec := method.Func.Call(args)

	return exec
}

// Returns the number of items that match the given conditions.
func (c *MongoDataSourceCollection) Count(terms ...interface{}) (int, error) {
	q := c.invoke("BuildQuery", terms)

	p := q[0].Interface().(*mgo.Query)

	count, err := p.Count()

	return count, err
}

// Returns a db.Item that matches the given conditions.
func (c *MongoDataSourceCollection) Find(terms ...interface{}) db.Item {

	var item db.Item

	terms = append(terms, db.Limit(1))

	result := c.invoke("FindAll", terms)

	if len(result) > 0 {
		response := result[0].Interface().([]db.Item)
		if len(response) > 0 {
			item = response[0]
		}
	}

	return item
}

// Returns a mgo.Query based on the given terms.
func (c *MongoDataSourceCollection) BuildQuery(terms ...interface{}) *mgo.Query {

	var sort interface{}

	limit := -1
	offset := -1
	sort = nil

	// Conditions
	query := c.compileQuery(terms)

	itop := len(terms)
	for i := 0; i < itop; i++ {
		term := terms[i]

		switch term.(type) {
		case db.Limit:
			limit = int(term.(db.Limit))
		case db.Offset:
			offset = int(term.(db.Offset))
		case db.Sort:
			sort = term.(db.Sort)
		}
	}

	// Actually executing query, returning a pointer.
	q := c.collection.Find(query)

	// Applying limits and offsets.
	if offset > -1 {
		q = q.Skip(offset)
	}

	if limit > -1 {
		q = q.Limit(limit)
	}

	// Sorting result
	if sort != nil {
		for key, val := range sort.(db.Sort) {
			sval := to.String(val)
			if sval == "-1" || sval == "DESC" {
				q = q.Sort("-" + key)
			} else if sval == "1" || sval == "ASC" {
				q = q.Sort(key)
			} else {
				panic(fmt.Sprintf(`Unknown sort value "%s".`, sval))
			}
		}
	}

	return q
}

// Transforms data from db.Item format into mgo format.
func toInternal(val interface{}) interface{} {

	switch val.(type) {
	case db.Id:
		return bson.ObjectIdHex(string(val.(db.Id)))
	case db.Item:
		for k, _ := range val.(db.Item) {
			val.(db.Item)[k] = toInternal(val.(db.Item)[k])
		}
	}

	return val
}

// Transforms data from mgo format into db.Item format.
func toNative(val interface{}) interface{} {

	switch val.(type) {
	case bson.M:
		v2 := map[string]interface{}{}
		for k, v := range val.(bson.M) {
			v2[k] = toNative(v)
		}
		return v2
	case bson.ObjectId:
		return db.Id(val.(bson.ObjectId).Hex())
	}

	return val

}

// Returns all the items that match the given conditions. See Find().
func (c *MongoDataSourceCollection) FindAll(terms ...interface{}) []db.Item {
	var items []db.Item
	var result []interface{}

	var relate interface{}
	var relateAll interface{}

	var itop int

	// Analyzing
	itop = len(terms)

	for i := 0; i < itop; i++ {
		term := terms[i]

		switch term.(type) {
		case db.Relate:
			relate = term.(db.Relate)
		case db.RelateAll:
			relateAll = term.(db.RelateAll)
		}
	}

	// Retrieving data
	q := c.invoke("BuildQuery", terms)

	p := q[0].Interface().(*mgo.Query)

	p.All(&result)

	var relations []sugar.Tuple

	// This query is related to other collections.
	if relate != nil {
		for rname, rterms := range relate.(db.Relate) {
			rcollection := c.parent.Collection(rname)

			ttop := len(rterms)
			for t := ttop - 1; t >= 0; t-- {
				rterm := rterms[t]
				switch rterm.(type) {
				case db.Collection:
					rcollection = rterm.(db.Collection)
				}
			}

			relations = append(relations, sugar.Tuple{"all": false, "name": rname, "collection": rcollection, "terms": rterms})
		}
	}

	if relateAll != nil {
		for rname, rterms := range relateAll.(db.RelateAll) {
			rcollection := c.parent.Collection(rname)

			ttop := len(rterms)
			for t := ttop - 1; t >= 0; t-- {
				rterm := rterms[t]
				switch rterm.(type) {
				case db.Collection:
					rcollection = rterm.(db.Collection)
				}
			}

			relations = append(relations, sugar.Tuple{"all": true, "name": rname, "collection": rcollection, "terms": rterms})
		}
	}

	var term interface{}

	jtop := len(relations)

	itop = len(result)
	items = make([]db.Item, itop)

	for i := 0; i < itop; i++ {

		item := db.Item{}

		// Default values.
		for key, val := range result[i].(bson.M) {
			item[key] = toNative(val)
		}

		// Querying relations
		for j := 0; j < jtop; j++ {

			relation := relations[j]

			terms := []interface{}{}

			ktop := len(relation["terms"].(db.On))

			for k := 0; k < ktop; k++ {

				//term = tcopy[k]
				term = relation["terms"].(db.On)[k]

				switch term.(type) {
				// Just waiting for db.Cond statements.
				case db.Cond:
					for wkey, wval := range term.(db.Cond) {
						//if reflect.TypeOf(wval).Kind() == reflect.String { // does not always work.
						if reflect.TypeOf(wval).Name() == "string" {
							// Matching dynamic values.
							matched, _ := regexp.MatchString("\\{.+\\}", wval.(string))
							if matched {
								// Replacing dynamic values.
								kname := strings.Trim(wval.(string), "{}")
								term = db.Cond{wkey: item[kname]}
							}
						}
					}
				}
				terms = append(terms, term)
			}

			// Executing external query.
			if relation["all"] == true {
				value := relation["collection"].(*MongoDataSourceCollection).invoke("FindAll", terms)
				item[relation["name"].(string)] = value[0].Interface().([]db.Item)
			} else {
				value := relation["collection"].(*MongoDataSourceCollection).invoke("Find", terms)
				item[relation["name"].(string)] = value[0].Interface().(db.Item)
			}

		}

		// Appending to results.
		items[i] = item
	}

	return items
}

// Returns an empty data source.
func Session(config db.DataSource) db.Database {
	m := &MongoDataSource{}
	m.config = config
	return m
}

func (m *MongoDataSource) Setup(config db.DataSource) error {
	m.config = config
	return m.Open()
}

// Sets the active database.
func (m *MongoDataSource) Use(database string) error {
	m.config.Database = database
	m.database = m.session.DB(m.config.Database)
	return nil
}

func (m *MongoDataSource) Exists() bool {
	return true
}

// Returns a collection from the current database.
func (m *MongoDataSource) Collection(name string) db.Collection {
	c := &MongoDataSourceCollection{}
	c.parent = m
	c.name = name
	c.collection = m.database.C(name)
	return c
}

// Returns the underlying driver (*mgo.Session).
func (m *MongoDataSource) Driver() interface{} {
	return m.session
}

// Opens a connection to the data source. See Session().
func (m *MongoDataSource) Open() error {
	var err error

	connURL := &url.URL{Scheme: "mongodb"}

	if m.config.Port == 0 {
		m.config.Port = 27017
	}

	if m.config.Host == "" {
		m.config.Host = "127.0.0.1"
	}

	connURL.Host = fmt.Sprintf("%s:%d", m.config.Host, m.config.Port)

	if m.config.User != "" {
		connURL.User = url.UserPassword(m.config.User, m.config.Password)
	}

	m.session, err = mgo.DialWithTimeout(connURL.String(), 5*time.Second)

	if err != nil {
		return fmt.Errorf("Could not connect to %v.", m.config.Host)
	}

	m.name = m.config.Database

	if m.config.Database != "" {
		m.Use(m.config.Database)
	}

	return nil
}

// Drops the active database.
func (m *MongoDataSource) Drop() error {
	err := m.database.DropDatabase()
	return err
}

// Closes the connection to the database.
func (m *MongoDataSource) Close() error {
	if m.session != nil {
		m.session.Close()
	}
	return nil
}

// Returns names for all collection on current database.
func (m *MongoDataSource) Collections() []string {
	names, _ := m.database.CollectionNames()
	return names
}
