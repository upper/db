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

package db

import (
	"fmt"
	. "github.com/xiam/gosexy"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"net/url"
	"reflect"
	"regexp"
	"strings"
	"time"
)

// MongoDataSource session.
type MongoDataSource struct {
	config   DataSource
	session  *mgo.Session
	database *mgo.Database
}

// MongoDataSource collection.
type MongoDataSourceCollection struct {
	parent     *MongoDataSource
	collection *mgo.Collection
}

// Converts Where keytypes into something that mgo can understand.
func (c *MongoDataSourceCollection) marshal(where Where) map[string]interface{} {
	conds := make(map[string]interface{})

	for key, val := range where {
		key = strings.Trim(key, " ")
		chunks := strings.Split(key, " ")

		if len(chunks) >= 2 {
			conds[chunks[0]] = map[string]interface{}{chunks[1]: val}
		} else {
			conds[key] = val
		}

	}

	return conds
}

// Deletes the whole collection.
func (c *MongoDataSourceCollection) Truncate() bool {
	err := c.collection.DropCollection()

	if err == nil {
		return false
	}

	return true
}

// Inserts items into the collection.
func (c *MongoDataSourceCollection) Append(items ...interface{}) bool {

	parent := reflect.TypeOf(c.collection)
	method, _ := parent.MethodByName("Insert")

	args := make([]reflect.Value, 1+len(items))
	args[0] = reflect.ValueOf(c.collection)

	itop := len(items)
	for i := 0; i < itop; i++ {
		args[i+1] = reflect.ValueOf(items[i])
	}

	exec := method.Func.Call(args)

	if exec[0].Interface() != nil {
		return false
	}

	return true
}

// Compiles terms into conditions that mgo can understand.
func (c *MongoDataSourceCollection) compileConditions(term interface{}) interface{} {
	switch term.(type) {
	case []interface{}:
		{
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
		}
	case Or:
		{
			values := []interface{}{}
			itop := len(term.(Or))
			for i := 0; i < itop; i++ {
				values = append(values, c.compileConditions(term.(Or)[i]))
			}
			condition := map[string]interface{}{"$or": values}
			return condition
		}
	case And:
		{
			values := []interface{}{}
			itop := len(term.(And))
			for i := 0; i < itop; i++ {
				values = append(values, c.compileConditions(term.(And)[i]))
			}
			condition := map[string]interface{}{"$and": values}
			return condition
		}
	case Where:
		{
			return c.marshal(term.(Where))
		}
	}
	return nil
}

// Compiles terms into a query that mgo can understand.
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

// Removes all the items that match the provided conditions.
func (c *MongoDataSourceCollection) Remove(terms ...interface{}) bool {

	query := c.compileQuery(terms)

	c.collection.RemoveAll(query)

	return true
}

// Updates all the items that match the provided conditions. You can specify the modification type by using Set, Modify or Upsert.
func (c *MongoDataSourceCollection) Update(terms ...interface{}) bool {

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
		case Set:
			{
				set = term.(Set)
			}
		case Upsert:
			{
				upsert = term.(Upsert)
			}
		case Modify:
			{
				modify = term.(Modify)
			}
		}
	}

	if set != nil {
		c.collection.UpdateAll(query, Item{"$set": set})
		return true
	}

	if modify != nil {
		c.collection.UpdateAll(query, modify)
		return true
	}

	if upsert != nil {
		c.collection.Upsert(query, upsert)
		return true
	}

	return false
}

// Calls a MongoDataSourceCollection function by string.
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

// Returns the number of total items matching the provided conditions.
func (c *MongoDataSourceCollection) Count(terms ...interface{}) int {
	q := c.invoke("BuildQuery", terms)

	p := q[0].Interface().(*mgo.Query)

	count, err := p.Count()

	if err != nil {
		panic(err)
	}

	return count
}

// Returns a document that matches all the provided conditions. Ordering of the terms doesn't matter but you must take in
// account that conditions are generally evaluated from left to right (or from top to bottom).
func (c *MongoDataSourceCollection) Find(terms ...interface{}) Item {

	var item Item

	terms = append(terms, Limit(1))

	result := c.invoke("FindAll", terms)

	if len(result) > 0 {
		response := result[0].Interface().([]Item)
		if len(response) > 0 {
			item = response[0]
		}
	}

	return item
}

// Returns a mgo.Query that matches the provided terms.
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
		case Limit:
			{
				limit = int(term.(Limit))
			}
		case Offset:
			{
				offset = int(term.(Offset))
			}
		case Sort:
			{
				sort = term.(Sort)
			}
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
		q = q.Sort(sort.(string))
	}

	return q
}

// Returns all the results that match the provided conditions. See Find().
func (c *MongoDataSourceCollection) FindAll(terms ...interface{}) []Item {
	var items []Item
	var result []interface{}

	var relate interface{}
	var relateAll interface{}

	var itop int

	// Analyzing
	itop = len(terms)

	for i := 0; i < itop; i++ {
		term := terms[i]

		switch term.(type) {
		case Relate:
			{
				relate = term.(Relate)
			}
		case RelateAll:
			{
				relateAll = term.(RelateAll)
			}
		}
	}

	// Retrieving data
	q := c.invoke("BuildQuery", terms)

	p := q[0].Interface().(*mgo.Query)

	p.All(&result)

	var relations []Tuple

	// This query is related to other collections.
	if relate != nil {
		for rname, rterms := range relate.(Relate) {
			rcollection := c.parent.Collection(rname)

			ttop := len(rterms)
			for t := ttop - 1; t >= 0; t-- {
				rterm := rterms[t]
				switch rterm.(type) {
				case Collection:
					{
						rcollection = rterm.(Collection)
					}
				}
			}

			relations = append(relations, Tuple{"all": false, "name": rname, "collection": rcollection, "terms": rterms})
		}
	}

	if relateAll != nil {
		for rname, rterms := range relateAll.(RelateAll) {
			rcollection := c.parent.Collection(rname)

			ttop := len(rterms)
			for t := ttop - 1; t >= 0; t-- {
				rterm := rterms[t]
				switch rterm.(type) {
				case Collection:
					{
						rcollection = rterm.(Collection)
					}
				}
			}

			relations = append(relations, Tuple{"all": true, "name": rname, "collection": rcollection, "terms": rterms})
		}
	}

	var term interface{}

	jtop := len(relations)

	itop = len(result)
	items = make([]Item, itop)

	for i := 0; i < itop; i++ {

		item := Item{}

		// Default values.
		for key, val := range result[i].(bson.M) {
			item[key] = val
		}

		// Querying relations
		for j := 0; j < jtop; j++ {

			relation := relations[j]

			terms := []interface{}{}

			ktop := len(relation["terms"].(On))

			for k := 0; k < ktop; k++ {

				//term = tcopy[k]
				term = relation["terms"].(On)[k]

				switch term.(type) {
				// Just waiting for Where statements.
				case Where:
					{
						for wkey, wval := range term.(Where) {
							//if reflect.TypeOf(wval).Kind() == reflect.String { // does not always work.
							if reflect.TypeOf(wval).Name() == "string" {
								// Matching dynamic values.
								matched, _ := regexp.MatchString("\\{.+\\}", wval.(string))
								if matched {
									// Replacing dynamic values.
									kname := strings.Trim(wval.(string), "{}")
									term = Where{wkey: item[kname]}
								}
							}
						}
					}
				}
				terms = append(terms, term)
			}

			// Executing external query.
			if relation["all"] == true {
				value := relation["collection"].(*MongoDataSourceCollection).invoke("FindAll", terms)
				item[relation["name"].(string)] = value[0].Interface().([]Item)
			} else {
				value := relation["collection"].(*MongoDataSourceCollection).invoke("Find", terms)
				item[relation["name"].(string)] = value[0].Interface().(Item)
			}

		}

		// Appending to results.
		items[i] = item
	}

	return items
}

// Returns a new MongoDataSource object.
func MongoSession(config DataSource) Database {
	m := &MongoDataSource{}
	m.config = config
	return m
}

// Switches the current session database to the provided name. See MongoSession().
func (m *MongoDataSource) Use(database string) error {
	m.config.Database = database
	m.database = m.session.DB(m.config.Database)
	return nil
}

// Returns a Collection from the currently active database given the name. See MongoSession().
func (m *MongoDataSource) Collection(name string) Collection {
	c := &MongoDataSourceCollection{}
	c.parent = m
	c.collection = m.database.C(name)
	return c
}

func (m *MongoDataSource) Driver() interface{} {
	return m.session
}

// Connects to the previously specified datasource. See MongoSession().
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

	if m.config.Database != "" {
		m.Use(m.config.Database)
	}

	return nil
}

// Entirely drops the active database.
func (m *MongoDataSource) Drop() error {
	err := m.database.DropDatabase()
	return err
}

// Entirely drops the active database.
func (m *MongoDataSource) Close() error {
	if m.session != nil {
		m.session.Close()
	}
	return nil
}

// Returns all the collection names in the active database.
func (m *MongoDataSource) Collections() []string {
	names, _ := m.database.CollectionNames()
	return names
}
