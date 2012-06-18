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
  "strings"
  "reflect"
  "time"
  "net/url"
  "regexp"
  "launchpad.net/mgo"
  "launchpad.net/mgo/bson"
  . "github.com/xiam/gosexy"
)

type MongoDB struct {
  config  *DataSource
  session  *mgo.Session
  database *mgo.Database
}

type MongoDBCollection struct {
  parent *MongoDB
  collection *mgo.Collection
}

type MongoDbQuery struct {

}

func (w Where) Marshal() map[string] interface{} {
  conds := make(map[string] interface{})

  for key, val := range(w) {
    key     = strings.Trim(key, " ")
    chunks := strings.Split(key, " ")

    if len(chunks) >= 2 {
      conds[chunks[0]] = map[string] interface{} { chunks[1]: val }
    } else {
      conds[key] = val
    }

  }

  return conds
}

func (c *MongoDBCollection) Truncate() bool {
  err := c.collection.DropCollection()

  if err == nil {
    return false
  }

  return true
}

func (c *MongoDBCollection) Append(items ...interface {}) bool {

  parent    := reflect.TypeOf(c.collection)
  method, _ := parent.MethodByName("Insert")

  args := make([]reflect.Value, 1 + len(items))
  args[0] = reflect.ValueOf(c.collection)

  itop := len(items)
  for i := 0; i < itop; i++ {
    args[i + 1] = reflect.ValueOf(items[i])
  }

  exec := method.Func.Call(args)

  if exec[0].Interface() != nil {
    return false
  }

  return true
}

func (c *MongoDBCollection) CompileConditions(term interface{}) interface{} {
  switch term.(type) {
    case []interface{}: {
      values  := []interface{} {}
      itop    := len(term.([]interface{}))
      for i := 0; i < itop; i++ {
        value := c.CompileConditions(term.([]interface{})[i])
        if value != nil {
          values = append(values, value)
        }
      }
      if len(values) > 0 {
        return values
      }
    }
    case Or: {
      values  := []interface{} {}
      itop    := len(term.(Or))
      for i := 0; i < itop; i++ {
        values = append(values, c.CompileConditions(term.(Or)[i]))
      }
      condition := map[string]interface{} { "$or": values }
      return condition
    }
    case And: {
      values  := []interface{} {}
      itop    := len(term.(And))
      for i := 0; i < itop; i++ {
        values = append(values, c.CompileConditions(term.(And)[i]))
      }
      condition := map[string]interface{} { "$and": values }
      return condition
    }
    case Where: {
      return term.(Where).Marshal()
    }
  }
  return nil
}

func (c *MongoDBCollection) CompileQuery(terms []interface{}) interface{} {
  var query interface {}

  compiled := c.CompileConditions(terms)

  if compiled != nil {
    conditions := compiled.([]interface{})
    if len(conditions) == 1 {
      query = conditions[0]
    } else {
      query = map[string] interface{} { "$and": conditions }
    }
  } else {
    query = map[string] interface {} {}
  }

  return query
}

func (c *MongoDBCollection) RemoveAll(terms ...interface{}) bool {

  terms = append(terms, Multi(true))

  result := c.Invoke("Remove", terms)

  return result[0].Bool()
}

func (c *MongoDBCollection) Remove(terms ...interface{}) bool {
  
  var multi interface{}

  query := c.CompileQuery(terms)

  itop := len(terms)

  for i := 0; i < itop; i++ {
    term := terms[i]

    switch term.(type) {
      case Multi: {
        multi = term.(Multi)
      }
    }
  }

  if multi != nil {
    c.collection.RemoveAll(query)
  } else {
    c.collection.Remove(query)
  }

  return true
}

func (c *MongoDBCollection) UpdateAll(terms ...interface{}) bool {

  terms = append(terms, Multi(true))

  result := c.Invoke("Update", terms)

  return result[0].Bool()
}

func (c *MongoDBCollection) Update(terms ...interface{}) bool {

  var set     interface{}
  var upsert  interface{}
  var modify  interface{}
  var multi   interface{}

  set     = nil
  upsert  = nil
  modify  = nil
  multi   = nil

  // TODO: make use Multi

  query := c.CompileQuery(terms)

  itop := len(terms)

  for i := 0; i < itop; i++ {
    term := terms[i]

    switch term.(type) {
      case Set: {
        set = term.(Set)
      }
      case Upsert: {
        upsert = term.(Upsert)
      }
      case Modify: {
        modify = term.(Modify)
      }
      case Multi: {
        multi = term.(Multi)
      }
    }
  }


  if multi != nil {

    if set != nil {
      c.collection.UpdateAll(query, Item { "$set": set })
      return true
    }

    if modify != nil {
      c.collection.UpdateAll(query, modify)
      return true
    }

  } else {

    if set != nil {
      c.collection.Update(query, Item { "$set": set })
      return true
    }

    if modify != nil {
      c.collection.Update(query, modify)
      return true
    }

  }
  
  if upsert != nil {
    c.collection.Upsert(query, upsert)
    return true
  }

  return false
}

func (c *MongoDBCollection) Invoke(fn string, terms []interface{}) []reflect.Value {

  self      := reflect.TypeOf(c)
  method, _ := self.MethodByName(fn)

  args := make([]reflect.Value, 1 + len(terms))

  args[0] = reflect.ValueOf(c)

  itop := len(terms)
  for i := 0; i < itop; i++ {
    args[i + 1] = reflect.ValueOf(terms[i])
  }

  exec := method.Func.Call(args)

  return exec
}

func (c *MongoDBCollection) Count(terms ...interface{}) int {
  q := c.Invoke("BuildQuery", terms)
  
  p := q[0].Interface().(*mgo.Query)

  count, err := p.Count()

  if err != nil {
    panic(err)
  }

  return count
}

func (c *MongoDBCollection) Find(terms ...interface{}) Item {

  var item Item

  terms = append(terms, Limit(1))

  result := c.Invoke("FindAll", terms)

  if len(result) > 0 {
    response := result[0].Interface().([]Item)
    if len(response) > 0 {
      item = response[0]
    }
  }

  return item
}

func (c *MongoDBCollection) BuildQuery(terms ...interface{}) *mgo.Query {

  var sort interface {}

  limit   := -1
  offset  := -1
  sort    = nil
  
  // Conditions
  query := c.CompileQuery(terms)

  itop := len(terms)
  for i := 0; i < itop; i++ {
    term := terms[i]

    switch term.(type) {
      case Limit: {
        limit   = int(term.(Limit))
      }
      case Offset: {
        offset  = int(term.(Offset))
      }
      case Sort: {
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
    q = q.Sort(sort)
  }

  return q
}

func (c *MongoDBCollection) FindAll(terms ...interface{}) []Item {
  var items []Item
  var result []interface {}
  
  var relate interface {}
  var relateAll interface {}

  var itop int

  // Analyzing
  itop = len(terms)

  for i := 0; i < itop; i++ {
    term := terms[i]

    switch term.(type) {
      case Relate: {
        relate = term.(Relate)
      }
      case RelateAll: {
        relateAll = term.(RelateAll)
      }
    }
  }

  // Retrieving data
  q := c.Invoke("BuildQuery", terms)

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
          case Collection: {
            rcollection = rterm.(Collection)
          }
        }
      }

      relations = append(relations, Tuple { "all": false, "name": rname, "collection": rcollection, "terms": rterms, })
    }
  }
  
  if relateAll != nil {
    for rname, rterms := range relateAll.(RelateAll) {
      rcollection := c.parent.Collection(rname)

      ttop := len(rterms)
      for t := ttop - 1; t >= 0; t-- {
        rterm := rterms[t]
        switch rterm.(type) {
          case Collection: {
            rcollection = rterm.(Collection)
          }
        }
      }

      relations = append(relations, Tuple { "all": true, "name": rname, "collection": rcollection, "terms": rterms, })
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
          case Where: {
            for wkey, wval := range term.(Where) {
              //if reflect.TypeOf(wval).Kind() == reflect.String { // does not always work.
              if reflect.TypeOf(wval).Name() == "string" {
                // Matching dynamic values.
                matched, _ := regexp.MatchString("\\{.+\\}", wval.(string))
                if matched {
                  // Replacing dynamic values.
                  kname := strings.Trim(wval.(string), "{}")
                  term = Where { wkey : item[kname] }
                }
              }
            }
          }
        }
        terms = append(terms, term)
      }
      
      // Executing external query.
      if relation["all"] == true {
        value := relation["collection"].(*MongoDBCollection).Invoke("FindAll", terms)
        item[relation["name"].(string)] = value[0].Interface().([]Item)
      } else {
        value := relation["collection"].(*MongoDBCollection).Invoke("Find", terms)
        item[relation["name"].(string)] = value[0].Interface().(Item)
      }

    }

    // Appending to results.
    items[i] = item
  }

  return items
}

func NewMongoDB(config *DataSource) *MongoDB {
  m := &MongoDB{}
  m.config = config
  return m
}

func (m *MongoDB) Use(database string) bool {
  m.config.Database = database
  m.database = m.session.DB(m.config.Database)
  return true
}

func (m *MongoDB) Collection(name string) Collection {
  c := &MongoDBCollection{}
  c.parent = m
  c.collection = m.database.C(name)
  return c
}

func (m *MongoDB) Connect() error {
  var err error
  
  connURL := &url.URL{ Scheme: "mongodb" }
  
  if m.config.Host != "" {
    connURL.Host = m.config.Host
  }
  
  if m.config.User != "" {
    connURL.User = url.UserPassword(m.config.User, m.config.Password)
  }

  m.session, err = mgo.DialWithTimeout(connURL.String(), 5*time.Second)

  if m.config.Database != "" {
    m.Use(m.config.Database)
  }

  return err
}

func (m *MongoDB) Drop() bool {
  err := m.database.DropDatabase()
  if err == nil {
    return false
  }
  return true
}

func (m *MongoDB) Collections() []string {
  names, _ := m.database.CollectionNames()
  return names
}
