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
  . "github.com/xiam/gosexy"
  "strings"
  "reflect"
  "launchpad.net/mgo"
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

func (c *MongoDBCollection) Append(items ...interface {}) bool {
  // TODO: use reflection
  length := len(items)
  for i := 0; i < length; i++ {
    err := c.collection.Insert(items[i])
    if err != nil {
      panic(err)
    }
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

  return true
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

  // TODO: catch response.

  return true
}

func (c *MongoDBCollection) Update(terms ...interface{}) bool {

  var set     interface{}
  var upsert  interface{}
  var modify  interface{}

  set     = nil
  upsert  = nil
  modify  = nil

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
    }
  }

  if set != nil {
    c.collection.UpdateAll(query, Tuple { "$set": set })
    return true
  }

  if upsert != nil {
    c.collection.Upsert(query, upsert)
    return true
  }

  if modify != nil {
    c.collection.UpdateAll(query, modify)
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

func (c *MongoDBCollection) Find(terms ...interface{}) interface{} {

  var result interface{}

  typeOf := reflect.TypeOf(c)
  method, _ := typeOf.MethodByName("FindAll")

  args := make([]reflect.Value, 2 + len(terms))

  // First argument is mandatory.
  args[0] = reflect.ValueOf(c)

  // Forced constraint.
  args[1] = reflect.ValueOf(Limit(1))

  itop := len(terms)
  for i := 0; i < itop; i++ {
    args[i+2] = reflect.ValueOf(terms[i])
  }

  exec := method.Func.Call(args)

  results := exec[0].Interface().([] interface {})

  if (len(results) > 0) {
    // We expect only the first result.
    result = results[0]
  }

  return result
}

func (c *MongoDBCollection) FindAll(terms ...interface{}) []interface{} {

  var result []interface {}
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
  p := c.collection.Find(query)

  // Applying limits and offsets.
  if offset > -1 {
    p = p.Skip(offset)
  }

  if limit > -1 {
    p = p.Limit(limit)
  }

  // Sorting result
  if sort != nil {
    p = p.Sort(sort)
  }

  // Retrieving data
  p.All(&result)

  return result
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
  m.session, err = mgo.Dial(m.config.Host)
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
