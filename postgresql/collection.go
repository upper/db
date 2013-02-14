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

package postgresql

import (
	"database/sql"
	"fmt"
	"github.com/gosexy/db"
	"github.com/gosexy/sugar"
	"github.com/gosexy/to"
	"reflect"
	"regexp"
	"strconv"
	"strings"
)

// Represents a PostgreSQL table.
type Table struct {
	parent *Source
	name   string
	types  map[string]reflect.Kind
}

func (self *Table) Name() string {
	return self.name
}

// Returns true if the collection exists.
func (self *Table) Exists() bool {
	result, err := self.parent.doQuery(
		fmt.Sprintf(`
				SELECT table_name
					FROM information_schema.tables
				WHERE table_catalog = '%s' AND table_name = '%s'
			`,
			self.parent.Name(),
			self.Name(),
		),
	)
	if err != nil {
		//panic(err.Error())
		return false
	}
	if result.Next() == true {
		result.Close()
		return true
	}
	return false
}

// Returns all items from a query.
func (self *Table) sqlFetchAll(rows *sql.Rows) []db.Item {

	items := []db.Item{}

	columns, _ := rows.Columns()

	for i, _ := range columns {
		columns[i] = strings.ToLower(columns[i])
	}

	expecting := len(columns)

	values := make([]*sql.RawBytes, expecting)
	scanArgs := make([]interface{}, expecting)

	for i := range columns {
		scanArgs[i] = &values[i]
	}

	for rows.Next() {
		item := db.Item{}

		err := rows.Scan(scanArgs...)

		if err != nil {
			panic(err)
		}

		// Pending cleaner reflection magic.
		for i, value := range values {
			column := columns[i]

			if value == nil {
				item[column] = nil
			} else {
				strval := fmt.Sprintf("%s", *value)

				switch self.types[column] {
				case reflect.Uint64:
					intval, _ := strconv.Atoi(strval)
					item[column] = uint64(intval)
				case reflect.Int64:
					intval, _ := strconv.Atoi(strval)
					item[column] = intval
				case reflect.Float64:
					floatval, _ := strconv.ParseFloat(strval, 10)
					item[column] = floatval
				default:
					item[column] = strval
				}
			}
		}

		items = append(items, item)
	}

	return items
}

func (t *Table) invoke(fn string, terms []interface{}) []reflect.Value {

	self := reflect.ValueOf(t)
	method := self.MethodByName(fn)

	args := make([]reflect.Value, len(terms))

	itop := len(terms)
	for i := 0; i < itop; i++ {
		args[i] = reflect.ValueOf(terms[i])
	}

	exec := method.Call(args)

	return exec
}

func (t *Table) compileSet(term db.Set) (string, db.SqlArgs) {
	sql := []string{}
	args := db.SqlArgs{}

	for key, arg := range term {
		sql = append(sql, fmt.Sprintf("%s = ?", key))
		args = append(args, fmt.Sprintf("%v", arg))
	}

	return strings.Join(sql, ", "), args
}

func (t *Table) compileConditions(term interface{}) (string, db.SqlArgs) {
	sql := []string{}
	args := db.SqlArgs{}

	switch term.(type) {
	case []interface{}:
		itop := len(term.([]interface{}))

		for i := 0; i < itop; i++ {
			rsql, rargs := t.compileConditions(term.([]interface{})[i])
			if rsql != "" {
				sql = append(sql, rsql)
				for j := 0; j < len(rargs); j++ {
					args = append(args, rargs[j])
				}
			}
		}

		if len(sql) > 0 {
			return "(" + strings.Join(sql, " AND ") + ")", args
		}
	case db.Or:
		itop := len(term.(db.Or))

		for i := 0; i < itop; i++ {
			rsql, rargs := t.compileConditions(term.(db.Or)[i])
			if rsql != "" {
				sql = append(sql, rsql)
				for j := 0; j < len(rargs); j++ {
					args = append(args, rargs[j])
				}
			}
		}

		if len(sql) > 0 {
			return "(" + strings.Join(sql, " OR ") + ")", args
		}
	case db.And:
		itop := len(term.(db.Or))

		for i := 0; i < itop; i++ {
			rsql, rargs := t.compileConditions(term.(db.Or)[i])
			if rsql != "" {
				sql = append(sql, rsql)
				for j := 0; j < len(rargs); j++ {
					args = append(args, rargs[j])
				}
			}
		}

		if len(sql) > 0 {
			return "(" + strings.Join(sql, " AND ") + ")", args
		}
	case db.Cond:
		return t.marshal(term.(db.Cond))
	}

	return "", args
}

func (t *Table) marshal(where db.Cond) (string, []string) {

	var placeholder string

	placeholders := []string{}
	args := []string{}

	for key, val := range where {
		chunks := strings.Split(strings.Trim(key, " "), " ")

		if len(chunks) >= 2 {
			placeholder = fmt.Sprintf("%s %s ?", chunks[0], chunks[1])
		} else {
			placeholder = fmt.Sprintf("%s = ?", chunks[0])
		}

		placeholders = append(placeholders, placeholder)
		args = append(args, fmt.Sprintf("%v", val))
	}

	return strings.Join(placeholders, " AND "), args
}

// Deletes all the rows in the table.
func (t *Table) Truncate() error {

	_, err := t.parent.doExec(
		fmt.Sprintf(`TRUNCATE TABLE "%s"`, t.Name()),
	)

	return err
}

// Deletes all the rows in the table that match certain conditions.
func (t *Table) Remove(terms ...interface{}) error {

	conditions, cargs := t.compileConditions(terms)

	if conditions == "" {
		conditions = "1 = 1"
	}

	_, err := t.parent.doExec(
		fmt.Sprintf("DELETE FROM %s", t.Name()),
		fmt.Sprintf("WHERE %s", conditions), cargs,
	)

	return err
}

// Modifies all the rows in the table that match certain conditions.
func (t *Table) Update(terms ...interface{}) error {
	var fields string
	var fargs db.SqlArgs

	conditions, cargs := t.compileConditions(terms)

	for _, term := range terms {
		switch term.(type) {
		case db.Set:
			fields, fargs = t.compileSet(term.(db.Set))
		}
	}

	if conditions == "" {
		conditions = "1 = 1"
	}

	_, err := t.parent.doExec(
		fmt.Sprintf("UPDATE %s SET %s", t.Name(), fields), fargs,
		fmt.Sprintf("WHERE %s", conditions), cargs,
	)

	return err
}

// Returns all the rows in the table that match certain conditions.
func (t *Table) FindAll(terms ...interface{}) []db.Item {
	var itop int

	var relate interface{}
	var relateAll interface{}

	fields := "*"
	conditions := ""
	limit := ""
	offset := ""
	sort := ""

	// Analyzing
	itop = len(terms)

	for i := 0; i < itop; i++ {
		term := terms[i]

		switch term.(type) {
		case db.Limit:
			limit = fmt.Sprintf("LIMIT %v", term.(db.Limit))
		case db.Sort:
			sortBy := []string{}
			for k, v := range term.(db.Sort) {
				v = strings.ToUpper(to.String(v))
				if v == "-1" {
					v = "DESC"
				}
				if v == "1" {
					v = "ASC"
				}
				sortBy = append(sortBy, fmt.Sprintf("%s %s", k, v))
			}
			sort = fmt.Sprintf("ORDER BY %s", strings.Join(sortBy, ", "))
		case db.Offset:
			offset = fmt.Sprintf("OFFSET %v", term.(db.Offset))
		case db.Fields:
			fields = strings.Join(term.(db.Fields), ", ")
		case db.Relate:
			relate = term.(db.Relate)
		case db.RelateAll:
			relateAll = term.(db.RelateAll)
		}
	}

	conditions, args := t.compileConditions(terms)

	if conditions == "" {
		conditions = "1 = 1"
	}

	rows, err := t.parent.doQuery(
		fmt.Sprintf("SELECT %s FROM %s", fields, t.Name()),
		fmt.Sprintf("WHERE %s", conditions), args,
		sort, limit, offset,
	)

	if err != nil {
		panic(err)
	}

	result := t.sqlFetchAll(rows)

	var relations []sugar.Map
	var rcollection db.Collection

	// This query is related to other collections.
	if relate != nil {
		for rname, rterms := range relate.(db.Relate) {

			rcollection = nil

			ttop := len(rterms)
			for t := ttop - 1; t >= 0; t-- {
				rterm := rterms[t]
				switch rterm.(type) {
				case db.Collection:
					rcollection = rterm.(db.Collection)
				}
			}

			if rcollection == nil {
				rcollection = t.parent.ExistentCollection(rname)
			}

			relations = append(relations, sugar.Map{"all": false, "name": rname, "collection": rcollection, "terms": rterms})
		}
	}

	if relateAll != nil {
		for rname, rterms := range relateAll.(db.RelateAll) {
			rcollection = nil

			ttop := len(rterms)
			for t := ttop - 1; t >= 0; t-- {
				rterm := rterms[t]
				switch rterm.(type) {
				case db.Collection:
					rcollection = rterm.(db.Collection)
				}
			}

			if rcollection == nil {
				rcollection = t.parent.ExistentCollection(rname)
			}

			relations = append(relations, sugar.Map{"all": true, "name": rname, "collection": rcollection, "terms": rterms})
		}
	}

	var term interface{}

	jtop := len(relations)

	itop = len(result)
	items := make([]db.Item, itop)

	for i := 0; i < itop; i++ {

		item := db.Item{}

		// Default values.
		for key, val := range result[i] {
			item[key] = val
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
				value := relation["collection"].(*Table).invoke("FindAll", terms)
				item[relation["name"].(string)] = value[0].Interface().([]db.Item)
			} else {
				value := relation["collection"].(*Table).invoke("Find", terms)
				item[relation["name"].(string)] = value[0].Interface().(db.Item)
			}

		}

		// Appending to results.
		items[i] = item
	}

	return items
}

// Returns the number of rows in the current table that match certain conditions.
func (t *Table) Count(terms ...interface{}) (int, error) {

	terms = append(terms, db.Fields{"COUNT(1) AS _total"})

	result := t.invoke("FindAll", terms)

	if len(result) > 0 {
		response := result[0].Interface().([]db.Item)
		if len(response) > 0 {
			val, _ := strconv.Atoi(response[0]["_total"].(string))
			return val, nil
		}
	}

	return 0, nil
}

// Returns the first row in the table that matches certain conditions.
func (t *Table) Find(terms ...interface{}) db.Item {

	var item db.Item

	terms = append(terms, db.Limit(1))

	result := t.invoke("FindAll", terms)

	if len(result) > 0 {
		response := result[0].Interface().([]db.Item)
		if len(response) > 0 {
			item = response[0]
		}
	}

	return item
}

// Inserts rows into the currently active table.
func (t *Table) Append(items ...interface{}) ([]db.Id, error) {

	ids := []db.Id{}

	itop := len(items)

	for i := 0; i < itop; i++ {

		values := []string{}
		fields := []string{}

		item := items[i]

		for field, value := range item.(db.Item) {
			fields = append(fields, field)
			values = append(values, toInternal(value))
		}

		// https://github.com/bmizerany/pq/issues/24
		row, err := t.parent.doQueryRow(
			"INSERT INTO",
			t.Name(),
			sqlFields(fields),
			"VALUES",
			sqlValues(values),
			"RETURNING id",
		)

		// Error ocurred, stop appending.
		if err != nil {
			return ids, err
		}

		var id int
		err = row.Scan(&id)

		// Error ocurred, stop appending.
		if err != nil {
			return ids, err
		}

		// Last inserted ID could be zero too.
		ids = append(ids, db.Id(to.String(id)))
	}

	return ids, nil
}
