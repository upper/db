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

package sqlite

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/gosexy/db"
	"github.com/gosexy/to"
	"reflect"
	"regexp"
	"strings"
	"time"
)

func compareColumnToField(s, c string) bool {
	return strings.ToLower(s) == strings.ToLower(c)
}

// Returns the name of the table.
func (self *Table) Name() string {
	return self.name
}

func (self *Table) Fetch(dst interface{}, terms ...interface{}) error {

	/*
		At this moment it is not possible to create a slice of a given element
		type: https://code.google.com/p/go/issues/detail?id=2339

		When it gets available this function should change, it must rely on
		FetchAll() the same way Find() relies on FindAll().
	*/

	found := self.Find(terms...)

	dstv := reflect.ValueOf(dst)

	if dstv.Kind() != reflect.Ptr || dstv.IsNil() {
		return errors.New("Fetch() expects a pointer.")
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
		return nil
	}

	return nil
}

func (self *Table) FetchAll(dst interface{}, terms ...interface{}) error {

	var err error

	var relate interface{}
	var relateAll interface{}

	fields := "*"
	conditions := ""
	limit := ""
	offset := ""
	sort := ""
	sortBy := []string{}

	// Analyzing
	for _, term := range terms {

		switch v := term.(type) {
		case db.Limit:
			limit = fmt.Sprintf("LIMIT %d", v)
		case db.Sort:
			for sk, sv := range v {
				sv = strings.ToUpper(to.String(sv))
				if sv == "-1" {
					sv = "DESC"
				}
				if sv == "1" {
					sv = "ASC"
				}
				sortBy = append(sortBy, fmt.Sprintf("%s %s", sk, sv))
			}
		case db.Offset:
			offset = fmt.Sprintf("OFFSET %d", v)
		case db.Fields:
			fields = strings.Join(v, ", ")
		case db.Relate:
			relate = v
		case db.RelateAll:
			relateAll = v
		}
	}

	if len(sortBy) > 0 {
		sort = fmt.Sprintf("ORDER BY %s", strings.Join(sortBy, ", "))
	}

	conditions, args := self.compileConditions(terms)

	if conditions == "" {
		conditions = "1 = 1"
	}

	var col db.Collection
	var relations []db.Relation

	// This query is related to other collections.
	if relate != nil {

		i := 0

		for name, terms := range relate.(db.Relate) {

			col = nil

			for _, term := range terms {
				switch t := term.(type) {
				case db.Collection:
					col = t
				}
			}

			if col == nil {
				col = self.parent.ExistentCollection(name)
			}

			relations = append(relations, db.Relation{All: false, Name: name, Collection: col, On: terms})

			i++
		}
	}

	if relateAll != nil {

		i := 0

		for name, terms := range relateAll.(db.RelateAll) {

			col = nil

			for _, term := range terms {
				switch t := term.(type) {
				case db.Collection:
					col = t
				}
			}

			if col == nil {
				col = self.parent.ExistentCollection(name)
			}

			relations = append(relations, db.Relation{All: true, Name: name, Collection: col, On: terms})

			i++
		}
	}

	rows, err := self.parent.doQuery(
		fmt.Sprintf("SELECT %s FROM %s", fields, self.Name()),
		fmt.Sprintf("WHERE %s", conditions), args,
		sort, limit, offset,
	)

	if err != nil {
		return err
	}

	err = self.fetchRows(dst, rows)

	if err != nil {
		return err
	}

	dstv := reflect.ValueOf(dst)

	if dstv.Kind() != reflect.Ptr || dstv.Elem().Kind() != reflect.Slice || dstv.IsNil() {
		return errors.New("fetchRows expects a pointer to slice.")
	}

	itemv := dstv.Elem().Type()

	switch itemv.Elem().Kind() {
	case reflect.Struct:
		fmt.Printf("got sutrct\n")
	case reflect.Map:
		// Iterate over all results.
		for i := 0; i < dstv.Elem().Len(); i++ {

			item := dstv.Elem().Index(i)

			for _, relation := range relations {

				terms := []interface{}{}

				for _, term := range relation.On {
					switch term.(type) {
					// Just waiting for db.Cond statements.
					case db.Cond:
						for k, v := range term.(db.Cond) {
							switch s := v.(type) {
							case string:
								// Matching dynamic values.
								matched, _ := regexp.MatchString("\\{.+\\}", s)
								if matched == true {
									// Replacing dynamic values.
									ik := strings.Trim(s, "{}")
									term = db.Cond{k: item.MapIndex(reflect.ValueOf(ik)).Interface()}
								}
							}
						}
					}
					terms = append(terms, term)
				}

				keyv := reflect.ValueOf(relation.Name)

				// Executing external query.
				if relation.All == true {
					item.SetMapIndex(keyv, reflect.ValueOf(relation.Collection.FindAll(terms...)))
				} else {
					item.SetMapIndex(keyv, reflect.ValueOf(relation.Collection.Find(terms...)))
				}
			}

		}
	}

	return nil
}

// Returns all items from a query.
func (self *Table) fetchRows(dst interface{}, rows *sql.Rows) error {

	dstv := reflect.ValueOf(dst)

	if dstv.Kind() != reflect.Ptr || dstv.Elem().Kind() != reflect.Slice || dstv.IsNil() {
		return errors.New("fetchRows expects a pointer to slice.")
	}

	columns, err := rows.Columns()

	if err != nil {
		return err
	}

	for i, _ := range columns {
		columns[i] = strings.ToLower(columns[i])
	}

	expecting := len(columns)

	slicev := dstv.Elem()
	itemt := slicev.Type().Elem()

	for rows.Next() {

		values := make([]*sql.RawBytes, expecting)
		scanArgs := make([]interface{}, expecting)

		for i := range columns {
			scanArgs[i] = &values[i]
		}

		var item reflect.Value

		switch itemt.Kind() {
		case reflect.Map:
			item = reflect.MakeMap(itemt)
		case reflect.Struct:
			item = reflect.New(itemt)
		}

		err := rows.Scan(scanArgs...)

		if err != nil {
			return err
		}

		for i, value := range values {
			if value != nil {
				column := columns[i]
				var cv reflect.Value
				if _, ok := self.types[column]; ok == true {
					v, _ := to.Convert(string(*value), self.types[column])
					cv = reflect.ValueOf(v)
				} else {
					v, _ := to.Convert(string(*value), reflect.String)
					cv = reflect.ValueOf(v)
				}
				switch itemt.Kind() {
				case reflect.Map:
					if cv.Type() != itemt {
						if item.Type().Elem().Kind() != reflect.Interface {
							c, _ := to.Convert(string(*value), item.Type().Elem().Kind())
							cv = reflect.ValueOf(c)
						}
					}
					item.SetMapIndex(reflect.ValueOf(column), cv)
				case reflect.Struct:
					f := func(s string) bool {
						return strings.ToLower(s) == column
					}
					v := item.Elem().FieldByNameFunc(f)
					if v.IsValid() {
						if cv.Type() != itemt {
							if v.Type().Kind() != reflect.Interface {
								c, _ := to.Convert(string(*value), v.Type().Kind())
								cv = reflect.ValueOf(c)
							}
						}
						v.Set(cv)
					}
				}
			}
		}

		slicev = reflect.Append(slicev, reflect.Indirect(item))
	}

	dstv.Elem().Set(slicev)

	return nil
}

// Transforms db.Set into arguments for sql.Exec/sql.Query.
func (self *Table) compileSet(term db.Set) (string, db.SqlArgs) {
	sql := make([]string, len(term))
	args := make(db.SqlArgs, len(term))

	i := 0
	for key, arg := range term {
		sql[i] = fmt.Sprintf("%s = ?", key)
		args[i] = to.String(arg)
		i++
	}

	return strings.Join(sql, ", "), args
}

// Transforms conditions into arguments for sql.Exec/sql.Query
func (self *Table) compileConditions(term interface{}) (string, db.SqlArgs) {
	sql := []string{}
	args := db.SqlArgs{}

	switch t := term.(type) {
	case []interface{}:
		for i := range t {
			rsql, rargs := self.compileConditions(t[i])
			if rsql != "" {
				sql = append(sql, rsql)
				args = append(args, rargs...)
			}
		}
		if len(sql) > 0 {
			return "(" + strings.Join(sql, " AND ") + ")", args
		}
	case db.Or:
		for i := range t {
			rsql, rargs := self.compileConditions(t[i])
			if rsql != "" {
				sql = append(sql, rsql)
				args = append(args, rargs...)
			}
		}
		if len(sql) > 0 {
			return "(" + strings.Join(sql, " OR ") + ")", args
		}
	case db.And:
		for i := range t {
			rsql, rargs := self.compileConditions(t[i])
			if rsql != "" {
				sql = append(sql, rsql)
				args = append(args, rargs...)
			}
		}
		if len(sql) > 0 {
			return "(" + strings.Join(sql, " AND ") + ")", args
		}
	case db.Cond:
		return self.compileStatement(t)
	}

	return "", args
}

// Transforms db.Cond into SQL conditions for sql.Exec/sql.Query
func (self *Table) compileStatement(where db.Cond) (string, []string) {

	for key, val := range where {
		key = strings.Trim(key, " ")
		chunks := strings.Split(key, " ")

		strval := to.String(val)

		if len(chunks) >= 2 {
			return fmt.Sprintf("%s %s ?", chunks[0], chunks[1]), []string{strval}
		} else {
			return fmt.Sprintf("%s = ?", chunks[0]), []string{strval}
		}

	}

	return "", []string{}
}

// Deletes all the rows in the table.
func (self *Table) Truncate() error {

	_, err := self.parent.doExec(
		fmt.Sprintf("DELETE FROM %s", self.Name()),
	)

	return err
}

// Deletes all the rows in the table that match certain conditions.
func (self *Table) Remove(terms ...interface{}) error {

	conds, args := self.compileConditions(terms)

	if conds == "" {
		conds = "1 = 1"
	}

	_, err := self.parent.doExec(
		fmt.Sprintf("DELETE FROM %s", self.Name()),
		fmt.Sprintf("WHERE %s", conds), args,
	)

	return err
}

// Modifies all the rows in the table that match certain conditions.
func (self *Table) Update(terms ...interface{}) error {
	var fields string
	var fargs db.SqlArgs

	conds, args := self.compileConditions(terms)

	for _, term := range terms {
		switch t := term.(type) {
		case db.Set:
			fields, fargs = self.compileSet(t)
		}
	}

	if conds == "" {
		conds = "1 = 1"
	}

	_, err := self.parent.doExec(
		fmt.Sprintf("UPDATE %s SET %s", self.Name(), fields), fargs,
		fmt.Sprintf("WHERE %s", conds), args,
	)

	return err
}

func (self *Table) FindAll(terms ...interface{}) []db.Item {
	results := []db.Item{}
	err := self.FetchAll(&results, terms...)
	if err != nil {
		panic(err)
	}
	return results
}

/*
// Returns all the rows in the table that match certain conditions.
func (self *Table) FindAll2(terms ...interface{}) []db.Item {

	var err error

	var relate interface{}
	var relateAll interface{}

	fields := "*"
	conditions := ""
	limit := ""
	offset := ""
	sort := ""
	sortBy := []string{}

	// Analyzing
	for _, term := range terms {

		switch v := term.(type) {
		case db.Limit:
			limit = fmt.Sprintf("LIMIT %d", v)
		case db.Sort:
			for sk, sv := range v {
				sv = strings.ToUpper(to.String(sv))
				if sv == "-1" {
					sv = "DESC"
				}
				if sv == "1" {
					sv = "ASC"
				}
				sortBy = append(sortBy, fmt.Sprintf("%s %s", sk, sv))
			}
		case db.Offset:
			offset = fmt.Sprintf("OFFSET %d", v)
		case db.Fields:
			fields = strings.Join(v, ", ")
		case db.Relate:
			relate = v
		case db.RelateAll:
			relateAll = v
		}
	}

	if len(sortBy) > 0 {
		sort = fmt.Sprintf("ORDER BY %s", strings.Join(sortBy, ", "))
	}

	conditions, args := self.compileConditions(terms)

	if conditions == "" {
		conditions = "1 = 1"
	}

	rows, err := self.parent.doQuery(
		fmt.Sprintf("SELECT %s FROM %s", fields, self.Name()),
		fmt.Sprintf("WHERE %s", conditions), args,
		sort, limit, offset,
	)

	// Will remove panics in a future version.
	if err != nil {
		panic(err)
	}

	result := []map[string]interface{}{}
	err = self.fetchRows(&result, rows)

	// Will remove panics in a future version.
	if err != nil {
		panic(err)
	}

	var col db.Collection
	var relations []db.Relation

	// This query is related to other collections.
	if relate != nil {

		i := 0

		for name, terms := range relate.(db.Relate) {

			col = nil

			for _, term := range terms {
				switch t := term.(type) {
				case db.Collection:
					col = t
				}
			}

			if col == nil {
				col = self.parent.ExistentCollection(name)
			}

			relations = append(relations, db.Relation{All: false, Name: name, Collection: col, On: terms})

			i++
		}
	}

	if relateAll != nil {

		i := 0

		for name, terms := range relateAll.(db.RelateAll) {

			col = nil

			for _, term := range terms {
				switch t := term.(type) {
				case db.Collection:
					col = t
				}
			}

			if col == nil {
				col = self.parent.ExistentCollection(name)
			}

			relations = append(relations, db.Relation{All: true, Name: name, Collection: col, On: terms})

			i++
		}
	}

	items := make([]db.Item, len(result))

	for i, item := range result {

		// Querying relations
		for _, relation := range relations {

			terms := []interface{}{}

			for _, term := range relation.On {
				switch term.(type) {
				// Just waiting for db.Cond statements.
				case db.Cond:
					for k, v := range term.(db.Cond) {
						switch s := v.(type) {
						case string:
							// Matching dynamic values.
							matched, _ := regexp.MatchString("\\{.+\\}", s)
							if matched == true {
								// Replacing dynamic values.
								ik := strings.Trim(s, "{}")
								term = db.Cond{k: item[ik]}
							}
						}
					}
				}
				terms = append(terms, term)
			}

			// Executing external query.
			if relation.All == true {
				item[relation.Name] = relation.Collection.FindAll(terms...)
			} else {
				item[relation.Name] = relation.Collection.Find(terms...)
			}

		}

		// Appending to results.
		items[i] = item
	}

	return items
}
*/

// Returns the number of rows in the current table that match certain conditions.
func (self *Table) Count(terms ...interface{}) (int, error) {

	terms = append(terms, db.Fields{"COUNT(1) AS _total"})

	result := self.FindAll(terms...)

	if len(result) > 0 {
		return to.Int(result[0]["_total"]), nil
	}

	return 0, nil
}

// Returns the first row in the table that matches certain conditions.
func (self *Table) Find(terms ...interface{}) db.Item {

	terms = append(terms, db.Limit(1))

	result := self.FindAll(terms...)

	if len(result) > 0 {
		return result[0]
	}

	return nil
}

func toInternal(val interface{}) string {

	switch t := val.(type) {
	case []byte:
		return string(t)
	case time.Time:
		return t.Format(DateFormat)
	case time.Duration:
		return fmt.Sprintf(TimeFormat, int(t.Hours()), int(t.Minutes())%60, int(t.Seconds())%60, uint64(t.Nanoseconds())%1e9)
	case bool:
		if t == true {
			return "1"
		} else {
			return "0"
		}
	}

	return to.String(val)
}

func toNative(val interface{}) interface{} {
	return val
}

// Inserts rows into the currently active table.
func (self *Table) Append(items ...interface{}) ([]db.Id, error) {

	ids := make([]db.Id, len(items))

	for i, item := range items {

		var values []string
		var fields []string

		itemv := reflect.ValueOf(item)
		itemt := itemv.Type()

		switch itemt.Kind() {
		case reflect.Struct:
			nfields := itemv.NumField()
			values = make([]string, nfields)
			fields = make([]string, nfields)
			for i := 0; i < nfields; i++ {
				fields[i] = itemt.Field(i).Name
				values[i] = toInternal(itemv.Field(i).Interface())
			}
		case reflect.Map:
			nfields := itemv.Len()
			values = make([]string, nfields)
			fields = make([]string, nfields)
			mkeys := itemv.MapKeys()
			for i, keyv := range mkeys {
				valv := itemv.MapIndex(keyv)
				fields[i] = to.String(keyv.Interface())
				values[i] = toInternal(valv.Interface())
			}
		default:
			return ids, fmt.Errorf("Append() accepts Struct or Map only, %v received.", itemt.Kind())
		}

		res, err := self.parent.doExec(
			"INSERT INTO",
			self.Name(),
			sqlFields(fields),
			"VALUES",
			sqlValues(values),
		)

		// Error ocurred, stop appending.
		if err != nil {
			return ids, err
		}

		// Last inserted ID could be zero too.
		id, _ := res.LastInsertId()
		ids[i] = db.Id(to.String(id))
	}

	return ids, nil
}

// Returns true if the collection exists.
func (self *Table) Exists() bool {
	result, err := self.parent.doQuery(
		fmt.Sprintf(`
			SELECT name
				FROM sqlite_master
				WHERE type = 'table' AND name = '%s'
			`,
			self.Name(),
		),
	)
	if err != nil {
		return false
	}
	if result.Next() == true {
		result.Close()
		return true
	}
	return false
}
