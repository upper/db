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
	"errors"
	"fmt"
	"github.com/gosexy/db"
	"github.com/gosexy/to"
	"reflect"
	"regexp"
	"strings"
	"time"
)

var extRelationPattern = regexp.MustCompile(`\{(.+)\}`)
var columnComparePattern = regexp.MustCompile(`[^a-zA-Z0-9]`)

var durationType = reflect.TypeOf(time.Duration(0))
var timeType = reflect.TypeOf(time.Time{})

// Represents a PostgreSQL table.
type Table struct {
	parent *Source
	name   string
	types  map[string]reflect.Kind
}

func (self *Table) columnLike(s string) string {
	for col, _ := range self.types {
		if compareColumnToField(s, col) == true {
			return col
		}
	}
	return s
}

func convertValue(src string, dstk reflect.Kind) (reflect.Value, error) {
	var srcv reflect.Value

	// Destination type.
	switch dstk {
	case reflect.Interface:
		// Destination is interface, nuff said.
		srcv = reflect.ValueOf(src)
	case durationType.Kind():
		// Destination is time.Duration
		srcv = reflect.ValueOf(to.Duration(src))
	case timeType.Kind():
		// Destination is time.Time
		srcv = reflect.ValueOf(to.Time(src))
	default:
		// Destination is of an unknown type.
		cv, _ := to.Convert(src, dstk)
		srcv = reflect.ValueOf(cv)
	}

	return srcv, nil
}

/*
	Returns true if a table column looks like a struct field.
*/
func compareColumnToField(s, c string) bool {
	s = columnComparePattern.ReplaceAllString(s, "")
	c = columnComparePattern.ReplaceAllString(c, "")
	return strings.ToLower(s) == strings.ToLower(c)
}

/*
	Returns the table name as a string.
*/
func (self *Table) Name() string {
	return self.name
}

/*
	Returns true if the collection exists.
*/
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
		return false
	}
	if result.Next() == true {
		result.Close()
		return true
	}
	return false
}

/*
	Fetches a result delimited by terms into a pointer to map or struct given by
	dst.
*/
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
func (self *Table) FetchAll(dst interface{}, terms ...interface{}) error {

	var err error

	var dstv reflect.Value
	var itemv reflect.Value
	var itemk reflect.Kind

	queryChunks := struct {
		Fields     []string
		Limit      string
		Offset     string
		Sort       string
		Relate     db.Relate
		RelateAll  db.RelateAll
		Relations  []db.Relation
		Conditions string
		Arguments  db.SqlArgs
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
			if queryChunks.Limit == "" {
				queryChunks.Limit = fmt.Sprintf("LIMIT %d", v)
			} else {
				return errors.New("A query can accept only one db.Limit() parameter.")
			}
		case db.Sort:
			if queryChunks.Sort == "" {
				sortChunks := make([]string, len(v))
				i := 0
				for column, sort := range v {
					sort = strings.ToUpper(to.String(sort))
					if sort == "-1" {
						sort = "DESC"
					}
					if sort == "1" {
						sort = "ASC"
					}
					sortChunks[i] = fmt.Sprintf("%s %s", column, sort)
					i++
				}
				queryChunks.Sort = fmt.Sprintf("ORDER BY %s", strings.Join(sortChunks, ", "))
			} else {
				return errors.New("A query can accept only one db.Sort{} parameter.")
			}
		case db.Offset:
			if queryChunks.Offset == "" {
				queryChunks.Offset = fmt.Sprintf("OFFSET %d", v)
			} else {
				return errors.New("A query can accept only one db.Offset() parameter.")
			}
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

	// Compiling conditions
	queryChunks.Conditions, queryChunks.Arguments = self.compileConditions(terms)

	if queryChunks.Conditions == "" {
		queryChunks.Conditions = "1 = 1"
	}

	// Actually executing query.
	rows, err := self.parent.doQuery(
		// Mandatory
		fmt.Sprintf(`SELECT %s FROM "%s"`, strings.Join(queryChunks.Fields, ", "), self.Name()),
		fmt.Sprintf("WHERE %s", queryChunks.Conditions), queryChunks.Arguments,
		// Optional
		queryChunks.Sort, queryChunks.Limit, queryChunks.Offset,
	)

	if err != nil {
		return err
	}

	// Fetching rows.
	err = self.fetchRows(dst, rows)

	if err != nil {
		return err
	}

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
										term = db.Cond{k: val.Interface()}
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

/*
	Copies *sql.Rows into the slice of maps or structs given by the pointer dst.
*/
func (self *Table) fetchRows(dst interface{}, rows *sql.Rows) error {

	// Destination.
	dstv := reflect.ValueOf(dst)

	if dstv.Kind() != reflect.Ptr || dstv.Elem().Kind() != reflect.Slice || dstv.IsNil() {
		return errors.New("fetchRows expects a pointer to slice.")
	}

	// Column names.
	columns, err := rows.Columns()

	if err != nil {
		return err
	}

	// Column names to lower case.
	for i, _ := range columns {
		columns[i] = strings.ToLower(columns[i])
	}

	expecting := len(columns)

	slicev := dstv.Elem()
	itemt := slicev.Type().Elem()

	for rows.Next() {

		// Allocating results.
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
		default:
			return fmt.Errorf("Don't know how to deal with %s, use either map or struct.", itemt.Kind())
		}

		err := rows.Scan(scanArgs...)

		if err != nil {
			return err
		}

		// Range over row values.
		for i, value := range values {
			if value != nil {
				column := columns[i]
				svalue := string(*value)

				var cv reflect.Value

				if _, ok := self.types[column]; ok == true {
					v, _ := to.Convert(string(*value), self.types[column])
					cv = reflect.ValueOf(v)
				} else {
					v, _ := to.Convert(string(*value), reflect.String)
					cv = reflect.ValueOf(v)
				}

				switch itemt.Kind() {
				// Destination is a map.
				case reflect.Map:
					if cv.Type() != itemt {
						// Converting value.
						cv, _ = convertValue(svalue, item.Type().Elem().Kind())
					}
					if cv.IsValid() {
						item.SetMapIndex(reflect.ValueOf(column), cv)
					}
				// Destionation is a struct.
				case reflect.Struct:
					// Get appropriate column.
					f := func(s string) bool {
						return compareColumnToField(s, column)
					}
					// Destination field.
					destf := item.Elem().FieldByNameFunc(f)
					if destf.IsValid() {
						if cv.Type().Kind() != destf.Type().Kind() {
							// Converting value.
							cv, _ = convertValue(svalue, destf.Type().Kind())
						}
						// Copying value.
						if cv.IsValid() {
							destf.Set(cv)
						}
					}
				}
			}
		}

		slicev = reflect.Append(slicev, reflect.Indirect(item))
	}

	dstv.Elem().Set(slicev)

	return nil
}

/*
	Transforms db.Set into arguments for sql.Exec/sql.Query.
*/
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

/*
	Transforms conditions into arguments for sql.Exec/sql.Query
*/
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

/*
	Transforms db.Cond into SQL conditions for sql.Exec/sql.Query
*/
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

/*
	Deletes all the rows in the table.
*/
func (t *Table) Truncate() error {

	_, err := t.parent.doExec(
		fmt.Sprintf(`TRUNCATE TABLE "%s"`, t.Name()),
	)

	return err
}

/*
	Deletes all the rows in the table that match certain conditions.
*/
func (t *Table) Remove(terms ...interface{}) error {

	conditions, cargs := t.compileConditions(terms)

	if conditions == "" {
		conditions = "1 = 1"
	}

	_, err := t.parent.doExec(
		fmt.Sprintf(`DELETE FROM "%s"`, t.Name()),
		fmt.Sprintf("WHERE %s", conditions), cargs,
	)

	return err
}

/*
	Modifies all the rows in the table that match certain conditions.
*/
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
		fmt.Sprintf(`UPDATE "%s" SET %s`, self.Name(), fields), fargs,
		fmt.Sprintf("WHERE %s", conds), args,
	)

	return err
}

/*
	Returns a slice of rows that match certain conditions.
*/
func (self *Table) FindAll(terms ...interface{}) []db.Item {
	results := []db.Item{}
	err := self.FetchAll(&results, terms...)
	if err != nil {
		panic(err)
	}
	return results
}

/*
	Returns the number of rows in the current table that match certain conditions.
*/
func (self *Table) Count(terms ...interface{}) (int, error) {

	terms = append(terms, db.Fields{"COUNT(1) AS _total"})

	result := self.FindAll(terms...)

	if len(result) > 0 {
		return to.Int(result[0]["_total"]), nil
	}

	return 0, nil
}

/*
	Returns the first row in the table that matches certain conditions.
*/
func (self *Table) Find(terms ...interface{}) db.Item {

	terms = append(terms, db.Limit(1))

	result := self.FindAll(terms...)

	if len(result) > 0 {
		return result[0]
	}

	return nil
}

/*
	Converts a Go value into internal database representation.
*/
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

/*
	Convers a database representation (after auto-conversion) into a Go value.
*/
func toNative(val interface{}) interface{} {
	return val
}

/*
	Appends items into the table. An item could be either a map or a struct.
*/
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
				fields[i] = self.columnLike(itemt.Field(i).Name)
				values[i] = toInternal(itemv.Field(i).Interface())
			}
		case reflect.Map:
			nfields := itemv.Len()
			values = make([]string, nfields)
			fields = make([]string, nfields)
			mkeys := itemv.MapKeys()
			for i, keyv := range mkeys {
				valv := itemv.MapIndex(keyv)
				fields[i] = self.columnLike(to.String(keyv.Interface()))
				values[i] = toInternal(valv.Interface())
			}
		default:
			return ids, fmt.Errorf("Append() accepts Struct or Map only, %v received.", itemt.Kind())
		}

		row, err := self.parent.doQueryRow(
			fmt.Sprintf(`INSERT INTO "%s"`, self.Name()),
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
		ids[i] = db.Id(to.String(id))
	}

	return ids, nil
}
