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
	"fmt"
	"github.com/xiam/gosexy"
	"github.com/xiam/gosexy/db"
	_ "github.com/xiam/gosqlite3"
	"reflect"
	"regexp"
	"strconv"
	"strings"
)

type slQuery struct {
	Query   []string
	SqlArgs []string
}

func slCompile(terms []interface{}) *slQuery {
	q := &slQuery{}

	q.Query = []string{}

	for _, term := range terms {
		switch term.(type) {
		case string:
			{
				q.Query = append(q.Query, term.(string))
			}
		case db.SqlArgs:
			{
				for _, arg := range term.(db.SqlArgs) {
					q.SqlArgs = append(q.SqlArgs, arg)
				}
			}
		case db.SqlValues:
			{
				args := make([]string, len(term.(db.SqlValues)))
				for i, arg := range term.(db.SqlValues) {
					args[i] = "?"
					q.SqlArgs = append(q.SqlArgs, arg)
				}
				q.Query = append(q.Query, "("+strings.Join(args, ", ")+")")
			}
		}
	}

	return q
}

func slTable(name string) string {
	return name
}

func slFields(names []string) string {
	return "(" + strings.Join(names, ", ") + ")"
}

func slValues(values []string) db.SqlValues {
	ret := make(db.SqlValues, len(values))
	for i, _ := range values {
		ret[i] = values[i]
	}
	return ret
}

// Stores driver's session data.
type SqliteDataSource struct {
	config      db.DataSource
	session     *sql.DB
	collections map[string]db.Collection
}

func (t *SqliteTable) slFetchAll(rows sql.Rows) []db.Item {

	items := []db.Item{}

	columns, _ := rows.Columns()

	for i, _ := range columns {
		columns[i] = strings.ToLower(columns[i])
	}

	res := map[string]*sql.RawBytes{}

	fargs := []reflect.Value{}

	for _, name := range columns {
		res[name] = &sql.RawBytes{}
		fargs = append(fargs, reflect.ValueOf(res[name]))
	}

	sn := reflect.ValueOf(&rows)
	fn := sn.MethodByName("Scan")

	for rows.Next() {
		item := db.Item{}

		ret := fn.Call(fargs)

		if ret[0].IsNil() != true {
			panic(ret[0].Elem().Interface().(error))
		}

		for _, name := range columns {
			strval := fmt.Sprintf("%s", *res[name])

			switch t.types[name] {
			case reflect.Uint64:
				{
					intval, _ := strconv.Atoi(strval)
					item[name] = uint64(intval)
				}
			case reflect.Int64:
				{
					intval, _ := strconv.Atoi(strval)
					item[name] = intval
				}
			case reflect.Float64:
				{
					floatval, _ := strconv.ParseFloat(strval, 10)
					item[name] = floatval
				}
			default:
				{
					item[name] = strval
				}
			}
		}

		items = append(items, item)
	}

	return items
}

func (sl *SqliteDataSource) slExec(method string, terms ...interface{}) sql.Rows {

	var rows sql.Rows

	sn := reflect.ValueOf(sl.session)
	fn := sn.MethodByName(method)

	q := slCompile(terms)

	/*
		fmt.Printf("Q: %v\n", q.Query)
		fmt.Printf("A: %v\n", q.SqlArgs)
	*/

	args := make([]reflect.Value, len(q.SqlArgs)+1)

	args[0] = reflect.ValueOf(strings.Join(q.Query, " "))

	for i := 0; i < len(q.SqlArgs); i++ {
		args[1+i] = reflect.ValueOf(q.SqlArgs[i])
	}

	res := fn.Call(args)

	if res[1].IsNil() == false {
		panic(res[1].Elem().Interface().(error))
	}

	switch res[0].Elem().Interface().(type) {
	case sql.Rows:
		{
			rows = res[0].Elem().Interface().(sql.Rows)
		}
	}

	return rows

	//return res[0].Elem().Interface().(sql.Rows)

}

// Represents a SQLite table.
type SqliteTable struct {
	parent *SqliteDataSource
	name   string
	types  map[string]reflect.Kind
}

// Configures and returns a SQLite database session.
func SqliteSession(config db.DataSource) db.Database {
	m := &SqliteDataSource{}
	m.config = config
	m.collections = make(map[string]db.Collection)
	return m
}

// Returns a *sql.DB object that represents an internal session.
func (sl *SqliteDataSource) Driver() interface{} {
	return sl.session
}

// Tries to open a connection to the current SQLite session.
func (sl *SqliteDataSource) Open() error {
	var err error

	if sl.config.Database == "" {
		panic("Database name is required.")
	}

	conn := sl.config.Database

	sl.session, err = sql.Open("sqlite3", conn)

	if err != nil {
		return fmt.Errorf("Could not connect to %s", sl.config.Host)
	}

	return nil
}

// Closes a previously opened SQLite database session.
func (sl *SqliteDataSource) Close() error {
	if sl.session != nil {
		return sl.session.Close()
	}
	return nil
}

// Changes the active database.
func (sl *SqliteDataSource) Use(database string) error {
	sl.config.Database = database
	sl.session.Query(fmt.Sprintf("USE %s", database))
	return nil
}

// Deletes the currently active database.
func (sl *SqliteDataSource) Drop() error {
	sl.session.Query(fmt.Sprintf("DROP DATABASE %s", sl.config.Database))
	return nil
}

// Returns the list of SQLite tables in the current database.
func (sl *SqliteDataSource) Collections() []string {
	var collections []string
	var collection string

	rows, _ := sl.session.Query("SELECT tbl_name FROM sqlite_master WHERE type = ?", "table")

	for rows.Next() {
		rows.Scan(&collection)
		collections = append(collections, collection)
	}

	return collections
}

func (t *SqliteTable) invoke(fn string, terms []interface{}) []reflect.Value {

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

func (t *SqliteTable) compileSet(term db.Set) (string, db.SqlArgs) {
	sql := []string{}
	args := db.SqlArgs{}

	for key, arg := range term {
		sql = append(sql, fmt.Sprintf("%s = ?", key))
		args = append(args, fmt.Sprintf("%v", arg))
	}

	return strings.Join(sql, ", "), args
}

func (t *SqliteTable) compileConditions(term interface{}) (string, db.SqlArgs) {
	sql := []string{}
	args := db.SqlArgs{}

	switch term.(type) {
	case []interface{}:
		{

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
		}
	case db.Or:
		{

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
		}
	case db.And:
		{

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
		}
	case db.Where:
		{
			return t.marshal(term.(db.Where))

		}
	}

	return "", args
}

func (t *SqliteTable) marshal(where db.Where) (string, []string) {

	for key, val := range where {
		key = strings.Trim(key, " ")
		chunks := strings.Split(key, " ")

		strval := fmt.Sprintf("%v", val)

		if len(chunks) >= 2 {
			return fmt.Sprintf("%s %s ?", chunks[0], chunks[1]), []string{strval}
		} else {
			return fmt.Sprintf("%s = ?", chunks[0]), []string{strval}
		}

	}

	return "", []string{}
}

// Deletes all the rows in the table.
func (t *SqliteTable) Truncate() bool {

	t.parent.slExec(
		"Exec",
		fmt.Sprintf("DELETE FROM %s", slTable(t.name)),
	)

	return false
}

// Deletes all the rows in the table that match certain conditions.
func (t *SqliteTable) Remove(terms ...interface{}) bool {

	conditions, cargs := t.compileConditions(terms)

	if conditions == "" {
		conditions = "1 = 1"
	}

	t.parent.slExec(
		"Exec",
		fmt.Sprintf("DELETE FROM %s", slTable(t.name)),
		fmt.Sprintf("WHERE %s", conditions), cargs,
	)

	return true
}

// Modifies all the rows in the table that match certain conditions.
func (t *SqliteTable) Update(terms ...interface{}) bool {
	var fields string
	var fargs db.SqlArgs

	conditions, cargs := t.compileConditions(terms)

	for _, term := range terms {
		switch term.(type) {
		case db.Set:
			{
				fields, fargs = t.compileSet(term.(db.Set))
			}
		}
	}

	if conditions == "" {
		conditions = "1 = 1"
	}

	t.parent.slExec(
		"Exec",
		fmt.Sprintf("UPDATE %s SET %s", slTable(t.name), fields), fargs,
		fmt.Sprintf("WHERE %s", conditions), cargs,
	)

	return true
}

// Returns all the rows in the table that match certain conditions.
func (t *SqliteTable) FindAll(terms ...interface{}) []db.Item {
	var itop int

	var relate interface{}
	var relateAll interface{}

	fields := "*"
	conditions := ""
	limit := ""
	offset := ""

	// Analyzing
	itop = len(terms)

	for i := 0; i < itop; i++ {
		term := terms[i]

		switch term.(type) {
		case db.Limit:
			{
				limit = fmt.Sprintf("LIMIT %v", term.(db.Limit))
			}
		case db.Offset:
			{
				offset = fmt.Sprintf("OFFSET %v", term.(db.Offset))
			}
		case db.Fields:
			{
				fields = strings.Join(term.(db.Fields), ", ")
			}
		case db.Relate:
			{
				relate = term.(db.Relate)
			}
		case db.RelateAll:
			{
				relateAll = term.(db.RelateAll)
			}
		}
	}

	conditions, args := t.compileConditions(terms)

	if conditions == "" {
		conditions = "1 = 1"
	}

	rows := t.parent.slExec(
		"Query",
		fmt.Sprintf("SELECT %s FROM %s", fields, slTable(t.name)),
		fmt.Sprintf("WHERE %s", conditions), args,
		limit, offset,
	)

	result := t.slFetchAll(rows)

	var relations []gosexy.Tuple
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
					{
						rcollection = rterm.(db.Collection)
					}
				}
			}

			if rcollection == nil {
				rcollection = t.parent.Collection(rname)
			}

			relations = append(relations, gosexy.Tuple{"all": false, "name": rname, "collection": rcollection, "terms": rterms})
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
					{
						rcollection = rterm.(db.Collection)
					}
				}
			}

			if rcollection == nil {
				rcollection = t.parent.Collection(rname)
			}

			relations = append(relations, gosexy.Tuple{"all": true, "name": rname, "collection": rcollection, "terms": rterms})
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
				// Just waiting for db.Where statements.
				case db.Where:
					{
						for wkey, wval := range term.(db.Where) {
							//if reflect.TypeOf(wval).Kind() == reflect.String { // does not always work.
							if reflect.TypeOf(wval).Name() == "string" {
								// Matching dynamic values.
								matched, _ := regexp.MatchString("\\{.+\\}", wval.(string))
								if matched {
									// Replacing dynamic values.
									kname := strings.Trim(wval.(string), "{}")
									term = db.Where{wkey: item[kname]}
								}
							}
						}
					}
				}
				terms = append(terms, term)
			}

			// Executing external query.
			if relation["all"] == true {
				value := relation["collection"].(*SqliteTable).invoke("FindAll", terms)
				item[relation["name"].(string)] = value[0].Interface().([]db.Item)
			} else {
				value := relation["collection"].(*SqliteTable).invoke("Find", terms)
				item[relation["name"].(string)] = value[0].Interface().(db.Item)
			}

		}

		// Appending to results.
		items[i] = item
	}

	return items
}

// Returns the number of rows in the current table that match certain conditions.
func (t *SqliteTable) Count(terms ...interface{}) int {

	terms = append(terms, db.Fields{"COUNT(1) AS _total"})

	result := t.invoke("FindAll", terms)

	if len(result) > 0 {
		response := result[0].Interface().([]db.Item)
		if len(response) > 0 {
			val, _ := strconv.Atoi(response[0]["_total"].(string))
			return val
		}
	}

	return 0
}

// Returns the first row in the table that matches certain conditions.
func (t *SqliteTable) Find(terms ...interface{}) db.Item {

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
func (t *SqliteTable) Append(items ...interface{}) bool {

	itop := len(items)

	for i := 0; i < itop; i++ {

		values := []string{}
		fields := []string{}

		item := items[i]

		for field, value := range item.(db.Item) {
			fields = append(fields, field)
			values = append(values, fmt.Sprintf("%v", value))
		}

		t.parent.slExec(
			"Exec",
			"INSERT INTO",
			slTable(t.name),
			slFields(fields),
			"VALUES",
			slValues(values),
		)

	}

	return true
}

// Returns a SQLite table structure by name.
func (sl *SqliteDataSource) Collection(name string) db.Collection {

	if collection, ok := sl.collections[name]; ok == true {
		return collection
	}

	t := &SqliteTable{}

	t.parent = sl
	t.name = name

	// Fetching table datatypes and mapping to internal gotypes.

	rows, err := t.parent.session.Query(fmt.Sprintf("PRAGMA TABLE_INFO('%s')", t.name))

	if err != nil {
		panic(err)
	}

	columns := t.slFetchAll(*rows)

	pattern, _ := regexp.Compile("^([a-z]+)\\(?([0-9,]+)?\\)?\\s?([a-z]*)?")

	t.types = make(map[string]reflect.Kind, len(columns))

	for _, column := range columns {

		cname := strings.ToLower(column["name"].(string))
		ctype := strings.ToLower(column["type"].(string))

		results := pattern.FindStringSubmatch(ctype)

		// Default properties.
		dextra := ""
		dtype := "text"

		dtype = results[1]

		if len(results) > 3 {
			dextra = results[3]
		}

		vtype := reflect.String

		// Guessing datatypes.
		switch dtype {
		case "integer":
			{
				if dextra == "unsigned" {
					vtype = reflect.Uint64
				} else {
					vtype = reflect.Int64
				}
			}
		case "real", "numeric":
			{
				vtype = reflect.Float64
			}
		default:
			{
				vtype = reflect.String
			}
		}

		/*
		   fmt.Printf("Imported %v (from %v)\n", vtype, dtype)
		*/

		t.types[cname] = vtype
	}

	sl.collections[name] = t

	return t
}
