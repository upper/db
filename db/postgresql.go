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
	"database/sql"
	"fmt"
	_ "github.com/xiam/gopostgresql"
	. "github.com/xiam/gosexy"
	"reflect"
	"regexp"
	"strconv"
	"strings"
)

type pgQuery struct {
	Query   []string
	sqlArgs []string
}

func pgCompile(terms []interface{}) *pgQuery {
	q := &pgQuery{}

	q.Query = []string{}

	for _, term := range terms {
		switch term.(type) {
		case string:
			{
				q.Query = append(q.Query, term.(string))
			}
		case sqlArgs:
			{
				for _, arg := range term.(sqlArgs) {
					q.sqlArgs = append(q.sqlArgs, arg)
				}
			}
		case sqlValues:
			{
				args := make([]string, len(term.(sqlValues)))
				for i, arg := range term.(sqlValues) {
					args[i] = "?"
					q.sqlArgs = append(q.sqlArgs, arg)
				}
				q.Query = append(q.Query, "("+strings.Join(args, ", ")+")")
			}
		}
	}

	return q
}

func pgTable(name string) string {
	return name
}

func pgFields(names []string) string {
	return "(" + strings.Join(names, ", ") + ")"
}

func pgValues(values []string) sqlValues {
	ret := make(sqlValues, len(values))
	for i, _ := range values {
		ret[i] = values[i]
	}
	return ret
}

type PostgresqlDataSource struct {
	config      DataSource
	session     *sql.DB
	collections map[string]Collection
}

func (t *PostgresqlTable) pgFetchAll(rows sql.Rows) []Item {

	items := []Item{}

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
		item := Item{}

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

func (pg *PostgresqlDataSource) pgExec(method string, terms ...interface{}) sql.Rows {

	sn := reflect.ValueOf(pg.session)
	fn := sn.MethodByName(method)

	q := pgCompile(terms)

	//fmt.Printf("Q: %v\n", q.Query)
	//fmt.Printf("A: %v\n", q.sqlArgs)

	qs := strings.Join(q.Query, " ")

	args := make([]reflect.Value, len(q.sqlArgs)+1)

	for i := 0; i < len(q.sqlArgs); i++ {
		qs = strings.Replace(qs, "?", fmt.Sprintf("$%d", i+1), 1)
		args[1+i] = reflect.ValueOf(q.sqlArgs[i])
	}

	args[0] = reflect.ValueOf(qs)

	res := fn.Call(args)

	if res[1].IsNil() == false {
		panic(res[1].Elem().Interface().(error))
	}

	return res[0].Elem().Interface().(sql.Rows)
}

type PostgresqlTable struct {
	parent *PostgresqlDataSource
	name   string
	types  map[string]reflect.Kind
}

func PostgresqlSession(config DataSource) Database {
	m := &PostgresqlDataSource{}
	m.config = config
	m.collections = make(map[string]Collection)
	return m
}

// Closes a previously opened MySQL database session.
func (pg *PostgresqlDataSource) Close() error {
	if pg.session != nil {
		return pg.session.Close()
	}
	return nil
}

func (pg *PostgresqlDataSource) Open() error {
	var err error

	if pg.config.Host == "" {
		pg.config.Host = "127.0.0.1"
	}

	if pg.config.Port == 0 {
		pg.config.Port = 5432
	}

	if pg.config.Database == "" {
		panic("Database name is required.")
	}

	conn := fmt.Sprintf("user=%s password=%s host=%s port=%d dbname=%s sslmode=disable", pg.config.User, pg.config.Password, pg.config.Host, pg.config.Port, pg.config.Database)

	pg.session, err = sql.Open("postgres", conn)

	if err != nil {
		return fmt.Errorf("Could not connect to %s", pg.config.Host)
	}

	return nil
}

func (pg *PostgresqlDataSource) Use(database string) error {
	pg.config.Database = database
	return pg.Open()
}

func (pg *PostgresqlDataSource) Drop() error {
	pg.session.Query(fmt.Sprintf("DROP DATABASE %s", pg.config.Database))
	return nil
}

// Returns a *sql.DB object that represents an internal session.
func (pg *PostgresqlDataSource) Driver() interface{} {
	return pg.session
}

func (pg *PostgresqlDataSource) Collections() []string {
	var collections []string
	var collection string
	rows, _ := pg.session.Query("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")

	for rows.Next() {
		rows.Scan(&collection)
		collections = append(collections, collection)
	}

	return collections
}

func (t *PostgresqlTable) invoke(fn string, terms []interface{}) []reflect.Value {

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

func (t *PostgresqlTable) compileSet(term Set) (string, sqlArgs) {
	sql := []string{}
	args := sqlArgs{}

	for key, arg := range term {
		sql = append(sql, fmt.Sprintf("%s = ?", key))
		args = append(args, fmt.Sprintf("%v", arg))
	}

	return strings.Join(sql, ", "), args
}

func (t *PostgresqlTable) compileConditions(term interface{}) (string, sqlArgs) {
	sql := []string{}
	args := sqlArgs{}

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
	case Or:
		{

			itop := len(term.(Or))

			for i := 0; i < itop; i++ {
				rsql, rargs := t.compileConditions(term.(Or)[i])
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
	case And:
		{

			itop := len(term.(Or))

			for i := 0; i < itop; i++ {
				rsql, rargs := t.compileConditions(term.(Or)[i])
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
	case Where:
		{
			return t.marshal(term.(Where))

		}
	}

	return "", args
}

func (t *PostgresqlTable) marshal(where Where) (string, []string) {

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

func (t *PostgresqlTable) Truncate() bool {

	t.parent.pgExec(
		"Query",
		fmt.Sprintf("TRUNCATE TABLE %s", pgTable(t.name)),
	)

	return false
}

func (t *PostgresqlTable) Remove(terms ...interface{}) bool {

	conditions, cargs := t.compileConditions(terms)

	if conditions == "" {
		conditions = "1 = 1"
	}

	t.parent.pgExec(
		"Query",
		fmt.Sprintf("DELETE FROM %s", pgTable(t.name)),
		fmt.Sprintf("WHERE %s", conditions), cargs,
	)

	return true
}

func (t *PostgresqlTable) Update(terms ...interface{}) bool {
	var fields string
	var fargs sqlArgs

	conditions, cargs := t.compileConditions(terms)

	for _, term := range terms {
		switch term.(type) {
		case Set:
			{
				fields, fargs = t.compileSet(term.(Set))
			}
		}
	}

	if conditions == "" {
		conditions = "1 = 1"
	}

	t.parent.pgExec(
		"Query",
		fmt.Sprintf("UPDATE %s SET %s", pgTable(t.name), fields), fargs,
		fmt.Sprintf("WHERE %s", conditions), cargs,
	)

	return true
}

func (t *PostgresqlTable) FindAll(terms ...interface{}) []Item {
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
		case Limit:
			{
				limit = fmt.Sprintf("LIMIT %v", term.(Limit))
			}
		case Offset:
			{
				offset = fmt.Sprintf("OFFSET %v", term.(Offset))
			}
		case Fields:
			{
				fields = strings.Join(term.(Fields), ", ")
			}
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

	conditions, args := t.compileConditions(terms)

	if conditions == "" {
		conditions = "1 = 1"
	}

	rows := t.parent.pgExec(
		"Query",
		fmt.Sprintf("SELECT %s FROM %s", fields, pgTable(t.name)),
		fmt.Sprintf("WHERE %s", conditions), args,
		limit, offset,
	)

	result := t.pgFetchAll(rows)

	var relations []Tuple
	var rcollection Collection

	// This query is related to other collections.
	if relate != nil {
		for rname, rterms := range relate.(Relate) {

			rcollection = nil

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

			if rcollection == nil {
				rcollection = t.parent.Collection(rname)
			}

			relations = append(relations, Tuple{"all": false, "name": rname, "collection": rcollection, "terms": rterms})
		}
	}

	if relateAll != nil {
		for rname, rterms := range relateAll.(RelateAll) {
			rcollection = nil

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

			if rcollection == nil {
				rcollection = t.parent.Collection(rname)
			}

			relations = append(relations, Tuple{"all": true, "name": rname, "collection": rcollection, "terms": rterms})
		}
	}

	var term interface{}

	jtop := len(relations)

	itop = len(result)
	items := make([]Item, itop)

	for i := 0; i < itop; i++ {

		item := Item{}

		// Default values.
		for key, val := range result[i] {
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
				value := relation["collection"].(*PostgresqlTable).invoke("FindAll", terms)
				item[relation["name"].(string)] = value[0].Interface().([]Item)
			} else {
				value := relation["collection"].(*PostgresqlTable).invoke("Find", terms)
				item[relation["name"].(string)] = value[0].Interface().(Item)
			}

		}

		// Appending to results.
		items[i] = item
	}

	return items
}

func (t *PostgresqlTable) Count(terms ...interface{}) int {

	terms = append(terms, Fields{"COUNT(1) AS _total"})

	result := t.invoke("FindAll", terms)

	if len(result) > 0 {
		response := result[0].Interface().([]Item)
		if len(response) > 0 {
			val, _ := strconv.Atoi(response[0]["_total"].(string))
			return val
		}
	}

	return 0
}

func (t *PostgresqlTable) Find(terms ...interface{}) Item {

	var item Item

	terms = append(terms, Limit(1))

	result := t.invoke("FindAll", terms)

	if len(result) > 0 {
		response := result[0].Interface().([]Item)
		if len(response) > 0 {
			item = response[0]
		}
	}

	return item
}

func (t *PostgresqlTable) Append(items ...interface{}) bool {

	itop := len(items)

	for i := 0; i < itop; i++ {

		values := []string{}
		fields := []string{}

		item := items[i]

		for field, value := range item.(Item) {
			fields = append(fields, field)
			values = append(values, fmt.Sprintf("%v", value))
		}

		t.parent.pgExec("Query",
			"INSERT INTO",
			pgTable(t.name),
			pgFields(fields),
			"VALUES",
			pgValues(values),
		)

	}

	return true
}

func (pg *PostgresqlDataSource) Collection(name string) Collection {

	if collection, ok := pg.collections[name]; ok == true {
		return collection
	}

	t := &PostgresqlTable{}

	t.parent = pg
	t.name = name

	// Fetching table datatypes and mapping to internal gotypes.

	rows := t.parent.pgExec(
		"Query",
		"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = ?", sqlArgs{t.name},
	)

	columns := t.pgFetchAll(rows)

	pattern, _ := regexp.Compile("^([a-z]+)\\(?([0-9,]+)?\\)?\\s?([a-z]*)?")

	t.types = make(map[string]reflect.Kind, len(columns))

	for _, column := range columns {
		cname := strings.ToLower(column["column_name"].(string))
		ctype := strings.ToLower(column["data_type"].(string))

		results := pattern.FindStringSubmatch(ctype)

		// Default properties.
		dextra := ""
		dtype := "varchar"

		dtype = results[1]

		if len(results) > 3 {
			dextra = results[3]
		}

		vtype := reflect.String

		// Guessing datatypes.
		switch dtype {
		case "smallint", "integer", "bigint", "serial", "bigserial":
			{
				if dextra == "unsigned" {
					vtype = reflect.Uint64
				} else {
					vtype = reflect.Int64
				}
			}
		case "real", "double":
			{
				vtype = reflect.Float64
			}
		}

		//fmt.Printf("Imported %v (from %v)\n", vtype, dtype)

		t.types[cname] = vtype
	}

	pg.collections[name] = t

	return t
}
