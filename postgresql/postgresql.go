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

package postgresql

import (
	"database/sql"
	"fmt"
	"github.com/gosexy/db"
	"github.com/gosexy/sugar"
	"github.com/gosexy/to"
	_ "github.com/xiam/gopostgresql"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"
)

func init() {
	db.Register("postgresql", &PostgresqlDataSource{})
}

var Debug = false

const dateFormat = "2006-01-02 15:04:05"
const timeFormat = "%d:%02d:%02d"

type pgQuery struct {
	Query   []string
	SqlArgs []string
}

func pgCompile(terms []interface{}) *pgQuery {
	q := &pgQuery{}

	q.Query = []string{}

	for _, term := range terms {
		switch term.(type) {
		case string:
			q.Query = append(q.Query, term.(string))
		case db.SqlArgs:
			for _, arg := range term.(db.SqlArgs) {
				q.SqlArgs = append(q.SqlArgs, arg)
			}
		case db.SqlValues:
			args := make([]string, len(term.(db.SqlValues)))
			for i, arg := range term.(db.SqlValues) {
				args[i] = "?"
				q.SqlArgs = append(q.SqlArgs, arg)
			}
			q.Query = append(q.Query, "("+strings.Join(args, ", ")+")")
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

func pgValues(values []string) db.SqlValues {
	ret := make(db.SqlValues, len(values))
	for i, _ := range values {
		ret[i] = values[i]
	}
	return ret
}

// Stores PostgreSQL session data.
type PostgresqlDataSource struct {
	config      db.DataSource
	session     *sql.DB
	collections map[string]db.Collection
}

func (self *PostgresqlDataSource) Name() string {
	return self.config.Database
}

func (self *PostgresqlTable) Name() string {
	return self.name
}

// Returns true if the collection exists.
func (self *PostgresqlTable) Exists() bool {
	result, err := self.parent.pgExec(
		"Query",
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
		panic(err.Error())
	}
	if result.Next() == true {
		result.Close()
		return true
	}
	return false
}

func (t *PostgresqlTable) pgFetchAll(rows sql.Rows) []db.Item {

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
				intval, _ := strconv.Atoi(strval)
				item[name] = uint64(intval)
			case reflect.Int64:
				intval, _ := strconv.Atoi(strval)
				item[name] = intval
			case reflect.Float64:
				floatval, _ := strconv.ParseFloat(strval, 10)
				item[name] = floatval
			default:
				item[name] = strval
			}
		}

		items = append(items, item)
	}

	return items
}

func (pg *PostgresqlDataSource) pgExec(method string, terms ...interface{}) (sql.Rows, error) {

	sn := reflect.ValueOf(pg.session)
	fn := sn.MethodByName(method)

	q := pgCompile(terms)

	if Debug {
		fmt.Printf("Q: %v\n", q.Query)
		fmt.Printf("A: %v\n", q.SqlArgs)
	}

	qs := strings.Join(q.Query, " ")

	args := make([]reflect.Value, len(q.SqlArgs)+1)

	for i := 0; i < len(q.SqlArgs); i++ {
		qs = strings.Replace(qs, "?", fmt.Sprintf("$%d", i+1), 1)
		args[1+i] = reflect.ValueOf(q.SqlArgs[i])
	}

	args[0] = reflect.ValueOf(qs)

	res := fn.Call(args)

	if res[1].IsNil() == false {
		return sql.Rows{}, res[1].Elem().Interface().(error)
	}

	switch res[0].Elem().Interface().(type) {
	case sql.Rows:
		return res[0].Elem().Interface().(sql.Rows), nil
	}

	return sql.Rows{}, nil
}

// Represents a PostgreSQL table.
type PostgresqlTable struct {
	parent *PostgresqlDataSource
	name   string
	types  map[string]reflect.Kind
}

// Configures and returns a PostgreSQL dabase session.
func Session(config db.DataSource) db.Database {
	m := &PostgresqlDataSource{}
	m.config = config
	m.collections = make(map[string]db.Collection)
	return m
}

// Closes a previously opened PostgreSQL database session.
func (pg *PostgresqlDataSource) Close() error {
	if pg.session != nil {
		return pg.session.Close()
	}
	return nil
}

// Configures a datasource and tries to open a connection.
func (self *PostgresqlDataSource) Setup(config db.DataSource) error {
	self.config = config
	self.collections = make(map[string]db.Collection)
	return self.Open()
}

// Tries to open a connection to the current PostgreSQL session.
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

// Changes the active database.
func (pg *PostgresqlDataSource) Use(database string) error {
	pg.config.Database = database
	return pg.Open()
}

// Deletes the currently active database.
func (pg *PostgresqlDataSource) Drop() error {
	pg.session.Query(fmt.Sprintf("DROP DATABASE %s", pg.config.Database))
	return nil
}

// Returns a *sql.DB object that represents an internal session.
func (pg *PostgresqlDataSource) Driver() interface{} {
	return pg.session
}

// Returns the list of PostgreSQL tables in the current database.
func (pg *PostgresqlDataSource) Collections() []string {
	var collections []string
	var collection string

	rows, err := pg.session.Query("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")

	if err == nil {
		for rows.Next() {
			rows.Scan(&collection)
			collections = append(collections, collection)
		}
	} else {
		panic(err)
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

func (t *PostgresqlTable) compileSet(term db.Set) (string, db.SqlArgs) {
	sql := []string{}
	args := db.SqlArgs{}

	for key, arg := range term {
		sql = append(sql, fmt.Sprintf("%s = ?", key))
		args = append(args, fmt.Sprintf("%v", arg))
	}

	return strings.Join(sql, ", "), args
}

func (t *PostgresqlTable) compileConditions(term interface{}) (string, db.SqlArgs) {
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

func (t *PostgresqlTable) marshal(where db.Cond) (string, []string) {

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
func (t *PostgresqlTable) Truncate() error {

	_, err := t.parent.pgExec(
		"Exec",
		fmt.Sprintf("TRUNCATE TABLE %s", pgTable(t.name)),
	)

	return err
}

// Deletes all the rows in the table that match certain conditions.
func (t *PostgresqlTable) Remove(terms ...interface{}) error {

	conditions, cargs := t.compileConditions(terms)

	if conditions == "" {
		conditions = "1 = 1"
	}

	_, err := t.parent.pgExec(
		"Exec",
		fmt.Sprintf("DELETE FROM %s", pgTable(t.name)),
		fmt.Sprintf("WHERE %s", conditions), cargs,
	)

	return err
}

// Modifies all the rows in the table that match certain conditions.
func (t *PostgresqlTable) Update(terms ...interface{}) error {
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

	_, err := t.parent.pgExec(
		"Exec",
		fmt.Sprintf("UPDATE %s SET %s", pgTable(t.name), fields), fargs,
		fmt.Sprintf("WHERE %s", conditions), cargs,
	)

	return err
}

// Returns all the rows in the table that match certain conditions.
func (t *PostgresqlTable) FindAll(terms ...interface{}) []db.Item {
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

	rows, err := t.parent.pgExec(
		"Query",
		fmt.Sprintf("SELECT %s FROM %s", fields, pgTable(t.name)),
		fmt.Sprintf("WHERE %s", conditions), args,
		sort, limit, offset,
	)

	if err != nil {
		panic(err)
	}

	result := t.pgFetchAll(rows)

	var relations []sugar.Tuple
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

			relations = append(relations, sugar.Tuple{"all": false, "name": rname, "collection": rcollection, "terms": rterms})
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

			relations = append(relations, sugar.Tuple{"all": true, "name": rname, "collection": rcollection, "terms": rterms})
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
				value := relation["collection"].(*PostgresqlTable).invoke("FindAll", terms)
				item[relation["name"].(string)] = value[0].Interface().([]db.Item)
			} else {
				value := relation["collection"].(*PostgresqlTable).invoke("Find", terms)
				item[relation["name"].(string)] = value[0].Interface().(db.Item)
			}

		}

		// Appending to results.
		items[i] = item
	}

	return items
}

// Returns the number of rows in the current table that match certain conditions.
func (t *PostgresqlTable) Count(terms ...interface{}) (int, error) {

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
func (t *PostgresqlTable) Find(terms ...interface{}) db.Item {

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

func toInternal(val interface{}) string {

	switch val.(type) {
	case []byte:
		return fmt.Sprintf("%s", string(val.([]byte)))
	case time.Time:
		return val.(time.Time).Format(dateFormat)
	case time.Duration:
		t := val.(time.Duration)
		return fmt.Sprintf(timeFormat, int(t.Hours()), int(t.Minutes())%60, int(t.Seconds())%60)
	case bool:
		if val.(bool) == true {
			return "1"
		} else {
			return "0"
		}
	}

	return fmt.Sprintf("%v", val)
}

// Inserts rows into the currently active table.
func (t *PostgresqlTable) Append(items ...interface{}) ([]db.Id, error) {

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

		_, err := t.parent.pgExec(
			"Exec",
			"INSERT INTO",
			pgTable(t.name),
			pgFields(fields),
			"VALUES",
			pgValues(values),
		)

		res, _ := t.parent.pgExec(
			"Query",
			fmt.Sprintf("SELECT CURRVAL(pg_get_serial_sequence('%s','id'))", t.name),
		)

		var lastId string

		res.Next()

		res.Scan(&lastId)

		ids = append(ids, db.Id(lastId))

		if err != nil {
			return ids, err
		}

	}

	return ids, nil
}

// Returns a collection. Panics if the collection does not exists.
func (self *PostgresqlDataSource) ExistentCollection(name string) db.Collection {
	col, err := self.Collection(name)
	if err != nil {
		panic(err.Error())
	}
	return col
}

// Returns a collection by name.
func (pg *PostgresqlDataSource) Collection(name string) (db.Collection, error) {

	if collection, ok := pg.collections[name]; ok == true {
		return collection, nil
	}

	t := &PostgresqlTable{}

	t.parent = pg
	t.name = name

	// Table exists?
	if t.Exists() == false {
		return t, fmt.Errorf("Table %s does not exists.", name)
	}

	// Fetching table datatypes and mapping to internal gotypes.

	rows, err := t.parent.pgExec(
		"Query",
		"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = ?", db.SqlArgs{t.name},
	)

	if err != nil {
		return t, err
	}

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
			if dextra == "unsigned" {
				vtype = reflect.Uint64
			} else {
				vtype = reflect.Int64
			}
		case "real", "double":
			vtype = reflect.Float64
		}

		//fmt.Printf("Imported %v (from %v)\n", vtype, dtype)

		t.types[cname] = vtype
	}

	pg.collections[name] = t

	return t, nil
}
