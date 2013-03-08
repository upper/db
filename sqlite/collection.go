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
	"errors"
	"fmt"
	"github.com/gosexy/db"
	"github.com/gosexy/db/util"
	"github.com/gosexy/db/util/sqlutil"
	"github.com/gosexy/to"
	"strings"
	"time"
)

/*
	Fetches a result delimited by terms into a pointer to map or struct given by
	dst.
*/
func (self *Table) Fetch(dst interface{}, terms ...interface{}) error {
	found := self.Find(terms...)
	return util.Fetch(dst, found)
}

/*
	Fetches results delimited by terms into an slice of maps or structs given by
	the pointer dst.
*/
func (self *Table) FetchAll(dst interface{}, terms ...interface{}) error {

	var err error

	queryChunks := sqlutil.NewQueryChunks()

	err = util.ValidateDestination(dst)

	if err != nil {
		return err
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
				col, err := self.RelationCollection(name, terms)
				if err != nil {
					return err
				}
				queryChunks.Relations = append(queryChunks.Relations, db.Relation{All: false, Name: name, Collection: col, On: terms})
			}
		case db.RelateAll:
			for name, terms := range v {
				col, err := self.RelationCollection(name, terms)
				if err != nil {
					return err
				}
				queryChunks.Relations = append(queryChunks.Relations, db.Relation{All: true, Name: name, Collection: col, On: terms})
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
	rows, err := self.source.doQuery(
		// Mandatory
		fmt.Sprintf("SELECT %s FROM '%s'", strings.Join(queryChunks.Fields, ", "), self.Name()),
		fmt.Sprintf("WHERE %s", queryChunks.Conditions), queryChunks.Arguments,
		// Optional
		queryChunks.Sort, queryChunks.Limit, queryChunks.Offset,
	)

	if err != nil {
		return err
	}

	// Fetching rows.
	err = self.FetchRows(dst, rows)

	if err != nil {
		return err
	}

	// Fetching relations
	err = self.FetchRelations(dst, queryChunks.Relations, toInternalInterface)

	if err != nil {
		return err
	}

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
func (self *Table) Truncate() error {

	_, err := self.source.doExec(
		fmt.Sprintf("DELETE FROM '%s'", self.Name()),
	)

	return err
}

/*
	Deletes all the rows in the table that match certain conditions.
*/
func (self *Table) Remove(terms ...interface{}) error {

	conds, args := self.compileConditions(terms)

	if conds == "" {
		conds = "1 = 1"
	}

	_, err := self.source.doExec(
		fmt.Sprintf("DELETE FROM '%s'", self.Name()),
		fmt.Sprintf("WHERE %s", conds), args,
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

	_, err := self.source.doExec(
		fmt.Sprintf("UPDATE '%s' SET %s", self.Name(), fields), fargs,
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
	Appends items into the table. An item could be either a map or a struct.
*/
func (self *Table) Append(items ...interface{}) ([]db.Id, error) {

	ids := make([]db.Id, len(items))

	for i, item := range items {

		fields, values, err := self.FieldValues(item, toInternal)

		// Error ocurred, stop appending.
		if err != nil {
			return ids, err
		}

		res, err := self.source.doExec(
			fmt.Sprintf("INSERT INTO '%s'", self.Name()),
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

/*
	Returns true if the collection exists.
*/
func (self *Table) Exists() bool {
	result, err := self.source.doQuery(
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

func toInternalInterface(val interface{}) interface{} {
	return toInternal(val)
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
