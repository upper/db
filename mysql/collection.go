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

package mysql

import (
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"menteslibres.net/gosexy/db"
	"menteslibres.net/gosexy/db/util/sqlutil"
	"menteslibres.net/gosexy/to"
	"strings"
	"time"
)

// Mysql table/collection.
type Table struct {
	source *Source
	sqlutil.T
}

func (self *Table) Query(terms ...interface{}) (db.Result, error) {

	var err error

	queryChunks := sqlutil.NewQueryChunks()

	// Analyzing given terms.
	for _, term := range terms {

		switch v := term.(type) {
		case db.Limit:
			if queryChunks.Limit == "" {
				queryChunks.Limit = fmt.Sprintf("LIMIT %d", v)
			} else {
				return nil, db.ErrQueryLimitParam
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
				return nil, db.ErrQuerySortParam
			}
		case db.Offset:
			if queryChunks.Offset == "" {
				queryChunks.Offset = fmt.Sprintf("OFFSET %d", v)
			} else {
				return nil, db.ErrQueryOffsetParam
			}
		case db.Fields:
			queryChunks.Fields = append(queryChunks.Fields, v...)
		case db.Relate:
			for name, terms := range v {
				col, err := self.RelationCollection(name, terms)
				if err != nil {
					return nil, err
				}
				queryChunks.Relations = append(queryChunks.Relations, db.Relation{All: false, Name: name, Collection: col, On: terms})
			}
		case db.RelateAll:
			for name, terms := range v {
				col, err := self.RelationCollection(name, terms)
				if err != nil {
					return nil, err
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
		fmt.Sprintf("SELECT %s FROM `%s`", strings.Join(queryChunks.Fields, ", "), self.Name()),
		fmt.Sprintf("WHERE %s", queryChunks.Conditions), queryChunks.Arguments,
		// Optional
		queryChunks.Sort, queryChunks.Limit, queryChunks.Offset,
	)

	if err != nil {
		return nil, err
	}

	result := &Result{
		sqlutil.Result{
			Rows:      rows,
			Table:     &self.T,
			Relations: queryChunks.Relations,
		},
	}

	return result, nil
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

	str := make([]string, len(where))
	arg := make([]string, len(where))

	i := 0

	for key, _ := range where {
		key = strings.Trim(key, " ")
		chunks := strings.SplitN(key, " ", 2)

		op := "="

		if len(chunks) > 1 {
			op = chunks[1]
		}

		str[i] = fmt.Sprintf("%s %s ?", chunks[0], op)
		arg[i] = toInternal(where[key])

		i++
	}

	switch len(str) {
	case 1:
		return str[0], arg
	case 0:
		return "", []string{}
	}

	return "(" + strings.Join(str, " AND ") + ")", arg
}

/*
	Deletes all the rows in the table.
*/
func (self *Table) Truncate() error {

	_, err := self.source.doExec(
		fmt.Sprintf("TRUNCATE TABLE `%s`", self.Name()),
	)

	return err
}

/*
	Deletes all the rows in the table that match certain conditions.
*/
func (self *Table) Remove(terms ...interface{}) error {

	conditions, cargs := self.compileConditions(terms)

	if conditions == "" {
		conditions = "1 = 1"
	}

	_, err := self.source.doExec(
		fmt.Sprintf("DELETE FROM `%s`", self.Name()),
		fmt.Sprintf("WHERE %s", conditions), cargs,
	)

	return err
}

/*
	Modifies all the rows in the table that match certain conditions.
*/
func (self *Table) Update(selector interface{}, update interface{}) error {
	var err error
	var updateFields []string
	var updateArgs db.SqlArgs

	selectorConds, selectorArgs := self.compileConditions(selector)

	if selectorConds == "" {
		return db.ErrMissingConditions
	}

	fields, values, err := self.FieldValues(update, toInternal)

	if err == nil {
		total := len(fields)
		updateFields = make([]string, total)
		updateArgs = make(db.SqlArgs, total)
		for i := 0; i < total; i++ {
			updateFields[i] = fmt.Sprintf("%s = ?", fields[i])
			updateArgs[i] = values[i]
		}
	} else {
		return err
	}

	_, err = self.source.doExec(
		fmt.Sprintf(
			"UPDATE `%s` SET %s",
			self.Name(),
			strings.Join(updateFields, ", "),
		),
		updateArgs,
		fmt.Sprintf("WHERE %s", selectorConds),
		selectorArgs,
	)

	return err
}

/*
	Returns a slice of rows that match certain conditions.
*/
func (self *Table) FindAll(terms ...interface{}) ([]db.Item, error) {
	items := []db.Item{}

	res, err := self.Query(terms...)

	if err == nil {
		err = res.All(&items)
	}

	return items, err
}

/*
	Returns the number of rows in the current table that match certain conditions.
*/
func (self *Table) Count(terms ...interface{}) (int, error) {
	terms = append(terms, db.Fields{"COUNT(1) AS _total"})

	result, err := self.FindAll(terms...)

	if err == nil {
		return int(to.Int64(result[0]["_total"])), nil
	}

	return 0, err
}

/*
	Returns true if the collection exists.
*/
func (self *Table) Exists() bool {
	result, err := self.source.doQuery(
		fmt.Sprintf(`
				SELECT table_name
					FROM information_schema.tables
				WHERE table_schema = '%s' AND table_name = '%s'
			`,
			self.source.Name(),
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
	Returns the first row in the table that matches certain conditions.
*/
func (self *Table) Find(terms ...interface{}) (db.Item, error) {
	var item db.Item
	var err error

	terms = append(terms, db.Limit(1))

	res, err := self.Query(terms...)

	if err == nil {
		err = res.One(&item)
	}

	return item, err
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
			fmt.Sprintf("INSERT INTO `%s`", self.Name()),
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

func toInternalInterface(val interface{}) interface{} {
	return toInternal(val)
}

func toInternal(val interface{}) string {

	switch t := val.(type) {
	case []byte:
		return string(t)
	case time.Time:
		return t.Format(DateFormat)
	case time.Duration:
		return fmt.Sprintf(TimeFormat, int(t/time.Hour), int(t/time.Minute%60), int(t/time.Second%60), t%time.Second/time.Millisecond)
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
