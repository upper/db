// Copyright (c) 2012-2014 José Carlos Nieto, https://menteslibres.net/xiam
//
// Permission is hereby granted, free of charge, to any person obtaining
// a copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to
// permit persons to whom the Software is furnished to do so, subject to
// the following conditions:
//
// The above copyright notice and this permission notice shall be
// included in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
// LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
// OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
// WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

package mysql

import (
	"database/sql"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"upper.io/cache"
	"upper.io/db"
	"upper.io/db/util/sqlgen"
	"upper.io/db/util/sqlutil"
)

const Adapter = `mysql`

var (
	// Format for saving dates.
	DateFormat = "2006-01-02 15:04:05.000"
	// Format for saving times.
	TimeFormat = "%d:%02d:%02d.%03d"
)

var template *sqlgen.Template

var (
	columnPattern  = regexp.MustCompile(`^([a-z]+)\(?([0-9,]+)?\)?\s?([a-z]*)?`)
	sqlPlaceholder = sqlgen.Value{sqlgen.Raw{`?`}}
)

type source struct {
	config      db.Settings
	session     *sql.DB
	collections map[string]db.Collection
	tx          *sql.Tx
}

type columnSchema_t struct {
	Name string `db:"column_name"`
}

func debugEnabled() bool {
	if os.Getenv(db.EnvEnableDebug) != "" {
		return true
	}
	return false
}

func debugLog(query string, args []interface{}, err error, start int64, end int64) {
	if debugEnabled() == true {
		d := sqlutil.Debug{query, args, err, start, end}
		d.Print()
	}
}

func init() {

	template = &sqlgen.Template{
		mysqlColumnSeparator,
		mysqlIdentifierSeparator,
		mysqlIdentifierQuote,
		mysqlValueSeparator,
		mysqlValueQuote,
		mysqlAndKeyword,
		mysqlOrKeyword,
		mysqlNotKeyword,
		mysqlDescKeyword,
		mysqlAscKeyword,
		mysqlDefaultOperator,
		mysqlClauseGroup,
		mysqlClauseOperator,
		mysqlColumnValue,
		mysqlTableAliasLayout,
		mysqlColumnAliasLayout,
		mysqlSortByColumnLayout,
		mysqlWhereLayout,
		mysqlOrderByLayout,
		mysqlInsertLayout,
		mysqlSelectLayout,
		mysqlUpdateLayout,
		mysqlDeleteLayout,
		mysqlTruncateLayout,
		mysqlDropDatabaseLayout,
		mysqlDropTableLayout,
		mysqlSelectCountLayout,
		mysqlGroupByLayout,
		cache.NewCache(),
	}

	db.Register(Adapter, &source{})
}

func (self *source) doExec(stmt sqlgen.Statement, args ...interface{}) (sql.Result, error) {
	var query string
	var res sql.Result
	var err error
	var start, end int64

	start = time.Now().UnixNano()

	defer func() {
		end = time.Now().UnixNano()
		debugLog(query, args, err, start, end)
	}()

	if self.session == nil {
		return nil, db.ErrNotConnected
	}

	query = stmt.Compile(template)

	if self.tx != nil {
		res, err = self.tx.Exec(query, args...)
	} else {
		res, err = self.session.Exec(query, args...)
	}

	return res, err
}

func (self *source) doQuery(stmt sqlgen.Statement, args ...interface{}) (*sql.Rows, error) {
	var rows *sql.Rows
	var query string
	var err error
	var start, end int64

	start = time.Now().UnixNano()

	defer func() {
		end = time.Now().UnixNano()
		debugLog(query, args, err, start, end)
	}()

	if self.session == nil {
		return nil, db.ErrNotConnected
	}

	query = stmt.Compile(template)

	if self.tx != nil {
		rows, err = self.tx.Query(query, args...)
	} else {
		rows, err = self.session.Query(query, args...)
	}

	return rows, err
}

func (self *source) doQueryRow(stmt sqlgen.Statement, args ...interface{}) (*sql.Row, error) {
	var query string
	var row *sql.Row
	var err error
	var start, end int64

	start = time.Now().UnixNano()

	defer func() {
		end = time.Now().UnixNano()
		debugLog(query, args, err, start, end)
	}()

	if self.session == nil {
		return nil, db.ErrNotConnected
	}

	query = stmt.Compile(template)

	if self.tx != nil {
		row = self.tx.QueryRow(query, args...)
	} else {
		row = self.session.QueryRow(query, args...)
	}

	return row, err
}

// Returns the string name of the database.
func (self *source) Name() string {
	return self.config.Database
}

//  Ping verifies a connection to the database is still alive,
//  establishing a connection if necessary.
func (self *source) Ping() error {
	return self.session.Ping()
}

func (self *source) clone() (*source, error) {
	src := &source{}
	src.Setup(self.config)

	if err := src.Open(); err != nil {
		return nil, err
	}

	return src, nil
}

func (self *source) Clone() (db.Database, error) {
	return self.clone()
}

func (self *source) Transaction() (db.Tx, error) {
	var err error
	var clone *source
	var sqlTx *sql.Tx

	if sqlTx, err = self.session.Begin(); err != nil {
		return nil, err
	}

	if clone, err = self.clone(); err != nil {
		return nil, err
	}

	tx := &tx{clone}

	clone.tx = sqlTx

	return tx, nil
}

// Stores database settings.
func (self *source) Setup(config db.Settings) error {
	self.config = config
	self.collections = make(map[string]db.Collection)
	return self.Open()
}

// Returns the underlying *sql.DB instance.
func (self *source) Driver() interface{} {
	return self.session
}

// Attempts to connect to a database using the stored settings.
func (self *source) Open() error {
	var err error

	if self.config.Host == "" {
		if self.config.Socket == "" {
			self.config.Host = `127.0.0.1`
		}
	}

	if self.config.Port == 0 {
		self.config.Port = 3306
	}

	if self.config.Database == "" {
		return db.ErrMissingDatabaseName
	}

	if self.config.Socket != "" && self.config.Host != "" {
		return db.ErrSockerOrHost
	}

	if self.config.Charset == "" {
		self.config.Charset = `utf8`
	}

	var conn string

	if self.config.Host != "" {
		conn = fmt.Sprintf(`%s:%s@tcp(%s:%d)/%s?charset=%s`, self.config.User, self.config.Password, self.config.Host, self.config.Port, self.config.Database, self.config.Charset)
	} else if self.config.Socket != `` {
		conn = fmt.Sprintf(`%s:%s@unix(%s)/%s?charset=%s`, self.config.User, self.config.Password, self.config.Socket, self.config.Database, self.config.Charset)
	}

	self.session, err = sql.Open(`mysql`, conn)

	if err != nil {
		return err
	}

	return nil
}

// Closes the current database session.
func (self *source) Close() error {
	if self.session != nil {
		return self.session.Close()
	}
	return nil
}

// Changes the active database.
func (self *source) Use(database string) error {
	self.config.Database = database
	return self.Open()
}

// Drops the currently active database.
func (self *source) Drop() error {

	_, err := self.doQuery(sqlgen.Statement{
		Type:     sqlgen.SqlDropDatabase,
		Database: sqlgen.Database{self.config.Database},
	})

	return err
}

// Returns a list of all tables within the currently active database.
func (self *source) Collections() ([]string, error) {
	var collections []string
	var collection string

	rows, err := self.doQuery(sqlgen.Statement{
		Type:  sqlgen.SqlSelect,
		Table: sqlgen.Table{`information_schema.tables`},
		Columns: sqlgen.Columns{
			{`table_name`},
		},
		Where: sqlgen.Where{
			sqlgen.ColumnValue{sqlgen.Column{`table_schema`}, `=`, sqlPlaceholder},
		},
	}, self.config.Database)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	for rows.Next() {
		rows.Scan(&collection)
		collections = append(collections, collection)
	}

	return collections, nil
}

func (self *source) tableExists(names ...string) error {
	for _, name := range names {

		rows, err := self.doQuery(sqlgen.Statement{
			Type:  sqlgen.SqlSelect,
			Table: sqlgen.Table{`information_schema.tables`},
			Columns: sqlgen.Columns{
				{`table_name`},
			},
			Where: sqlgen.Where{
				sqlgen.ColumnValue{sqlgen.Column{`table_schema`}, `=`, sqlPlaceholder},
				sqlgen.ColumnValue{sqlgen.Column{`table_name`}, `=`, sqlPlaceholder},
			},
		}, self.config.Database, name)

		if err != nil {
			return db.ErrCollectionDoesNotExist
		}

		defer rows.Close()

		if rows.Next() == false {
			return db.ErrCollectionDoesNotExist
		}
	}

	return nil
}

// Returns a collection instance by name.
func (self *source) Collection(names ...string) (db.Collection, error) {

	if len(names) == 0 {
		return nil, db.ErrMissingCollectionName
	}

	col := &table{
		source: self,
		names:  names,
	}

	columns_t := []columnSchema_t{}

	for _, name := range names {
		chunks := strings.SplitN(name, " ", 2)

		if len(chunks) > 0 {

			name = chunks[0]

			if err := self.tableExists(name); err != nil {
				return nil, err
			}

			rows, err := self.doQuery(sqlgen.Statement{
				Type:  sqlgen.SqlSelect,
				Table: sqlgen.Table{`information_schema.columns`},
				Columns: sqlgen.Columns{
					{`column_name`},
					{`data_type`},
				},
				Where: sqlgen.Where{
					sqlgen.ColumnValue{sqlgen.Column{`table_schema`}, `=`, sqlPlaceholder},
					sqlgen.ColumnValue{sqlgen.Column{`table_name`}, `=`, sqlPlaceholder},
				},
			}, self.config.Database, name)

			if err != nil {
				return nil, err
			}

			if err = sqlutil.FetchRows(rows, &columns_t); err != nil {
				return nil, err
			}

			col.Columns = make([]string, 0, len(columns_t))

			for _, column := range columns_t {
				column.Name = strings.ToLower(column.Name)
				col.Columns = append(col.Columns, column.Name)
			}
		}
	}

	return col, nil
}
