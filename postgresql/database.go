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

package postgresql

import (
	"database/sql"
	"fmt"
	_ "github.com/xiam/gopostgresql"
	"os"
	"reflect"
	"regexp"
	"strings"
	"upper.io/db"
	"upper.io/db/util/sqlgen"
	"upper.io/db/util/sqlutil"
)

const Adapter = `postgresql`

var (
	// Format for saving dates.
	DateFormat = "2006-01-02 15:04:05"
	// Format for saving times.
	TimeFormat = "%d:%02d:%02d.%d"
	SSLMode    = "disable"
)

var template *sqlgen.Template

var (
	columnPattern  = regexp.MustCompile(`^([a-z]+)\(?([0-9,]+)?\)?\s?([a-z]*)?`)
	sqlPlaceholder = sqlgen.Value{sqlgen.Raw{`?`}}
)

type Source struct {
	config      db.Settings
	session     *sql.DB
	collections map[string]db.Collection
	tx          *sql.Tx
}

type columnSchema_t struct {
	ColumnName string `db:"column_name"`
	DataType   string `db:"data_type"`
}

func debugEnabled() bool {
	if os.Getenv(db.EnvEnableDebug) != "" {
		return true
	}
	return false
}

func init() {

	template = &sqlgen.Template{
		pgsqlColumnSeparator,
		pgsqlIdentifierSeparator,
		pgsqlIdentifierQuote,
		pgsqlValueSeparator,
		pgsqlValueQuote,
		pgsqlAndKeyword,
		pgsqlOrKeyword,
		pgsqlNotKeyword,
		pgsqlDescKeyword,
		pgsqlAscKeyword,
		pgsqlDefaultOperator,
		pgsqlClauseGroup,
		pgsqlClauseOperator,
		pgsqlColumnValue,
		pgsqlTableAliasLayout,
		pgsqlColumnAliasLayout,
		pgsqlSortByColumnLayout,
		pgsqlWhereLayout,
		pgsqlOrderByLayout,
		pgsqlInsertLayout,
		pgsqlSelectLayout,
		pgsqlUpdateLayout,
		pgsqlDeleteLayout,
		pgsqlTruncateLayout,
		pgsqlDropDatabaseLayout,
		pgsqlDropTableLayout,
		pgsqlSelectCountLayout,
	}

	db.Register(Adapter, &Source{})
}

func (self *Source) doExec(stmt sqlgen.Statement, args ...interface{}) (sql.Result, error) {

	if self.session == nil {
		return nil, db.ErrNotConnected
	}

	query := stmt.Compile(template)

	l := len(args)
	for i := 0; i < l; i++ {
		query = strings.Replace(query, `?`, fmt.Sprintf(`$%d`, i+1), 1)
	}

	if debugEnabled() == true {
		sqlutil.DebugQuery(query, args)
	}

	if self.tx != nil {
		return self.tx.Exec(query, args...)
	}

	return self.session.Exec(query, args...)
}

func (self *Source) doQuery(stmt sqlgen.Statement, args ...interface{}) (*sql.Rows, error) {
	if self.session == nil {
		return nil, db.ErrNotConnected
	}

	query := stmt.Compile(template)

	l := len(args)
	for i := 0; i < l; i++ {
		query = strings.Replace(query, `?`, fmt.Sprintf(`$%d`, i+1), 1)
	}

	if debugEnabled() == true {
		sqlutil.DebugQuery(query, args)
	}

	if self.tx != nil {
		return self.tx.Query(query, args...)
	}

	return self.session.Query(query, args...)
}

func (self *Source) doQueryRow(stmt sqlgen.Statement, args ...interface{}) (*sql.Row, error) {
	if self.session == nil {
		return nil, db.ErrNotConnected
	}

	query := stmt.Compile(template)

	l := len(args)
	for i := 0; i < l; i++ {
		query = strings.Replace(query, `?`, fmt.Sprintf(`$%d`, i+1), 1)
	}

	if debugEnabled() == true {
		sqlutil.DebugQuery(query, args)
	}

	if self.tx != nil {
		return self.tx.QueryRow(query, args...), nil
	}

	return self.session.QueryRow(query, args...), nil
}

// Returns the string name of the database.
func (self *Source) Name() string {
	return self.config.Database
}

//  Ping verifies a connection to the database is still alive,
//  establishing a connection if necessary.
func (self *Source) Ping() error {
	return self.session.Ping()
}

func (self *Source) clone() (*Source, error) {
	src := &Source{}
	src.Setup(self.config)

	if err := src.Open(); err != nil {
		return nil, err
	}

	return src, nil
}

func (self *Source) Clone() (db.Database, error) {
	return self.clone()
}

func (self *Source) Transaction() (db.Tx, error) {
	var err error
	var clone *Source
	var sqlTx *sql.Tx

	if sqlTx, err = self.session.Begin(); err != nil {
		return nil, err
	}

	if clone, err = self.clone(); err != nil {
		return nil, err
	}

	tx := &Tx{clone}

	clone.tx = sqlTx

	return tx, nil
}

// Stores database settings.
func (self *Source) Setup(config db.Settings) error {
	self.config = config
	self.collections = make(map[string]db.Collection)
	return self.Open()
}

// Returns the underlying *sql.DB instance.
func (self *Source) Driver() interface{} {
	return self.session
}

// Attempts to connect to a database using the stored settings.
func (self *Source) Open() error {
	var err error

	if self.config.Host == "" {
		if self.config.Socket == "" {
			self.config.Host = `127.0.0.1`
		}
	}

	if self.config.Port == 0 {
		self.config.Port = 5432
	}

	if self.config.Database == "" {
		return db.ErrMissingDatabaseName
	}

	if self.config.Socket != "" && self.config.Host != "" {
		return db.ErrSockerOrHost
	}

	var conn string

	if self.config.Host != "" {
		conn = fmt.Sprintf(`user=%s password=%s host=%s port=%d dbname=%s sslmode=%s`, self.config.User, self.config.Password, self.config.Host, self.config.Port, self.config.Database, SSLMode)
	} else if self.config.Socket != `` {
		conn = fmt.Sprintf(`user=%s password=%s host=%s dbname=%s sslmode=%s`, self.config.User, self.config.Password, self.config.Socket, self.config.Database, SSLMode)
	}

	self.session, err = sql.Open(`postgres`, conn)

	if err != nil {
		return err
	}

	return nil
}

// Closes the current database session.
func (self *Source) Close() error {
	if self.session != nil {
		return self.session.Close()
	}
	return nil
}

// Changes the active database.
func (self *Source) Use(database string) error {
	self.config.Database = database
	return self.Open()
}

// Drops the currently active database.
func (self *Source) Drop() error {

	_, err := self.doQuery(sqlgen.Statement{
		Type:     sqlgen.SqlDropDatabase,
		Database: sqlgen.Database{self.config.Database},
	})

	return err
}

// Returns a list of all tables within the currently active database.
func (self *Source) Collections() ([]string, error) {
	var collections []string
	var collection string

	rows, err := self.doQuery(sqlgen.Statement{
		Type: sqlgen.SqlSelect,
		Columns: sqlgen.Columns{
			{"table_name"},
		},
		Table: sqlgen.Table{"information_schema.tables"},
		Where: sqlgen.Where{
			sqlgen.ColumnValue{sqlgen.Column{"table_schema"}, "=", sqlgen.Value{"public"}},
		},
	})

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

func (self *Source) tableExists(names ...string) error {
	for _, name := range names {

		rows, err := self.doQuery(sqlgen.Statement{
			Type:  sqlgen.SqlSelect,
			Table: sqlgen.Table{`information_schema.tables`},
			Columns: sqlgen.Columns{
				{`table_name`},
			},
			Where: sqlgen.Where{
				sqlgen.ColumnValue{sqlgen.Column{`table_catalog`}, `=`, sqlPlaceholder},
				sqlgen.ColumnValue{sqlgen.Column{`table_name`}, `=`, sqlPlaceholder},
			},
		}, self.config.Database, name)

		if err != nil {
			return db.ErrCollectionDoesNotExists
		}

		defer rows.Close()

		if rows.Next() == false {
			return db.ErrCollectionDoesNotExists
		}
	}

	return nil
}

// Returns a collection instance by name.
func (self *Source) Collection(names ...string) (db.Collection, error) {

	if len(names) == 0 {
		return nil, db.ErrMissingCollectionName
	}

	col := &Table{
		source: self,
		names:  names,
	}

	col.PrimaryKey = `id`

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
					sqlgen.ColumnValue{sqlgen.Column{`table_catalog`}, `=`, sqlPlaceholder},
					sqlgen.ColumnValue{sqlgen.Column{`table_name`}, `=`, sqlPlaceholder},
				},
			}, self.config.Database, name)

			if err != nil {
				return nil, err
			}

			if err = col.FetchRows(&columns_t, rows); err != nil {
				return nil, err
			}

			col.ColumnTypes = make(map[string]reflect.Kind, len(columns_t))

			for _, column := range columns_t {

				column.ColumnName = strings.ToLower(column.ColumnName)
				column.DataType = strings.ToLower(column.DataType)

				results := columnPattern.FindStringSubmatch(column.DataType)

				// Default properties.
				dextra := ""
				dtype := `varchar`

				dtype = results[1]

				if len(results) > 3 {
					dextra = results[3]
				}

				ctype := reflect.String

				// Guessing datatypes.
				switch dtype {
				case `smallint`, `integer`, `bigint`, `serial`, `bigserial`:
					if dextra == `unsigned` {
						ctype = reflect.Uint64
					} else {
						ctype = reflect.Int64
					}
				case `real`, `double`:
					ctype = reflect.Float64
				}

				col.ColumnTypes[column.ColumnName] = ctype
			}

		}
	}

	return col, nil
}
