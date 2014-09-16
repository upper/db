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
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/xiam/gopostgresql"
	"upper.io/cache"
	"upper.io/db"
	"upper.io/db/util/schema"
	"upper.io/db/util/sqlgen"
	"upper.io/db/util/sqlutil"
)

const Adapter = `postgresql`

var (
	// Format for saving dates.
	DateFormat = "2006-01-02 15:04:05.999999999 MST"
	// Format for saving times.
	TimeFormat = "%d:%02d:%02d.%d"
	SSLMode    = "disable"
)

var template *sqlgen.Template

var (
	sqlPlaceholder = sqlgen.Value{sqlgen.Raw{`?`}}
)

type source struct {
	config  db.Settings
	session *sql.DB
	tx      *tx
	schema  *schema.DatabaseSchema
}

type columnSchemaT struct {
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
		pgsqlGroupByLayout,
		cache.NewCache(),
	}

	db.Register(Adapter, &source{})
}

func (self *source) populateSchema() (err error) {
	var collections []string

	self.schema = schema.NewDatabaseSchema()

	self.schema.Name = self.config.Database

	// The Collections() call will populate schema if its nil.
	if collections, err = self.Collections(); err != nil {
		return err
	}

	for i := range collections {
		// Populate each collection.
		if _, err = self.Collection(collections[i]); err != nil {
			return err
		}
	}

	return err
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

	l := len(args)
	for i := 0; i < l; i++ {
		query = strings.Replace(query, `?`, fmt.Sprintf(`$%d`, i+1), 1)
	}

	if self.tx != nil {
		res, err = self.tx.sqlTx.Exec(query, args...)
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

	l := len(args)
	for i := 0; i < l; i++ {
		query = strings.Replace(query, `?`, fmt.Sprintf(`$%d`, i+1), 1)
	}

	if self.tx != nil {
		rows, err = self.tx.sqlTx.Query(query, args...)
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

	l := len(args)
	for i := 0; i < l; i++ {
		query = strings.Replace(query, `?`, `$`+strconv.Itoa(i+1), 1)
	}

	if self.tx != nil {
		row = self.tx.sqlTx.QueryRow(query, args...)
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
	src := new(source)
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

	tx := &tx{source: clone, sqlTx: sqlTx}

	clone.tx = tx

	return tx, nil
}

// Stores database settings.
func (self *source) Setup(config db.Settings) error {
	self.config = config
	//self.collections = make(map[string]db.Collection)
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
		self.config.Port = 5432
	}

	if self.config.Database == "" {
		return db.ErrMissingDatabaseName
	}

	if self.config.Socket != "" && self.config.Host != "" {
		return db.ErrSockerOrHost
	}

	var conn string
	if user := self.config.User; user != "" {
		conn += fmt.Sprintf(`user=%s `, user)
	}
	if pass := self.config.Password; pass != "" {
		conn += fmt.Sprintf(`password=%s `, pass)
	}
	if self.config.Host != "" {
		conn += fmt.Sprintf(`host=%s port=%d `, self.config.Host, self.config.Port)
	} else {
		conn += fmt.Sprintf(`host=%s `, self.config.Socket)
	}
	conn += fmt.Sprintf(`dbname=%s sslmode=%s`, self.config.Database, SSLMode)

	if self.session, err = sql.Open(`postgres`, conn); err != nil {
		return err
	}

	if err = self.populateSchema(); err != nil {
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

// Collections() Returns a list of non-system tables/collections contained
// within the currently active database.
func (self *source) Collections() (collections []string, err error) {

	var tablesInSchema int = len(self.schema.Tables)

	// Is schema already populated?
	if tablesInSchema > 0 {
		// Pulling table names from schema.
		return self.schema.Tables, nil
	}

	// Schema is empty.

	// Querying table names.
	stmt := sqlgen.Statement{
		Type: sqlgen.SqlSelect,
		Columns: sqlgen.Columns{
			{`table_name`},
		},
		Table: sqlgen.Table{
			`information_schema.tables`,
		},
		Where: sqlgen.Where{
			sqlgen.ColumnValue{
				sqlgen.Column{`table_schema`},
				`=`,
				sqlgen.Value{`public`},
			},
		},
	}

	// Executing statement.
	var rows *sql.Rows
	if rows, err = self.doQuery(stmt); err != nil {
		return nil, err
	}

	defer rows.Close()

	collections = []string{}

	var name string

	for rows.Next() {
		// Getting table name.
		if err = rows.Scan(&name); err != nil {
			return nil, err
		}

		// Adding table entry to schema.
		self.schema.AddTable(name)

		// Adding table to collections array.
		collections = append(collections, name)
	}

	return collections, nil
}

func (self *source) tableExists(names ...string) error {
	var stmt sqlgen.Statement
	var err error
	var rows *sql.Rows

	for i := range names {

		if self.schema.HasTable(names[i]) {
			// We already know this table exists.
			continue
		}

		stmt = sqlgen.Statement{
			Type:  sqlgen.SqlSelect,
			Table: sqlgen.Table{`information_schema.tables`},
			Columns: sqlgen.Columns{
				{`table_name`},
			},
			Where: sqlgen.Where{
				sqlgen.ColumnValue{sqlgen.Column{`table_catalog`}, `=`, sqlPlaceholder},
				sqlgen.ColumnValue{sqlgen.Column{`table_name`}, `=`, sqlPlaceholder},
			},
		}

		if rows, err = self.doQuery(stmt, self.config.Database, names[i]); err != nil {
			return db.ErrCollectionDoesNotExist
		}

		defer rows.Close()

		if rows.Next() == false {
			return db.ErrCollectionDoesNotExist
		}
	}

	return nil
}

func (self *source) tableColumns(tableName string) ([]string, error) {

	// Making sure this table is allocated.
	tableSchema := self.schema.Table(tableName)

	if len(tableSchema.Columns) > 0 {
		return tableSchema.Columns, nil
	}

	stmt := sqlgen.Statement{
		Type: sqlgen.SqlSelect,
		Table: sqlgen.Table{
			`information_schema.columns`,
		},
		Columns: sqlgen.Columns{
			{`column_name`},
			{`data_type`},
		},
		Where: sqlgen.Where{
			sqlgen.ColumnValue{
				sqlgen.Column{`table_catalog`},
				`=`,
				sqlPlaceholder,
			},
			sqlgen.ColumnValue{
				sqlgen.Column{`table_name`},
				`=`,
				sqlPlaceholder,
			},
		},
	}

	var rows *sql.Rows
	var err error

	if rows, err = self.doQuery(stmt, self.config.Database, tableName); err != nil {
		return nil, err
	}

	tableFields := []columnSchemaT{}

	if err = sqlutil.FetchRows(rows, &tableFields); err != nil {
		return nil, err
	}

	self.schema.TableInfo[tableName].Columns = make([]string, 0, len(tableFields))

	for i := range tableFields {
		self.schema.TableInfo[tableName].Columns = append(self.schema.TableInfo[tableName].Columns, tableFields[i].Name)
	}

	return self.schema.TableInfo[tableName].Columns, nil
}

// Returns a collection instance by name.
func (self *source) Collection(names ...string) (db.Collection, error) {
	var err error

	if len(names) == 0 {
		return nil, db.ErrMissingCollectionName
	}

	if self.tx != nil {
		if self.tx.done {
			return nil, sql.ErrTxDone
		}
	}

	col := &table{
		source: self,
		names:  names,
	}

	for _, name := range names {
		chunks := strings.SplitN(name, ` `, 2)

		if len(chunks) == 0 {
			return nil, db.ErrMissingCollectionName
		}

		tableName := chunks[0]

		if err := self.tableExists(tableName); err != nil {
			return nil, err
		}

		if col.Columns, err = self.tableColumns(tableName); err != nil {
			return nil, err
		}
	}

	return col, nil
}

func (self *source) getPrimaryKey(tableName string) (string, error) {

	tableSchema := self.schema.Table(tableName)

	if tableSchema.PrimaryKey != "" {
		return tableSchema.PrimaryKey, nil
	}

	// Getting primary key. See https://github.com/upper/db/issues/24.
	stmt := sqlgen.Statement{
		Type:  sqlgen.SqlSelect,
		Table: sqlgen.Table{`pg_index, pg_class, pg_attribute`},
		Columns: sqlgen.Columns{
			{`pg_attribute.attname`},
		},
		Where: sqlgen.Where{
			sqlgen.ColumnValue{sqlgen.Column{`pg_class.oid`}, `=`, sqlgen.Value{sqlgen.Raw{`'"` + tableName + `"'::regclass`}}},
			sqlgen.ColumnValue{sqlgen.Column{`indrelid`}, `=`, sqlgen.Value{sqlgen.Raw{`pg_class.oid`}}},
			sqlgen.ColumnValue{sqlgen.Column{`pg_attribute.attrelid`}, `=`, sqlgen.Value{sqlgen.Raw{`pg_class.oid`}}},
			sqlgen.ColumnValue{sqlgen.Column{`pg_attribute.attnum`}, `=`, sqlgen.Value{sqlgen.Raw{`any(pg_index.indkey)`}}},
			sqlgen.Raw{`indisprimary`},
		},
		Limit: 1,
	}

	var row *sql.Row
	var err error

	if row, err = self.doQueryRow(stmt); err != nil {
		return "", err
	}

	if err = row.Scan(&tableSchema.PrimaryKey); err != nil {
		return "", err
	}

	return tableSchema.PrimaryKey, nil
}
