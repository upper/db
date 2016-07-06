package lib

import (
	"database/sql"
	"database/sql/driver"
	"fmt"

	"upper.io/db.v2"
	"upper.io/db.v2/sqlbuilder"
)

// SQLDatabase represents a SQL database.
type SQLDatabase interface {
	db.Database
	builder.Builder

	NewTx() (SQLTx, error)
	Tx(fn func(tx SQLTx) error) error
}

type Tx interface {
	Commit() error
	Rollback() error
}

// Tx represents a transaction.
type SQLTx interface {
	SQLDatabase
	Tx
}

type SQLAdapter struct {
	New   func(sqlDB *sql.DB) (SQLDatabase, error)
	NewTx func(sqlTx *sql.Tx) (SQLTx, error)
	Open  func(settings db.ConnectionURL) (SQLDatabase, error)
}

var adapters map[string]*SQLAdapter

func init() {
	adapters = make(map[string]*SQLAdapter)
}

func RegisterSQLAdapter(name string, fn *SQLAdapter) {
	if _, ok := adapters[name]; ok {
		panic(fmt.Errorf("upper: Adapter %q was already registered", name))
	}
	adapters[name] = fn
}

func Adapter(name string) SQLAdapter {
	if fn, ok := adapters[name]; ok {
		return *fn
	}
	return missingAdapter(name)
}

func missingAdapter(name string) SQLAdapter {
	err := fmt.Errorf("upper: Missing adapter %q, forgot to import?", name)
	return SQLAdapter{
		New: func(*sql.DB) (SQLDatabase, error) {
			return nil, err
		},
		NewTx: func(*sql.Tx) (SQLTx, error) {
			return nil, err
		},
		Open: func(db.ConnectionURL) (SQLDatabase, error) {
			return nil, err
		},
	}
}

type SQLTransaction interface {
	SQLDriver

	Commit() error
	Rollback() error
}

type SQLDriver interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	Prepare(query string) (*sql.Stmt, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
}

type SQLSession interface {
	SQLDriver

	Begin() (*sql.Tx, error)
	Close() error
	Driver() driver.Driver
	Ping() error
}
