package db

import (
	"database/sql"
)

// QueryBuilder is an experimental interface.
type QueryBuilder interface {
	Select(fields ...interface{}) QuerySelector
	InsertInto(table string) QueryInserter
	//Update(table string) QueryUpdater
}

type QuerySelector interface {
	From(table ...string) Result
}

type QueryInserter interface {
	Values(...interface{}) QueryInserter
	Columns(...string) QueryInserter
	Exec() (sql.Result, error)
}

type QueryUpdater interface {
	Set() QueryUpdater

	Do() error
}
