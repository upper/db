package db

import (
	"database/sql"
)

// QueryBuilder is an experimental interface.
type QueryBuilder interface {
	Select(fields ...interface{}) QuerySelector
	InsertInto(table string) QueryInserter
	DeleteFrom(table string) QueryDeleter
	Update(table string) QueryUpdater
}

type QuerySelector interface {
	From(table ...string) Result
}

type QueryInserter interface {
	Values(...interface{}) QueryInserter
	Columns(...string) QueryInserter
	QueryExecer
}

type QueryDeleter interface {
	Where(...interface{}) QueryDeleter
	Limit(int) QueryDeleter
	QueryExecer
}

type QueryUpdater interface {
	Set(...interface{}) QueryUpdater
	Where(...interface{}) QueryUpdater
	Limit(int) QueryUpdater
	QueryExecer
}

type QueryExecer interface {
	Exec() (sql.Result, error)
}
