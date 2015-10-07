package db

import (
	"database/sql"
	"fmt"
	"github.com/jmoiron/sqlx"
)

// QueryBuilder is an experimental interface.
type QueryBuilder interface {
	Select(columns ...interface{}) QuerySelector
	SelectAllFrom(table string) QuerySelector

	InsertInto(table string) QueryInserter
	DeleteFrom(table string) QueryDeleter
	Update(table string) QueryUpdater

	Exec(query interface{}, args ...interface{}) (sql.Result, error)
}

type QuerySelector interface {
	Columns(columns ...interface{}) QuerySelector
	From(tables ...string) QuerySelector
	Distinct() QuerySelector
	Where(...interface{}) QuerySelector
	GroupBy(...interface{}) QuerySelector
	//Having(...interface{}) QuerySelector
	OrderBy(...interface{}) QuerySelector
	Using(...interface{}) QuerySelector
	FullJoin(...interface{}) QuerySelector
	CrossJoin(...interface{}) QuerySelector
	RightJoin(...interface{}) QuerySelector
	LeftJoin(...interface{}) QuerySelector
	Join(...interface{}) QuerySelector
	On(...interface{}) QuerySelector
	Limit(int) QuerySelector
	Offset(int) QuerySelector

	Iterator() Iterator

	QueryGetter
	fmt.Stringer
}

type QueryInserter interface {
	Values(...interface{}) QueryInserter
	Columns(...string) QueryInserter
	Extra(string) QueryInserter

	Iterator() Iterator

	QueryExecer
	QueryGetter

	fmt.Stringer
}

type QueryDeleter interface {
	Where(...interface{}) QueryDeleter
	Limit(int) QueryDeleter

	QueryExecer
	fmt.Stringer
}

type QueryUpdater interface {
	Set(...interface{}) QueryUpdater
	Where(...interface{}) QueryUpdater
	Limit(int) QueryUpdater

	QueryExecer
	fmt.Stringer
}

type QueryExecer interface {
	Exec() (sql.Result, error)
}

type QueryGetter interface {
	Query() (*sqlx.Rows, error)
	QueryRow() (*sqlx.Row, error)
}

type Iterator interface {
	All(dest interface{}) error
	One(dest interface{}) error
	Scan(dest ...interface{}) error
	Next(dest ...interface{}) bool
	Err() error
	Close() error
}
