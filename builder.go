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
}

type QuerySelector interface {
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

	QueryGetter
	ResultIterator
	fmt.Stringer
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

type QueryGetter interface {
	Query() (*sqlx.Rows, error)
	QueryRow() (*sqlx.Row, error)
}

type ResultIterator interface {
	All(interface{}) error
	Next(interface{}) error
	One(interface{}) error
	Close() error
}
