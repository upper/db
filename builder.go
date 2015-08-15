package db

// QueryBuilder is an experimental interface.
type QueryBuilder interface {
	Select(fields ...interface{}) QuerySelector
	//Update(table string) QueryUpdater
}

type QuerySelector interface {
	From(table ...string) Result
}

type QueryUpdater interface {
	Set() QueryUpdater

	Do() error
}
