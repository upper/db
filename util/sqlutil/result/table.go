package result

import (
	"database/sql"
	"github.com/jmoiron/sqlx"
	"upper.io/v2/db/util/sqlgen"
)

type DataProvider interface {
	Name() string
	Query(sqlgen.Statement, ...interface{}) (*sqlx.Rows, error)
	QueryRow(sqlgen.Statement, ...interface{}) (*sqlx.Row, error)
	Exec(sqlgen.Statement, ...interface{}) (sql.Result, error)
	FieldValues(interface{}) ([]string, []interface{}, error)
}
