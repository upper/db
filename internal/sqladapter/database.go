package sqladapter

import (
	"upper.io/db"
)

type Database interface {
	db.Database
	TableExists(name string) error
	TablePrimaryKey(name string) ([]string, error)
}
