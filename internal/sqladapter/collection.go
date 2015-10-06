package sqladapter

import (
	"upper.io/db"
	"upper.io/db/util/sqlutil/result"
)

type Collection struct {
	database  Database
	tableName string
}

// NewCollection returns a collection with basic methods.
func NewCollection(d Database, tableName string) *Collection {
	return &Collection{database: d, tableName: tableName}
}

// Name returns the name of the table.
func (c *Collection) Name() string {
	return c.tableName
}

// Exists returns true if the collection exists.
func (c *Collection) Exists() bool {
	if err := c.Database().TableExists(c.Name()); err != nil {
		return false
	}
	return true
}

// Find creates a result set with the given conditions.
func (c *Collection) Find(conds ...interface{}) db.Result {
	return result.NewResult(c.Database().Builder(), c.Name(), conds)
}

// Database returns the database session that backs the collection.
func (c *Collection) Database() Database {
	return c.database
}
