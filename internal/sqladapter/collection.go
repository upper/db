package sqladapter

import (
	"fmt"
	"reflect"

	"upper.io/db.v2"
	"upper.io/db.v2/internal/sqladapter/exql"
)

// Collection represents a SQL table.
type Collection interface {
	PartialCollection
	BaseCollection
}

// PartialCollection defines methods to be implemented by the adapter.
type PartialCollection interface {
	Database() Database
	Name() string
	Conds(...interface{}) []interface{}
	Insert(interface{}) (interface{}, error)
}

// BaseCollection defines methods that are implemented by sqladapter.
type BaseCollection interface {
	Exists() bool
	Find(conds ...interface{}) db.Result
	Truncate() error
	InsertReturning(interface{}) error
	PrimaryKeys() []string
}

// collection is the implementation of Collection.
type collection struct {
	p  PartialCollection
	pk []string
}

// NewBaseCollection returns a collection with basic methods.
func NewBaseCollection(p PartialCollection) BaseCollection {
	c := &collection{p: p}
	c.pk, _ = c.p.Database().FindTablePrimaryKeys(c.p.Name())
	return c
}

// PrimaryKeys returns the collection's primary keys, if any.
func (c *collection) PrimaryKeys() []string {
	return c.pk
}

// Find creates a result set with the given conditions.
func (c *collection) Find(conds ...interface{}) db.Result {
	return NewResult(
		c.p.Database(),
		c.p.Name(),
		c.p.Conds(conds...),
	)
}

// Exists returns true if the collection exists.
func (c *collection) Exists() bool {
	if err := c.p.Database().TableExists(c.p.Name()); err != nil {
		return false
	}
	return true
}

// InsertReturning inserts an item and updates the given variable reference.
func (c *collection) InsertReturning(item interface{}) error {
	if reflect.TypeOf(item).Kind() != reflect.Ptr {
		return fmt.Errorf("Expecting a pointer to map or string but got %T", item)
	}

	var tx DatabaseTx
	inTx := false

	if currTx := c.p.Database().Transaction(); currTx != nil {
		tx = newTxWrapper(c.p.Database())
		inTx = true
	} else {
		// Not within a transaction, let's create one.
		var err error
		tx, err = c.p.Database().NewLocalTransaction()
		if err != nil {
			return err
		}
		defer tx.(Database).Close()
	}

	var res db.Result

	col := tx.(Database).Collection(c.p.Name())

	id, err := col.Insert(item)
	if err != nil {
		goto cancel
	}
	if id == nil {
		err = fmt.Errorf("Insertion did not return any ID, aborted.")
		goto cancel
	}

	res = col.Find(id)
	if err = res.One(item); err != nil {
		goto cancel
	}

	if !inTx {
		// This is only executed if t.Database() was **not** a transaction and if
		// sess was created with sess.NewTransaction().
		return tx.Commit()
	}
	return err

cancel:
	// This goto label should only be used when we got an error within a
	// transaction and we don't want to continue.

	if !inTx {
		// This is only executed if t.Database() was **not** a transaction and if
		// sess was created with sess.NewTransaction().
		tx.Rollback()
	}
	return err
}

// Truncate deletes all rows from the table.
func (c *collection) Truncate() error {
	stmt := exql.Statement{
		Type:  exql.Truncate,
		Table: exql.TableWithName(c.p.Name()),
	}
	if _, err := c.p.Database().Exec(&stmt); err != nil {
		return err
	}
	return nil
}
