package sqladapter

import (
	"fmt"
	"log"
	"reflect"
	"upper.io/db.v2"
	"upper.io/db.v2/builder/exql"
)

type PartialCollection interface {
	Database() BaseDatabase
	Name() string
	Conds(...interface{}) []interface{}
	Insert(interface{}) (interface{}, error)
}

type BaseCollection interface {
	Exists() bool
	Find(conds ...interface{}) db.Result
	Truncate() error
	InsertReturning(interface{}) error
}

type baseCollection struct {
	p PartialCollection
}

// NewCollection returns a collection with basic methods.
func NewBaseCollection(p PartialCollection) BaseCollection {
	return &baseCollection{p: p}
}

func (c *baseCollection) Find(conds ...interface{}) db.Result {
	return NewResult(
		c.p.Database().Builder(),
		c.p.Name(),
		c.p.Conds(conds...),
	)
}

// Exists returns true if the collection exists.
func (c *baseCollection) Exists() bool {
	if err := c.p.Database().TableExists(c.p.Name()); err != nil {
		return false
	}
	return true
}

// InsertReturning inserts an item and updates the variable.
func (c *baseCollection) InsertReturning(item interface{}) error {
	if reflect.TypeOf(item).Kind() != reflect.Ptr {
		return fmt.Errorf("Expecting a pointer to map or string but got %T", item)
	}

	var tx db.Tx
	inTx := false

	if currTx := c.p.Database().Tx(); currTx != nil {
		log.Printf("GOT TRANS")
		tx = c.p.Database()
		inTx = true
	} else {
		// Not within a transaction, let's create one.
		var err error
		tx, err = c.p.Database().Transaction()
		if err != nil {
			return err
		}
		log.Printf("NO TRANS")
	}

	var res db.Result

	col := tx.Collection(c.p.Name())

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
		// sess was created with sess.Transaction().
		log.Printf("COMITTED")
		return tx.Commit()
	}
	return err

cancel:
	// This goto label should only be used when we got an error within a
	// transaction and we don't want to continue.

	if !inTx {
		// This is only executed if t.Database() was **not** a transaction and if
		// sess was created with sess.Transaction().
		log.Printf("ROLLED BACK")
		tx.Rollback()
	}
	return err
}

// Truncate deletes all rows from the table.
func (c *baseCollection) Truncate() error {
	stmt := exql.Statement{
		Type:  exql.Truncate,
		Table: exql.TableWithName(c.p.Name()),
	}
	if _, err := c.p.Database().Builder().Exec(&stmt); err != nil {
		return err
	}
	return nil
}
