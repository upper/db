package sqladapter

import (
	"errors"
	"fmt"
	"reflect"

	db "github.com/upper/db"
	"github.com/upper/db/internal/reflectx"
	"github.com/upper/db/internal/sqladapter/exql"
	"github.com/upper/db/sqlbuilder"
)

var mapper = reflectx.NewMapper("db")

var errMissingPrimaryKeys = errors.New("Table %q has no primary keys")

// AdapterCollection defines collection methods that must be implemented by
// adapters.
type AdapterCollection interface {
	// Insert inserts a new item into the collection.
	Insert(Collection, interface{}) (interface{}, error)
}

// Collection provides logic for methods that can be shared across all SQL
// adapters.
type Collection interface {
	Insert(interface{}) (*db.InsertResult, error)

	Name() string

	Session() db.Session

	SQLBuilder() sqlbuilder.SQLBuilder

	// Exists returns true if the collection exists.
	Exists() bool

	// Find creates and returns a new result set.
	Find(conds ...interface{}) db.Result

	// Truncate removes all items on the collection.
	Truncate() error

	// InsertReturning inserts a new item and updates it with the
	// actual values from the database.
	InsertReturning(interface{}) error

	// UpdateReturning updates an item and returns the actual values from the
	// database.
	UpdateReturning(interface{}) error

	// PrimaryKeys returns the names of all primary keys in the table.
	PrimaryKeys() []string
}

type finder interface {
	Find(Collection, *Result, ...interface{}) db.Result
}

type condsFilter interface {
	FilterConds(...interface{}) []interface{}
}

// collection is the implementation of Collection.
type collection struct {
	name string
	sess Session

	adapter AdapterCollection

	pk  []string
	err error
}

var (
	_ = Collection(&collection{})
)

func NewCollection(sess Session, name string, adapterCollection AdapterCollection) Collection {
	c := &collection{
		sess:    sess,
		name:    name,
		adapter: adapterCollection,
	}
	c.pk, c.err = c.sess.PrimaryKeys(c.Name())
	return c
}

func (c *collection) SQLBuilder() sqlbuilder.SQLBuilder {
	return c.sess.(sqlbuilder.SQLBuilder)
}

func (c *collection) Session() db.Session {
	return c.sess
}

func (c *collection) Name() string {
	return c.name
}

func (c *collection) Insert(item interface{}) (*db.InsertResult, error) {
	id, err := c.adapter.Insert(c, item)
	if err != nil {
		return nil, err
	}
	return db.NewInsertResult(id), nil
}

// PrimaryKeys returns the collection's primary keys, if any.
func (c *collection) PrimaryKeys() []string {
	return c.pk
}

func (c *collection) filterConds(conds ...interface{}) []interface{} {
	if len(conds) == 1 && len(c.pk) == 1 {
		if id := conds[0]; IsKeyValue(id) {
			conds[0] = db.Cond{c.pk[0]: db.Eq(id)}
		}
	}
	if tr, ok := c.adapter.(condsFilter); ok {
		return tr.FilterConds(conds...)
	}
	return conds
}

// Find creates a result set with the given conditions.
func (c *collection) Find(conds ...interface{}) db.Result {
	if c.err != nil {
		res := &Result{}
		res.setErr(c.err)
		return res
	}

	res := NewResult(
		c.sess,
		c.Name(),
		c.filterConds(conds...),
	)
	if f, ok := c.adapter.(finder); ok {
		return f.Find(c, res, conds...)
	}
	return res
}

// Exists returns true if the collection exists.
func (c *collection) Exists() bool {
	if err := c.sess.TableExists(c.Name()); err != nil {
		return false
	}
	return true
}

// InsertReturning inserts an item and updates the given variable reference.
func (c *collection) InsertReturning(item interface{}) error {
	if item == nil || reflect.TypeOf(item).Kind() != reflect.Ptr {
		return fmt.Errorf("Expecting a pointer but got %T", item)
	}

	// Grab primary keys
	pks := c.PrimaryKeys()
	if len(pks) == 0 {
		if !c.Exists() {
			return db.ErrCollectionDoesNotExist
		}
		return fmt.Errorf(errMissingPrimaryKeys.Error(), c.Name())
	}

	var tx SessionTx
	inTx := false

	if currTx := c.sess.Transaction(); currTx != nil {
		tx = NewSessionTx(c.sess)
		inTx = true
	} else {
		// Not within a transaction, let's create one.
		var err error
		tx, err = c.sess.NewSessionTx(c.sess.Context())
		if err != nil {
			return err
		}
		defer tx.(Session).Close()
	}

	// Allocate a clone of item.
	newItem := reflect.New(reflect.ValueOf(item).Elem().Type()).Interface()
	var newItemFieldMap map[string]reflect.Value

	itemValue := reflect.ValueOf(item)

	col := tx.(Session).Collection(c.Name())

	// Insert item as is and grab the returning ID.
	var newItemRes db.Result
	id, err := col.Insert(item)
	if err != nil {
		goto cancel
	}
	if id == nil {
		err = fmt.Errorf("InsertReturning: Could not get a valid ID after inserting. Does the %q table have a primary key?", c.Name())
		goto cancel
	}

	if len(pks) > 1 {
		newItemRes = col.Find(id)
	} else {
		// We have one primary key, build a explicit db.Cond with it to prevent
		// string keys to be considered as raw conditions.
		newItemRes = col.Find(db.Cond{pks[0]: id}) // We already checked that pks is not empty, so pks[0] is defined.
	}

	// Fetch the row that was just interted into newItem
	err = newItemRes.One(newItem)
	if err != nil {
		goto cancel
	}

	switch reflect.ValueOf(newItem).Elem().Kind() {
	case reflect.Struct:
		// Get valid fields from newItem to overwrite those that are on item.
		newItemFieldMap = mapper.ValidFieldMap(reflect.ValueOf(newItem))
		for fieldName := range newItemFieldMap {
			mapper.FieldByName(itemValue, fieldName).Set(newItemFieldMap[fieldName])
		}
	case reflect.Map:
		newItemV := reflect.ValueOf(newItem).Elem()
		itemV := reflect.ValueOf(item)
		if itemV.Kind() == reflect.Ptr {
			itemV = itemV.Elem()
		}
		for _, keyV := range newItemV.MapKeys() {
			itemV.SetMapIndex(keyV, newItemV.MapIndex(keyV))
		}
	default:
		err = fmt.Errorf("InsertReturning: expecting a pointer to map or struct, got %T", newItem)
		goto cancel
	}

	if !inTx {
		// This is only executed if t.Session() was **not** a transaction and if
		// sess was created with sess.NewTransaction().
		return tx.Commit()
	}

	return err

cancel:
	// This goto label should only be used when we got an error within a
	// transaction and we don't want to continue.

	if !inTx {
		// This is only executed if t.Session() was **not** a transaction and if
		// sess was created with sess.NewTransaction().
		_ = tx.Rollback()
	}
	return err
}

func (c *collection) UpdateReturning(item interface{}) error {
	if item == nil || reflect.TypeOf(item).Kind() != reflect.Ptr {
		return fmt.Errorf("Expecting a pointer but got %T", item)
	}

	// Grab primary keys
	pks := c.PrimaryKeys()
	if len(pks) == 0 {
		if !c.Exists() {
			return db.ErrCollectionDoesNotExist
		}
		return fmt.Errorf(errMissingPrimaryKeys.Error(), c.Name())
	}

	var tx SessionTx
	inTx := false

	if currTx := c.sess.Transaction(); currTx != nil {
		tx = NewSessionTx(c.sess)
		inTx = true
	} else {
		// Not within a transaction, let's create one.
		var err error
		tx, err = c.sess.NewSessionTx(c.sess.Context())
		if err != nil {
			return err
		}
		defer tx.(Session).Close()
	}

	// Allocate a clone of item.
	defaultItem := reflect.New(reflect.ValueOf(item).Elem().Type()).Interface()
	var defaultItemFieldMap map[string]reflect.Value

	itemValue := reflect.ValueOf(item)

	conds := db.Cond{}
	for _, pk := range pks {
		conds[pk] = db.Eq(mapper.FieldByName(itemValue, pk).Interface())
	}

	col := tx.(Session).Collection(c.Name())

	err := col.Find(conds).Update(item)
	if err != nil {
		goto cancel
	}

	if err = col.Find(conds).One(defaultItem); err != nil {
		goto cancel
	}

	switch reflect.ValueOf(defaultItem).Elem().Kind() {
	case reflect.Struct:
		// Get valid fields from defaultItem to overwrite those that are on item.
		defaultItemFieldMap = mapper.ValidFieldMap(reflect.ValueOf(defaultItem))
		for fieldName := range defaultItemFieldMap {
			mapper.FieldByName(itemValue, fieldName).Set(defaultItemFieldMap[fieldName])
		}
	case reflect.Map:
		defaultItemV := reflect.ValueOf(defaultItem).Elem()
		itemV := reflect.ValueOf(item)
		if itemV.Kind() == reflect.Ptr {
			itemV = itemV.Elem()
		}
		for _, keyV := range defaultItemV.MapKeys() {
			itemV.SetMapIndex(keyV, defaultItemV.MapIndex(keyV))
		}
	default:
		panic("default")
	}

	if !inTx {
		// This is only executed if t.Session() was **not** a transaction and if
		// sess was created with sess.NewTransaction().
		return tx.Commit()
	}
	return err

cancel:
	// This goto label should only be used when we got an error within a
	// transaction and we don't want to continue.

	if !inTx {
		// This is only executed if t.Session() was **not** a transaction and if
		// sess was created with sess.NewTransaction().
		_ = tx.Rollback()
	}
	return err
}

// Truncate deletes all rows from the table.
func (c *collection) Truncate() error {
	stmt := exql.Statement{
		Type:  exql.Truncate,
		Table: exql.TableWithName(c.Name()),
	}
	if _, err := c.sess.Exec(&stmt); err != nil {
		return err
	}
	return nil
}
