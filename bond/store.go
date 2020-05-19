// Copyright (c) 2012-present The upper.io/db authors. All rights reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining
// a copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to
// permit persons to whom the Software is furnished to do so, subject to
// the following conditions:
//
// The above copyright notice and this permission notice shall be
// included in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
// LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
// OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
// WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

package bond

import (
	"reflect"

	"github.com/upper/db"
	"github.com/upper/db/internal/reflectx"
	"github.com/upper/db/internal/sqladapter"
)

var mapper = reflectx.NewMapper("db")

// Store represents a mechanism to save items.
type Store interface {
	db.Collection

	// Session returns the underlying session the store belongs to.
	Session() Session

	// Save creates or updates the given item (this depends on the values of the
	// primary key, if the primary key is defined an update operation will be
	// attempted, a create operation will be tried otherwise). If the given model
	// satisfies the HasSave interface this method will delegate the save task to
	// the model.
	Save(Model) error

	// Delete removes the item from the store.
	Delete(Model) error

	// Create inserts a new item into the store.
	Create(Model) error

	// Update modifies the item in the store. This will work as long as the
	// primary keys are non-zero values.
	Update(Model) error
}

type bondStore struct {
	db.Collection

	session Session
}

func (st *bondStore) getPrimaryKeyFieldValues(item interface{}) ([]string, []interface{}) {
	pKeys := st.Collection.(sqladapter.HasPrimaryKeys).PrimaryKeys()
	fields := mapper.FieldsByName(reflect.ValueOf(item), pKeys)

	values := make([]interface{}, 0, len(fields))
	for i := range fields {
		if fields[i].IsValid() {
			values = append(values, fields[i].Interface())
		}
	}

	return pKeys, values
}

func (st *bondStore) Save(item Model) error {
	if saver, ok := item.(HasSave); ok {
		return st.Session().Tx(func(tx Session) error {
			return saver.Save(tx)
		})
	}

	if st.Collection == nil {
		return ErrInvalidCollection
	}

	if reflect.TypeOf(item).Kind() != reflect.Ptr {
		return ErrExpectingPointerToStruct
	}

	_, fields := st.getPrimaryKeyFieldValues(item)
	isCreate := true
	for i := range fields {
		if fields[i] != reflect.Zero(reflect.TypeOf(fields[i])).Interface() {
			isCreate = false
		}
	}

	if isCreate {
		return st.Create(item)
	}

	return st.Update(item)
}

func (st *bondStore) Create(item Model) error {
	if st.Collection == nil {
		return ErrInvalidCollection
	}

	if validator, ok := item.(Validator); ok {
		if err := validator.Validate(); err != nil {
			return err
		}
	}

	if m, ok := item.(BeforeCreateHook); ok {
		if err := m.BeforeCreate(st.session); err != nil {
			return err
		}
	}

	if reflect.TypeOf(item).Kind() == reflect.Ptr {
		if err := st.Collection.InsertReturning(item); err != nil {
			return err
		}
	} else {
		if _, err := st.Collection.Insert(item); err != nil {
			return err
		}
	}

	if m, ok := item.(AfterCreateHook); ok {
		if err := m.AfterCreate(st.session); err != nil {
			return err
		}
	}
	return nil
}

func (st *bondStore) Update(item Model) error {
	if st.Collection == nil {
		return ErrInvalidCollection
	}

	if validator, ok := item.(Validator); ok {
		if err := validator.Validate(); err != nil {
			return err
		}
	}

	if m, ok := item.(BeforeUpdateHook); ok {
		if err := m.BeforeUpdate(st.session); err != nil {
			return err
		}
	}

	cond := db.And()
	pKeys, fields := st.getPrimaryKeyFieldValues(item)
	for i := range pKeys {
		cond = cond.And(db.Cond{pKeys[i]: fields[i]})
	}
	if cond.Empty() {
		return ErrZeroItemID
	}

	if reflect.TypeOf(item).Kind() == reflect.Ptr {
		if err := st.Collection.UpdateReturning(item); err != nil {
			return err
		}
	} else {
		if err := st.Collection.Find(cond).Update(item); err != nil {
			return err
		}
	}

	if m, ok := item.(AfterUpdateHook); ok {
		if err := m.AfterUpdate(st.session); err != nil {
			return err
		}
	}

	return nil
}

func (st *bondStore) Delete(item Model) error {
	if st.Collection == nil {
		return ErrInvalidCollection
	}

	if reflect.TypeOf(item).Kind() != reflect.Ptr {
		return ErrExpectingPointerToStruct
	}

	cond := db.And()
	keys, values := st.getPrimaryKeyFieldValues(item)
	for i := range keys {
		cond = cond.And(db.Cond{keys[i]: values[i]})
	}
	if cond.Empty() {
		return ErrZeroItemID
	}

	if m, ok := item.(BeforeDeleteHook); ok {
		if err := m.BeforeDelete(st.session); err != nil {
			return err
		}
	}

	if err := st.Collection.Find(cond).Delete(); err != nil {
		return err
	}

	if m, ok := item.(AfterDeleteHook); ok {
		if err := m.AfterDelete(st.session); err != nil {
			return err
		}
	}

	return nil
}

// Session returns the underlying Session.
func (st *bondStore) Session() Session {
	return st.session
}
