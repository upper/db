package bond

import (
	"reflect"

	"github.com/upper/db"
	"github.com/upper/db/internal/reflectx"
	"github.com/upper/db/internal/sqladapter"
)

var mapper = reflectx.NewMapper("db")

type Store interface {
	db.Collection

	Session() Session

	Save(Model) error
	Delete(Model) error
	Create(Model) error
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
		return st.Session().SessionTx(nil, func(tx Session) error {
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

	if validator, ok := item.(HasValidate); ok {
		if err := validator.Validate(); err != nil {
			return err
		}
	}

	if m, ok := item.(HasBeforeCreate); ok {
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

	if m, ok := item.(HasAfterCreate); ok {
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

	if validator, ok := item.(HasValidate); ok {
		if err := validator.Validate(); err != nil {
			return err
		}
	}

	if m, ok := item.(HasBeforeUpdate); ok {
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

	if m, ok := item.(HasAfterUpdate); ok {
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

	if m, ok := item.(HasBeforeDelete); ok {
		if err := m.BeforeDelete(st.session); err != nil {
			return err
		}
	}

	if err := st.Collection.Find(cond).Delete(); err != nil {
		return err
	}

	if m, ok := item.(HasAfterDelete); ok {
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
