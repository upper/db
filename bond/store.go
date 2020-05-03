package bond

import (
	"reflect"
	"github.com/upper/db"
	"github.com/upper/db/internal/reflectx"
)

var mapper = reflectx.NewMapper("db")

type hasPrimaryKeys interface {
	PrimaryKeys() []string
}

type Store interface {
	db.Collection

	Session() Session
	WithSession(sess Session) Store

	Save(interface{}) error
	Delete(interface{}) error
	Create(interface{}) error
	Update(interface{}) error
}

type store struct {
	db.Collection

	session Session
}

func (s *store) getPrimaryKeyFields(item interface{}) ([]string, []interface{}) {
	pKeys := s.Collection.(hasPrimaryKeys).PrimaryKeys()
	fields := mapper.FieldsByName(reflect.ValueOf(item), pKeys)

	values := make([]interface{}, 0, len(fields))
	for i := range fields {
		if fields[i].IsValid() {
			values = append(values, fields[i].Interface())
		}
	}

	return pKeys, values
}

// WithSession returns a copy of the store that runs in the context of the given
// transaction.
func (s *store) WithSession(sess Session) Store {
	return &store{
		Collection: sess.Collection(s.Collection.Name()),
		session:    sess,
	}
}

func (s *store) Save(item interface{}) error {
	if saver, ok := item.(HasSave); ok {
		return s.Session().SessionTx(nil, func(tx Session) error {
			return saver.Save(tx)
		})
	}

	if s.Collection == nil {
		return ErrInvalidCollection
	}

	if reflect.TypeOf(item).Kind() != reflect.Ptr {
		return ErrExpectingPointerToStruct
	}

	_, fields := s.getPrimaryKeyFields(item)
	isCreate := true
	for i := range fields {
		if fields[i] != reflect.Zero(reflect.TypeOf(fields[i])).Interface() {
			isCreate = false
		}
	}

	if isCreate {
		return s.Create(item)
	}

	return s.Update(item)
}

func (s *store) Create(item interface{}) error {
	if s.Collection == nil {
		return ErrInvalidCollection
	}

	if validator, ok := item.(HasValidate); ok {
		if err := validator.Validate(); err != nil {
			return err
		}
	}

	if m, ok := item.(HasBeforeCreate); ok {
		if err := m.BeforeCreate(s.session); err != nil {
			return err
		}
	}

	if reflect.TypeOf(item).Kind() == reflect.Ptr {
		if err := s.Collection.InsertReturning(item); err != nil {
			return err
		}
	} else {
		if _, err := s.Collection.Insert(item); err != nil {
			return err
		}
	}

	if m, ok := item.(HasAfterCreate); ok {
		if err := m.AfterCreate(s.session); err != nil {
			return err
		}
	}
	return nil
}

func (s *store) Update(item interface{}) error {
	if s.Collection == nil {
		return ErrInvalidCollection
	}

	if validator, ok := item.(HasValidate); ok {
		if err := validator.Validate(); err != nil {
			return err
		}
	}

	if m, ok := item.(HasBeforeUpdate); ok {
		if err := m.BeforeUpdate(s.session); err != nil {
			return err
		}
	}

	cond := db.And()
	pKeys, fields := s.getPrimaryKeyFields(item)
	for i := range pKeys {
		cond = cond.And(db.Cond{pKeys[i]: fields[i]})
	}
	if cond.Empty() {
		return ErrZeroItemID
	}

	if reflect.TypeOf(item).Kind() == reflect.Ptr {
		if err := s.Collection.UpdateReturning(item); err != nil {
			return err
		}
	} else {
		if err := s.Collection.Find(cond).Update(item); err != nil {
			return err
		}
	}

	if m, ok := item.(HasAfterUpdate); ok {
		if err := m.AfterUpdate(s.session); err != nil {
			return err
		}
	}

	return nil
}

func (s *store) Delete(item interface{}) error {
	if s.Collection == nil {
		return ErrInvalidCollection
	}

	if reflect.TypeOf(item).Kind() != reflect.Ptr {
		return ErrExpectingPointerToStruct
	}

	cond := db.And()
	pKeys, fields := s.getPrimaryKeyFields(item)
	for i := range pKeys {
		cond = cond.And(db.Cond{pKeys[i]: fields[i]})
	}
	if cond.Empty() {
		return ErrZeroItemID
	}

	if m, ok := item.(HasBeforeDelete); ok {
		if err := m.BeforeDelete(s.session); err != nil {
			return err
		}
	}

	if err := s.Collection.Find(cond).Delete(); err != nil {
		return err
	}

	if m, ok := item.(HasAfterDelete); ok {
		if err := m.AfterDelete(s.session); err != nil {
			return err
		}
	}

	return nil
}

func (s *store) Session() Session {
	return s.session
}
