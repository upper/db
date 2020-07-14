package sqlbuilder

import (
	"reflect"

	db "github.com/upper/db/v4"
)

type hasPrimaryKeys interface {
	PrimaryKeys() []string
}

type Item struct {
	model    db.Model
	pristine db.M
}

func (z *Item) Reflect(model db.Model) {
	z.SetPristine(model)
}

func mapValues(model db.Model) db.M {
	m := db.M{}
	if model == nil {
		return m
	}
	fieldMap := Mapper.FieldMap(reflect.ValueOf(model))
	for column := range fieldMap {
		m[column] = fieldMap[column].Interface()
	}
	return m
}

func NewItem(sess Session, model db.Model) db.Item {
	item := &Item{}
	item.SetPristine(model)
	return item
}

func (z *Item) SetPristine(model db.Model) {
	z.model = model               // current value
	z.pristine = mapValues(model) // copy
}

func (z *Item) Changes() db.M {
	diff := db.M{}
	state := mapValues(z.model)
	for column := range z.pristine {
		// TODO: deep equality
		if z.pristine[column] != state[column] {
			diff[column] = state[column]
		}
	}
	return diff
}

func (z *Item) getPrimaryKeyFieldValues(sess db.Session) ([]string, []interface{}) {
	pKeys := z.model.Collection(sess).(hasPrimaryKeys).PrimaryKeys()
	fields := Mapper.FieldsByName(reflect.ValueOf(z.model), pKeys)

	values := make([]interface{}, 0, len(fields))
	for i := range fields {
		if fields[i].IsValid() {
			values = append(values, fields[i].Interface())
		}
	}

	return pKeys, values
}

func (z *Item) id(sess db.Session) (db.Cond, error) {
	if z.model == nil {
		return nil, db.ErrNilItem
	}

	id := db.Cond{}

	keys, fields := z.getPrimaryKeyFieldValues(sess)
	for i := range fields {
		if fields[i] == reflect.Zero(reflect.TypeOf(fields[i])).Interface() {
			return nil, db.ErrZeroItemID
		}
		id[keys[i]] = fields[i]
	}
	if len(id) < 1 {
		return nil, db.ErrZeroItemID
	}

	return id, nil
}

func (z *Item) Save(sess db.Session) error {
	if z.model == nil {
		return db.ErrNilItem
	}

	if reflect.TypeOf(z).Kind() != reflect.Ptr {
		return db.ErrExpectingPointerToStruct
	}

	_, fields := z.getPrimaryKeyFieldValues(sess)
	isCreate := true
	for i := range fields {
		if fields[i] != reflect.Zero(reflect.TypeOf(fields[i])).Interface() {
			isCreate = false
		}
	}

	if isCreate {
		return z.doCreate(sess)
	}
	return z.doUpdate(sess)
}

func (z *Item) doUpdate(sess db.Session) error {
	if validator, ok := z.model.(db.Validator); ok {
		if err := validator.Validate(); err != nil {
			return err
		}
	}

	if hook, ok := z.model.(db.BeforeUpdateHook); ok {
		if err := hook.BeforeUpdate(sess); err != nil {
			return err
		}
	}

	if err := z.model.Collection(sess).UpdateReturning(z.model); err != nil {
		return err
	}

	if hook, ok := z.model.(db.AfterUpdateHook); ok {
		if err := hook.AfterUpdate(sess); err != nil {
			return err
		}
	}
	return nil
}

func (z *Item) doCreate(sess db.Session) error {
	if validator, ok := z.model.(db.Validator); ok {
		if err := validator.Validate(); err != nil {
			return err
		}
	}

	if hook, ok := z.model.(db.BeforeCreateHook); ok {
		if err := hook.BeforeCreate(sess); err != nil {
			return err
		}
	}

	if err := z.model.Collection(sess).InsertReturning(z.model); err != nil {
		return err
	}

	if hook, ok := z.model.(db.AfterCreateHook); ok {
		if err := hook.AfterCreate(sess); err != nil {
			return err
		}
	}
	return nil
}

func (z *Item) Delete(sess db.Session) error {
	if z.model == nil {
		return db.ErrNilItem
	}

	if reflect.TypeOf(z.model).Kind() != reflect.Ptr {
		return db.ErrExpectingPointerToStruct
	}

	conds, err := z.id(sess)
	if err != nil {
		return err
	}

	if hook, ok := z.model.(db.BeforeDeleteHook); ok {
		if err := hook.BeforeDelete(sess); err != nil {
			return err
		}
	}

	if err := z.model.Collection(sess).Find(conds).Delete(); err != nil {
		return err
	}

	if hook, ok := z.model.(db.AfterDeleteHook); ok {
		if err := hook.AfterDelete(sess); err != nil {
			return err
		}
	}

	return nil
}

func (z *Item) Update(sess db.Session, m db.M) error {
	if z.model == nil {
		return db.ErrNilItem
	}

	if reflect.TypeOf(z.model).Kind() != reflect.Ptr {
		return db.ErrExpectingPointerToStruct
	}

	conds, err := z.id(sess)
	if err != nil {
		return err
	}

	if hook, ok := z.model.(db.BeforeDeleteHook); ok {
		if err := hook.BeforeDelete(sess); err != nil {
			return err
		}
	}

	if err := z.model.Collection(sess).Find(conds).Update(m); err != nil {
		return err
	}

	if hook, ok := z.model.(db.AfterDeleteHook); ok {
		if err := hook.AfterDelete(sess); err != nil {
			return err
		}
	}

	return nil
}
