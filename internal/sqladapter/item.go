package sqladapter

import (
	"reflect"

	db "github.com/upper/db"
	"github.com/upper/db/sqlbuilder"
)

type item struct {
	sess     *session
	model    db.Model
	pristine db.M
}

func mapValues(model db.Model) db.M {
	m := db.M{}
	if model == nil {
		return m
	}
	fieldMap := sqlbuilder.Mapper.FieldMap(reflect.ValueOf(model))
	for column := range fieldMap {
		m[column] = fieldMap[column].Interface()
	}
	return m
}

func newItem(sess *session, model db.Model) db.Item {
	return &item{
		sess:     sess,
		model:    model,
		pristine: mapValues(model),
	}
}

func (z *item) Changes() db.M {
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

func (z *item) getPrimaryKeyFieldValues() ([]string, []interface{}) {
	pKeys := z.model.Collection(z.sess).(Collection).PrimaryKeys()
	fields := sqlbuilder.Mapper.FieldsByName(reflect.ValueOf(z.model), pKeys)

	values := make([]interface{}, 0, len(fields))
	for i := range fields {
		if fields[i].IsValid() {
			values = append(values, fields[i].Interface())
		}
	}

	return pKeys, values
}

func (z *item) id() (db.Cond, error) {
	if z.model == nil {
		return nil, db.ErrNilItem
	}

	id := db.Cond{}

	keys, fields := z.getPrimaryKeyFieldValues()
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

func (z *item) Save() error {
	if z.model == nil {
		return db.ErrNilItem
	}

	if reflect.TypeOf(z).Kind() != reflect.Ptr {
		return db.ErrExpectingPointerToStruct
	}

	_, fields := z.getPrimaryKeyFieldValues()
	isCreate := true
	for i := range fields {
		if fields[i] != reflect.Zero(reflect.TypeOf(fields[i])).Interface() {
			isCreate = false
		}
	}

	if isCreate {
		return z.doCreate()
	}
	return z.doUpdate()
}

func (z *item) doUpdate() error {
	if validator, ok := z.model.(db.Validator); ok {
		if err := validator.Validate(); err != nil {
			return err
		}
	}

	if hook, ok := z.model.(db.BeforeUpdateHook); ok {
		if err := hook.BeforeUpdate(z.sess); err != nil {
			return err
		}
	}

	if err := z.model.Collection(z.sess).UpdateReturning(z.model); err != nil {
		return err
	}

	if hook, ok := z.model.(db.AfterUpdateHook); ok {
		if err := hook.AfterUpdate(z.sess); err != nil {
			return err
		}
	}
	return nil
}

func (z *item) doCreate() error {
	if validator, ok := z.model.(db.Validator); ok {
		if err := validator.Validate(); err != nil {
			return err
		}
	}

	if hook, ok := z.model.(db.BeforeCreateHook); ok {
		if err := hook.BeforeCreate(z.sess); err != nil {
			return err
		}
	}

	if err := z.model.Collection(z.sess).InsertReturning(z.model); err != nil {
		return err
	}

	if hook, ok := z.model.(db.AfterCreateHook); ok {
		if err := hook.AfterCreate(z.sess); err != nil {
			return err
		}
	}
	return nil
}

func (z *item) Delete() error {
	if z.model == nil {
		return db.ErrNilItem
	}

	if reflect.TypeOf(z.model).Kind() != reflect.Ptr {
		return db.ErrExpectingPointerToStruct
	}

	conds, err := z.id()
	if err != nil {
		return err
	}

	if hook, ok := z.model.(db.BeforeDeleteHook); ok {
		if err := hook.BeforeDelete(z.sess); err != nil {
			return err
		}
	}

	if err := z.model.Collection(z.sess).Find(conds).Delete(); err != nil {
		return err
	}

	if hook, ok := z.model.(db.AfterDeleteHook); ok {
		if err := hook.AfterDelete(z.sess); err != nil {
			return err
		}
	}

	return nil
}

func (z *item) Update(m db.M) error {
	if z.model == nil {
		return db.ErrNilItem
	}

	if reflect.TypeOf(z.model).Kind() != reflect.Ptr {
		return db.ErrExpectingPointerToStruct
	}

	conds, err := z.id()
	if err != nil {
		return err
	}

	if hook, ok := z.model.(db.BeforeDeleteHook); ok {
		if err := hook.BeforeDelete(z.sess); err != nil {
			return err
		}
	}

	if err := z.model.Collection(z.sess).Find(conds).Update(m); err != nil {
		return err
	}

	if hook, ok := z.model.(db.AfterDeleteHook); ok {
		if err := hook.AfterDelete(z.sess); err != nil {
			return err
		}
	}

	return nil
}
