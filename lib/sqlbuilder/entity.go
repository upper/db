package sqlbuilder

import (
	"errors"
	"sync"

	"upper.io/db.v3"
)

var ErrMapperNotInitialized = errors.New("Mapper not initialized")

type Mapper interface {
	Store(interface{}) error
	Changeset() (db.Changeset, error)

	changesetWithOptions(options *MapOptions) (db.Changeset, error)
}

type Entity struct {
	initialValues db.Changeset
	ref           interface{}
	mu            sync.RWMutex
}

var _ = Mapper(&Entity{})

func (e *Entity) changesetWithOptions(options *MapOptions) (db.Changeset, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.ref == nil {
		return nil, ErrMapperNotInitialized
	}

	cols, vals, err := doMap(e.ref, options)
	if err != nil {
		return nil, err
	}

	var changeset db.Changeset
	for i := range vals {
		if vals[i] == e.initialValues[cols[i]] {
			continue
		}
		if changeset == nil {
			changeset = make(db.Changeset)
		}
		changeset[cols[i]] = vals[i]
	}
	return changeset, nil
}

func (e *Entity) Changeset() (db.Changeset, error) {
	return e.changesetWithOptions(nil)
}

func (e *Entity) Store(v interface{}) error {
	cols, vals, err := doMap(v, nil)
	if err != nil {
		return err
	}

	e.mu.Lock()
	e.initialValues = make(db.Changeset)
	for i := range cols {
		e.initialValues[cols[i]] = vals[i]
	}
	e.ref = v
	e.mu.Unlock()

	return nil
}
