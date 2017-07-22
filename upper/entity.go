package repo

import (
	"sync"
	"upper.io/db.v3"
	"upper.io/db.v3/lib/sqlbuilder"
)

type Mapper interface {
	Store(interface{}) error
	Changeset() (db.Changeset, error)
}

type Entity struct {
	initialValues db.Changeset
	ref           interface{}
	mu            sync.RWMutex
}

var _ = Mapper(&Entity{})

func (e *Entity) Changeset() (db.Changeset, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	cols, vals, err := sqlbuilder.Map(e.ref, nil)
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

func (e *Entity) Store(v interface{}) error {
	cols, vals, err := sqlbuilder.Map(v, nil)
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
