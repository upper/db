package bond

import (
	"errors"
)

var (
	ErrUnknownCollection = errors.New("unknown collection")
	ErrInvalidQuery      = errors.New("invalid query")
	ErrZeroItemID        = errors.New("item id is empty")
)

type Model interface {
	HasStore
}

type HasCollectionName interface {
	CollectionName() string
}

type HasStore interface {
	Store(sess Session) Store
}

type HasSave interface {
	Save(sess Session) error
}

type HasValidate interface {
	Validate() error
}

type HasBeforeCreate interface {
	BeforeCreate(Session) error
}

type HasAfterCreate interface {
	AfterCreate(Session) error
}

type HasBeforeUpdate interface {
	BeforeUpdate(Session) error
}

type HasAfterUpdate interface {
	AfterUpdate(Session) error
}

type HasBeforeDelete interface {
	BeforeDelete(Session) error
}

type HasAfterDelete interface {
	AfterDelete(Session) error
}

type StoreFunc func(sess Session) Store
