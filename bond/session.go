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
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"sync"

	"github.com/pkg/errors"
	"github.com/upper/db"
	"github.com/upper/db/sqlbuilder"
)

type hasContext interface {
	Context() context.Context
}

// Engine represents a bond database engine.
type Engine interface {
	db.Database

	sqlbuilder.SQLBuilder
}

// Session represents
type Session interface {
	Engine

	// Store returns a suitable store for the given table name (string), Model or
	// db.Collection.
	Store(item interface{}) Store

	// Save looks up the given model's store and delegates a Save call to it.
	Save(Model) error

	// Delete looks up the model's store and delegates the Delete call to it.
	Delete(Model) error

	// Context returns the context the session is running in.
	Context() context.Context

	// Transaction runs a transactional operation.
	Transaction(func(Session) error) error

	// TransactionContext runs a transactional operation on the given context.
	TransactionContext(context.Context, func(Session) error) error
}

type session struct {
	Engine

	memoStores map[string]*bondStore
	mu         sync.Mutex
}

// New wraps an Engine and returns a Session
func New(conn Engine) Session {
	return &session{
		Engine: conn,

		memoStores: make(map[string]*bondStore),
	}
}

// Open connects to a database and returns a Session.
func Open(adapter string, url db.ConnectionURL) (Session, error) {
	conn, err := sqlbuilder.Open(adapter, url)
	if err != nil {
		return nil, err
	}

	sess := New(conn)
	return sess, nil
}

// Bind creates a binding between an adapter and a *sql.Tx or a *sql.DB.
func Bind(adapter string, backend sqlbuilder.SQLEngine) (Session, error) {
	var conn Engine

	switch t := backend.(type) {
	case *sql.Tx:
		var err error
		conn, err = sqlbuilder.NewTx(adapter, t)
		if err != nil {
			return nil, err
		}
	case *sql.DB:
		var err error
		conn, err = sqlbuilder.New(adapter, t)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("Unknown backend type: %T", t)
	}

	return &session{
		Engine:     conn,
		memoStores: make(map[string]*bondStore),
	}, nil
}

func (sess *session) WithContext(ctx context.Context) Session {
	var backendCtx Engine
	switch t := sess.Engine.(type) {
	case interface {
		WithContext(context.Context) sqlbuilder.Database
	}:
		backendCtx = t.WithContext(ctx)
	case interface {
		WithContext(context.Context) sqlbuilder.Tx
	}:
		backendCtx = t.WithContext(ctx)
	default:
		panic("unexpected engine")
	}

	return &session{
		Engine:     backendCtx,
		memoStores: make(map[string]*bondStore),
	}
}

func (sess *session) Context() context.Context {
	if ctx, ok := sess.Engine.(hasContext); ok {
		return ctx.Context()
	}
	return context.Background()
}

func (sess *session) NewTx(ctx context.Context) (sqlbuilder.Tx, error) {
	return sess.Engine.(sqlbuilder.Database).NewTx(ctx)
}

func (sess *session) NewSessionTx(ctx context.Context) (Session, error) {
	tx, err := sess.NewTx(ctx)
	if err != nil {
		return nil, err
	}
	return &session{
		Engine:     tx,
		memoStores: make(map[string]*bondStore),
	}, nil
}

func (sess *session) Transaction(fn func(sess Session) error) error {
	return sess.TransactionContext(context.Background(), fn)
}

func (sess *session) TransactionContext(ctx context.Context, fn func(sess Session) error) error {
	txFn := func(sess sqlbuilder.Tx) error {
		return fn(&session{
			Engine:     sess,
			memoStores: make(map[string]*bondStore),
		})
	}

	switch t := sess.Engine.(type) {
	case sqlbuilder.Database:
		return t.Tx(ctx, txFn)
	case sqlbuilder.Tx:
		defer t.Close()
		err := txFn(t)
		if err != nil {
			if rErr := t.Rollback(); rErr != nil {
				return errors.Wrap(err, rErr.Error())
			}
			return err
		}
		return t.Commit()
	}

	return errors.New("Missing backend, forgot to use bond.New?")
}

func (sess *session) Save(item Model) error {
	if item == nil {
		return ErrExpectingNonNilModel
	}
	return item.Store(sess).Save(item)
}

func (sess *session) Delete(item Model) error {
	if item == nil {
		return ErrExpectingNonNilModel
	}
	return item.Store(sess).Delete(item)
}

func (sess *session) Store(item interface{}) Store {
	storeName := sess.resolveStoreName(item)
	if storeName == "" {
		return &bondStore{session: sess}
	}

	sess.mu.Lock()
	defer sess.mu.Unlock()

	if store, ok := sess.memoStores[storeName]; ok {
		return store
	}

	store := &bondStore{
		Collection: sess.Collection(storeName),
		session:    sess,
	}
	sess.memoStores[storeName] = store
	return sess.memoStores[storeName]
}

func (sess *session) resolveStoreName(item interface{}) string {
	// TODO: detect loops

	switch t := item.(type) {
	case string:
		return t
	case Model:
		return t.Store(sess).Name()
	case func(Session) db.Collection:
		return t(sess).Name()
	case db.Collection:
		return t.Name()
	default:
		itemv := reflect.ValueOf(item)
		if itemv.Kind() == reflect.Ptr {
			itemv = reflect.Indirect(itemv)
		}
		item = itemv.Interface()
		if m, ok := item.(Model); ok {
			return m.Store(sess).Name()
		}
	}

	return ""
}
