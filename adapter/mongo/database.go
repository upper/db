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

// Package mongo wraps the official MongoDB driver. See
// https://github.com/upper/db/adapter/mongo for documentation, particularities
// and usage examples.
package mongo

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	db "github.com/upper/db/v4"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Adapter holds the name of the mongodb adapter.
const Adapter = `mongo`

var connTimeout = time.Second * 5

// Source represents a MongoDB database.
type Source struct {
	db.Settings

	ctx context.Context

	name          string
	connURL       db.ConnectionURL
	session       *mongo.Client // rename to client
	database      *mongo.Database
	version       []int
	collections   map[string]*Collection
	collectionsMu sync.Mutex
}

type mongoAdapter struct {
}

func (mongoAdapter) Open(dsn db.ConnectionURL) (db.Session, error) {
	return Open(dsn)
}

func init() {
	db.RegisterAdapter(Adapter, db.Adapter(&mongoAdapter{}))
}

// Open stablishes a new connection to a SQL server.
func Open(connURL db.ConnectionURL) (db.Session, error) {
	ctx := context.Background()
	settings := db.NewSettings()

	d := &Source{
		Settings: settings,
		ctx:      ctx,
	}
	if err := d.Open(connURL); err != nil {
		return nil, fmt.Errorf("Open: %w", err)
	}

	return d, nil
}

func (s *Source) TxContext(context.Context, func(tx db.Session) error, *sql.TxOptions) error {
	return db.ErrNotSupportedByAdapter
}

func (s *Source) Tx(func(db.Session) error) error {
	return db.ErrNotSupportedByAdapter
}

func (s *Source) SQL() db.SQL {
	// Not supported
	panic("sql builder is not supported by mongodb")
}

func (s *Source) ConnectionURL() db.ConnectionURL {
	return s.connURL
}

// SetConnMaxLifetime is not supported.
func (s *Source) SetConnMaxLifetime(time.Duration) {
	s.Settings.SetConnMaxLifetime(time.Duration(0))
}

// SetMaxIdleConns is not supported.
func (s *Source) SetMaxIdleConns(int) {
	s.Settings.SetMaxIdleConns(0)
}

// SetMaxOpenConns is not supported.
func (s *Source) SetMaxOpenConns(int) {
	s.Settings.SetMaxOpenConns(0)
}

// Name returns the name of the database.
func (s *Source) Name() string {
	return s.name
}

// Open attempts to connect to the database.
func (s *Source) Open(connURL db.ConnectionURL) error {
	s.connURL = connURL
	return s.open()
}

// Clone returns a cloned db.Session session.
func (s *Source) Clone() (db.Session, error) {
	clone := &Source{
		Settings: db.NewSettings(),

		name:        s.name,
		connURL:     s.connURL,
		version:     s.version,
		collections: map[string]*Collection{},
	}

	if err := clone.open(); err != nil {
		return nil, err
	}

	return clone, nil
}

// Ping checks whether a connection to the database is still alive by pinging
// it, establishing a connection if necessary.
func (s *Source) Ping() error {
	return s.session.Ping(context.Background(), nil)
}

func (s *Source) Reset() {
	s.collectionsMu.Lock()
	defer s.collectionsMu.Unlock()

	s.collections = make(map[string]*Collection)
}

func (s *Source) Driver() interface{} {
	return s.session
}

func (s *Source) open() error {
	var err error

	ctx, cancel := context.WithTimeout(context.Background(), connTimeout)
	defer cancel()

	opts := []*options.ClientOptions{
		options.Client().ApplyURI(s.connURL.String()),
	}

	if s.session, err = mongo.Connect(ctx, opts...); err != nil {
		return fmt.Errorf("mongo.Connect: %w", err)
	}

	s.collections = map[string]*Collection{}
	s.database = s.session.Database(s.connURL.(ConnectionURL).Database)

	// ping
	if err = s.Ping(); err != nil {
		return fmt.Errorf("Ping: %w", err)
	}

	return nil
}

// Close terminates the current database session.
func (s *Source) Close() error {
	if s.session != nil {
		return s.session.Disconnect(context.Background())
	}

	return nil
}

// Collections returns a list of non-system tables from the database.
func (s *Source) Collections() (cols []db.Collection, err error) {
	ctx := context.Background()

	var mgocols []string
	var col string

	if mgocols, err = s.database.ListCollectionNames(ctx, bson.D{}); err != nil {
		return nil, fmt.Errorf("ListCollectionNames: %w", err)
	}

	cols = make([]db.Collection, 0, len(mgocols))

	for _, col = range mgocols {
		if !strings.HasPrefix(col, "system.") {
			cols = append(cols, s.Collection(col))
		}
	}

	return cols, nil
}

func (s *Source) Delete(db.Record) error {
	return db.ErrNotImplemented
}

func (s *Source) Get(db.Record, interface{}) error {
	return db.ErrNotImplemented
}

func (s *Source) Save(db.Record) error {
	return db.ErrNotImplemented
}

func (s *Source) Context() context.Context {
	return s.ctx
}

func (s *Source) WithContext(ctx context.Context) db.Session {
	return &Source{
		ctx:      ctx,
		Settings: s.Settings,
		name:     s.name,
		connURL:  s.connURL,
		session:  s.session,
		database: s.database,
		version:  s.version,
	}
}

// Collection returns a collection by name.
func (s *Source) Collection(name string) db.Collection {
	s.collectionsMu.Lock()
	defer s.collectionsMu.Unlock()

	var col *Collection
	var ok bool

	if col, ok = s.collections[name]; !ok {
		col = &Collection{
			parent:     s,
			collection: s.database.Collection(name),
		}
		s.collections[name] = col
	}

	return col
}
