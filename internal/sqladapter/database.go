package sqladapter

import (
	"database/sql"
	"sync"
	"time"

	"upper.io/db.v2/builder"
	"upper.io/db.v2/builder/sqlbuilder"
	"upper.io/db.v2/builder/sqlgen"
	"upper.io/db.v2"
	"upper.io/db.v2/internal/adapter"
	"upper.io/db.v2/internal/debug"
	"upper.io/db.v2/internal/schema"
	"upper.io/db.v2/builder/cache"
	"upper.io/db.v2/internal/sqlutil/tx"
)

type HasExecStatement interface {
	Exec(stmt *sql.Stmt, args ...interface{}) (sql.Result, error)
}

type PartialDatabase interface {
	PopulateSchema() error
	TableExists(name string) error
	TablePrimaryKey(name string) ([]string, error)
	NewTable(name string) db.Collection
	CompileAndReplacePlaceholders(stmt *sqlgen.Statement) (query string)
	Err(in error) (out error)
}

type Database interface {
	db.Database
	TableExists(name string) error
	TablePrimaryKey(name string) ([]string, error)
}

type BaseDatabase struct {
	partial PartialDatabase
	sess    *sql.DB
	tx      *sqltx.Tx

	connURL          db.ConnectionURL
	schema           *schema.DatabaseSchema
	cachedStatements *cache.Cache
	collections      map[string]db.Collection
	collectionsMu    sync.Mutex
	builder          builder.Builder

	template *sqlgen.Template
}

type cachedStatement struct {
	*sql.Stmt
	query string
}

func NewDatabase(partial PartialDatabase, connURL db.ConnectionURL, template *sqlgen.Template) *BaseDatabase {
	d := &BaseDatabase{
		partial:  partial,
		connURL:  connURL,
		template: template,
	}

	d.builder, _ = sqlbuilder.New(d, d.t)
	d.cachedStatements = cache.NewCache()

	return d
}

func (d *BaseDatabase) t() *sqlgen.Template {
	return d.template
}

func (d *BaseDatabase) Session() *sql.DB {
	return d.sess
}

func (d *BaseDatabase) Template() *sqlgen.Template {
	return d.template
}

func (d *BaseDatabase) BindTx(tx *sql.Tx) {
	d.tx = sqltx.New(tx)
}

func (d *BaseDatabase) Tx() *sqltx.Tx {
	return d.tx
}

func (d *BaseDatabase) NewSchema() {
	d.schema = schema.NewDatabaseSchema()
}

func (d *BaseDatabase) Schema() *schema.DatabaseSchema {
	return d.schema
}

func (d *BaseDatabase) Bind(sess *sql.DB) error {
	d.sess = sess
	return d.populate()
}

func (d *BaseDatabase) populate() error {

	d.collections = make(map[string]db.Collection)

	if d.schema == nil {
		if err := d.partial.PopulateSchema(); err != nil {
			return err
		}
	}

	return nil
}

func (d *BaseDatabase) Clone(partial PartialDatabase) *BaseDatabase {
	clone := NewDatabase(partial, d.connURL, d.template)
	clone.schema = d.schema
	return clone
}

// Ping checks whether a connection to the database is still alive by pinging
// it, establishing a connection if necessary.
func (d *BaseDatabase) Ping() error {
	return d.sess.Ping()
}

// Close terminates the current database session.
func (d *BaseDatabase) Close() error {
	defer func() {
		d.sess = nil
		d.tx = nil
	}()
	if d.sess != nil {
		if d.tx != nil && !d.tx.Done() {
			d.tx.Rollback()
		}
		d.cachedStatements.Clear()
		return d.sess.Close()
	}
	return nil
}

// C returns a collection interface.
func (d *BaseDatabase) C(name string) db.Collection {
	if c, ok := d.collections[name]; ok {
		return c
	}

	c, err := d.Collection(name)
	if err != nil {
		return &adapter.NonExistentCollection{Err: err}
	}

	return c
}

// Collection returns the table that matches the given name.
func (d *BaseDatabase) Collection(name string) (db.Collection, error) {
	if d.tx != nil {
		if d.tx.Done() {
			return nil, sql.ErrTxDone
		}
	}

	if err := d.partial.TableExists(name); err != nil {
		return nil, err
	}

	col := d.partial.NewTable(name)

	d.collectionsMu.Lock()
	d.collections[name] = col
	d.collectionsMu.Unlock()

	return col, nil
}

func (d *BaseDatabase) ConnectionURL() db.ConnectionURL {
	return d.connURL
}

// Name returns the name of the database.
func (d *BaseDatabase) Name() string {
	return d.schema.Name
}

// Exec compiles and executes a statement that does not return any rows.
func (d *BaseDatabase) Exec(stmt *sqlgen.Statement, args ...interface{}) (sql.Result, error) {
	var query string
	var p *sql.Stmt
	var err error

	if db.Debug {
		var start, end int64
		start = time.Now().UnixNano()

		defer func() {
			end = time.Now().UnixNano()
			debug.Log(query, args, err, start, end)
		}()
	}

	if p, query, err = d.prepareStatement(stmt); err != nil {
		return nil, err
	}

	if execer, ok := d.partial.(HasExecStatement); ok {
		return execer.Exec(p, args...)
	}

	return p.Exec(args...)
}

// Query compiles and executes a statement that returns rows.
func (d *BaseDatabase) Query(stmt *sqlgen.Statement, args ...interface{}) (*sql.Rows, error) {
	var query string
	var p *sql.Stmt
	var err error

	if db.Debug {
		var start, end int64
		start = time.Now().UnixNano()

		defer func() {
			end = time.Now().UnixNano()
			debug.Log(query, args, err, start, end)
		}()
	}

	if p, query, err = d.prepareStatement(stmt); err != nil {
		return nil, err
	}

	return p.Query(args...)
}

// QueryRow compiles and executes a statement that returns at most one row.
func (d *BaseDatabase) QueryRow(stmt *sqlgen.Statement, args ...interface{}) (*sql.Row, error) {
	var query string
	var p *sql.Stmt
	var err error

	if db.Debug {
		var start, end int64
		start = time.Now().UnixNano()

		defer func() {
			end = time.Now().UnixNano()
			debug.Log(query, args, err, start, end)
		}()
	}

	if p, query, err = d.prepareStatement(stmt); err != nil {
		return nil, err
	}

	return p.QueryRow(args...), nil
}

// Builder returns a custom query builder.
func (d *BaseDatabase) Builder() builder.Builder {
	return d.builder
}

// Driver returns the underlying *sql.DB or *sql.Tx instance.
func (d *BaseDatabase) Driver() interface{} {
	if d.tx != nil {
		return d.tx.Tx
	}
	return d.sess
}

func (d *BaseDatabase) prepareStatement(stmt *sqlgen.Statement) (p *sql.Stmt, query string, err error) {
	if d.sess == nil {
		return nil, "", db.ErrNotConnected
	}

	pc, ok := d.cachedStatements.ReadRaw(stmt)

	if ok {
		ps := pc.(*cachedStatement)
		p = ps.Stmt
		query = ps.query
	} else {
		query = d.partial.CompileAndReplacePlaceholders(stmt)

		if d.tx != nil {
			p, err = d.tx.Prepare(query)
		} else {
			p, err = d.sess.Prepare(query)
		}

		if err != nil {
			return nil, query, err
		}

		d.cachedStatements.Write(stmt, &cachedStatement{p, query})
	}

	return p, query, nil
}

var waitForConnMu sync.Mutex

// waitForConnection tries to execute the connectFn function, if connectFn
// returns an error, then waitForConnection will keep trying until connectFn
// returns nil. Maximum waiting time is 5s after having acquired the lock.
func (d *BaseDatabase) WaitForConnection(connectFn func() error) error {
	// This lock ensures first-come, first-served and prevents opening too many
	// file descriptors.
	waitForConnMu.Lock()
	defer waitForConnMu.Unlock()

	// Minimum waiting time.
	waitTime := time.Millisecond * 10

	// Waitig 5 seconds for a successful connection.
	for timeStart := time.Now(); time.Now().Sub(timeStart) < time.Second*5; {
		if err := connectFn(); err != nil {
			if d.partial.Err(err) == db.ErrTooManyClients {
				// Sleep and try again if, and only if, the server replied with a "too
				// many clients" error.
				time.Sleep(waitTime)
				if waitTime < time.Millisecond*500 {
					// Wait a bit more next time.
					waitTime = waitTime * 2
				}
				continue
			}
			// Return any other error immediately.
			return err
		}
		return nil
	}

	return db.ErrGivingUpTryingToConnect
}
