package sqladapter

import (
	"database/sql"
	"sync"
	"time"

	"upper.io/db.v2"
	"upper.io/db.v2/builder"
	"upper.io/db.v2/builder/cache"
	"upper.io/db.v2/builder/exql"
	"upper.io/db.v2/internal/logger"
)

type HasExecStatement interface {
	Exec(stmt *sql.Stmt, args ...interface{}) (sql.Result, error)
}

type PartialDatabase interface {
	PopulateSchema() error
	TableExists(name string) error
	TablePrimaryKey(name string) ([]string, error)
	NewTable(name string) db.Collection
	CompileAndReplacePlaceholders(stmt *exql.Statement) (query string)
	Err(in error) (out error)
	Builder() builder.Builder
	Transaction() (db.Tx, error)
	Clone() (db.Database, error)
	Collections() ([]string, error)
	Open(db.ConnectionURL) error
}

type BaseDatabase interface {
	PartialDatabase
	BaseTx

	WaitForConnection(func() error) error
	Close() error
	Ping() error
	Collection(string) db.Collection
	Name() string
	Driver() interface{}
	Session() *sql.DB
	Tx() BaseTx

	BindSession(*sql.DB) error
	BindTx(*sql.Tx) error

	NewSchema() *DatabaseSchema
	Schema() *DatabaseSchema
}

type baseDatabase struct {
	PartialDatabase
	BaseTx

	sess *sql.DB

	connURL          db.ConnectionURL
	schema           *DatabaseSchema
	cachedStatements *cache.Cache
	collections      map[string]db.Collection
	collectionsMu    sync.Mutex

	template *exql.Template
}

type cachedStatement struct {
	*sql.Stmt
	query string
}

func (c *cachedStatement) OnPurge() {
	c.Stmt.Close()
}

func NewBaseDatabase(p PartialDatabase) BaseDatabase {
	d := &baseDatabase{
		PartialDatabase: p,
	}

	d.cachedStatements = cache.NewCache()

	return d
}

func (d *baseDatabase) t() *exql.Template {
	return d.template
}

func (d *baseDatabase) Session() *sql.DB {
	return d.sess
}

func (d *baseDatabase) Template() *exql.Template {
	return d.template
}

func (d *baseDatabase) BindTx(t *sql.Tx) error {
	d.BaseTx = newTx(t)
	return nil
}

func (d *baseDatabase) Tx() BaseTx {
	return d.BaseTx
}

func (d *baseDatabase) NewSchema() *DatabaseSchema {
	d.schema = NewDatabaseSchema()
	return d.schema
}

func (d *baseDatabase) Schema() *DatabaseSchema {
	return d.schema
}

func (d *baseDatabase) BindSession(sess *sql.DB) error {
	d.sess = sess
	return d.populate()
}

func (d *baseDatabase) populate() error {
	d.collections = make(map[string]db.Collection)

	if d.schema == nil {
		if err := d.PartialDatabase.PopulateSchema(); err != nil {
			return err
		}
	}

	return nil
}

/*
func (d *baseDatabase) xClone(partial PartialDatabase) *baseDatabase {
	clone := NewBaseDatabase(partial, d.connURL, d.template)
	clone.schema = d.schema
	return clone
}
*/

// Ping checks whether a connection to the database is still alive by pinging
// it, establishing a connection if necessary.
func (d *baseDatabase) Ping() error {
	return d.sess.Ping()
}

// Close terminates the current database session.
func (d *baseDatabase) Close() error {
	defer func() {
		d.sess = nil
		d.BaseTx = nil
	}()
	if d.sess != nil {
		if d.Tx() != nil && !d.Tx().Done() {
			d.Tx().Rollback()
		}
		d.cachedStatements.Clear() // Closes prepared statements as well.
		return d.sess.Close()
	}
	return nil
}

// Collection returns a Collection given a name.
func (d *baseDatabase) Collection(name string) db.Collection {
	d.collectionsMu.Lock()
	if c, ok := d.collections[name]; ok {
		d.collectionsMu.Unlock()
		return c
	}

	col := d.PartialDatabase.NewTable(name)
	d.collections[name] = col
	d.collectionsMu.Unlock()

	return col
}

/*
func (d *baseDatabase) ConnectionURL() db.ConnectionURL {
	return d.connURL
}
*/

// Name returns the name of the database.
func (d *baseDatabase) Name() string {
	return d.Name()
}

// Exec compiles and executes a statement that does not return any rows.
func (d *baseDatabase) Exec(stmt *exql.Statement, args ...interface{}) (sql.Result, error) {
	var query string
	var p *sql.Stmt
	var err error

	if db.Debug {
		var start, end int64
		start = time.Now().UnixNano()

		defer func() {
			end = time.Now().UnixNano()
			logger.Log(query, args, err, start, end)
		}()
	}

	if p, query, err = d.prepareStatement(stmt); err != nil {
		return nil, err
	}

	if execer, ok := d.PartialDatabase.(HasExecStatement); ok {
		return execer.Exec(p, args...)
	}

	return p.Exec(args...)
}

// Query compiles and executes a statement that returns rows.
func (d *baseDatabase) Query(stmt *exql.Statement, args ...interface{}) (*sql.Rows, error) {
	var query string
	var p *sql.Stmt
	var err error

	if db.Debug {
		var start, end int64
		start = time.Now().UnixNano()

		defer func() {
			end = time.Now().UnixNano()
			logger.Log(query, args, err, start, end)
		}()
	}

	if p, query, err = d.prepareStatement(stmt); err != nil {
		return nil, err
	}

	return p.Query(args...)
}

// QueryRow compiles and executes a statement that returns at most one row.
func (d *baseDatabase) QueryRow(stmt *exql.Statement, args ...interface{}) (*sql.Row, error) {
	var query string
	var p *sql.Stmt
	var err error

	if db.Debug {
		var start, end int64
		start = time.Now().UnixNano()

		defer func() {
			end = time.Now().UnixNano()
			logger.Log(query, args, err, start, end)
		}()
	}

	if p, query, err = d.prepareStatement(stmt); err != nil {
		return nil, err
	}

	return p.QueryRow(args...), nil
}

// Driver returns the underlying *sql.DB or *sql.Tx instance.
func (d *baseDatabase) Driver() interface{} {
	if tx := d.Tx(); tx != nil {
		return tx.(*baseTx).Tx
	}
	return d.sess
}

func (d *baseDatabase) prepareStatement(stmt *exql.Statement) (p *sql.Stmt, query string, err error) {
	if d.sess == nil {
		return nil, "", db.ErrNotConnected
	}

	pc, ok := d.cachedStatements.ReadRaw(stmt)

	if ok {
		ps := pc.(*cachedStatement)
		p = ps.Stmt
		query = ps.query
	} else {
		query = d.PartialDatabase.CompileAndReplacePlaceholders(stmt)

		if d.Tx() != nil {
			p, err = d.Tx().(*baseTx).Prepare(query)
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
func (d *baseDatabase) WaitForConnection(connectFn func() error) error {
	// This lock ensures first-come, first-served and prevents opening too many
	// file descriptors.
	waitForConnMu.Lock()
	defer waitForConnMu.Unlock()

	// Minimum waiting time.
	waitTime := time.Millisecond * 10

	// Waitig 5 seconds for a successful connection.
	for timeStart := time.Now(); time.Now().Sub(timeStart) < time.Second*5; {
		if err := connectFn(); err != nil {
			if d.PartialDatabase.Err(err) == db.ErrTooManyClients {
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
