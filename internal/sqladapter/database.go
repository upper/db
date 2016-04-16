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

type Database interface {
	db.Database
	Builder() builder.Builder
}

type PartialDatabase interface {
	TableExists(name string) error

	FindDatabaseName() (string, error)
	FindTablePrimaryKeys(name string) ([]string, error)

	NewCollection(name string) db.Collection
	CompileStatement(stmt *exql.Statement) (query string)

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
	Name() string
	Close() error
	Ping() error
	Collection(string) db.Collection
	Driver() interface{}

	BindSession(*sql.DB) error
	Session() *sql.DB

	BindTx(*sql.Tx) error
	Tx() BaseTx
}

type baseDatabase struct {
	PartialDatabase
	BaseTx

	mu sync.Mutex

	name string
	sess *sql.DB

	cachedStatements  *cache.Cache
	cachedCollections *cache.Cache

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
		PartialDatabase:   p,
		cachedCollections: cache.NewCache(),
		cachedStatements:  cache.NewCache(),
	}

	return d
}

func (d *baseDatabase) Session() *sql.DB {
	return d.sess
}

func (d *baseDatabase) BindTx(t *sql.Tx) error {
	d.BaseTx = newTx(t)
	return d.Ping()
}

func (d *baseDatabase) Tx() BaseTx {
	return d.BaseTx
}

func (d *baseDatabase) Name() string {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.name == "" {
		d.name, _ = d.PartialDatabase.FindDatabaseName()
	}

	return d.name
}

func (d *baseDatabase) BindSession(sess *sql.DB) error {
	d.sess = sess

	if err := d.Ping(); err != nil {
		return err
	}

	name, err := d.PartialDatabase.FindDatabaseName()
	if err != nil {
		return err
	}
	d.name = name

	return nil
}

// Ping checks whether a connection to the database is alive by pinging it.
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
	d.mu.Lock()
	defer d.mu.Unlock()

	h := cache.String(name)

	ccol, ok := d.cachedCollections.ReadRaw(h)
	if ok {
		return ccol.(db.Collection)
	}

	col := d.PartialDatabase.NewCollection(name)
	d.cachedCollections.Write(h, col)

	return col
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

// prepareStatement converts a *exql.Statement representation into an actual
// *sql.Stmt.  This method will attempt to used a cached prepared statement, if
// available.
func (d *baseDatabase) prepareStatement(stmt *exql.Statement) (*sql.Stmt, string, error) {
	if d.sess == nil {
		return nil, "", db.ErrNotConnected
	}

	pc, ok := d.cachedStatements.ReadRaw(stmt)

	if ok {
		// The statement was cached.
		ps := pc.(*cachedStatement)
		return ps.Stmt, ps.query, nil
	}

	// Plain SQL query.
	query := d.PartialDatabase.CompileStatement(stmt)

	var p *sql.Stmt
	var err error

	if d.Tx() != nil {
		p, err = d.Tx().(*baseTx).Prepare(query)
	} else {
		p, err = d.sess.Prepare(query)
	}

	if err != nil {
		return nil, query, err
	}

	d.cachedStatements.Write(stmt, &cachedStatement{p, query})

	return p, query, nil
}

var waitForConnMu sync.Mutex

// WaitForConnection tries to execute the given connectFn function, if
// connectFn returns an error, then WaitForConnection will keep trying until
// connectFn returns nil. Maximum waiting time is 5s after having acquired the
// lock.
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

// The methods below here complete the db.Database interface.

func (d *baseDatabase) TableExists(name string) error {
	return db.ErrNotImplemented
}

func (d *baseDatabase) FindDatabaseName() (string, error) {
	return "", db.ErrNotImplemented
}

func (d *baseDatabase) FindTablePrimaryKeys(string) ([]string, error) {
	return nil, db.ErrNotImplemented
}

func (d *baseDatabase) NewCollection(name string) db.Collection {
	return nil
}

func (d *baseDatabase) Err(in error) error {
	return in
}

func (c *baseDatabase) Open(db.ConnectionURL) error {
	return db.ErrNotImplemented
}

func (c *baseDatabase) Clone() (db.Database, error) {
	return nil, db.ErrNotImplemented
}

func (c *baseDatabase) Collections() ([]string, error) {
	return nil, db.ErrNotImplemented
}

func (c *baseDatabase) Transaction() (db.Tx, error) {
	return nil, db.ErrNotImplemented
}

var (
	_ = db.Database(&baseDatabase{})
	_ = Database(&baseDatabase{})
)
