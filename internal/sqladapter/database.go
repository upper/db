package sqladapter

import (
	"database/sql"
	"strconv"
	"sync"
	"time"

	"upper.io/db.v2"
	"upper.io/db.v2/builder"
	"upper.io/db.v2/builder/cache"
	"upper.io/db.v2/builder/exql"
	"upper.io/db.v2/internal/logger"
)

// HasExecStatement allows the adapter to have its own exec statement.
type HasExecStatement interface {
	ExecStatement(stmt *sql.Stmt, args ...interface{}) (sql.Result, error)
}

// Database represents a SQL database.
type Database interface {
	PartialDatabase
	BaseDatabase
}

// PartialDatabase defines all the methods an adapter must provide.
type PartialDatabase interface {
	builder.Builder

	Collections() ([]string, error)
	Open(db.ConnectionURL) error

	TableExists(name string) error

	FindDatabaseName() (string, error)
	FindTablePrimaryKeys(name string) ([]string, error)

	NewLocalCollection(name string) db.Collection
	CompileStatement(stmt *exql.Statement) (query string)
	ConnectionURL() db.ConnectionURL

	Err(in error) (out error)
	NewLocalTransaction() (Tx, error)
}

// BaseDatabase defines the methods provided by sqladapter that do not have to
// be implemented by adapters.
type BaseDatabase interface {
	Name() string
	Close() error
	Ping() error
	Collection(string) db.Collection
	Driver() interface{}

	WaitForConnection(func() error) error

	BindSession(*sql.DB) error
	Session() *sql.DB

	BindTx(*sql.Tx) error
	Tx() BaseTx
}

// NewBaseDatabase provides a BaseDatabase given a PartialDatabase
func NewBaseDatabase(p PartialDatabase) BaseDatabase {
	d := &baseDatabase{
		PartialDatabase:   p,
		cachedCollections: cache.NewCache(),
		cachedStatements:  cache.NewCache(),
	}
	return d
}

// baseDatabase is the actual implementation of Database and joins methods from
// BaseDatabase and PartialDatabase
type baseDatabase struct {
	PartialDatabase
	baseTx BaseTx

	mu sync.Mutex

	name string
	sess *sql.DB

	cachedStatements  *cache.Cache
	cachedCollections *cache.Cache

	template *exql.Template
}

// Session returns the underlying *sql.DB
func (d *baseDatabase) Session() *sql.DB {
	return d.sess
}

// BindTx binds a *sql.Tx into *baseDatabase
func (d *baseDatabase) BindTx(t *sql.Tx) error {
	d.baseTx = newTx(t)
	return d.Ping()
}

// Tx returns a BaseTx, which, if not nil, means that this session is within a
// transaction
func (d *baseDatabase) Tx() BaseTx {
	return d.baseTx
}

// Name returns the database named
func (d *baseDatabase) Name() string {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.name == "" {
		d.name, _ = d.PartialDatabase.FindDatabaseName()
	}

	return d.name
}

// BindSession binds a *sql.DB into *baseDatabase
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

// Ping checks whether a connection to the database is still alive by pinging
// it
func (d *baseDatabase) Ping() error {
	return d.sess.Ping()
}

// Close terminates the current database session
func (d *baseDatabase) Close() error {
	defer func() {
		d.sess = nil
		d.baseTx = nil
	}()
	if d.sess != nil {
		if d.Tx() != nil && !d.Tx().Commited() {
			d.Tx().Rollback()
		}
		d.cachedStatements.Clear() // Closes prepared statements as well.
		return d.sess.Close()
	}
	return nil
}

// Collection returns a db.Collection given a name. Results are cached.
func (d *baseDatabase) Collection(name string) db.Collection {
	d.mu.Lock()
	defer d.mu.Unlock()

	h := cache.String(name)

	ccol, ok := d.cachedCollections.ReadRaw(h)
	if ok {
		return ccol.(db.Collection)
	}

	col := d.PartialDatabase.NewLocalCollection(name)
	d.cachedCollections.Write(h, col)

	return col
}

// ExecStatement compiles and executes a statement that does not return any
// rows.
func (d *baseDatabase) ExecStatement(stmt *exql.Statement, args ...interface{}) (sql.Result, error) {
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
		return execer.ExecStatement(p, args...)
	}

	return p.Exec(args...)
}

// QueryStatement compiles and executes a statement that returns rows.
func (d *baseDatabase) QueryStatement(stmt *exql.Statement, args ...interface{}) (*sql.Rows, error) {
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

// QueryRowStatement compiles and executes a statement that returns at most one
// row.
func (d *baseDatabase) QueryRowStatement(stmt *exql.Statement, args ...interface{}) (*sql.Row, error) {
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
		// A transaction
		return tx.(*sqlTx).Tx
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
		p, err = d.Tx().(*sqlTx).Prepare(query)
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
		err := connectFn()
		if err == nil {
			return nil // Connected!
		}

		// Only attempt to reconnect if the error is too many clients.
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

	return db.ErrGivingUpTryingToConnect
}

// The methods below here complete the db.Database interface.

/*
func (d *baseDatabase) TableExists(name string) error {
	return db.ErrNotImplemented
}

func (d *baseDatabase) FindDatabaseName() (string, error) {
	return "", db.ErrNotImplemented
}

func (d *baseDatabase) FindTablePrimaryKeys(string) ([]string, error) {
	return nil, db.ErrNotImplemented
}

func (d *baseDatabase) NewLocalCollection(name string) db.Collection {
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
*/

var (
	_ = db.Database(&baseDatabase{})
)

// ReplaceWithDollarSign turns a SQL statament with '?' placeholders into
// dollar placeholders, like $1, $2, ..., $n
func ReplaceWithDollarSign(in string) string {
	buf := []byte(in)
	out := make([]byte, 0, len(buf))

	i, j, k, t := 0, 1, 0, len(buf)

	for i < t {
		if buf[i] == '?' {
			out = append(out, buf[k:i]...)
			out = append(out, []byte("$"+strconv.Itoa(j))...)
			k = i + 1
			j++
		}
		i++
	}
	out = append(out, buf[k:i]...)

	return string(out)
}
