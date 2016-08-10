package sqladapter

import (
	"database/sql"
	"strconv"
	"sync"
	"time"

	"upper.io/db.v2"
	"upper.io/db.v2/internal/cache"
	"upper.io/db.v2/internal/logger"
	"upper.io/db.v2/internal/sqladapter/exql"
	"upper.io/db.v2/lib/sqlbuilder"
)

// HasCleanUp
type HasCleanUp interface {
	CleanUp() error
}

// HasStatementExec allows the adapter to have its own exec statement.
type HasStatementExec interface {
	StatementExec(stmt *sql.Stmt, args ...interface{}) (sql.Result, error)
}

// Database represents a SQL database.
type Database interface {
	PartialDatabase
	BaseDatabase
}

// PartialDatabase defines all the methods an adapter must provide.
type PartialDatabase interface {
	sqlbuilder.Builder

	Collections() ([]string, error)
	Open(db.ConnectionURL) error

	TableExists(name string) error

	FindDatabaseName() (string, error)
	FindTablePrimaryKeys(name string) ([]string, error)

	NewLocalCollection(name string) db.Collection
	CompileStatement(stmt *exql.Statement) (query string)
	ConnectionURL() db.ConnectionURL

	Err(in error) (out error)
	NewLocalTransaction() (DatabaseTx, error)
}

// BaseDatabase defines the methods provided by sqladapter that do not have to
// be implemented by adapters.
type BaseDatabase interface {
	Name() string
	Close() error
	Ping() error
	ClearCache()
	Collection(string) db.Collection
	Driver() interface{}

	WaitForConnection(func() error) error

	BindSession(*sql.DB) error
	Session() *sql.DB

	BindTx(*sql.Tx) error
	Transaction() BaseTx
}

// NewBaseDatabase provides a BaseDatabase given a PartialDatabase
func NewBaseDatabase(p PartialDatabase) BaseDatabase {
	d := &database{
		PartialDatabase:   p,
		cachedCollections: cache.NewCache(),
		cachedStatements:  cache.NewCache(),
	}
	return d
}

// database is the actual implementation of Database and joins methods from
// BaseDatabase and PartialDatabase
type database struct {
	PartialDatabase
	baseTx BaseTx

	collectionMu sync.Mutex
	databaseMu   sync.Mutex

	name   string
	sess   *sql.DB
	sessMu sync.Mutex

	cachedStatements  *cache.Cache
	cachedCollections *cache.Cache

	template *exql.Template
}

var (
	_ = db.Database(&database{})
)

// Session returns the underlying *sql.DB
func (d *database) Session() *sql.DB {
	return d.sess
}

// BindTx binds a *sql.Tx into *database
func (d *database) BindTx(t *sql.Tx) error {
	d.sessMu.Lock()
	d.baseTx = newTx(t)
	defer d.sessMu.Unlock()
	return d.Ping()
}

// Tx returns a BaseTx, which, if not nil, means that this session is within a
// transaction
func (d *database) Transaction() BaseTx {
	return d.baseTx
}

// Name returns the database named
func (d *database) Name() string {
	d.databaseMu.Lock()
	defer d.databaseMu.Unlock()

	if d.name == "" {
		d.name, _ = d.PartialDatabase.FindDatabaseName()
	}

	return d.name
}

// BindSession binds a *sql.DB into *database
func (d *database) BindSession(sess *sql.DB) error {
	d.sessMu.Lock()
	d.sess = sess
	d.sessMu.Unlock()

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
func (d *database) Ping() error {
	if d.sess != nil {
		return d.sess.Ping()
	}
	return nil
}

// ClearCache removes all caches.
func (d *database) ClearCache() {
	d.collectionMu.Lock()
	defer d.collectionMu.Unlock()
	d.cachedCollections.Clear()
	d.cachedStatements.Clear()
	if d.template != nil {
		d.template.Cache.Clear()
	}
}

// Close terminates the current database session
func (d *database) Close() error {
	defer func() {
		d.sessMu.Lock()
		d.sess = nil
		d.baseTx = nil
		d.sessMu.Unlock()
	}()
	if d.sess != nil {
		if cleaner, ok := d.PartialDatabase.(HasCleanUp); ok {
			cleaner.CleanUp()
		}
		d.cachedCollections.Clear()
		d.cachedStatements.Clear() // Closes prepared statements as well.

		tx := d.Transaction()
		if tx == nil {
			// Not within a transaction.
			return d.sess.Close()
		}

		if !tx.Committed() {
			tx.Rollback()
		}
	}
	return nil
}

// Collection returns a db.Collection given a name. Results are cached.
func (d *database) Collection(name string) db.Collection {
	d.collectionMu.Lock()
	defer d.collectionMu.Unlock()

	h := cache.String(name)

	ccol, ok := d.cachedCollections.ReadRaw(h)
	if ok {
		return ccol.(db.Collection)
	}

	col := d.PartialDatabase.NewLocalCollection(name)
	d.cachedCollections.Write(h, col)

	return col
}

// StatementExec compiles and executes a statement that does not return any
// rows.
func (d *database) StatementExec(stmt *exql.Statement, args ...interface{}) (sql.Result, error) {
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

	if execer, ok := d.PartialDatabase.(HasStatementExec); ok {
		return execer.StatementExec(p, args...)
	}

	return p.Exec(args...)
}

// StatementQuery compiles and executes a statement that returns rows.
func (d *database) StatementQuery(stmt *exql.Statement, args ...interface{}) (*sql.Rows, error) {
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

// StatementQueryRow compiles and executes a statement that returns at most one
// row.
func (d *database) StatementQueryRow(stmt *exql.Statement, args ...interface{}) (*sql.Row, error) {
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
func (d *database) Driver() interface{} {
	if tx := d.Transaction(); tx != nil {
		// A transaction
		return tx.(*sqlTx).Tx
	}
	return d.sess
}

// prepareStatement converts a *exql.Statement representation into an actual
// *sql.Stmt.  This method will attempt to used a cached prepared statement, if
// available.
func (d *database) prepareStatement(stmt *exql.Statement) (*sql.Stmt, string, error) {
	if d.sess == nil && d.Transaction() == nil {
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

	if d.Transaction() != nil {
		p, err = d.Transaction().(*sqlTx).Prepare(query)
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
func (d *database) WaitForConnection(connectFn func() error) error {
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
