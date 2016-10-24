package sqladapter

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"upper.io/db.v2"
	"upper.io/db.v2/internal/cache"
	"upper.io/db.v2/internal/sqladapter/exql"
	"upper.io/db.v2/lib/sqlbuilder"
)

var (
	// Needs to be multiplied by maxConnectionRetryTime to get the total time
	// upper-db will use to retry before giving up and returning an error.
	maxQueryRetryAttempts = 2

	minConnectionRetryInterval = time.Millisecond * 10
	maxConnectionRetryInterval = time.Millisecond * 500

	// The amount of time each retry can take.
	maxConnectionRetryTime = time.Second * 5
)

var (
	errNothingToRecoverFrom = errors.New("Nothing to recover from")
)

var (
	lastSessID uint64
	lastTxID   uint64
)

// HasCleanUp is implemented by structs that have a clean up routine that needs
// to be called before Close().
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

	recoverFromErrMu sync.Mutex

	collectionMu sync.Mutex
	databaseMu   sync.Mutex

	name   string
	sess   *sql.DB
	sessMu sync.Mutex

	sessID uint64
	txID   uint64

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
	defer d.sessMu.Unlock()

	d.baseTx = newTx(t)
	if err := d.Ping(); err != nil {
		return err
	}

	d.txID = newTxID()
	return nil
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

	d.sessID = newSessionID()
	name, err := d.PartialDatabase.FindDatabaseName()
	if err != nil {
		return err
	}

	d.name = name

	return nil
}

// recoverFromErr attempts to reestablish a connection after a recoverable
// error (like "bad driver").  Returns nil if the connection was reestablished,
// afther this the query can be retried.
func (d *database) recoverFromErr(err error) error {
	if err == nil {
		return errNothingToRecoverFrom
	}

	if d.Transaction() != nil {
		// Don't even attempt to recover from within a transaction.
		return err
	}

	d.recoverFromErrMu.Lock()
	defer d.recoverFromErrMu.Unlock()

	waitTime := minConnectionRetryInterval

	// Attempt to stablish a new connection using the current session.
	lastErr := d.PartialDatabase.Err(err)
	for ts := time.Now(); time.Now().Sub(ts) < maxConnectionRetryTime; {
		switch lastErr {
		// According to database/sql, this is returned by an sql driver when the
		// connection is in bad state and it should not be returned if there's a
		// possibility that the database server might have performed the operation.
		case driver.ErrBadConn:
			time.Sleep(waitTime)    // Wait a bit before retrying.
			waitTime = waitTime * 2 // Double the previous waiting time.
			if waitTime > maxConnectionRetryInterval {
				// Won't wait more than maxConnectionRetryInterval
				waitTime = maxConnectionRetryInterval
			}
		default:
			// We don't know how to deal with this error, nor if we can recover
			// from it, return the original error.
			return err
		}
		lastErr = d.PartialDatabase.Err(d.Ping())
		if lastErr == nil {
			return nil // Connection was reestablished.
		}
	}

	// Return original error
	return err
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
func (d *database) StatementExec(stmt *exql.Statement, args ...interface{}) (res sql.Result, err error) {
	var query string

	if db.Conf.LoggingEnabled() {
		defer func(start time.Time) {

			status := db.QueryStatus{
				TxID:   d.txID,
				SessID: d.sessID,
				Query:  query,
				Args:   args,
				Err:    err,
				Start:  start,
				End:    time.Now(),
			}

			if res != nil {
				if rowsAffected, err := res.RowsAffected(); err == nil {
					status.RowsAffected = &rowsAffected
				}

				if lastInsertID, err := res.LastInsertId(); err == nil {
					status.LastInsertID = &lastInsertID
				}
			}

			db.Log(&status)
		}(time.Now())
	}

	var p *Stmt
	if p, query, err = d.prepareStatement(stmt); err != nil {
		return nil, err
	}
	defer p.Close()

	for i := 0; i < maxQueryRetryAttempts; i++ {
		if execer, ok := d.PartialDatabase.(HasStatementExec); ok {
			res, err = execer.StatementExec(p.Stmt, args...)
		} else {
			res, err = p.Exec(args...)
		}
		if err == nil {
			return res, nil // successful query
		}
		if d.recoverFromErr(err) == nil {
			continue // Connection was reestablished, retry.
		}
		// We got another error from recoverFromErr that means it could not
		// recover.
		return res, err
	}

	// All retry attempts failed.
	return res, err
}

// StatementQuery compiles and executes a statement that returns rows.
func (d *database) StatementQuery(stmt *exql.Statement, args ...interface{}) (rows *sql.Rows, err error) {
	var query string

	if db.Conf.LoggingEnabled() {
		defer func(start time.Time) {
			db.Log(&db.QueryStatus{
				TxID:   d.txID,
				SessID: d.sessID,
				Query:  query,
				Args:   args,
				Err:    err,
				Start:  start,
				End:    time.Now(),
			})
		}(time.Now())
	}

	var p *Stmt
	if p, query, err = d.prepareStatement(stmt); err != nil {
		return nil, err
	}
	defer p.Close()

	for i := 0; i < maxQueryRetryAttempts; i++ {
		rows, err = p.Query(args...)
		if err == nil {
			return rows, err // this was a successful query
		}

		if d.recoverFromErr(err) == nil {
			continue // we can retry
		}
		return rows, err
	}

	// All retry attempts failed.
	return rows, err
}

// StatementQueryRow compiles and executes a statement that returns at most one
// row.
func (d *database) StatementQueryRow(stmt *exql.Statement, args ...interface{}) (row *sql.Row, err error) {
	var query string

	if db.Conf.LoggingEnabled() {
		defer func(start time.Time) {
			db.Log(&db.QueryStatus{
				TxID:   d.txID,
				SessID: d.sessID,
				Query:  query,
				Args:   args,
				Err:    err,
				Start:  start,
				End:    time.Now(),
			})
		}(time.Now())
	}

	var p *Stmt
	if p, query, err = d.prepareStatement(stmt); err != nil {
		return nil, err
	}
	defer p.Close()

	row, err = p.QueryRow(args...), nil // We're setting row and err like this because they're going to be logged.
	return row, err
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
func (d *database) prepareStatement(stmt *exql.Statement) (*Stmt, string, error) {
	if d.sess == nil && d.Transaction() == nil {
		return nil, "", db.ErrNotConnected
	}

	pc, ok := d.cachedStatements.ReadRaw(stmt)
	if ok {
		// The statement was cached.
		ps, err := pc.(*Stmt).Open()
		if err == nil {
			return ps, ps.query, nil
		}
	}

	// Plain SQL query.
	query := d.PartialDatabase.CompileStatement(stmt)

	sqlStmt, err := func() (*sql.Stmt, error) {
		if d.Transaction() != nil {
			return d.Transaction().(*sqlTx).Prepare(query)
		}
		return d.sess.Prepare(query)
	}()
	if err != nil {
		return nil, query, err
	}

	p := NewStatement(sqlStmt, query)
	d.cachedStatements.Write(stmt, p)
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
	waitTime := minConnectionRetryInterval

	// Waitig 5 seconds for a successful connection.
	for timeStart := time.Now(); time.Now().Sub(timeStart) < maxConnectionRetryTime; {
		err := connectFn()
		if err == nil {
			return nil // Connected!
		}

		// Only attempt to reconnect if the error is too many clients.
		switch d.PartialDatabase.Err(err) {
		case db.ErrTooManyClients, db.ErrServerRefusedConnection, driver.ErrBadConn:
			// Sleep and try again if, and only if, the server replied with a
			// temporary error.
			time.Sleep(waitTime)
			if waitTime < maxConnectionRetryInterval {
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

func newSessionID() uint64 {
	if atomic.LoadUint64(&lastSessID) == math.MaxUint64 {
		atomic.StoreUint64(&lastSessID, 0)
		return 0
	}
	return atomic.AddUint64(&lastSessID, 1)
}

func newTxID() uint64 {
	if atomic.LoadUint64(&lastTxID) == math.MaxUint64 {
		atomic.StoreUint64(&lastTxID, 0)
		return 0
	}
	return atomic.AddUint64(&lastTxID, 1)
}
