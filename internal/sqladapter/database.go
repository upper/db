package sqladapter

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"log"
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
	maxQueryRetryAttempts = 6 // Each retry adds a max wait time of maxConnectionRetryTime

	minConnectionRetryInterval = time.Millisecond * 100
	maxConnectionRetryInterval = time.Millisecond * 1000

	maxConnectionRetryTime = time.Second * 20
)

var (
	errNothingToRecoverFrom  = errors.New("Nothing to recover from")
	errUnableToRecover       = errors.New("Unable to recover from this error")
	errAllAttemptsHaveFailed = errors.New("All attempts to recover have failed")
)

var (
	lastSessID      uint64
	lastTxID        uint64
	lastOperationID uint64
)

// HasCleanUp is implemented by structs that have a clean up routine that
// needs to be called before Close().
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

	reconnectMu sync.Mutex

	collectionMu sync.Mutex
	databaseMu   sync.Mutex

	connFn func() error

	name string

	sess         *sql.DB
	sessErr      error
	sessMu       sync.Mutex
	reconnecting int32

	sessID uint64
	txID   uint64

	cachedStatements  *cache.Cache
	cachedCollections *cache.Cache

	template *exql.Template
}

var (
	_ = db.Database(&database{})
)

func (d *database) reconnect() error {
	log.Printf("reconnect: wait...")

	d.reconnectMu.Lock()
	defer d.reconnectMu.Unlock()

	lastErr := d.PartialDatabase.Err(d.Ping())
	if lastErr == nil {
		log.Printf("reconnect: Conn is still there!")
		return nil
	}
	log.Printf("reconnect: Ping: %v", lastErr)

	waitTime := minConnectionRetryInterval

	for start, i := time.Now(), 0; time.Now().Sub(start) < maxConnectionRetryTime; i++ {
		switch lastErr {
		case io.EOF, db.ErrTooManyClients, db.ErrServerRefusedConnection, driver.ErrBadConn, db.ErrGivingUpTryingToConnect:
			log.Printf("reconnect[%d]: Sleeping... %v", i, waitTime)
			time.Sleep(waitTime)

			waitTime = waitTime * 2
			if waitTime > maxConnectionRetryInterval {
				waitTime = maxConnectionRetryInterval
			}
		default:
			// We don't know how to deal with this error.
			log.Printf("reconnect[%d]: We don't know how to handle: %v (%T), %v (%T)", i, lastErr, lastErr, driver.ErrBadConn, driver.ErrBadConn)
			return lastErr
		}
		log.Printf("reconnect[%d]: Attempt to reconnect...", i)

		// Attempt to reconnect.
		atomic.StoreInt32(&d.reconnecting, 1)
		err := d.connFn()
		log.Printf("reconnect[%d]: connFn: %v", i, err)
		if err == nil {
			atomic.StoreInt32(&d.reconnecting, 0)
			log.Printf("reconnect[%d]: Reconnected!", i)
			return nil
		}
		atomic.StoreInt32(&d.reconnecting, 0)

		lastErr = d.PartialDatabase.Err(err)
	}

	log.Printf("reconnect: Failed to reconnect")
	return errAllAttemptsHaveFailed
}

// Session returns the underlying *sql.DB
func (d *database) Session() *sql.DB {
	d.reconnectMu.Lock()
	defer d.reconnectMu.Unlock()

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
	log.Printf("bindSession")

	d.sessMu.Lock()
	if d.sess != nil {
		d.sess.Close()
	}
	d.sess = sess
	d.sessMu.Unlock()

	if err := d.Ping(); err != nil {
		return err
	}

	if atomic.LoadInt32(&d.reconnecting) == 1 {
		return nil
	}

	d.sessID = newSessionID()
	name, err := d.PartialDatabase.FindDatabaseName()
	if err != nil {
		return err
	}
	d.name = name

	return nil
}

// recoverFromErr attempts to reestablish a connection after a temporary error,
// returns nil if the connection was reestablished and the query can be retried.
func (d *database) recoverFromErr(err error) error {
	if err == nil {
		return errNothingToRecoverFrom
	}

	if d.Transaction() != nil {
		// Don't even attempt to recover from within a transaction.
		return err
	}

	switch err {
	case io.EOF, db.ErrTooManyClients, db.ErrServerRefusedConnection, driver.ErrBadConn, db.ErrGivingUpTryingToConnect:
		err = d.reconnect()
		log.Printf("recoverFromErr: %v", err)
		return err
	}

	return errUnableToRecover
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

	queryID := newOperationID()

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

	log.Printf("Exec[%d] start", queryID)
	for i := 0; i < maxQueryRetryAttempts; i++ {
		var p *Stmt
		if p, query, err = d.prepareStatement(stmt); err != nil {
			log.Printf("prepareStatement[%d] (%d): %v", queryID, i, err)
			goto fail
		}

		if execer, ok := d.PartialDatabase.(HasStatementExec); ok {
			res, err = execer.StatementExec(p.Stmt, args...)
		} else {
			res, err = p.Exec(args...)
		}
		log.Printf("Exec[%d] (%d): %v", queryID, i, err)
		if err == nil {
			return res, nil // successful query
		}

	fail:
		if d.recoverFromErr(err) == nil {
			continue // retry
		}
		return nil, err
	}

	// All retry attempts failed, return res and err.
	return nil, err
}

// StatementQuery compiles and executes a statement that returns rows.
func (d *database) StatementQuery(stmt *exql.Statement, args ...interface{}) (rows *sql.Rows, err error) {
	var query string

	queryID := newOperationID()

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

	log.Printf("Query[%d] start", queryID)
	for i := 0; i < maxQueryRetryAttempts; i++ {
		var p *Stmt
		if p, query, err = d.prepareStatement(stmt); err != nil {
			log.Printf("prepareStatement[%d] (%d): %v", queryID, i, err)
			goto fail
		}

		rows, err = p.Query(args...)
		log.Printf("Query[%d] (%d): %v", queryID, i, err)
		if err == nil {
			return rows, nil // successful query
		}

	fail:
		if d.recoverFromErr(err) == nil {
			continue // retry
		}
		return nil, err
	}

	// All retry attempts failed, return rows and err.
	return nil, err
}

// StatementQueryRow compiles and executes a statement that returns at most one
// row.
func (d *database) StatementQueryRow(stmt *exql.Statement, args ...interface{}) (row *sql.Row, err error) {
	var query string

	queryID := newOperationID()

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

	log.Printf("QueryRow[%d] start", queryID)
	for i := 0; i < maxQueryRetryAttempts; i++ {
		var p *Stmt
		if p, query, err = d.prepareStatement(stmt); err != nil {
			log.Printf("prepareStatement [%d] (%d): %v", queryID, i, err)
			goto fail
		}

		row, err = p.QueryRow(args...), nil
		return row, err
	fail:
		if d.recoverFromErr(err) == nil {
			continue // retry
		}
		return nil, err
	}

	return nil, err
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
	if d.Session() == nil && d.Transaction() == nil {
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

// WaitForConnection tries to execute the given connFn function, if connFn
// returns an error, then WaitForConnection will keep trying until connFn
// returns nil. Maximum waiting time is 5s after having acquired the lock.
func (d *database) WaitForConnection(connFn func() error) error {
	// This lock ensures first-come, first-served and prevents opening too many
	// file descriptors.
	waitForConnMu.Lock()
	defer waitForConnMu.Unlock()

	// Minimum waiting time.
	waitTime := minConnectionRetryInterval

	// Waitig 5 seconds for a successful connection.
	for timeStart := time.Now(); time.Now().Sub(timeStart) < maxConnectionRetryTime; {
		err := connFn()
		if err == nil {
			d.connFn = connFn
			return nil // Connected!
		}

		// Only attempt to reconnect if the error is too many clients.
		switch d.PartialDatabase.Err(err) {
		case db.ErrTooManyClients, db.ErrServerRefusedConnection, driver.ErrBadConn:
			// Sleep and try again if, and only if, the server replied with a
			// temporary error.
			time.Sleep(waitTime)
			waitTime = waitTime * 2
			if waitTime > maxConnectionRetryInterval {
				waitTime = maxConnectionRetryInterval
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

func newOperationID() uint64 {
	if atomic.LoadUint64(&lastOperationID) == math.MaxUint64 {
		atomic.StoreUint64(&lastOperationID, 0)
		return 0
	}
	return atomic.AddUint64(&lastOperationID, 1)
}
