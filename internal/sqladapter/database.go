package sqladapter

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
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

// A list of errors that mean the server is not working and that we should
// try to connect and retry the query.
var recoverableErrors = []error{
	io.EOF,
	driver.ErrBadConn,
	db.ErrNotConnected,
	db.ErrTooManyClients,
	db.ErrServerRefusedConnection,
}

var (
	// If a query fails with a recoverable error the connection is going to be
	// re-estalished and the query can be retried, each retry adds a max wait
	// time of maxConnectionRetryTime
	maxQueryRetryAttempts = 3

	// Minimum interval when waiting before trying to reconnect.
	minConnectionRetryInterval = time.Millisecond * 100

	// Maximum interval when waiting before trying to reconnect.
	maxConnectionRetryInterval = time.Millisecond * 2500

	// Maximun time each connection retry attempt can take.
	maxConnectionRetryTime = time.Second * 5

	// Maximun reconnection attempts per session before giving up.
	maxReconnectionAttempts uint64 = 12
)

var (
	errNothingToRecoverFrom = errors.New("Nothing to recover from")
	errUnableToRecover      = errors.New("Unable to recover from this error")
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
		PartialDatabase: p,

		cachedCollections: cache.NewCache(),
		cachedStatements:  cache.NewCache(),
		connFn:            defaultConnFn,
	}
	return d
}

var defaultConnFn = func() error {
	return errors.New("No connection function was defined.")
}

// database is the actual implementation of Database and joins methods from
// BaseDatabase and PartialDatabase
type database struct {
	PartialDatabase
	baseTx BaseTx

	connectMu    sync.Mutex
	collectionMu sync.Mutex

	connFn func() error

	name string

	sess    *sql.DB
	sessErr error
	sessMu  sync.Mutex

	sessID uint64
	txID   uint64

	connectAttempts uint64

	cachedStatements  *cache.Cache
	cachedCollections *cache.Cache

	template *exql.Template
}

var (
	_ = db.Database(&database{})
)

func (d *database) reconnect() error {
	if d.Transaction() != nil {
		// Don't even attempt to recover from within a transaction, this is not
		// possible.
		return errors.New("Can't recover from within a bad transaction.")
	}

	err := d.PartialDatabase.Err(d.Ping())
	if err == nil {
		return nil
	}

	return d.connect(d.connFn)
}

func (d *database) connect(connFn func() error) error {
	if connFn == nil {
		return errors.New("Missing connect function")
	}

	d.connectMu.Lock()
	defer d.connectMu.Unlock()

	// Attempt to (re)connect
	if atomic.AddUint64(&d.connectAttempts, 1) >= maxReconnectionAttempts {
		return db.ErrTooManyReconnectionAttempts
	}

	waitTime := minConnectionRetryInterval

	for start, i := time.Now(), 1; time.Now().Sub(start) < maxConnectionRetryTime; i++ {
		waitTime = time.Duration(i) * minConnectionRetryInterval
		if waitTime > maxConnectionRetryInterval {
			waitTime = maxConnectionRetryInterval
		}
		// Wait a bit until retrying.
		if waitTime > time.Duration(0) {
			time.Sleep(waitTime)
		}

		err := connFn()
		if err == nil {
			atomic.StoreUint64(&d.connectAttempts, 0)
			d.connFn = connFn
			return nil
		}

		if !d.isRecoverableError(err) {
			return err
		}
	}

	return db.ErrGivingUpTryingToConnect
}

// Session returns the underlying *sql.DB
func (d *database) Session() *sql.DB {
	if atomic.LoadUint64(&d.sessID) == 0 {
		// This means the session is connecting for the first time, in this case we
		// don't block because the session hasn't been returned yet.
		return d.sess
	}

	// Prevents goroutines from using the session until the connection is
	// re-established.
	d.connectMu.Lock()
	defer d.connectMu.Unlock()

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
	if atomic.LoadUint64(&d.sessID) == 0 {
		// This means the session is connecting for the first time, in this case we
		// don't block because the session hasn't been returned yet.
		return d.baseTx
	}

	d.sessMu.Lock()
	defer d.sessMu.Unlock()

	return d.baseTx
}

// Name returns the database named
func (d *database) Name() string {
	return d.name
}

func (d *database) getDBName() error {
	name, err := d.PartialDatabase.FindDatabaseName()
	if err != nil {
		return err
	}
	d.name = name
	return nil
}

// BindSession binds a *sql.DB into *database
func (d *database) BindSession(sess *sql.DB) error {
	if err := sess.Ping(); err != nil {
		return err
	}

	d.sessMu.Lock()
	if d.sess != nil {
		d.ClearCache()
		d.sess.Close() // Close before rebind.
	}
	d.sess = sess
	d.sessMu.Unlock()

	// Does this session already have a session ID?
	if atomic.LoadUint64(&d.sessID) != 0 {
		return nil
	}

	// Is this connection really working?
	if err := d.getDBName(); err != nil {
		return err
	}

	// Assign an ID if everyting was OK.
	d.sessID = newSessionID()
	return nil
}

func (d *database) isRecoverableError(err error) bool {
	err = d.PartialDatabase.Err(err)
	for i := 0; i < len(recoverableErrors); i++ {
		if err == recoverableErrors[i] {
			return true
		}
	}
	return false
}

// recoverFromErr attempts to reestablish a connection after a temporary error,
// returns nil if the connection was reestablished and the query can be retried.
func (d *database) recoverFromErr(err error) error {
	if err == nil {
		return errNothingToRecoverFrom
	}

	if d.isRecoverableError(err) {
		err := d.reconnect()
		return err
	}

	// This is not an error we can recover from.
	return errUnableToRecover
}

// Ping checks whether a connection to the database is still alive by pinging
// it
func (d *database) Ping() error {
	if sess := d.Session(); sess != nil {
		if err := sess.Ping(); err != nil {
			return err
		}

		_, err := sess.Exec("SELECT 1")
		if err != nil {
			return err
		}
		return nil
	}
	if tx := d.Transaction(); tx != nil {
		// When upper wraps a transaction with no original session.
		return nil
	}
	return db.ErrNotConnected
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
	if sess := d.Session(); sess != nil {
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

		// Don't close the parent session if within a transaction.
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

	cachedCollection, ok := d.cachedCollections.ReadRaw(h)
	if ok {
		return cachedCollection.(db.Collection)
	}

	col := d.PartialDatabase.NewLocalCollection(name)
	d.cachedCollections.Write(h, col)

	return col
}

func (d *database) prepareAndExec(stmt *exql.Statement, args ...interface{}) (string, sql.Result, error) {
	p, query, err := d.prepareStatement(stmt)
	if err != nil {
		return query, nil, err
	}

	if execer, ok := d.PartialDatabase.(HasStatementExec); ok {
		res, err := execer.StatementExec(p.Stmt, args...)
		return query, res, err
	}

	res, err := p.Exec(args...)
	return query, res, err
}

// StatementExec compiles and executes a statement that does not return any
// rows.
func (d *database) StatementExec(stmt *exql.Statement, args ...interface{}) (res sql.Result, err error) {
	var query string

	queryID := newOperationID()

	if db.Conf.LoggingEnabled() {
		defer func(start time.Time) {
			status := db.QueryStatus{
				TxID:    d.txID,
				SessID:  d.sessID,
				QueryID: queryID,
				Query:   query,
				Args:    args,
				Err:     err,
				Start:   start,
				End:     time.Now(),
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

	for i := 0; ; i++ {
		query, res, err = d.prepareAndExec(stmt, args...)
		if err == nil || i >= maxQueryRetryAttempts {
			return res, err
		}

		// Try to recover
		if recoverErr := d.recoverFromErr(err); recoverErr != nil {
			return nil, err // Unable to recover.
		}
	}

	panic("reached")
}

func (d *database) prepareAndQuery(stmt *exql.Statement, args ...interface{}) (string, *sql.Rows, error) {
	p, query, err := d.prepareStatement(stmt)
	if err != nil {
		return query, nil, err
	}

	rows, err := p.Query(args...)
	return query, rows, err
}

// StatementQuery compiles and executes a statement that returns rows.
func (d *database) StatementQuery(stmt *exql.Statement, args ...interface{}) (rows *sql.Rows, err error) {
	var query string

	queryID := newOperationID()

	if db.Conf.LoggingEnabled() {
		defer func(start time.Time) {
			db.Log(&db.QueryStatus{
				TxID:    d.txID,
				SessID:  d.sessID,
				QueryID: queryID,
				Query:   query,
				Args:    args,
				Err:     err,
				Start:   start,
				End:     time.Now(),
			})
		}(time.Now())
	}

	for i := 0; ; i++ {
		query, rows, err = d.prepareAndQuery(stmt, args...)
		if err == nil || i >= maxQueryRetryAttempts {
			return rows, err
		}

		// Try to recover
		if recoverErr := d.recoverFromErr(err); recoverErr != nil {
			return nil, err // Unable to recover.
		}
	}

	panic("reached")
}

func (d *database) prepareAndQueryRow(stmt *exql.Statement, args ...interface{}) (string, *sql.Row, error) {
	p, query, err := d.prepareStatement(stmt)
	if err != nil {
		return query, nil, err
	}

	// Would be nice to find a way to check if this succeeded before using
	// Scan.
	rows, err := p.QueryRow(args...), nil
	return query, rows, nil
}

// StatementQueryRow compiles and executes a statement that returns at most one
// row.
func (d *database) StatementQueryRow(stmt *exql.Statement, args ...interface{}) (row *sql.Row, err error) {
	var query string

	queryID := newOperationID()

	if db.Conf.LoggingEnabled() {
		defer func(start time.Time) {
			db.Log(&db.QueryStatus{
				TxID:    d.txID,
				SessID:  d.sessID,
				QueryID: queryID,
				Query:   query,
				Args:    args,
				Err:     err,
				Start:   start,
				End:     time.Now(),
			})
		}(time.Now())
	}

	for i := 0; ; i++ {
		query, row, err = d.prepareAndQueryRow(stmt, args...)
		if err == nil || i >= maxQueryRetryAttempts {
			return row, err
		}

		// Try to recover
		if recoverErr := d.recoverFromErr(err); recoverErr != nil {
			return nil, err // Unable to recover.
		}
	}

	panic("reached")
}

// Driver returns the underlying *sql.DB or *sql.Tx instance.
func (d *database) Driver() interface{} {
	if tx := d.Transaction(); tx != nil {
		return tx.(*sqlTx).Tx
	}
	return d.Session()
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
		// This prepared statement was cached, no need to build or to prepare
		// again.
		ps, err := pc.(*Stmt).Open()
		if err == nil {
			return ps, ps.query, nil
		}
	}

	// Building the actual SQL query.
	query := d.PartialDatabase.CompileStatement(stmt)

	sqlStmt, err := func() (*sql.Stmt, error) {
		if tx := d.Transaction(); tx != nil {
			return tx.(*sqlTx).Prepare(query)
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
	if err := d.connect(connFn); err != nil {
		return err
	}
	// Success.
	return nil
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
		atomic.StoreUint64(&lastSessID, 1)
		return 1
	}
	return atomic.AddUint64(&lastSessID, 1)
}

func newTxID() uint64 {
	if atomic.LoadUint64(&lastTxID) == math.MaxUint64 {
		atomic.StoreUint64(&lastTxID, 1)
		return 1
	}
	return atomic.AddUint64(&lastTxID, 1)
}

func newOperationID() uint64 {
	if atomic.LoadUint64(&lastOperationID) == math.MaxUint64 {
		atomic.StoreUint64(&lastOperationID, 1)
		return 1
	}
	return atomic.AddUint64(&lastOperationID, 1)
}
