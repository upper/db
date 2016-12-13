package sqladapter

import (
	"database/sql"
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
	StatementExec(query string, args ...interface{}) (sql.Result, error)
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
	CompileStatement(stmt *exql.Statement, args []interface{}) (string, []interface{})
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

	SetConnMaxLifetime(time.Duration)
	SetMaxIdleConns(int)
	SetMaxOpenConns(int)

	BindClone(PartialDatabase) (BaseDatabase, error)
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

	psMu sync.Mutex

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

// Ping checks whether a connection to the database is still alive by pinging
// it
func (d *database) Ping() error {
	if d.sess != nil {
		return d.sess.Ping()
	}
	return nil
}

// SetConnMaxLifetime sets the maximum amount of time a connection may be
// reused.
func (d *database) SetConnMaxLifetime(t time.Duration) {
	if sess := d.Session(); sess != nil {
		sess.SetConnMaxLifetime(t)
	}
}

// SetMaxIdleConns sets the maximum number of connections in the idle
// connection pool.
func (d *database) SetMaxIdleConns(n int) {
	if sess := d.Session(); sess != nil {
		sess.SetMaxIdleConns(n)
	}
}

// SetMaxOpenConns sets the maximum number of open connections to the
// database.
func (d *database) SetMaxOpenConns(n int) {
	if sess := d.Session(); sess != nil {
		sess.SetMaxOpenConns(n)
	}
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

// BindClone binds a clone that is linked to the current
// session. This is commonly done before creating a transaction
// session.
func (d *database) BindClone(p PartialDatabase) (BaseDatabase, error) {
	nd := NewBaseDatabase(p).(*database)
	nd.name = d.name
	nd.sess = d.sess
	if err := nd.Ping(); err != nil {
		return nil, err
	}
	nd.sessID = newSessionID()
	return nd, nil
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
			return nil
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

	if execer, ok := d.PartialDatabase.(HasStatementExec); ok {
		query, args = d.compileStatement(stmt, args)
		res, err = execer.StatementExec(query, args...)
		return
	}

	tx := d.Transaction()

	if db.Conf.PreparedStatementCacheEnabled() && tx == nil {
		var p *Stmt
		if p, query, args, err = d.prepareStatement(stmt, args); err != nil {
			return nil, err
		}
		defer p.Close()

		res, err = p.Exec(args...)
		return
	}

	query, args = d.compileStatement(stmt, args)
	if tx != nil {
		res, err = tx.(*sqlTx).Exec(query, args...)
		return
	}

	res, err = d.sess.Exec(query, args...)
	return
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

	tx := d.Transaction()

	if db.Conf.PreparedStatementCacheEnabled() && tx == nil {
		var p *Stmt
		if p, query, args, err = d.prepareStatement(stmt, args); err != nil {
			return nil, err
		}
		defer p.Close()

		rows, err = p.Query(args...)
		return
	}

	query, args = d.compileStatement(stmt, args)
	if tx != nil {
		rows, err = tx.(*sqlTx).Query(query, args...)
		return
	}

	rows, err = d.sess.Query(query, args...)
	return

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

	tx := d.Transaction()

	if db.Conf.PreparedStatementCacheEnabled() && tx == nil {
		var p *Stmt
		if p, query, args, err = d.prepareStatement(stmt, args); err != nil {
			return nil, err
		}
		defer p.Close()

		row = p.QueryRow(args...)
		return
	}

	query, args = d.compileStatement(stmt, args)
	if tx != nil {
		row = tx.(*sqlTx).QueryRow(query, args...)
		return
	}

	row = d.sess.QueryRow(query, args...)
	return
}

// Driver returns the underlying *sql.DB or *sql.Tx instance.
func (d *database) Driver() interface{} {
	if tx := d.Transaction(); tx != nil {
		// A transaction
		return tx.(*sqlTx).Tx
	}
	return d.sess
}

// compileStatement compiles the given statement into a string.
func (d *database) compileStatement(stmt *exql.Statement, args []interface{}) (string, []interface{}) {
	return d.PartialDatabase.CompileStatement(stmt, args)
}

// prepareStatement compiles a query and tries to use previously generated
// statement.
func (d *database) prepareStatement(stmt *exql.Statement, args []interface{}) (*Stmt, string, []interface{}, error) {
	d.sessMu.Lock()
	defer d.sessMu.Unlock()

	sess, tx := d.sess, d.Transaction()
	if sess == nil && tx == nil {
		return nil, "", nil, db.ErrNotConnected
	}

	pc, ok := d.cachedStatements.ReadRaw(stmt)
	if ok {
		// The statement was cached.
		ps, err := pc.(*Stmt).Open()
		if err == nil {
			_, args = d.compileStatement(stmt, args)
			return ps, ps.query, args, nil
		}
	}

	query, args := d.compileStatement(stmt, args)
	sqlStmt, err := func(query *string) (*sql.Stmt, error) {
		if tx != nil {
			return tx.(*sqlTx).Prepare(*query)
		}
		return sess.Prepare(*query)
	}(&query)
	if err != nil {
		return nil, "", nil, err
	}

	p, err := NewStatement(sqlStmt, query).Open()
	if err != nil {
		return nil, query, args, err
	}
	d.cachedStatements.Write(stmt, p)
	return p, p.query, args, nil
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
