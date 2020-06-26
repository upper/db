package sqladapter

import (
	"context"
	"database/sql"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	db "github.com/upper/db"
	"github.com/upper/db/internal/cache"
	"github.com/upper/db/internal/sqladapter/compat"
	"github.com/upper/db/internal/sqladapter/exql"
	"github.com/upper/db/sqlbuilder"
)

var (
	lastSessID uint64
	lastTxID   uint64
)

// hasCleanUp is implemented by structs that have a clean up routine that needs
// to be called before Close().
type hasCleanUp interface {
	CleanUp() error
}

// statementExecer allows the adapter to have its own exec statement.
type statementExecer interface {
	StatementExec(sess Session, ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

type hasConvertValues interface {
	ConvertValues(values []interface{}) []interface{}
}

// AdapterSession defines methods to be implemented by SQL database adapters.
type AdapterSession interface {
	Template() *exql.Template

	NewCollection() AdapterCollection

	// Open opens a new connection
	Open(sess Session, dsn string) (*sql.DB, error)

	// Collections returns a list of non-system tables from the database.
	Collections(sess Session) ([]string, error)

	// TableExists returns an error if the given table does not exist.
	TableExists(sess Session, name string) error

	// LookupName returns the name of the database.
	LookupName(sess Session) (string, error)

	// PrimaryKeys returns all primary keys on the table.
	PrimaryKeys(sess Session, name string) ([]string, error)

	// CompileStatement transforms an internal statement into a format
	// database/sql can understand.
	CompileStatement(sess Session, stmt *exql.Statement, args []interface{}) (string, []interface{})

	// Err wraps specific database errors (given in string form) and transforms
	// them into error values.
	Err(sess Session, in error) (out error)
}

// Session provides logic for methods that can be shared across all SQL
// adapters.
type Session interface {
	sqlbuilder.SQLBuilder

	PrimaryKeys(tableName string) ([]string, error)

	Collections() ([]db.Collection, error)

	// Name returns the name of the database.
	Name() string

	// Close closes the database session
	Close() error

	// Ping checks if the database server is reachable.
	Ping() error

	// Reset clears all caches the session is using
	Reset()

	// Collection returns a new collection.
	Collection(string) db.Collection

	ConnectionURL() db.ConnectionURL

	Open() error

	TableExists(name string) error

	// Driver returns the underlying driver the session is using
	Driver() interface{}

	//
	Item(db.Model) db.Item

	// WaitForConnection attempts to run the given connection function a fixed
	// number of times before failing.
	WaitForConnection(func() error) error

	// BindDB sets the *sql.DB the session will use.
	BindDB(*sql.DB) error

	// Session returns the *sql.DB the session is using.
	DB() *sql.DB

	// BindTx binds a transaction to the current session.
	BindTx(context.Context, *sql.Tx) error

	// Returns the current transaction the session is using.
	Transaction() BaseTx

	// NewClone clones the database using the given AdapterSession as base.
	NewClone(AdapterSession, bool) (Session, error)

	// Context returns the default context the session is using.
	Context() context.Context

	// SetContext sets a default context for the session.
	SetContext(context.Context)

	// TxOptions returns the default TxOptions for new transactions in the
	// session.
	TxOptions() *sql.TxOptions

	// SetTxOptions sets default TxOptions for the session.
	SetTxOptions(txOptions sql.TxOptions)

	NewSessionTx(ctx context.Context) (SessionTx, error)

	NewTx(ctx context.Context) (sqlbuilder.Tx, error)

	Tx(fn func(sess sqlbuilder.Tx) error) error

	TxContext(ctx context.Context, fn func(sess sqlbuilder.Tx) error) error

	WithContext(context.Context) sqlbuilder.Session

	db.Settings
}

func NewTx(adapter AdapterSession, tx *sql.Tx) (sqlbuilder.Tx, error) {
	sess := &session{
		Settings: db.DefaultSettings,

		adapter:           adapter,
		cachedCollections: cache.NewCache(),
		cachedStatements:  cache.NewCache(),
	}
	sess.SQLBuilder = sqlbuilder.WithSession(sess, adapter.Template())
	sess.baseTx = newBaseTx(tx)
	return &txWrapper{SessionTx: NewSessionTx(sess)}, nil
}

// NewSession provides a Session given a AdapterSession
func NewSession(connURL db.ConnectionURL, adapter AdapterSession) Session {
	sess := &session{
		Settings: db.DefaultSettings,

		connURL:           connURL,
		adapter:           adapter,
		cachedCollections: cache.NewCache(),
		cachedStatements:  cache.NewCache(),
	}
	sess.SQLBuilder = sqlbuilder.WithSession(sess, adapter.Template())
	return sess
}

// database is the actual implementation of Session and joins methods from
// Session and.adapter
type session struct {
	db.Settings

	sqlbuilder.SQLBuilder

	connURL db.ConnectionURL

	adapter AdapterSession

	lookupNameOnce sync.Once
	name           string

	mu        sync.Mutex // guards ctx, txOptions
	ctx       context.Context
	txOptions *sql.TxOptions

	sqlDBMu sync.Mutex // guards sess, baseTx
	sqlDB   *sql.DB
	baseTx  BaseTx

	sessID uint64
	txID   uint64

	cacheMu           sync.Mutex // guards cachedStatements and cachedCollections
	cachedStatements  *cache.Cache
	cachedCollections *cache.Cache

	template *exql.Template
}

var (
	_ = db.Session(&session{})
)

func (sess *session) WithContext(ctx context.Context) sqlbuilder.Session {
	newDB, _ := sess.NewClone(sess.adapter, false)
	return newDB
}

func (sess *session) Tx(fn func(tx sqlbuilder.Tx) error) error {
	return TxContext(sess, sess.Context(), fn)
}

func (sess *session) TxContext(ctx context.Context, fn func(sess sqlbuilder.Tx) error) error {
	return TxContext(sess, ctx, fn)
}

func (sess *session) PrimaryKeys(tableName string) ([]string, error) {
	return sess.adapter.PrimaryKeys(sess, tableName)
}

func (sess *session) TableExists(name string) error {
	return sess.adapter.TableExists(sess, name)
}

// NewSessionTx begins a transaction block.
func (sess *session) NewSessionTx(ctx context.Context) (SessionTx, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	clone, err := sess.NewClone(sess.adapter, false)
	if err != nil {
		return nil, err
	}
	//clone.mu.Lock()
	//defer clone.mu.Unlock()

	connFn := func() error {
		sqlTx, err := compat.BeginTx(clone.DB(), ctx, clone.TxOptions())
		if err == nil {
			return clone.BindTx(ctx, sqlTx)
		}
		return err
	}

	if err := clone.WaitForConnection(connFn); err != nil {
		return nil, err
	}

	return NewSessionTx(clone), nil
}

func (sess *session) Collections() ([]db.Collection, error) {
	names, err := sess.adapter.Collections(sess)
	if err != nil {
		return nil, err
	}

	collections := make([]db.Collection, 0, len(names))
	for i := range names {
		collections = append(collections, sess.Collection(names[i]))
	}

	return collections, nil
}

func (sess *session) ConnectionURL() db.ConnectionURL {
	return sess.connURL
}

func (sess *session) Open() error {
	var sqlDB *sql.DB
	var err error

	connFn := func() error {
		sqlDB, err = sess.adapter.Open(sess, sess.connURL.String())
		if err != nil {
			return err
		}

		sqlDB.SetConnMaxLifetime(sess.ConnMaxLifetime())
		sqlDB.SetMaxIdleConns(sess.MaxIdleConns())
		sqlDB.SetMaxOpenConns(sess.MaxOpenConns())
		return nil
	}

	if err := sess.WaitForConnection(connFn); err != nil {
		return err
	}

	return sess.BindDB(sqlDB)
}

func (d *session) Item(m db.Model) db.Item {
	return newItem(d, m)
}

// Session returns the underlying *sql.DB
func (sess *session) DB() *sql.DB {
	return sess.sqlDB
}

// SetContext sets the session's default context.
func (d *session) SetContext(ctx context.Context) {
	d.mu.Lock()
	d.ctx = ctx
	d.mu.Unlock()
}

// Context returns the session's default context.
func (d *session) Context() context.Context {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.ctx == nil {
		return context.Background()
	}
	return d.ctx
}

// SetTxOptions sets the session's default TxOptions.
func (d *session) SetTxOptions(txOptions sql.TxOptions) {
	d.mu.Lock()
	d.txOptions = &txOptions
	d.mu.Unlock()
}

// TxOptions returns the session's default TxOptions.
func (d *session) TxOptions() *sql.TxOptions {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.txOptions == nil {
		return nil
	}
	return d.txOptions
}

// BindTx binds a *sql.Tx into *session
func (d *session) BindTx(ctx context.Context, t *sql.Tx) error {
	d.sqlDBMu.Lock()
	defer d.sqlDBMu.Unlock()

	d.baseTx = newBaseTx(t)

	d.SetContext(ctx)
	d.txID = newBaseTxID()
	return nil
}

// Tx returns a BaseTx, which, if not nil, means that this session is within a
// transaction
func (d *session) Transaction() BaseTx {
	return d.baseTx
}

// Name returns the database named
func (sess *session) Name() string {
	sess.lookupNameOnce.Do(func() {
		if sess.name == "" {
			sess.name, _ = sess.adapter.LookupName(sess)
		}
	})

	return sess.name
}

// BindDB binds a *sql.DB into *session
func (sess *session) BindDB(sqlDB *sql.DB) error {
	sess.sqlDBMu.Lock()
	sess.sqlDB = sqlDB
	sess.sqlDBMu.Unlock()

	if err := sess.Ping(); err != nil {
		return err
	}

	sess.sessID = newSessionID()
	name, err := sess.adapter.LookupName(sess)
	if err != nil {
		return err
	}
	sess.name = name

	return nil
}

// Ping checks whether a connection to the database is still alive by pinging
// it
func (sess *session) Ping() error {
	if sess.sqlDB != nil {
		return sess.sqlDB.Ping()
	}
	return db.ErrNotConnected
}

// SetConnMaxLifetime sets the maximum amount of time a connection may be
// reused.
func (d *session) SetConnMaxLifetime(t time.Duration) {
	d.Settings.SetConnMaxLifetime(t)
	if sess := d.DB(); sess != nil {
		sess.SetConnMaxLifetime(d.Settings.ConnMaxLifetime())
	}
}

// SetMaxIdleConns sets the maximum number of connections in the idle
// connection pool.
func (d *session) SetMaxIdleConns(n int) {
	d.Settings.SetMaxIdleConns(n)
	if sess := d.DB(); sess != nil {
		sess.SetMaxIdleConns(d.MaxIdleConns())
	}
}

// SetMaxOpenConns sets the maximum number of open connections to the
// database.
func (d *session) SetMaxOpenConns(n int) {
	d.Settings.SetMaxOpenConns(n)
	if sess := d.DB(); sess != nil {
		sess.SetMaxOpenConns(d.MaxOpenConns())
	}
}

// Reset removes all caches.
func (d *session) Reset() {
	d.cacheMu.Lock()
	defer d.cacheMu.Unlock()
	d.cachedCollections.Clear()
	d.cachedStatements.Clear()
	if d.template != nil {
		d.template.Cache.Clear()
	}
}

// NewClone binds a clone that is linked to the current
// session. This is commonly done before creating a transaction
// session.
func (sess *session) NewClone(adapter AdapterSession, checkConn bool) (Session, error) {
	newSess := NewSession(sess.connURL, adapter).(*session)

	newSess.name = sess.name
	newSess.sqlDB = sess.sqlDB

	if checkConn {
		if err := newSess.Ping(); err != nil {
			// Retry once if ping fails.
			return sess.NewClone(adapter, false)
		}
	}

	newSess.sessID = newSessionID()

	// New transaction should inherit parent settings
	copySettings(sess, newSess)

	return newSess, nil
}

// Close terminates the current database session
func (sess *session) Close() error {

	defer func() {
		sess.sqlDBMu.Lock()
		sess.sqlDB = nil
		sess.baseTx = nil
		sess.sqlDBMu.Unlock()
	}()
	if sess.sqlDB == nil {
		return nil
	}

	sess.cachedCollections.Clear()
	sess.cachedStatements.Clear() // Closes prepared statements as well.

	tx := sess.Transaction()
	if tx == nil {
		if cleaner, ok := sess.adapter.(hasCleanUp); ok {
			if err := cleaner.CleanUp(); err != nil {
				return err
			}
		}
		// Not within a transaction.
		return sess.sqlDB.Close()
	}

	if !tx.Committed() {
		_ = tx.Rollback()
	}
	return nil
}

// NewTx begins a transaction block with the given context.
func (sess *session) NewTx(ctx context.Context) (sqlbuilder.Tx, error) {
	newTx, err := sess.NewSessionTx(ctx)
	if err != nil {
		return nil, err
	}

	return &txWrapper{SessionTx: newTx}, nil
}

// Collection returns a db.Collection given a name. Results are cached.
func (sess *session) Collection(name string) db.Collection {
	sess.cacheMu.Lock()
	defer sess.cacheMu.Unlock()

	h := cache.String(name)

	cachedCol, ok := sess.cachedCollections.ReadRaw(h)
	if ok {
		return cachedCol.(db.Collection)
	}

	col := NewCollection(sess, name, sess.adapter.NewCollection())
	sess.cachedCollections.Write(h, col)

	return col
}

// StatementPrepare creates a prepared statement.
func (d *session) StatementPrepare(ctx context.Context, stmt *exql.Statement) (sqlStmt *sql.Stmt, err error) {
	var query string

	if d.Settings.LoggingEnabled() {
		defer func(start time.Time) {
			d.Logger().Log(&db.QueryStatus{
				TxID:    d.txID,
				SessID:  d.sessID,
				Query:   query,
				Err:     err,
				Start:   start,
				End:     time.Now(),
				Context: ctx,
			})
		}(time.Now())
	}

	tx := d.Transaction()

	query, _ = d.compileStatement(stmt, nil)
	if tx != nil {
		sqlStmt, err = compat.PrepareContext(tx.(*baseTx), ctx, query)
		return
	}

	sqlStmt, err = compat.PrepareContext(d.sqlDB, ctx, query)
	return
}

// ConvertValues converts native values into driver specific values.
func (d *session) ConvertValues(values []interface{}) []interface{} {
	if converter, ok := d.adapter.(hasConvertValues); ok {
		return converter.ConvertValues(values)
	}
	return values
}

// StatementExec compiles and executes a statement that does not return any
// rows.
func (d *session) StatementExec(ctx context.Context, stmt *exql.Statement, args ...interface{}) (res sql.Result, err error) {
	var query string

	if d.Settings.LoggingEnabled() {
		defer func(start time.Time) {
			status := db.QueryStatus{
				TxID:    d.txID,
				SessID:  d.sessID,
				Query:   query,
				Args:    args,
				Err:     err,
				Start:   start,
				End:     time.Now(),
				Context: ctx,
			}

			if res != nil {
				if rowsAffected, err := res.RowsAffected(); err == nil {
					status.RowsAffected = &rowsAffected
				}

				if lastInsertID, err := res.LastInsertId(); err == nil {
					status.LastInsertID = &lastInsertID
				}
			}

			d.Logger().Log(&status)
		}(time.Now())
	}

	if execer, ok := d.adapter.(statementExecer); ok {
		query, args = d.compileStatement(stmt, args)
		res, err = execer.StatementExec(d, ctx, query, args...)
		return
	}

	tx := d.Transaction()

	if d.Settings.PreparedStatementCacheEnabled() && tx == nil {
		var p *Stmt
		if p, query, args, err = d.prepareStatement(ctx, stmt, args); err != nil {
			return nil, err
		}
		defer p.Close()

		res, err = compat.PreparedExecContext(p, ctx, args)
		return
	}

	query, args = d.compileStatement(stmt, args)
	if tx != nil {
		res, err = compat.ExecContext(tx.(*baseTx), ctx, query, args)
		return
	}

	res, err = compat.ExecContext(d.sqlDB, ctx, query, args)
	return
}

// StatementQuery compiles and executes a statement that returns rows.
func (d *session) StatementQuery(ctx context.Context, stmt *exql.Statement, args ...interface{}) (rows *sql.Rows, err error) {
	var query string

	if d.Settings.LoggingEnabled() {
		defer func(start time.Time) {
			d.Logger().Log(&db.QueryStatus{
				TxID:    d.txID,
				SessID:  d.sessID,
				Query:   query,
				Args:    args,
				Err:     err,
				Start:   start,
				End:     time.Now(),
				Context: ctx,
			})
		}(time.Now())
	}

	tx := d.Transaction()

	if d.Settings.PreparedStatementCacheEnabled() && tx == nil {
		var p *Stmt
		if p, query, args, err = d.prepareStatement(ctx, stmt, args); err != nil {
			return nil, err
		}
		defer p.Close()

		rows, err = compat.PreparedQueryContext(p, ctx, args)
		return
	}

	query, args = d.compileStatement(stmt, args)
	if tx != nil {
		rows, err = compat.QueryContext(tx.(*baseTx), ctx, query, args)
		return
	}

	rows, err = compat.QueryContext(d.sqlDB, ctx, query, args)
	return

}

// StatementQueryRow compiles and executes a statement that returns at most one
// row.
func (d *session) StatementQueryRow(ctx context.Context, stmt *exql.Statement, args ...interface{}) (row *sql.Row, err error) {
	var query string

	if d.Settings.LoggingEnabled() {
		defer func(start time.Time) {
			d.Logger().Log(&db.QueryStatus{
				TxID:    d.txID,
				SessID:  d.sessID,
				Query:   query,
				Args:    args,
				Err:     err,
				Start:   start,
				End:     time.Now(),
				Context: ctx,
			})
		}(time.Now())
	}

	tx := d.Transaction()

	if d.Settings.PreparedStatementCacheEnabled() && tx == nil {
		var p *Stmt
		if p, query, args, err = d.prepareStatement(ctx, stmt, args); err != nil {
			return nil, err
		}
		defer p.Close()

		row = compat.PreparedQueryRowContext(p, ctx, args)
		return
	}

	query, args = d.compileStatement(stmt, args)
	if tx != nil {
		row = compat.QueryRowContext(tx.(*baseTx), ctx, query, args)
		return
	}

	row = compat.QueryRowContext(d.sqlDB, ctx, query, args)
	return
}

// Driver returns the underlying *sql.DB or *sql.Tx instance.
func (sess *session) Driver() interface{} {
	if tx := sess.Transaction(); tx != nil {
		// A transaction
		return tx.(*baseTx).Tx
	}
	return sess.sqlDB
}

// compileStatement compiles the given statement into a string.
func (sess *session) compileStatement(stmt *exql.Statement, args []interface{}) (string, []interface{}) {
	if converter, ok := sess.adapter.(hasConvertValues); ok {
		args = converter.ConvertValues(args)
	}
	return sess.adapter.CompileStatement(sess, stmt, args)
}

// prepareStatement compiles a query and tries to use previously generated
// statement.
func (sess *session) prepareStatement(ctx context.Context, stmt *exql.Statement, args []interface{}) (*Stmt, string, []interface{}, error) {
	sess.sqlDBMu.Lock()
	defer sess.sqlDBMu.Unlock()

	sqlDB, tx := sess.sqlDB, sess.Transaction()
	if sqlDB == nil && tx == nil {
		return nil, "", nil, db.ErrNotConnected
	}

	pc, ok := sess.cachedStatements.ReadRaw(stmt)
	if ok {
		// The statement was cachesess.
		ps, err := pc.(*Stmt).Open()
		if err == nil {
			_, args = sess.compileStatement(stmt, args)
			return ps, ps.query, args, nil
		}
	}

	query, args := sess.compileStatement(stmt, args)
	sqlStmt, err := func(query *string) (*sql.Stmt, error) {
		if tx != nil {
			return compat.PrepareContext(tx.(*baseTx), ctx, *query)
		}
		return compat.PrepareContext(sess.sqlDB, ctx, *query)
	}(&query)
	if err != nil {
		return nil, "", nil, err
	}

	p, err := NewStatement(sqlStmt, query).Open()
	if err != nil {
		return nil, query, args, err
	}
	sess.cachedStatements.Write(stmt, p)
	return p, p.query, args, nil
}

var waitForConnMu sync.Mutex

// WaitForConnection tries to execute the given connectFn function, if
// connectFn returns an error, then WaitForConnection will keep trying until
// connectFn returns nil. Maximum waiting time is 5s after having acquired the
// lock.
func (d *session) WaitForConnection(connectFn func() error) error {
	// This lock ensures first-come, first-served and prevents opening too many
	// file descriptors.
	waitForConnMu.Lock()
	defer waitForConnMu.Unlock()

	// Minimum waiting time.
	waitTime := time.Millisecond * 10

	// Waitig 5 seconds for a successful connection.
	for timeStart := time.Now(); time.Since(timeStart) < time.Second*5; {
		err := connectFn()
		if err == nil {
			return nil // Connected!
		}

		// Only attempt to reconnect if the error is too many clients.
		if d.adapter.Err(d, err) == db.ErrTooManyClients {
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
			k = i + 1

			if k < t && buf[k] == '?' {
				i = k
			} else {
				out = append(out, []byte("$"+strconv.Itoa(j))...)
				j++
			}
		}
		i++
	}
	out = append(out, buf[k:i]...)

	return string(out)
}

func copySettings(from Session, into Session) {
	into.SetLogging(from.LoggingEnabled())
	into.SetLogger(from.Logger())
	into.SetPreparedStatementCache(from.PreparedStatementCacheEnabled())
	into.SetConnMaxLifetime(from.ConnMaxLifetime())
	into.SetMaxIdleConns(from.MaxIdleConns())
	into.SetMaxOpenConns(from.MaxOpenConns())

	txOptions := from.TxOptions()
	if txOptions != nil {
		into.SetTxOptions(*txOptions)
	}
}

func newSessionID() uint64 {
	if atomic.LoadUint64(&lastSessID) == math.MaxUint64 {
		atomic.StoreUint64(&lastSessID, 0)
		return 0
	}
	return atomic.AddUint64(&lastSessID, 1)
}

func newBaseTxID() uint64 {
	if atomic.LoadUint64(&lastTxID) == math.MaxUint64 {
		atomic.StoreUint64(&lastTxID, 0)
		return 0
	}
	return atomic.AddUint64(&lastTxID, 1)
}

var _ sqlbuilder.Session = &session{}
