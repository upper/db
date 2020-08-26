package sqladapter

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	db "github.com/upper/db/v4"
	"github.com/upper/db/v4/internal/cache"
	"github.com/upper/db/v4/internal/sqladapter/compat"
	"github.com/upper/db/v4/internal/sqladapter/exql"
	"github.com/upper/db/v4/internal/sqlbuilder"
)

var (
	lastSessID uint64
	lastTxID   uint64
)

var (
	slowQueryThreshold       = time.Millisecond * 200
	retryTransactionWaitTime = time.Millisecond * 20
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

// statementCompiler transforms an internal statement into a format
// database/sql can understand.
type statementCompiler interface {
	CompileStatement(sess Session, stmt *exql.Statement, args []interface{}) (string, []interface{}, error)
}

// valueConverter converts values before being passed to the underlying driver.
type valueConverter interface {
	ConvertValues(values []interface{}) []interface{}
}

// errorConverter converts an error value from the underlying driver into
// something different.
type errorConverter interface {
	Err(errIn error) (errOut error)
}

// AdapterSession defines methods to be implemented by SQL adapters.
type AdapterSession interface {
	Template() *exql.Template

	NewCollection() CollectionAdapter

	// Open opens a new connection
	OpenDSN(sess Session, dsn string) (*sql.DB, error)

	// Collections returns a list of non-system tables from the database.
	Collections(sess Session) ([]string, error)

	// TableExists returns an error if the given table does not exist.
	TableExists(sess Session, name string) error

	// LookupName returns the name of the database.
	LookupName(sess Session) (string, error)

	// PrimaryKeys returns all primary keys on the table.
	PrimaryKeys(sess Session, name string) ([]string, error)
}

// Session satisfies db.Session.
type Session interface {
	SQL() db.SQL

	// PrimaryKeys returns all primary keys on the table.
	PrimaryKeys(tableName string) ([]string, error)

	// Collections returns a list of references to all collections in the
	// database.
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

	// ConnectionURL returns the ConnectionURL that was used to create the
	// Session.
	ConnectionURL() db.ConnectionURL

	// Open attempts to establish a connection to the database server.
	Open() error

	// TableExists returns an error if the table doesn't exists.
	TableExists(name string) error

	// Driver returns the underlying driver the session is using
	Driver() interface{}

	Save(db.Record) error

	Get(db.Record, interface{}) error

	Delete(db.Record) error

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
	Transaction() *sql.Tx

	// NewClone clones the database using the given AdapterSession as base.
	NewClone(AdapterSession, bool) (Session, error)

	// Context returns the default context the session is using.
	Context() context.Context

	// SetContext sets a default context for the session.
	SetContext(context.Context)

	NewTransaction(ctx context.Context, opts *sql.TxOptions) (Session, error)

	Tx(fn func(sess db.Session) error) error

	TxContext(ctx context.Context, fn func(sess db.Session) error, opts *sql.TxOptions) error

	WithContext(context.Context) db.Session

	IsTransaction() bool

	Commit() error

	Rollback() error

	db.Settings
}

// NewTx wraps a *sql.Tx and returns a Tx.
func NewTx(adapter AdapterSession, tx *sql.Tx) (Session, error) {
	sess := &session{
		Settings: db.DefaultSettings,

		sqlTx:             tx,
		adapter:           adapter,
		cachedPKs:         cache.NewCache(),
		cachedCollections: cache.NewCache(),
		cachedStatements:  cache.NewCache(),
	}
	sess.builder = sqlbuilder.WithSession(sess, adapter.Template())
	return sess, nil
}

// NewSession creates a new Session.
func NewSession(connURL db.ConnectionURL, adapter AdapterSession) Session {
	sess := &session{
		Settings: db.DefaultSettings,

		connURL:           connURL,
		adapter:           adapter,
		cachedPKs:         cache.NewCache(),
		cachedCollections: cache.NewCache(),
		cachedStatements:  cache.NewCache(),
	}
	sess.builder = sqlbuilder.WithSession(sess, adapter.Template())
	return sess
}

type session struct {
	db.Settings

	adapter AdapterSession

	connURL db.ConnectionURL

	builder db.SQL

	lookupNameOnce sync.Once
	name           string

	mu        sync.Mutex // guards ctx, txOptions
	ctx       context.Context
	txOptions *sql.TxOptions

	sqlDBMu sync.Mutex // guards sess, baseTx

	sqlDB *sql.DB
	sqlTx *sql.Tx

	sessID uint64
	txID   uint64

	cacheMu           sync.Mutex // guards cachedStatements and cachedCollections
	cachedPKs         *cache.Cache
	cachedStatements  *cache.Cache
	cachedCollections *cache.Cache

	template *exql.Template
}

var (
	_ = db.Session(&session{})
)

func (sess *session) WithContext(ctx context.Context) db.Session {
	newDB, _ := sess.NewClone(sess.adapter, false)
	return newDB
}

func (sess *session) Tx(fn func(sess db.Session) error) error {
	return TxContext(sess.Context(), sess, fn, nil)
}

func (sess *session) TxContext(ctx context.Context, fn func(sess db.Session) error, opts *sql.TxOptions) error {
	return TxContext(ctx, sess, fn, opts)
}

func (sess *session) SQL() db.SQL {
	return sess.builder
}

func (sess *session) Err(errIn error) (errOur error) {
	if convertError, ok := sess.adapter.(errorConverter); ok {
		return convertError.Err(errIn)
	}
	return errIn
}

func (sess *session) PrimaryKeys(tableName string) ([]string, error) {
	h := cache.String(tableName)
	cachedPK, ok := sess.cachedPKs.ReadRaw(h)
	if ok {
		return cachedPK.([]string), nil
	}

	pk, err := sess.adapter.PrimaryKeys(sess, tableName)
	if err != nil {
		return nil, err
	}

	sess.cachedPKs.Write(h, pk)
	return pk, nil
}

func (sess *session) TableExists(name string) error {
	return sess.adapter.TableExists(sess, name)
}

func (sess *session) NewTransaction(ctx context.Context, opts *sql.TxOptions) (Session, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	clone, err := sess.NewClone(sess.adapter, false)
	if err != nil {
		return nil, err
	}

	connFn := func() error {
		sqlTx, err := compat.BeginTx(clone.DB(), clone.Context(), opts)
		if err == nil {
			return clone.BindTx(ctx, sqlTx)
		}
		return err
	}

	if err := clone.WaitForConnection(connFn); err != nil {
		return nil, err
	}

	return clone, nil
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
		sqlDB, err = sess.adapter.OpenDSN(sess, sess.connURL.String())
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

func (sess *session) Get(record db.Record, id interface{}) error {
	store := record.Store(sess)
	if getter, ok := store.(db.StoreGetter); ok {
		return getter.Get(record, id)
	}
	return store.Find(id).One(record)
}

func (sess *session) Save(record db.Record) error {
	if record == nil {
		return db.ErrNilRecord
	}

	if reflect.TypeOf(record).Kind() != reflect.Ptr {
		return db.ErrExpectingPointerToStruct
	}

	store := record.Store(sess)

	if saver, ok := store.(db.StoreSaver); ok {
		return saver.Save(record)
	}

	_, fields, err := recordPrimaryKeyFieldValues(store, record)
	if err != nil {
		return err
	}
	isCreate := true
	for i := range fields {
		if fields[i] != reflect.Zero(reflect.TypeOf(fields[i])).Interface() {
			isCreate = false
			break
		}
	}

	if isCreate {
		return recordCreate(store, record)
	}
	return recordUpdate(store, record)
}

func (sess *session) Delete(record db.Record) error {
	if record == nil {
		return db.ErrNilRecord
	}

	if reflect.TypeOf(record).Kind() != reflect.Ptr {
		return db.ErrExpectingPointerToStruct
	}

	store := record.Store(sess)

	if hook, ok := record.(db.BeforeDeleteHook); ok {
		if err := hook.BeforeDelete(sess); err != nil {
			return err
		}
	}

	if deleter, ok := store.(db.StoreDeleter); ok {
		if err := deleter.Delete(record); err != nil {
			return err
		}
	} else {
		conds, err := recordID(store, record)
		if err != nil {
			return err
		}
		if err := store.Find(conds).Delete(); err != nil {
			return err
		}
	}

	if hook, ok := record.(db.AfterDeleteHook); ok {
		if err := hook.AfterDelete(sess); err != nil {
			return err
		}
	}

	return nil
}

func (sess *session) DB() *sql.DB {
	return sess.sqlDB
}

func (sess *session) SetContext(ctx context.Context) {
	sess.mu.Lock()
	sess.ctx = ctx
	sess.mu.Unlock()
}

func (sess *session) Context() context.Context {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if sess.ctx == nil {
		return context.Background()
	}
	return sess.ctx
}

func (sess *session) SetTxOptions(txOptions sql.TxOptions) {
	sess.mu.Lock()
	sess.txOptions = &txOptions
	sess.mu.Unlock()
}

func (sess *session) TxOptions() *sql.TxOptions {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if sess.txOptions == nil {
		return nil
	}
	return sess.txOptions
}

func (sess *session) BindTx(ctx context.Context, tx *sql.Tx) error {
	sess.sqlDBMu.Lock()
	defer sess.sqlDBMu.Unlock()

	sess.sqlTx = tx
	sess.SetContext(ctx)

	sess.txID = newBaseTxID()

	return nil
}

func (sess *session) Commit() error {
	if sess.sqlTx != nil {
		return sess.sqlTx.Commit()
	}
	return db.ErrNotWithinTransaction
}

func (sess *session) Rollback() error {
	if sess.sqlTx != nil {
		return sess.sqlTx.Rollback()
	}
	return db.ErrNotWithinTransaction
}

func (sess *session) IsTransaction() bool {
	return sess.sqlTx != nil
}

func (sess *session) Transaction() *sql.Tx {
	return sess.sqlTx
}

func (sess *session) Name() string {
	sess.lookupNameOnce.Do(func() {
		if sess.name == "" {
			sess.name, _ = sess.adapter.LookupName(sess)
		}
	})

	return sess.name
}

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

func (sess *session) Ping() error {
	if sess.sqlDB != nil {
		return sess.sqlDB.Ping()
	}
	return db.ErrNotConnected
}

func (sess *session) SetConnMaxLifetime(t time.Duration) {
	sess.Settings.SetConnMaxLifetime(t)
	if sessDB := sess.DB(); sessDB != nil {
		sessDB.SetConnMaxLifetime(sess.Settings.ConnMaxLifetime())
	}
}

func (sess *session) SetMaxIdleConns(n int) {
	sess.Settings.SetMaxIdleConns(n)
	if sessDB := sess.DB(); sessDB != nil {
		sessDB.SetMaxIdleConns(sess.Settings.MaxIdleConns())
	}
}

func (sess *session) SetMaxOpenConns(n int) {
	sess.Settings.SetMaxOpenConns(n)
	if sessDB := sess.DB(); sessDB != nil {
		sessDB.SetMaxOpenConns(sess.Settings.MaxOpenConns())
	}
}

// Reset removes all caches.
func (sess *session) Reset() {
	sess.cacheMu.Lock()
	defer sess.cacheMu.Unlock()

	sess.cachedPKs.Clear()
	sess.cachedCollections.Clear()
	sess.cachedStatements.Clear()

	if sess.template != nil {
		sess.template.Cache.Clear()
	}
}

func (sess *session) NewClone(adapter AdapterSession, checkConn bool) (Session, error) {
	newSess := NewSession(sess.connURL, adapter).(*session)

	newSess.name = sess.name
	newSess.sqlDB = sess.sqlDB
	newSess.cachedPKs = sess.cachedPKs

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

func (sess *session) Close() error {
	defer func() {
		sess.sqlDBMu.Lock()
		sess.sqlDB = nil
		sess.sqlTx = nil
		sess.sqlDBMu.Unlock()
	}()

	if sess.sqlDB == nil {
		return nil
	}

	sess.cachedCollections.Clear()
	sess.cachedStatements.Clear() // Closes prepared statements as well.

	if !sess.IsTransaction() {
		if cleaner, ok := sess.adapter.(hasCleanUp); ok {
			if err := cleaner.CleanUp(); err != nil {
				return err
			}
		}
		// Not within a transaction.
		return sess.sqlDB.Close()
	}

	return nil
}

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

func queryLog(status *QueryStatus) {
	diff := status.End.Sub(status.Start)

	slowQuery := false
	if diff >= slowQueryThreshold {
		status.Err = db.ErrWarnSlowQuery
		slowQuery = true
	}

	if status.Err != nil || slowQuery {
		db.LC().Warn(status)
		return
	}

	db.LC().Debug(status)
}

func (sess *session) StatementPrepare(ctx context.Context, stmt *exql.Statement) (sqlStmt *sql.Stmt, err error) {
	var query string

	defer func(start time.Time) {
		queryLog(&QueryStatus{
			TxID:    sess.txID,
			SessID:  sess.sessID,
			Query:   query,
			Err:     err,
			Start:   start,
			End:     time.Now(),
			Context: ctx,
		})
	}(time.Now())

	query, _, err = sess.compileStatement(stmt, nil)
	if err != nil {
		return nil, err
	}

	tx := sess.Transaction()
	if tx != nil {
		sqlStmt, err = compat.PrepareContext(tx, ctx, query)
		return
	}

	sqlStmt, err = compat.PrepareContext(sess.sqlDB, ctx, query)
	return
}

func (sess *session) ConvertValues(values []interface{}) []interface{} {
	if converter, ok := sess.adapter.(valueConverter); ok {
		return converter.ConvertValues(values)
	}
	return values
}

func (sess *session) StatementExec(ctx context.Context, stmt *exql.Statement, args ...interface{}) (res sql.Result, err error) {
	var query string

	defer func(start time.Time) {
		status := QueryStatus{
			TxID:    sess.txID,
			SessID:  sess.sessID,
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

		queryLog(&status)
	}(time.Now())

	if execer, ok := sess.adapter.(statementExecer); ok {
		query, args, err = sess.compileStatement(stmt, args)
		if err != nil {
			return nil, err
		}
		res, err = execer.StatementExec(sess, ctx, query, args...)
		return
	}

	tx := sess.Transaction()
	if sess.Settings.PreparedStatementCacheEnabled() && tx == nil {
		var p *Stmt
		if p, query, args, err = sess.prepareStatement(ctx, stmt, args); err != nil {
			return nil, err
		}
		defer p.Close()

		res, err = compat.PreparedExecContext(p, ctx, args)
		return
	}

	query, args, err = sess.compileStatement(stmt, args)
	if err != nil {
		return nil, err
	}

	if tx != nil {
		res, err = compat.ExecContext(tx, ctx, query, args)
		return
	}

	res, err = compat.ExecContext(sess.sqlDB, ctx, query, args)
	return
}

// StatementQuery compiles and executes a statement that returns rows.
func (sess *session) StatementQuery(ctx context.Context, stmt *exql.Statement, args ...interface{}) (rows *sql.Rows, err error) {
	var query string

	defer func(start time.Time) {
		status := QueryStatus{
			TxID:    sess.txID,
			SessID:  sess.sessID,
			Query:   query,
			Args:    args,
			Err:     err,
			Start:   start,
			End:     time.Now(),
			Context: ctx,
		}
		queryLog(&status)
	}(time.Now())

	tx := sess.Transaction()

	if sess.Settings.PreparedStatementCacheEnabled() && tx == nil {
		var p *Stmt
		if p, query, args, err = sess.prepareStatement(ctx, stmt, args); err != nil {
			return nil, err
		}
		defer p.Close()

		rows, err = compat.PreparedQueryContext(p, ctx, args)
		return
	}

	query, args, err = sess.compileStatement(stmt, args)
	if err != nil {
		return nil, err
	}
	if tx != nil {
		rows, err = compat.QueryContext(tx, ctx, query, args)
		return
	}

	rows, err = compat.QueryContext(sess.sqlDB, ctx, query, args)
	return

}

// StatementQueryRow compiles and executes a statement that returns at most one
// row.
func (sess *session) StatementQueryRow(ctx context.Context, stmt *exql.Statement, args ...interface{}) (row *sql.Row, err error) {
	var query string

	defer func(start time.Time) {
		status := QueryStatus{
			TxID:    sess.txID,
			SessID:  sess.sessID,
			Query:   query,
			Args:    args,
			Err:     err,
			Start:   start,
			End:     time.Now(),
			Context: ctx,
		}
		queryLog(&status)
	}(time.Now())

	tx := sess.Transaction()

	if sess.Settings.PreparedStatementCacheEnabled() && tx == nil {
		var p *Stmt
		if p, query, args, err = sess.prepareStatement(ctx, stmt, args); err != nil {
			return nil, err
		}
		defer p.Close()

		row = compat.PreparedQueryRowContext(p, ctx, args)
		return
	}

	query, args, err = sess.compileStatement(stmt, args)
	if err != nil {
		return nil, err
	}
	if tx != nil {
		row = compat.QueryRowContext(tx, ctx, query, args)
		return
	}

	row = compat.QueryRowContext(sess.sqlDB, ctx, query, args)
	return
}

// Driver returns the underlying *sql.DB or *sql.Tx instance.
func (sess *session) Driver() interface{} {
	if sess.sqlTx != nil {
		return sess.sqlTx
	}
	return sess.sqlDB
}

// compileStatement compiles the given statement into a string.
func (sess *session) compileStatement(stmt *exql.Statement, args []interface{}) (string, []interface{}, error) {
	if converter, ok := sess.adapter.(valueConverter); ok {
		args = converter.ConvertValues(args)
	}
	if statementCompiler, ok := sess.adapter.(statementCompiler); ok {
		return statementCompiler.CompileStatement(sess, stmt, args)
	}

	compiled, err := stmt.Compile(sess.adapter.Template())
	if err != nil {
		return "", nil, err
	}
	query, args := sqlbuilder.Preprocess(compiled, args)
	return query, args, nil
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
			_, args, err = sess.compileStatement(stmt, args)
			if err != nil {
				return nil, "", nil, err
			}
			return ps, ps.query, args, nil
		}
	}

	query, args, err := sess.compileStatement(stmt, args)
	if err != nil {
		return nil, "", nil, err
	}
	sqlStmt, err := func(query *string) (*sql.Stmt, error) {
		if tx != nil {
			return compat.PrepareContext(tx, ctx, *query)
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
func (sess *session) WaitForConnection(connectFn func() error) error {
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
		if sess.Err(err) == db.ErrTooManyClients {
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
	into.SetPreparedStatementCache(from.PreparedStatementCacheEnabled())
	into.SetConnMaxLifetime(from.ConnMaxLifetime())
	into.SetMaxIdleConns(from.MaxIdleConns())
	into.SetMaxOpenConns(from.MaxOpenConns())
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

var _ db.Session = &session{}

// TxContext creates a transaction context and runs fn within it.
func TxContext(ctx context.Context, sess db.Session, fn func(tx db.Session) error, opts *sql.TxOptions) error {
	txFn := func(sess db.Session) error {
		tx, err := sess.(Session).NewTransaction(ctx, opts)
		if err != nil {
			return err
		}
		defer tx.Close()

		if err := fn(tx); err != nil {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				return fmt.Errorf("%s: %w", rollbackErr, err)
			}
			return err
		}
		return tx.Commit()
	}

	retryTime := retryTransactionWaitTime

	var txErr error
	for i := 0; i < sess.MaxTransactionRetries(); i++ {
		txErr = sess.(*session).Err(txFn(sess))
		if txErr == nil {
			return nil
		}
		if errors.Is(txErr, db.ErrTransactionAborted) {
			time.Sleep(retryTime)
			if retryTime < time.Second {
				retryTime = retryTime * 2
			}
			continue
		}
		return txErr
	}

	return fmt.Errorf("db: giving up trying to commit transaction: %w", txErr)
}
