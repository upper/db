package db

import (
	"fmt"
	"log"
	"regexp"
	"strings"
	"time"
)

const (
	fmtLogSessID       = `Session ID:     %05d`
	fmtLogTxID         = `Transaction ID: %05d`
	fmtLogQuery        = `Query:          %s`
	fmtLogArgs         = `Arguments:      %#v`
	fmtLogRowsAffected = `Rows affected:  %d`
	fmtLogLastInsertID = `Last insert ID: %d`
	fmtLogError        = `Error:          %v`
	fmtLogTimeTaken    = `Time taken:     %0.5fs`
)

var (
	reInvisibleChars       = regexp.MustCompile(`[\s\r\n\t]+`)
	reColumnCompareExclude = regexp.MustCompile(`[^a-zA-Z0-9]`)
)

// QueryStatus represents a query after being executed.
type QueryStatus struct {
	SessID uint64
	TxID   uint64

	RowsAffected *int64
	LastInsertID *int64

	Query string
	Args  []interface{}

	Err error

	Start time.Time
	End   time.Time
}

func (q *QueryStatus) String() string {
	s := make([]string, 0, 8)

	if q.SessID > 0 {
		s = append(s, fmt.Sprintf(fmtLogSessID, q.SessID))
	}

	if q.TxID > 0 {
		s = append(s, fmt.Sprintf(fmtLogTxID, q.TxID))
	}

	if qry := q.Query; qry != "" {
		qry = reInvisibleChars.ReplaceAllString(qry, ` `)
		qry = strings.TrimSpace(qry)
		s = append(s, fmt.Sprintf(fmtLogQuery, qry))
	}

	if len(q.Args) > 0 {
		s = append(s, fmt.Sprintf(fmtLogArgs, q.Args))
	}

	if q.RowsAffected != nil {
		s = append(s, fmt.Sprintf(fmtLogRowsAffected, *q.RowsAffected))
	}
	if q.LastInsertID != nil {
		s = append(s, fmt.Sprintf(fmtLogLastInsertID, *q.LastInsertID))
	}

	if q.Err != nil {
		s = append(s, fmt.Sprintf(fmtLogError, q.Err))
	}

	s = append(s, fmt.Sprintf(fmtLogTimeTaken, float64(q.End.UnixNano()-q.Start.UnixNano())/float64(1e9)))

	return strings.Join(s, "\n")
}

// EnvEnableDebug can be used by adapters to determine if the user has enabled
// debugging.
//
// If the user sets the `UPPERIO_DB_DEBUG` environment variable to a
// non-empty value, all generated statements will be printed at runtime to
// the standard logger.
//
// Example:
//
//	UPPERIO_DB_DEBUG=1 go test
//
//	UPPERIO_DB_DEBUG=1 ./go-program
const (
	EnvEnableDebug = `UPPERIO_DB_DEBUG`
)

func init() {
	if envEnabled(EnvEnableDebug) {
		Conf.SetLogging(true)
	}
}

// Logger represents a logging collector. You can pass a logging collector to
// db.Conf.SetLogger(myCollector) to make it collect db.QueryStatus messages
// after every query.
type Logger interface {
	Log(*QueryStatus)
}

// Log sends a query status report to the configured logger.
func Log(m *QueryStatus) {
	Conf.Logger().Log(m)
}

type defaultLogger struct {
}

func (lg *defaultLogger) Log(m *QueryStatus) {
	log.Printf("\n\t%s\n\n", strings.Replace(m.String(), "\n", "\n\t", -1))
}

var _ = Logger(&defaultLogger{})
