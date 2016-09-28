package db

import (
	"fmt"
	"log"
	"regexp"
	"strings"
	"time"
)

const (
	fmtLogQuery        = `Query:          %s`
	fmtLogArgs         = `Arguments:      %#v`
	fmtLogRowsAffected = `Rows affected:  %d`
	fmtLogLastInsertId = `Last insert ID: %d`
	fmtLogError        = `Error:          %v`
	fmtLogTimeTaken    = `Time taken:     %0.5fs`
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
		Conf.SetLogger(&defaultLogger{}) // Using default logger.
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
	if lg := Conf.Logger(); lg != nil {

		m.Query = reInvisibleChars.ReplaceAllString(m.Query, ` `)
		m.Query = strings.TrimSpace(m.Query)

		lg.Log(m)
		return
	}
	log.Printf("No logger has been configured, use db.Conf.SetLogger()")
}

var (
	reInvisibleChars       = regexp.MustCompile(`[\s\r\n\t]+`)
	reColumnCompareExclude = regexp.MustCompile(`[^a-zA-Z0-9]`)
)

type defaultLogger struct {
}

func (lg *defaultLogger) Log(m *QueryStatus) {
	s := make([]string, 0, 6)

	if m.Query != "" {
		s = append(s, fmt.Sprintf(fmtLogQuery, m.Query))
	}

	if len(m.Args) > 0 {
		s = append(s, fmt.Sprintf(fmtLogArgs, m.Args))
	}

	if m.RowsAffected != nil {
		s = append(s, fmt.Sprintf(fmtLogRowsAffected, *m.RowsAffected))
	}
	if m.LastInsertID != nil {
		s = append(s, fmt.Sprintf(fmtLogLastInsertId, *m.LastInsertID))
	}

	if m.Err != nil {
		s = append(s, fmt.Sprintf(fmtLogError, m.Err))
	}

	s = append(s, fmt.Sprintf(fmtLogTimeTaken, float64(m.End.UnixNano()-m.Start.UnixNano())/float64(1e9)))

	log.Printf("\n\t%s\n\n", strings.Join(s, "\n\t"))
}
