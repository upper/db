package db

import (
	"fmt"
	"log"
	"regexp"
	"strings"
	"time"
)

// QueryStatus represents a query after being executed.
type QueryStatus struct {
	SessionID uint64
	TxID      uint64

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
		Config.SetLogger(&defaultLogger{}) // Using default logger.
		Config.SetLogging(true)
	}
}

// Logger represents a logging collector. You can pass a logging collector to
// db.Config.SetLogger(myCollector) to make it collect db.QueryStatus messages
// after every query.
type Logger interface {
	Log(*QueryStatus)
}

// Log sends a query status report to the configured logger.
func Log(m *QueryStatus) {
	if lg := Config.Logger(); lg != nil {
		lg.Log(m)
		return
	}
	log.Printf("No logger has been configured, use db.Config.SetLogger()")
}

var (
	reInvisibleChars       = regexp.MustCompile(`[\s\r\n\t]+`)
	reColumnCompareExclude = regexp.MustCompile(`[^a-zA-Z0-9]`)
)

type defaultLogger struct {
}

func (lg *defaultLogger) Log(m *QueryStatus) {
	m.Query = reInvisibleChars.ReplaceAllString(m.Query, ` `)
	m.Query = strings.TrimSpace(m.Query)

	s := make([]string, 0, 4)

	if m.Query != "" {
		s = append(s, fmt.Sprintf(`Q: %s`, m.Query))
	}

	if len(m.Args) > 0 {
		s = append(s, fmt.Sprintf(`A: %#v`, m.Args))
	}

	if m.Err != nil {
		s = append(s, fmt.Sprintf(`E: %q`, m.Err))
	}

	s = append(s, fmt.Sprintf(`T: %0.5fs`, float64(m.End.UnixNano()-m.Start.UnixNano())/float64(1e9)))

	log.Printf("\n\t%s\n\n", strings.Join(s, "\n\t"))
}
