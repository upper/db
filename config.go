package db

import (
	"sync/atomic"
)

// Settings defines methods to get or set configuration settings, use db.Conf
// to get or set global configuration settings.
type Settings interface {
	// SetLogging enables or disables logging.
	SetLogging(bool)
	// LoggingEnabled returns true if logging is enabled, false otherwise.
	LoggingEnabled() bool

	// SetLogger defines which logger to use.
	SetLogger(Logger)
	// Returns the configured logger.
	Logger() Logger
}

type conf struct {
	loggingEnabled uint32
	queryLogger    atomic.Value
}

func (c *conf) Logger() Logger {
	if lg := c.queryLogger.Load(); lg != nil {
		return lg.(Logger)
	}
	return nil
}

func (c *conf) SetLogger(lg Logger) {
	c.queryLogger.Store(lg)
}

func (c *conf) SetLogging(value bool) {
	if value {
		atomic.StoreUint32(&c.loggingEnabled, 1)
		return
	}
	atomic.StoreUint32(&c.loggingEnabled, 0)
}

func (c *conf) LoggingEnabled() bool {
	if v := atomic.LoadUint32(&c.loggingEnabled); v == 1 {
		return true
	}
	return false
}

// Conf has global configuration settings for upper-db.
var Conf Settings = &conf{}
