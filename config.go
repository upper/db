package db

import (
	"sync/atomic"
)

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

var Conf = conf{}
