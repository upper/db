// Copyright (c) 2012-present The upper.io/db authors. All rights reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining
// a copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to
// permit persons to whom the Software is furnished to do so, subject to
// the following conditions:
//
// The above copyright notice and this permission notice shall be
// included in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
// LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
// OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
// WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

package db

import (
	"sync"
	"sync/atomic"
)

// Settings defines methods to get or set configuration values.
type Settings interface {
	// SetLogging enables or disables logging.
	SetLogging(bool)
	// LoggingEnabled returns true if logging is enabled, false otherwise.
	LoggingEnabled() bool

	// SetLogger defines which logger to use.
	SetLogger(Logger)
	// Returns the currently configured logger.
	Logger() Logger
}

type conf struct {
	loggingEnabled uint32

	queryLogger   Logger
	queryLoggerMu sync.RWMutex
	defaultLogger defaultLogger
}

func (c *conf) Logger() Logger {
	c.queryLoggerMu.RLock()
	defer c.queryLoggerMu.RUnlock()

	if c.queryLogger == nil {
		return &c.defaultLogger
	}

	return c.queryLogger
}

func (c *conf) SetLogger(lg Logger) {
	c.queryLoggerMu.Lock()
	defer c.queryLoggerMu.Unlock()

	c.queryLogger = lg
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

// Conf provides global configuration settings for upper-db.
var Conf Settings = &conf{}
