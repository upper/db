package sqladapter

import (
	"database/sql"
)

// cachedStatement represents a *sql.Stmt that is cached and provides the
// OnPurge method to allow it to clean after itself.
type cachedStatement struct {
	*sql.Stmt
	query string
}

func (c *cachedStatement) OnPurge() {
	c.Stmt.Close()
}
