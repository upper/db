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

package cockroachdb

import (
	"context"

	"upper.io/db.v3/internal/sqladapter"
	"upper.io/db.v3/lib/sqlbuilder"
	"github.com/lib/pq"
)

type tx struct {
	sqladapter.DatabaseTx
}

var (
	_ = sqlbuilder.Tx(&tx{})
)

func (t *tx) WithContext(ctx context.Context) sqlbuilder.Tx {
	var newTx tx
	newTx = *t
	newTx.DatabaseTx.SetContext(ctx)
	return &newTx
}

// ExecuteInTx runs fn inside tx which should already have begun.
// *WARNING*: Do not execute any statements on the supplied tx before calling this function.
// ExecuteInTx will only retry statements that are performed within the supplied
// closure (fn). Any statements performed on the tx before ExecuteInTx is invoked will *not*
// be re-run if the transaction needs to be retried.
// borrowed from https://github.com/cockroachdb/cockroach-go/blob/master/crdb/tx.go
func ExecuteInTx(ctx context.Context, tx sqlbuilder.Tx, fn func() error) (err error) {

	defer func() {
		if err == nil {
			// Ignore commit errors. The tx has already been committed by RELEASE.
			_ = tx.Commit()
		} else {
			// We always need to execute a Rollback() so sql.DB releases the
			// connection.
			_ = tx.Rollback()
		}
	}()
	// Specify that we intend to retry this txn in case of CockroachDB retryable
	// errors.
	if _, err = tx.ExecContext(ctx, "SAVEPOINT cockroach_restart"); err != nil {
		return err
	}

	for {
		released := false
		err = fn()
		if err == nil {
			// RELEASE acts like COMMIT in CockroachDB. We use it since it gives us an
			// opportunity to react to retryable errors, whereas tx.Commit() doesn't.
			released = true
			if _, err = tx.ExecContext(ctx, "RELEASE SAVEPOINT cockroach_restart"); err == nil {
				return nil
			}
		}
		// We got an error; let's see if it's a retryable one and, if so, restart. We look
		// for either the standard PG errcode SerializationFailureError:40001 or the Cockroach extension
		// errcode RetriableError:CR000. The Cockroach extension has been removed server-side, but support
		// for it has been left here for now to maintain backwards compatibility.
		pqErr, ok := err.(*pq.Error)
		if retryable := ok && (pqErr.Code == "CR000" || pqErr.Code == "40001"); !retryable {
			if released {
				err = newAmbiguousCommitError(err)
			}
			return err
		}
		if _, retryErr := tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT cockroach_restart"); retryErr != nil {
			return newTxnRestartError(retryErr, err)
		}
	}
}
