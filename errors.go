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
	"errors"
	"fmt"
	"sort"
	"strings"
)

type Error interface {
	Details() string
	Wrap(err error) Error
	WrapWithDetails(err error, details map[string]interface{}) Error

	Error() string
	Unwrap() error
	Is(err error) bool
}

type dbError struct {
	parent *dbError

	details map[string]interface{}
	err     error
}

func newError(msg string) Error {
	return &dbError{
		err: errors.New(msg),
	}
}

func (e *dbError) Unwrap() error {
	if e.parent == nil {
		return nil
	}
	return e.parent
}

func (e *dbError) Is(err error) bool {
	return e.err == err
}

func (e *dbError) Wrap(err error) Error {
	return e.WrapWithDetails(err, nil)
}

func (e *dbError) WrapWithDetails(err error, details map[string]interface{}) Error {
	return &dbError{
		parent: e,

		err:     err,
		details: details,
	}
}

func (e *dbError) Error() string {
	return e.err.Error()
}

func (e *dbError) Details() string {
	details := map[string]interface{}{}
	keys := []string{}

	p := &e
	for (*p).parent != nil {
		for k := range (*p).details {
			if details[k] == nil {
				details[k] = (*p).details[k]
				keys = append(keys, k)
			}
		}
		p = &((*p).parent)
	}

	sort.Strings(keys)
	chunks := []string{}
	for _, k := range keys {
		chunks = append(chunks, fmt.Sprintf("%s: %v", k, details[k]))
	}

	return strings.Join(chunks, "\n")
}

// TODO: review severities, classes and messages.
// Error messages
var (
	// Error severities
	ErrGeneric  = newError(`error`)
	ErrCritical = newError(`critical`)
	ErrUser     = newError(`error`)
	ErrServer   = newError(`aborted`)

	// Error classes
	ErrNoSuchObject     = ErrUser.Wrap(newError(`no such object`))
	ErrWrongParameter   = ErrUser.Wrap(newError(`wrong parameter`))
	ErrEmptyResult      = ErrUser.Wrap(newError(`empty result`))
	ErrTransationFailed = ErrUser.Wrap(newError(`transaction failed`))

	// Error messages
	ErrAlreadyWithinTransaction = ErrTransationFailed.Wrap(newError(`already within a transaction`))
	ErrCollectionDoesNotExist   = ErrNoSuchObject.Wrap(newError(`collection does not exist`))
	ErrExpectingNonNilModel     = ErrWrongParameter.Wrap(newError(`expecting non nil model`))
	ErrExpectingPointerToStruct = ErrWrongParameter.Wrap(newError(`expecting pointer to struct`))
	ErrGivingUpTryingToConnect  = ErrServer.Wrap(newError(`giving up trying to connect: too many clients`))
	ErrInvalidCollection        = ErrWrongParameter.Wrap(newError(`invalid collection`))
	ErrMissingCollectionName    = ErrWrongParameter.Wrap(newError(`missing collection name`))
	ErrMissingConditions        = ErrWrongParameter.Wrap(newError(`missing selector conditions`))
	ErrMissingConnURL           = ErrWrongParameter.Wrap(newError(`missing DSN`))
	ErrMissingDatabaseName      = ErrWrongParameter.Wrap(newError(`missing database name`))
	ErrNoMoreRows               = ErrEmptyResult.Wrap(newError(`no more rows in this result set`))
	ErrNotConnected             = ErrUser.Wrap(newError(`not connected to a database`))
	ErrNotImplemented           = ErrCritical.Wrap(newError(`call not implemented`))
	ErrQueryIsPending           = ErrTransationFailed.Wrap(newError(`can't execute this instruction while the result set is still open`))
	ErrQueryLimitParam          = ErrWrongParameter.Wrap(newError(`a query can accept only one limit parameter`))
	ErrQueryOffsetParam         = ErrWrongParameter.Wrap(newError(`a query can accept only one offset parameter`))
	ErrQuerySortParam           = ErrWrongParameter.Wrap(newError(`a query can accept only one order-by parameter`))
	ErrSockerOrHost             = ErrUser.Wrap(newError(`you may connect either to a UNIX socket or a TCP address, but not both`))
	ErrTooManyClients           = ErrServer.Wrap(newError(`can't connect to database server: too many clients`))
	ErrUndefined                = ErrWrongParameter.Wrap(newError(`value is undefined`))
	ErrUnknownConditionType     = ErrWrongParameter.Wrap(newError(`arguments of type %T can't be used as constraints`))
	ErrUnsupported              = ErrServer.Wrap(newError(`action is not supported by the DBMS`))
	ErrUnsupportedDestination   = ErrWrongParameter.Wrap(newError(`unsupported destination type`))
	ErrUnsupportedType          = ErrWrongParameter.Wrap(newError(`type does not support marshaling`))
	ErrUnsupportedValue         = ErrWrongParameter.Wrap(newError(`value does not support unmarshaling`))
	ErrNilItem                  = ErrWrongParameter.Wrap(newError(`invalid item (nil)`))
	ErrZeroItemID               = ErrWrongParameter.Wrap(newError(`item ID is not defined`))
	ErrMissingPrimaryKeys       = ErrWrongParameter.Wrap(newError(`collection has no primary keys`))

	ErrWarnSlowQuery = ErrUser.Wrap(newError(`slow query`))
)
