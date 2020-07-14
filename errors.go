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
)

// Error messages
var (
	ErrAlreadyWithinTransaction = errors.New(`already within a transaction`)
	ErrCollectionDoesNotExist   = errors.New(`collection does not exist`)
	ErrExpectingNonNilModel     = errors.New(`expecting non nil model`)
	ErrExpectingPointerToStruct = errors.New(`expecting pointer to struct`)
	ErrGivingUpTryingToConnect  = errors.New(`giving up trying to connect: too many clients`)
	ErrInvalidCollection        = errors.New(`invalid collection`)
	ErrMissingCollectionName    = errors.New(`missing collection name`)
	ErrMissingConditions        = errors.New(`missing selector conditions`)
	ErrMissingConnURL           = errors.New(`missing DSN`)
	ErrMissingDatabaseName      = errors.New(`missing database name`)
	ErrNoMoreRows               = errors.New(`no more rows in this result set`)
	ErrNotConnected             = errors.New(`not connected to a database`)
	ErrNotImplemented           = errors.New(`call not implemented`)
	ErrQueryIsPending           = errors.New(`can't execute this instruction while the result set is still open`)
	ErrQueryLimitParam          = errors.New(`a query can accept only one limit parameter`)
	ErrQueryOffsetParam         = errors.New(`a query can accept only one offset parameter`)
	ErrQuerySortParam           = errors.New(`a query can accept only one order-by parameter`)
	ErrSockerOrHost             = errors.New(`you may connect either to a UNIX socket or a TCP address, but not both`)
	ErrTooManyClients           = errors.New(`can't connect to database server: too many clients`)
	ErrUndefined                = errors.New(`value is undefined`)
	ErrUnknownConditionType     = errors.New(`arguments of type %T can't be used as constraints`)
	ErrUnsupported              = errors.New(`action is not supported by the DBMS`)
	ErrUnsupportedDestination   = errors.New(`unsupported destination type`)
	ErrUnsupportedType          = errors.New(`type does not support marshaling`)
	ErrUnsupportedValue         = errors.New(`value does not support unmarshaling`)
	ErrNilItem                  = errors.New(`invalid item (nil)`)
	ErrZeroItemID               = errors.New(`item ID is not defined`)
	ErrMissingPrimaryKeys       = errors.New(`collection has no primary keys`)
	ErrWarnSlowQuery            = errors.New(`slow query`)
)
