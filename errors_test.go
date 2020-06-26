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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInterface(t *testing.T) {
	var _ Error = &dbError{}
}

func TestWrap(t *testing.T) {
	adapterFakeErr := errors.New("adapter: fake error")

	wrappedFakeErr := ErrCollectionDoesNotExist.WrapWithDetails(adapterFakeErr, map[string]interface{}{
		"foo":        "bar",
		"collection": "users",
	})

	assert.Equal(t, adapterFakeErr.Error(), wrappedFakeErr.Error())

	assert.True(t, errors.Is(wrappedFakeErr, wrappedFakeErr))
	assert.True(t, errors.Is(wrappedFakeErr, adapterFakeErr))
	assert.True(t, errors.Is(wrappedFakeErr, ErrCollectionDoesNotExist))
	assert.True(t, errors.Is(wrappedFakeErr, ErrNoSuchObject))
	assert.True(t, errors.Is(wrappedFakeErr, ErrUser))

	errPrimaryKeys := wrappedFakeErr.WrapWithDetails(wrappedFakeErr, map[string]interface{}{
		"id":  "id",
		"foo": "baz",
	})

	assert.True(t, errors.Is(errPrimaryKeys, errPrimaryKeys))
	assert.True(t, errors.Is(errPrimaryKeys, wrappedFakeErr))
	assert.True(t, errors.Is(errPrimaryKeys, adapterFakeErr))
	assert.True(t, errors.Is(errPrimaryKeys, ErrCollectionDoesNotExist))
	assert.True(t, errors.Is(errPrimaryKeys, ErrNoSuchObject))
	assert.True(t, errors.Is(errPrimaryKeys, ErrUser))

	assert.Equal(t, "collection: users\nfoo: baz\nid: id", errPrimaryKeys.Details())

	assert.Equal(t, adapterFakeErr.Error(), errPrimaryKeys.Error())
}
