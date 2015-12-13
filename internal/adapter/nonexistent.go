// Copyright (c) 2012-2015 The upper.io/db authors. All rights reserved.
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

package adapter

import (
	"fmt"

	"upper.io/db.v2"
)

var (
	_ = db.Result(&NonExistentResult{})
	_ = db.Collection(&NonExistentCollection{})
)

func err(e error) error {
	if e == nil {
		return db.ErrCollectionDoesNotExist
	}
	return e
}

// NonExistentCollection represents a collection that does not exist.
type NonExistentCollection struct {
	Err error
}

// NonExistentResult represents a result set that was based on a non existent
// collection and therefore does not exist.
type NonExistentResult struct {
	Err error
}

// Append returns error.
func (c *NonExistentCollection) Append(interface{}) (interface{}, error) {
	return nil, err(c.Err)
}

// Exists returns false.
func (c *NonExistentCollection) Exists() bool {
	return false
}

// Find returns a NonExistentResult.
func (c *NonExistentCollection) Find(...interface{}) db.Result {
	if c.Err != nil {
		return &NonExistentResult{Err: c.Err}
	}
	return &NonExistentResult{Err: fmt.Errorf("Collection reported an error: %q", err(c.Err))}
}

// Truncate returns error.
func (c *NonExistentCollection) Truncate() error {
	return err(c.Err)
}

// Name returns an empty string.
func (c *NonExistentCollection) Name() string {
	return ""
}

// Limit returns a NonExistentResult.
func (r *NonExistentResult) Limit(uint) db.Result {
	return r
}

// Skip returns a NonExistentResult.
func (r *NonExistentResult) Skip(uint) db.Result {
	return r
}

// Sort returns a NonExistentResult.
func (r *NonExistentResult) Sort(...interface{}) db.Result {
	return r
}

// Select returns a NonExistentResult.
func (r *NonExistentResult) Select(...interface{}) db.Result {
	return r
}

// Where returns a NonExistentResult.
func (r *NonExistentResult) Where(...interface{}) db.Result {
	return r
}

// On returns a NonExistentResult.
func (r *NonExistentResult) On(...interface{}) db.Result {
	return r
}

// Join returns a NonExistentResult.
func (r *NonExistentResult) Join(...interface{}) db.Result {
	return r
}

// LeftJoin returns a NonExistentResult.
func (r *NonExistentResult) LeftJoin(...interface{}) db.Result {
	return r
}

// RightJoin returns a NonExistentResult.
func (r *NonExistentResult) RightJoin(...interface{}) db.Result {
	return r
}

// FullJoin returns a NonExistentResult.
func (r *NonExistentResult) FullJoin(...interface{}) db.Result {
	return r
}

// Using returns a NonExistentResult.
func (r *NonExistentResult) Using(...interface{}) db.Result {
	return r
}

// CrossJoin returns a NonExistentResult.
func (r *NonExistentResult) CrossJoin(...interface{}) db.Result {
	return r
}

// Group returns a NonExistentResult.
func (r *NonExistentResult) Group(...interface{}) db.Result {
	return r
}

// Remove returns error.
func (r *NonExistentResult) Remove() error {
	return err(r.Err)
}

// Update returns error.
func (r *NonExistentResult) Update(interface{}) error {
	return err(r.Err)
}

// Count returns 0 and error.
func (r *NonExistentResult) Count() (uint64, error) {
	return 0, err(r.Err)
}

// Next returns error.
func (r *NonExistentResult) Next(interface{}) error {
	return err(r.Err)
}

// One returns error.
func (r *NonExistentResult) One(interface{}) error {
	return err(r.Err)
}

// All returns error.
func (r *NonExistentResult) All(interface{}) error {
	return err(r.Err)
}

// Close returns error.
func (r *NonExistentResult) Close() error {
	return err(r.Err)
}
