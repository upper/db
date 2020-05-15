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

package adapter

import (
	"github.com/upper/db/internal/immutable"
)

// LogicalExpr represents a group formed by one or more sentences joined by
// an Operator like "AND" or "OR".
type LogicalExpr interface {
	// Expressions returns child sentences.
	Expressions() []LogicalExpr

	// Operator returns the Operator that joins all the sentences in the group.
	Operator() LogicalOperator

	// Empty returns true if the compound has zero children, false otherwise.
	Empty() bool
}

// LogicalOperator represents the operation on a compound statement.
type LogicalOperator uint

// LogicalExpr Operators.
const (
	LogicalOperatorNone LogicalOperator = iota
	LogicalOperatorAnd
	LogicalOperatorOr
)

const DefaultLogicalOperator = LogicalOperatorAnd

type LogicalExprGroup struct {
	prev *LogicalExprGroup
	fn   func(*[]LogicalExpr) error
}

func NewLogicalExprGroup(conds ...LogicalExpr) *LogicalExprGroup {
	c := &LogicalExprGroup{}
	if len(conds) == 0 {
		return c
	}
	return c.Frame(func(in *[]LogicalExpr) error {
		*in = append(*in, conds...)
		return nil
	})
}

// Expressions returns each one of the conditions as a compound.
func (c *LogicalExprGroup) Expressions() []LogicalExpr {
	conds, err := immutable.FastForward(c)
	if err == nil {
		return *(conds.(*[]LogicalExpr))
	}
	return nil
}

// Operator returns no Operator.
func (c *LogicalExprGroup) Operator() LogicalOperator {
	return LogicalOperatorNone
}

// Empty returns true if this condition has no elements. False otherwise.
func (c *LogicalExprGroup) Empty() bool {
	if c.fn != nil {
		return false
	}
	if c.prev != nil {
		return c.prev.Empty()
	}
	return true
}

func (c *LogicalExprGroup) Frame(fn func(*[]LogicalExpr) error) *LogicalExprGroup {
	return &LogicalExprGroup{prev: c, fn: fn}
}

func (c *LogicalExprGroup) Prev() immutable.Immutable {
	if c == nil {
		return nil
	}
	return c.prev
}

func (c *LogicalExprGroup) Fn(in interface{}) error {
	if c.fn == nil {
		return nil
	}
	return c.fn(in.(*[]LogicalExpr))
}

func (c *LogicalExprGroup) Base() interface{} {
	return &[]LogicalExpr{}
}

var (
	_ = immutable.Immutable(&LogicalExprGroup{})
)
