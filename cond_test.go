package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCond(t *testing.T) {
	t.Run("Base", func(t *testing.T) {
		var c Cond

		c = Cond{}
		assert.True(t, c.Empty())

		c = Cond{"id": 1}
		assert.False(t, c.Empty())
	})

	t.Run("And", func(t *testing.T) {
		var a *AndExpr

		a = And()
		assert.True(t, a.Empty())

		_ = a.And(Cond{"id": 1})
		assert.True(t, a.Empty(), "conditions are immutable")

		a = a.And(Cond{"name": "Ana"})
		assert.False(t, a.Empty())

		a = a.And().And()
		assert.False(t, a.Empty())
	})

	t.Run("Or", func(t *testing.T) {
		var a *OrExpr

		a = Or()
		assert.True(t, a.Empty())

		_ = a.Or(Cond{"id": 1})
		assert.True(t, a.Empty(), "conditions are immutable")

		a = a.Or(Cond{"name": "Ana"})
		assert.False(t, a.Empty())

		a = a.Or().Or()
		assert.False(t, a.Empty())
	})
}
