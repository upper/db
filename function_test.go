package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCustomFunctions(t *testing.T) {
	t.Run("Nil arguments", func(t *testing.T) {
		fn := Func("HELLO")
		assert.Equal(t, "HELLO", fn.Name())
		assert.Equal(t, []interface{}(nil), fn.Arguments())
	})

	t.Run("Single argument", func(t *testing.T) {
		fn := Func("CONCAT", "a")
		assert.Equal(t, "CONCAT", fn.Name())
		assert.Equal(t, []interface{}{"a"}, fn.Arguments())
	})

	t.Run("Two arguments", func(t *testing.T) {
		fn := Func("MOD", 29, 9)
		assert.Equal(t, "MOD", fn.Name())
		assert.Equal(t, []interface{}{29, 9}, fn.Arguments())
	})

	t.Run("Multiple arguments", func(t *testing.T) {
		fn := Func("CONCAT", "a", "b", "c")
		assert.Equal(t, "CONCAT", fn.Name())
		assert.Equal(t, []interface{}{"a", "b", "c"}, fn.Arguments())
	})

	t.Run("Slice argument", func(t *testing.T) {
		fn := Func("IN", []interface{}{"a", "b", "c"})
		assert.Equal(t, "IN", fn.Name())
		assert.Equal(t, []interface{}{[]interface{}{"a", "b", "c"}}, fn.Arguments())
	})

	t.Run("Slice argument with one element", func(t *testing.T) {
		fn := Func("IN", []interface{}{"a"})
		assert.Equal(t, "IN", fn.Name())
		assert.Equal(t, []interface{}{[]interface{}{"a"}}, fn.Arguments())
	})

	t.Run("Nil slice argument", func(t *testing.T) {
		fn := Func("IN", []interface{}(nil))
		assert.Equal(t, "IN", fn.Name())
		assert.Equal(t, []interface{}{[]interface{}(nil)}, fn.Arguments())
	})
}
