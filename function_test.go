package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFunction(t *testing.T) {
	{
		fn := Func("MOD", 29, 9)
		assert.Equal(t, "MOD", fn.Name())
		assert.Equal(t, []interface{}{29, 9}, fn.Arguments())
	}

	{
		fn := Func("HELLO")
		assert.Equal(t, "HELLO", fn.Name())
		assert.Equal(t, []interface{}(nil), fn.Arguments())
	}

	{
		fn := Func("CONCAT", "a")
		assert.Equal(t, "CONCAT", fn.Name())
		assert.Equal(t, []interface{}{"a"}, fn.Arguments())
	}

	{
		fn := Func("CONCAT", "a", "b", "c")
		assert.Equal(t, "CONCAT", fn.Name())
		assert.Equal(t, []interface{}{"a", "b", "c"}, fn.Arguments())
	}

	{
		fn := Func("IN", []interface{}{"a", "b", "c"})
		assert.Equal(t, "IN", fn.Name())
		assert.Equal(t, []interface{}{[]interface{}{"a", "b", "c"}}, fn.Arguments())
	}

	{
		fn := Func("IN", []interface{}{"a"})
		assert.Equal(t, "IN", fn.Name())
		assert.Equal(t, []interface{}{[]interface{}{"a"}}, fn.Arguments())
	}

	{
		fn := Func("IN", []interface{}(nil))
		assert.Equal(t, "IN", fn.Name())
		assert.Equal(t, []interface{}{[]interface{}(nil)}, fn.Arguments())
	}
}
