// Copyright (c) 2014-present José Carlos Nieto, https://menteslibres.net/xiam
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

package cache

import (
	"fmt"
	"hash/fnv"
	"testing"

	"github.com/stretchr/testify/assert"
)

type cacheableT struct {
	Name string
}

func (ct *cacheableT) Hash() uint64 {
	s := fnv.New64()
	s.Sum([]byte(ct.Name))
	return s.Sum64()
}

func TestCache(t *testing.T) {
	var c *Cache

	var (
		key   = cacheableT{"foo"}
		value = "bar"
	)

	t.Run("New", func(t *testing.T) {
		c = NewCache()
		assert.NotNil(t, c)
	})

	t.Run("ReadNonExistentValue", func(t *testing.T) {
		_, ok := c.Read(&key)
		assert.False(t, ok)
	})

	t.Run("Write", func(t *testing.T) {
		c.Write(&key, value)
		c.Write(&key, value)
	})

	t.Run("ReadExistentValue", func(t *testing.T) {
		v, ok := c.Read(&key)
		assert.True(t, ok)
		assert.Equal(t, value, v)
	})
}

func BenchmarkNewCache(b *testing.B) {
	for i := 0; i < b.N; i++ {
		NewCache()
	}
}

func BenchmarkNewCacheAndClear(b *testing.B) {
	for i := 0; i < b.N; i++ {
		c := NewCache()
		c.Clear()
	}
}

func BenchmarkReadNonExistentValue(b *testing.B) {
	key := cacheableT{"foo"}

	z := NewCache()
	for i := 0; i < b.N; i++ {
		z.Read(&key)
	}
}

func BenchmarkWriteSameValue(b *testing.B) {
	key := cacheableT{"foo"}
	value := "bar"

	z := NewCache()
	for i := 0; i < b.N; i++ {
		z.Write(&key, value)
	}
}

func BenchmarkWriteNewValue(b *testing.B) {
	value := "bar"

	z := NewCache()
	for i := 0; i < b.N; i++ {
		key := cacheableT{fmt.Sprintf("item-%d", i)}
		z.Write(&key, value)
	}
}

func BenchmarkReadExistentValue(b *testing.B) {
	key := cacheableT{"foo"}
	value := "bar"

	z := NewCache()
	z.Write(&key, value)
	for i := 0; i < b.N; i++ {
		z.Read(&key)
	}
}
