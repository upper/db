// Copyright (c) 2014-2015 José Carlos Nieto, https://menteslibres.net/xiam
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
	"math/rand"
	"strconv"
	"sync"
	"time"
	"upper.io/db.v2/builder/cache/hashstructure"
)

const (
	maxCachedObjects    = 1024 * 8
	mapCleanDivisor     = 1000
	mapCleanProbability = 1
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

// Cache holds a map of volatile key -> values.
type Cache struct {
	cache map[string]interface{}
	mu    sync.RWMutex
}

// NewCache initializes a new caching space.
func NewCache() (c *Cache) {
	return &Cache{
		cache: make(map[string]interface{}),
	}
}

// Read attempts to retrieve a cached value from memory. If the value does not
// exists returns an empty string and false.
func (c *Cache) Read(ob Hashable) (string, bool) {
	c.mu.RLock()
	data, ok := c.cache[ob.Hash()]
	c.mu.RUnlock()

	if ok {
		if s, ok := data.(string); ok {
			return s, true
		}
	}

	return "", false
}

func (c *Cache) ReadRaw(ob Hashable) (interface{}, bool) {
	c.mu.RLock()
	data, ok := c.cache[ob.Hash()]
	c.mu.RUnlock()
	return data, ok
}

// Write stores a value in memory. If the value already exists its overwritten.
func (c *Cache) Write(ob Hashable, v interface{}) {

	if maxCachedObjects > 0 && maxCachedObjects < len(c.cache) {
		c.Clear()
	} else if rand.Intn(mapCleanDivisor) <= mapCleanProbability {
		c.Clear()
	}

	c.mu.Lock()
	c.cache[ob.Hash()] = v
	c.mu.Unlock()
}

// Clear generates a new memory space, leaving the old memory unreferenced, so
// it can be claimed by the garbage collector.
func (c *Cache) Clear() {
	c.mu.Lock()
	c.cache = make(map[string]interface{})
	c.mu.Unlock()
}

// Hash returns a hash of the given struct.
func Hash(v interface{}) string {
	q, err := hashstructure.Hash(v, nil)
	if err != nil {
		panic(fmt.Sprintf("Could not hash struct: ", err.Error()))
	}
	return strconv.FormatUint(q, 10)
}
