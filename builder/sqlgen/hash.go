package sqlgen

import (
	"reflect"
	"sync/atomic"

	"upper.io/db.v2/builder/cache"
)

type Hasher interface {
	Hash() string
}

type MemHash struct {
	v atomic.Value
}

func (h *MemHash) Hash(i interface{}) string {
	v := h.v.Load()
	if r, ok := v.(string); ok && r != "" {
		return r
	}
	s := reflect.TypeOf(i).String() + "." + cache.Hash(i)
	h.v.Store(s)
	return s
}

func (h *MemHash) Reset() {
	h.v.Store("")
}
