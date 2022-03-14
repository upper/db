package exql

import (
	"fmt"

	"github.com/segmentio/fasthash/fnv1a"
	"github.com/upper/db/v4/internal/cache"
)

func initHash(t FragmentType) uint64 {
	h := fnv1a.Init64
	return fnv1a.AddUint64(h, uint64(t))
}

func addToHash(h uint64, value interface{}) uint64 {
	switch value.(type) {
	case string:
		h = fnv1a.AddString64(h, value.(string))
	case int:
		h = fnv1a.AddUint64(h, uint64(value.(int)))
	case bool:
		if value.(bool) {
			h = fnv1a.AddUint64(h, 1)
		} else {
			h = fnv1a.AddUint64(h, 2)
		}
	case uint8:
		h = fnv1a.AddUint64(h, uint64(value.(uint8)))
	case uint32:
		h = fnv1a.AddUint64(h, uint64(value.(uint32)))
	case uint64:
		h = fnv1a.AddUint64(h, value.(uint64))
	case *Raw:
		h = fnv1a.AddString64(h, value.(*Raw).String())
	case cache.Hashable:
		h = fnv1a.AddUint64(h, value.(cache.Hashable).Hash())
	case nil:
		h = fnv1a.AddUint64(h, uint64(FragmentType_Nil))
	default:
		panic(fmt.Sprintf("hash: unexpected type %T", value))
	}
	return h
}

func quickHash(t FragmentType, values ...interface{}) uint64 {
	h := fnv1a.Init64
	h = fnv1a.AddUint64(h, uint64(t))
	for i := range values {
		h = addToHash(h, values[i])
	}
	return uint64(h)
}
