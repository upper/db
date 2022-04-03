package exql

import (
	"fmt"

	"github.com/segmentio/fasthash/fnv1a"
	"github.com/upper/db/v4/internal/cache"
)

const (
	hashVariantInt uint64 = 1 << iota
	hashVariantSignedInt
	hashVariantBool
	hashVariantString
	hashVariantRaw
	hashVariantHashable
	hashVariantNil
)

func initHash(t FragmentType) uint64 {
	return fnv1a.AddUint64(fnv1a.Init64, uint64(t))
}

func addToHash(h uint64, value interface{}) uint64 {
	switch t := value.(type) {
	case int:
		if t >= 0 {
			h = fnv1a.AddUint64(h, hashVariantInt)
			h = fnv1a.AddUint64(h, uint64(t))
		} else {
			h = fnv1a.AddUint64(h, hashVariantSignedInt)
			h = fnv1a.AddUint64(h, uint64(-t))
		}
	case int8:
		if t >= 0 {
			h = fnv1a.AddUint64(h, hashVariantInt)
			h = fnv1a.AddUint64(h, uint64(t))
		} else {
			h = fnv1a.AddUint64(h, hashVariantSignedInt)
			h = fnv1a.AddUint64(h, uint64(-t))
		}
	case int16:
		if t >= 0 {
			h = fnv1a.AddUint64(h, hashVariantInt)
			h = fnv1a.AddUint64(h, uint64(t))
		} else {
			h = fnv1a.AddUint64(h, hashVariantSignedInt)
			h = fnv1a.AddUint64(h, uint64(-t))
		}
	case int32:
		if t >= 0 {
			h = fnv1a.AddUint64(h, hashVariantInt)
			h = fnv1a.AddUint64(h, uint64(t))
		} else {
			h = fnv1a.AddUint64(h, hashVariantSignedInt)
			h = fnv1a.AddUint64(h, uint64(-t))
		}
	case int64:
		if t >= 0 {
			h = fnv1a.AddUint64(h, hashVariantInt)
			h = fnv1a.AddUint64(h, uint64(t))
		} else {
			h = fnv1a.AddUint64(h, hashVariantSignedInt)
			h = fnv1a.AddUint64(h, uint64(-t))
		}
	case bool:
		h = fnv1a.AddUint64(h, hashVariantBool)
		if t {
			h = fnv1a.AddUint64(h, 1)
		} else {
			h = fnv1a.AddUint64(h, 2)
		}
	case string:
		h = fnv1a.AddUint64(h, hashVariantString)
		h = fnv1a.AddString64(h, t)
	case uint8:
		h = fnv1a.AddUint64(h, hashVariantInt)
		h = fnv1a.AddUint64(h, uint64(t))
	case uint16:
		h = fnv1a.AddUint64(h, hashVariantInt)
		h = fnv1a.AddUint64(h, uint64(t))
	case uint32:
		h = fnv1a.AddUint64(h, hashVariantInt)
		h = fnv1a.AddUint64(h, uint64(t))
	case uint64:
		h = fnv1a.AddUint64(h, hashVariantInt)
		h = fnv1a.AddUint64(h, t)
	case *Raw:
		h = fnv1a.AddUint64(h, hashVariantRaw)
		h = fnv1a.AddString64(h, value.(*Raw).String())
	case cache.Hashable:
		h = fnv1a.AddUint64(h, hashVariantHashable)
		h = fnv1a.AddUint64(h, value.(cache.Hashable).Hash())
	case nil:
		h = fnv1a.AddUint64(h, hashVariantNil)
		h = fnv1a.AddUint64(h, uint64(FragmentType_Nil))
	default:
		panic(fmt.Sprintf("hash: unexpected type %T", value))
	}
	return h
}

func quickHash(t FragmentType, values ...interface{}) uint64 {
	h := initHash(t)
	for i := range values {
		h = addToHash(h, values[i])
	}
	return uint64(h)
}
