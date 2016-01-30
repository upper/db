package sqlgen

import (
	"upper.io/db.v1/util/cache"
)

type cc interface {
	cache.Cacheable
	compilable
}

type compilable interface {
	Compile(*Template) string
}
