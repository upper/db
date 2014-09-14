package sqlgen

import (
	"upper.io/cache"
)

type cc interface {
	cache.Cacheable
	compilable
}

type compilable interface {
	Compile(*Template) string
}
