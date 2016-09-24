package sqlgen

import (
	"upper.io/db/internal/cache"
)

type cc interface {
	cache.Cacheable
	compilable
}

type compilable interface {
	Compile(*Template) string
}
