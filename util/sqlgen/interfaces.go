package sqlgen

import (
	"github.com/upper/db/util/cache"
)

type cc interface {
	cache.Cacheable
	compilable
}

type compilable interface {
	Compile(*Template) string
}
