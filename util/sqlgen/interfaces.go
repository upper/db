package sqlgen

import (
	"upper.io/cache"
)

type cc interface {
	cache.Hashable
	compilable
}

type compilable interface {
	Compile(*Template) string
}
