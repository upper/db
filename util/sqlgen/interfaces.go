package sqlgen

import (
	"upper.io/cache"
)

type Fragment interface {
	cache.Hashable
	compilable
}

type compilable interface {
	Compile(*Template) string
}
