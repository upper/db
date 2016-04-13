package sqlgen

import (
	"upper.io/db/builder/cache"
)

type Fragment interface {
	cache.Hashable
	compilable
}

type compilable interface {
	Compile(*Template) string
}
