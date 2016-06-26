package sqlgen

import (
	"upper.io/db/internal/cache"
)

type Fragment interface {
	cache.Hashable
	compilable
}

type compilable interface {
	Compile(*Template) string
}
