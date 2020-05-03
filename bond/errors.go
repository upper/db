package bond

import (
	"errors"
)

// Public errors
var (
	ErrExpectingPointerToStruct = errors.New(`Expecting pointer to struct`)
	ErrExpectingNonNilModel     = errors.New(`Expecting non nil model`)
	ErrInvalidCollection        = errors.New(`Invalid collection`)
)
