package sqlbuilder

import (
	"errors"
)

// Error messages.
var (
	ErrExpectingPointerToEitherMapOrStruct = errors.New(`Expecting a pointer to either a map or a struct.`)
)
