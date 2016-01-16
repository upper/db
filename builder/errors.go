package builder

import (
	"errors"
)

// Common error messages.
var (
	ErrNoMoreRows              = errors.New(`There are no more rows in this result set.`)
	ErrExpectingPointer        = errors.New(`Argument must be an address.`)
	ErrExpectingSlicePointer   = errors.New(`Argument must be a slice address.`)
	ErrExpectingSliceMapStruct = errors.New(`Argument must be a slice address of maps or structs.`)
	ErrExpectingMapOrStruct    = errors.New(`Argument must be either a map or a struct.`)
)
