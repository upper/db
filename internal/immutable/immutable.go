package immutable

// Immutable represents immutable chains
type Immutable interface {
	Prev() Immutable
	Fn(interface{}) error
	Base() interface{}
}

// FastForward applies all Fn methods in order on the given new Base.
func FastForward(curr Immutable) (interface{}, error) {
	prev := curr.Prev()
	if prev == nil {
		return curr.Base(), nil
	}
	in, err := FastForward(prev)
	if err != nil {
		return nil, err
	}
	err = curr.Fn(in)
	return in, err
}
