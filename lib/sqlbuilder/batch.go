package sqlbuilder

type BatchInserter struct {
	inserter *inserter
	size     int
	Values   chan interface{}
	err      error
}

func newBatchInserter(inserter *inserter, size int) *BatchInserter {
	if size < 1 {
		size = 1
	}
	b := &BatchInserter{
		inserter: inserter,
		size:     size,
		Values:   make(chan interface{}, size),
	}
	return b
}

func (b *BatchInserter) Next(dst interface{}) bool {
	clone := b.inserter.clone()
	i := 0
	for value := range b.Values {
		i++
		clone.Values(value)
		if b.size == i {
			break
		}
	}
	if i == 0 {
		return false
	}
	b.err = clone.Iterator().All(dst)
	return (b.err == nil)
}

func (b *BatchInserter) Exec() error {
	var nop []struct{}
	for b.Next(&nop) {
	}
	return b.err
}

func (b *BatchInserter) Error() error {
	return b.err
}
