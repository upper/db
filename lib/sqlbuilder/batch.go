package sqlbuilder

type BatchInserter struct {
	inserter *inserter
	size     int
	values   chan []interface{}
	err      error
}

func newBatchInserter(inserter *inserter, size int) *BatchInserter {
	if size < 1 {
		size = 1
	}
	b := &BatchInserter{
		inserter: inserter,
		size:     size,
		values:   make(chan []interface{}, size),
	}
	return b
}

// Values pushes column values to be inserted as part of the batch.
func (b *BatchInserter) Values(values ...interface{}) *BatchInserter {
	b.values <- values
	return b
}

func (b *BatchInserter) NextResult(dst interface{}) bool {
	clone := b.inserter.clone()
	i := 0
	for values := range b.values {
		i++
		clone.Values(values...)
		if i == b.size {
			break
		}
	}
	if i == 0 {
		return false
	}
	b.err = clone.Iterator().All(dst)
	return (b.err == nil)
}

func (b *BatchInserter) Done() {
	close(b.values)
}

func (b *BatchInserter) Wait() error {
	var nop []struct{}
	for b.NextResult(&nop) {
	}
	return b.err
}

func (b *BatchInserter) Error() error {
	return b.err
}
