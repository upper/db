package sqlbuilder

import (
	"sync"
)

type BatchInserter struct {
	inserter *inserter
	size     int
	values   [][]interface{}
	next     chan Inserter
	mu       sync.Mutex
}

func newBatchInserter(inserter *inserter, size int) *BatchInserter {
	if size < 1 {
		size = 1
	}
	b := &BatchInserter{
		inserter: inserter,
		size:     size,
		next:     make(chan Inserter),
	}
	b.reset()
	return b
}

func (b *BatchInserter) reset() {
	b.values = make([][]interface{}, 0, b.size)
}

func (b *BatchInserter) flush() {
	if len(b.values) > 0 {
		clone := b.inserter.clone()
		for i := range b.values {
			clone.Values(b.values[i]...)
		}
		b.next <- clone
		b.reset()
	}
}

// Values pushes a value to be inserted as part of the batch.
func (b *BatchInserter) Values(values ...interface{}) *BatchInserter {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.values = append(b.values, values)
	if len(b.values) >= b.size {
		b.flush()
	}
	return b
}

// Next returns a channel that receives new q elements.
func (b *BatchInserter) Next() chan Inserter {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.next
}

func (b *BatchInserter) Done() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.flush()
	close(b.next)
}
