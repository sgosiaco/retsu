package pool

import "sync"

// Pool is a generic wrapper around sync.Pool
type Pool[T any] struct {
	pool sync.Pool
}

// New creates a new Pool with the provided new function
func New[T any](newFunc func() T) Pool[T] {
	return Pool[T]{
		pool: sync.Pool{New: func() interface{} { return newFunc() }},
	}
}

// Get is a generic wrapper around sync.Pool's Get method
func (p *Pool[T]) Get() T {
	return p.pool.Get().(T)
}

// Put is a generic wrapper around sync.Pool's Put method
func (p *Pool[T]) Put(x T) {
	p.pool.Put(x)
}
