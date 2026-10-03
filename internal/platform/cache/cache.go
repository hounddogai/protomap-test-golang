// Package cache is a small bounded cache for the records handlers look up again within a request or a few.
package cache

import "sync"

// Cache keeps at most a fixed number of values, forgetting the oldest first.
type Cache[K comparable, V any] struct {
	mu    sync.Mutex
	limit int
	order []K
	items map[K]V
}

// New returns a cache that keeps at most limit values.
func New[K comparable, V any](limit int) *Cache[K, V] {
	return &Cache[K, V]{limit: limit, items: make(map[K]V, limit)}
}

// Put keeps a value under a key.
func (c *Cache[K, V]) Put(key K, value V) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.items[key]; !ok {
		c.order = append(c.order, key)
		if len(c.order) > c.limit {
			delete(c.items, c.order[0])
			c.order = c.order[1:]
		}
	}
	c.items[key] = value
}

// Get returns the value kept under a key.
func (c *Cache[K, V]) Get(key K) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	value, ok := c.items[key]
	return value, ok
}
