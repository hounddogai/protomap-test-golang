// Package registry keeps the clients the monolith dialed at start-up, by the name of the service they reach, so
// code that learns which service to call at run time can find its client.
package registry

import (
	"fmt"
	"sort"
	"sync"
)

// Registry maps service names to clients.
type Registry struct {
	mu      sync.RWMutex
	clients map[string]any
}

// New returns an empty registry.
func New() *Registry { return &Registry{clients: map[string]any{}} }

// Put records the client of a service, replacing any earlier one.
func (r *Registry) Put(service string, client any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.clients[service] = client
}

// Get returns the client of a service, or nil.
func (r *Registry) Get(service string) any {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.clients[service]
}

// Names lists the services the registry has clients of.
func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.clients))
	for name := range r.clients {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// Lookup returns the client of a service as the type the caller expects.
func Lookup[T any](r *Registry, service string) (T, error) {
	client, ok := r.Get(service).(T)
	if !ok {
		var zero T
		return zero, fmt.Errorf("registry: no client of %s of type %T", service, zero)
	}
	return client, nil
}
