// Package eventbus is the in-process bus the domains publish their events on. The workers the monolith starts
// subscribe to the topics they handle.
package eventbus

import (
	"context"
	"sync"
)

// Event is something that happened in a domain.
type Event struct {
	Topic      string
	Key        string
	Attributes map[string]string
	Payload    []byte
}

// Bus delivers events to the subscribers of their topics.
type Bus struct {
	mu          sync.RWMutex
	subscribers map[string][]chan Event
}

// New returns a bus with no subscribers.
func New() *Bus { return &Bus{subscribers: map[string][]chan Event{}} }

// Subscribe returns the events published on a topic from now on.
func (b *Bus) Subscribe(topic string) <-chan Event {
	ch := make(chan Event, 256)
	b.mu.Lock()
	b.subscribers[topic] = append(b.subscribers[topic], ch)
	b.mu.Unlock()
	return ch
}

// Publish hands an event to every subscriber of its topic, or gives up once the context ends.
func (b *Bus) Publish(ctx context.Context, event Event) error {
	b.mu.RLock()
	subscribers := b.subscribers[event.Topic]
	b.mu.RUnlock()
	for _, ch := range subscribers {
		select {
		case ch <- event:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}
