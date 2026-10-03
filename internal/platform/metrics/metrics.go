// Package metrics records how long calls take.
package metrics

import (
	"log/slog"
	"sync"
	"time"
)

// Recorder keeps a running count and total duration per name.
type Recorder struct {
	mu     sync.Mutex
	counts map[string]int
	totals map[string]time.Duration
	log    *slog.Logger
}

// NewRecorder returns an empty recorder.
func NewRecorder(log *slog.Logger) *Recorder {
	return &Recorder{counts: map[string]int{}, totals: map[string]time.Duration{}, log: log}
}

// Observe records one call of name that started at start.
func (r *Recorder) Observe(name string, start time.Time) {
	elapsed := time.Since(start)
	r.mu.Lock()
	r.counts[name]++
	r.totals[name] += elapsed
	r.mu.Unlock()
	if elapsed > time.Second {
		r.log.Warn("slow call", "name", name, "elapsed", elapsed)
	}
}
