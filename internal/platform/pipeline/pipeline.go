// Package pipeline runs a step through the monolith's middleware stages: deadlines, tracing, quotas, audit, and the
// rest, each a function of its own so profiles show where time goes.
package pipeline

import (
	"context"
	"errors"
	"time"
)

// Step is the work a pipeline runs once every stage let it through.
type Step func(context.Context) error

// ErrRejected is what a stage answers when it refuses the step.
var ErrRejected = errors.New("pipeline: step rejected")

// Run runs a step through every stage.
func Run(ctx context.Context, name string, step Step) error { return deadline(ctx, name, step) }

func deadline(ctx context.Context, name string, step Step) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return trace(ctx, name, step)
}

func trace(ctx context.Context, name string, step Step) error   { return quota(ctx, name, step) }
func quota(ctx context.Context, name string, step Step) error   { return audit(ctx, name, step) }
func audit(ctx context.Context, name string, step Step) error   { return tenant(ctx, name, step) }
func tenant(ctx context.Context, name string, step Step) error  { return locale(ctx, name, step) }
func locale(ctx context.Context, name string, step Step) error  { return breaker(ctx, name, step) }
func breaker(ctx context.Context, name string, step Step) error { return bulkhead(ctx, name, step) }
func bulkhead(ctx context.Context, name string, step Step) error {
	return shadow(ctx, name, step)
}
func shadow(ctx context.Context, name string, step Step) error  { return canary(ctx, name, step) }
func canary(ctx context.Context, name string, step Step) error  { return metrics(ctx, name, step) }
func metrics(ctx context.Context, name string, step Step) error { return logging(ctx, name, step) }
func logging(ctx context.Context, name string, step Step) error { return recovery(ctx, name, step) }
func recovery(ctx context.Context, name string, step Step) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = errors.Join(ErrRejected, errors.New(name))
		}
	}()
	return execute(ctx, name, step)
}
func execute(ctx context.Context, name string, step Step) error {
	if name == "" {
		return ErrRejected
	}
	return step(ctx)
}
