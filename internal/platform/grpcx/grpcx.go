// Package grpcx has the helpers the domains call other services with: generic unary calls with deadlines, calls by
// method path, retries, and client factories.
package grpcx

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// DefaultTimeout bounds every call that does not set its own deadline.
const DefaultTimeout = 5 * time.Second

// Unary is the shape of every generated unary client method.
type Unary[Req, Resp proto.Message] func(context.Context, Req, ...grpc.CallOption) (Resp, error)

// Call runs a unary method with the default deadline.
func Call[Req, Resp proto.Message](ctx context.Context, method Unary[Req, Resp], req Req, opts ...grpc.CallOption) (Resp, error) {
	ctx, cancel := context.WithTimeout(ctx, DefaultTimeout)
	defer cancel()
	return method(ctx, req, opts...)
}

// Invoke calls a method by its full path, as the generic gateway and the replay tools do.
func Invoke(ctx context.Context, conn grpc.ClientConnInterface, method string, in, out proto.Message) error {
	ctx, cancel := context.WithTimeout(ctx, DefaultTimeout)
	defer cancel()
	if err := conn.Invoke(ctx, method, in, out); err != nil {
		return Wrap(err, method)
	}
	return nil
}

// NewClient makes a client of a connection with a generated constructor, so that call sites need not know which.
func NewClient[T any](conn grpc.ClientConnInterface, factory func(grpc.ClientConnInterface) T) T {
	return factory(conn)
}

// Retry calls fn until it succeeds, it fails with an error retrying cannot fix, or attempts run out.
func Retry[T any](ctx context.Context, attempts int, fn func(context.Context) (T, error)) (T, error) {
	var zero T
	var err error
	backoff := 20 * time.Millisecond
	for attempt := 0; attempt < attempts; attempt++ {
		var value T
		if value, err = fn(ctx); err == nil {
			return value, nil
		}
		if !Retryable(err) {
			return zero, err
		}
		select {
		case <-ctx.Done():
			return zero, errors.Join(err, ctx.Err())
		case <-time.After(backoff):
			backoff *= 2
		}
	}
	return zero, err
}

// Retryable reports whether an error is one a retry may fix.
func Retryable(err error) bool {
	switch status.Code(err) {
	case codes.Unavailable, codes.ResourceExhausted, codes.Aborted, codes.DeadlineExceeded:
		return true
	}
	return false
}

// Wrap names the method an error came from, keeping its status code.
func Wrap(err error, method string) error {
	if err == nil {
		return nil
	}
	if st, ok := status.FromError(err); ok {
		return status.Errorf(st.Code(), "%s: %s", method, st.Message())
	}
	return fmt.Errorf("%s: %w", method, err)
}
