package grpcx

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

// LegacyInvoke calls a method by its path with the package-level function the first services were written with.
// New code uses Invoke.
//
// Deprecated: use Invoke.
func LegacyInvoke(ctx context.Context, conn *grpc.ClientConn, method string, in, out proto.Message) error {
	//nolint:staticcheck // Kept until the billing batch jobs move off it.
	return grpc.Invoke(ctx, method, in, out, conn)
}
