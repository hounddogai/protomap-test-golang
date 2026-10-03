package network

import (
	"context"
	"errors"
	"testing"
	"time"

	commonpb "example.com/protomap-fixtures/gen/common"
	identitypb "example.com/protomap-fixtures/gen/identity"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type blockingTokenServer struct {
	identitypb.UnimplementedAccessTokenValidationServer
	started chan struct{}
}

func (s *blockingTokenServer) ValidateToken(ctx context.Context, _ *commonpb.Request) (*commonpb.Response, error) {
	select {
	case s.started <- struct{}{}:
	default:
	}
	<-ctx.Done()
	return nil, status.FromContextError(ctx.Err()).Err()
}

func blockingCluster(t *testing.T) (*Cluster, <-chan struct{}) {
	t.Helper()
	started := make(chan struct{}, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cluster, err := Start(ctx, []string{"identity"}, func(_ string, registrar grpc.ServiceRegistrar, _ Pool) {
		identitypb.RegisterAccessTokenValidationServer(registrar, &blockingTokenServer{started: started})
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := cluster.Close(ctx); err != nil && !errors.Is(err, context.DeadlineExceeded) {
			t.Error(err)
		}
	})
	return cluster, started
}

func TestCanceledRPCStopsTheHandler(t *testing.T) {
	cluster, started := blockingCluster(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := identitypb.NewAccessTokenValidationClient(cluster.Pool["identity"]).ValidateToken(ctx, &commonpb.Request{})
		result <- err
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("handler never started")
	}
	cancel()
	if err := <-result; status.Code(err) != codes.Canceled {
		t.Fatalf("expected canceled RPC, got %v", err)
	}
}

func TestRPCRespectsTheClientDeadline(t *testing.T) {
	cluster, _ := blockingCluster(t)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	_, err := identitypb.NewAccessTokenValidationClient(cluster.Pool["identity"]).ValidateToken(ctx, &commonpb.Request{})
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}
}

func TestShutdownClosesAnActiveRPC(t *testing.T) {
	cluster, started := blockingCluster(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := identitypb.NewAccessTokenValidationClient(cluster.Pool["identity"]).ValidateToken(ctx, &commonpb.Request{})
		result <- err
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("handler never started")
	}
	shutdown, stop := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer stop()
	if err := cluster.Close(shutdown); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected bounded shutdown, got %v", err)
	}
	if err := <-result; err == nil {
		t.Fatal("active RPC succeeded after forced shutdown")
	}
}

func TestRequestsRejectInvalidHopBudgets(t *testing.T) {
	for _, hops := range []int32{-1, MaxHops + 1} {
		if err := ValidateRequest(context.Background(), &commonpb.Request{RemainingHops: hops}); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("hop budget %d was accepted: %v", hops, err)
		}
	}
	if err := ValidateRequest(context.Background(), nil); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("nil request was accepted: %v", err)
	}
}

func TestServerAddsADefaultDeadline(t *testing.T) {
	_, err := deadlineInterceptor(context.Background(), nil, nil, func(ctx context.Context, _ any) (any, error) {
		deadline, exists := ctx.Deadline()
		if !exists || time.Until(deadline) > DefaultDeadline {
			t.Fatal("server did not bound the deadline")
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
