package interceptors

import (
	"context"
	"net"
	"sync/atomic"
	"testing"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// identity answers session checks as the identity service does, which needs the tenant in the request
// and its header, and counts the calls it serves. It accepts every token but a revoked one.
type identity struct {
	identityv1.UnimplementedUserServiceServer
	calls atomic.Int32
}

func (s *identity) GetUser(ctx context.Context, req *identityv1.GetUserRequest) (*identityv1.GetUserResponse, error) {
	s.calls.Add(1)
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	incoming, _ := metadata.FromIncomingContext(ctx)
	if first(incoming, "x-tenant-id") != req.GetContext().GetTenantId() {
		return nil, status.Error(codes.InvalidArgument, "x-tenant-id is not the tenant of the request")
	}
	if req.GetUserId() == "revoked-token" {
		return &identityv1.GetUserResponse{Result: &commonv1.OperationResult{Status: "revoked"}}, nil
	}
	return &identityv1.GetUserResponse{Result: &commonv1.OperationResult{Status: "ok"}, User: &identityv1.User{UserId: req.GetUserId()}}, nil
}

// serve serves the identity service behind the monolith's interceptors on an in-memory listener. The
// interceptors check sessions through a connection signed as the monolith's calls are, as the mesh does,
// and dial connects a caller's client to it.
func serve(t *testing.T, serviceToken string) (*identity, func(...grpc.DialOption) identityv1.UserServiceClient) {
	listener := bufconn.Listen(1 << 20)
	dial := func(options ...grpc.DialOption) identityv1.UserServiceClient {
		options = append(options, grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
				return listener.DialContext(ctx)
			}))
		conn, err := grpc.NewClient("passthrough:///mesh", options...)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		return identityv1.NewUserServiceClient(conn)
	}
	sessions := dial(grpc.WithChainUnaryInterceptor(Workload(serviceToken)),
		grpc.WithChainStreamInterceptor(WorkloadStream(serviceToken)))
	server := grpc.NewServer(grpc.ChainUnaryInterceptor(Authenticate(sessions, serviceToken)),
		grpc.ChainStreamInterceptor(AuthenticateStream(sessions, serviceToken)))
	service := &identity{}
	identityv1.RegisterUserServiceServer(server, service)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	return service, dial
}

func TestAuthenticateChecksTheSessionOnce(t *testing.T) {
	service, dial := serve(t, "monolith-token")
	client := dial()
	ctx := metadata.AppendToOutgoingContext(t.Context(), "authorization", "Bearer user-token", "x-tenant-id", "tenant-a")
	if _, err := client.GetUser(ctx, &identityv1.GetUserRequest{Context: &commonv1.RequestContext{TenantId: "tenant-a"}}); err != nil {
		t.Fatal(err)
	}
	if calls := service.calls.Load(); calls != 2 {
		t.Fatalf("got %d calls, want the session check and the call", calls)
	}
}

func TestAuthenticateRejectsCallsWithoutSessionOrTenant(t *testing.T) {
	service, dial := serve(t, "monolith-token")
	client := dial()
	for _, pairs := range [][]string{nil, {"x-tenant-id", "tenant-a"},
		{"authorization", "Bearer user-token"}} {
		ctx := metadata.AppendToOutgoingContext(t.Context(), pairs...)
		if _, err := client.GetUser(ctx, &identityv1.GetUserRequest{}); status.Code(err) != codes.Unauthenticated {
			t.Fatalf("with %v, got %v, want Unauthenticated", pairs, err)
		}
	}
	if calls := service.calls.Load(); calls != 0 {
		t.Fatalf("got %d calls, want none", calls)
	}
}

func TestAuthenticateRejectsATokenTheSessionCheckRejects(t *testing.T) {
	service, dial := serve(t, "monolith-token")
	client := dial()
	ctx := metadata.AppendToOutgoingContext(t.Context(), "authorization", "Bearer revoked-token", "x-tenant-id", "tenant-a")
	if _, err := client.GetUser(ctx, &identityv1.GetUserRequest{Context: &commonv1.RequestContext{TenantId: "tenant-a"}}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("got %v, want Unauthenticated", err)
	}
	if calls := service.calls.Load(); calls != 1 {
		t.Fatalf("got %d calls, want the session check alone", calls)
	}
}

func TestAuthenticateTrustsTheMonolithsOwnCalls(t *testing.T) {
	service, dial := serve(t, "monolith-token")
	client := dial(grpc.WithChainUnaryInterceptor(Workload("monolith-token")))
	ctx := t.Context()
	if _, err := client.GetUser(ctx, &identityv1.GetUserRequest{Context: &commonv1.RequestContext{TenantId: "tenant-a"}}); err != nil {
		t.Fatal(err)
	}
	if calls := service.calls.Load(); calls != 1 {
		t.Fatalf("got %d calls, want the call alone", calls)
	}
}
