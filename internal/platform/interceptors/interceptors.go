// Package interceptors has the interceptors of the monolith: authentication, the credentials of the calls it
// makes, and recovery.
package interceptors

import (
	"context"
	"crypto/subtle"
	"log/slog"
	"strings"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// first returns the first value of a metadata key, or "".
func first(md metadata.MD, key string) string {
	if values := md.Get(key); len(values) > 0 {
		return values[0]
	}
	return ""
}

// bearer returns the token of a call's authorization header, or "".
func bearer(md metadata.MD) string {
	return strings.TrimPrefix(first(md, "authorization"), "Bearer ")
}

// tenantOf returns the tenant a call acts for: its x-tenant-id header, or else the context of its
// request, which the interceptor of a stream does not have.
func tenantOf(md metadata.MD, req any) string {
	if tenant := first(md, "x-tenant-id"); tenant != "" {
		return tenant
	}
	if scoped, ok := req.(interface {
		GetContext() *commonv1.RequestContext
	}); ok {
		return scoped.GetContext().GetTenantId()
	}
	return ""
}

// check asks the identity service whether the caller's bearer token is a live session of its tenant,
// and answers a context that carries who the caller is. A call that bears the monolith's service token
// is the monolith's own, such as a worker's call or a session check, and passes unchecked: checking a
// session check would never end.
func check(ctx context.Context, req any, sessions identityv1.UserServiceClient, serviceToken string) (context.Context, error) {
	incoming, _ := metadata.FromIncomingContext(ctx)
	token := bearer(incoming)
	if token == "" {
		return nil, status.Error(codes.Unauthenticated, "missing bearer token")
	}
	if subtle.ConstantTimeCompare([]byte(token), []byte(serviceToken)) == 1 {
		return ctx, nil
	}
	tenant := tenantOf(incoming, req)
	if tenant == "" {
		return nil, status.Error(codes.Unauthenticated, "missing tenant")
	}
	outgoing := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+serviceToken)
	reply, err := sessions.GetUser(outgoing, &identityv1.GetUserRequest{UserId: token, Context: &commonv1.RequestContext{TenantId: tenant, TraceId: first(incoming, "x-trace-id"), Actor: &commonv1.Actor{UserId: "monolith", ServiceAccount: true}}})
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "session check failed: %v", err)
	}
	if reply.GetResult().GetStatus() != "ok" || reply.GetUser().GetUserId() != token {
		return nil, status.Error(codes.Unauthenticated, "the session check rejected the token")
	}
	return tenancy.With(ctx, "session", reply.GetResult().GetCorrelationId()), nil
}

// Authenticate checks the session of every unary call with acme.identity.v1.UserService/GetUser.
func Authenticate(sessions identityv1.UserServiceClient, serviceToken string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if strings.HasPrefix(info.FullMethod, "/grpc.health.") {
			return handler(ctx, req)
		}
		ctx, err := check(ctx, req, sessions, serviceToken)
		if err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

// AuthenticateStream checks the session of every streaming call, as Authenticate does.
func AuthenticateStream(sessions identityv1.UserServiceClient, serviceToken string) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if _, err := check(ss.Context(), nil, sessions, serviceToken); err != nil {
			return err
		}
		return handler(srv, ss)
	}
}

// sign gives a call the monolith makes its service token, unless the call has credentials of its own,
// and the tenant it acts for, which every server of the mesh needs: its request's, or else the tenant of
// the call it serves, as a stream's request is not at hand when it opens.
func sign(ctx context.Context, serviceToken string, req any) context.Context {
	outgoing, _ := metadata.FromOutgoingContext(ctx)
	if len(outgoing.Get("authorization")) == 0 {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+serviceToken)
	}
	if len(outgoing.Get("x-tenant-id")) > 0 {
		return ctx
	}
	incoming, _ := metadata.FromIncomingContext(ctx)
	tenant := tenantOf(nil, req)
	if tenant == "" {
		tenant = first(incoming, "x-tenant-id")
	}
	if tenant == "" {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, "x-tenant-id", tenant)
}

// Workload signs every unary call the monolith makes with its service token, so that the services it
// calls, its own included, know the caller.
func Workload(serviceToken string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		return invoker(sign(ctx, serviceToken, req), method, req, reply, cc, opts...)
	}
}

// WorkloadStream signs every stream the monolith opens, as Workload does.
func WorkloadStream(serviceToken string) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		return streamer(sign(ctx, serviceToken, nil), desc, cc, method, opts...)
	}
}

// Recover turns a handler's panic into an Internal error, and logs it.
func Recover(log *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (reply any, err error) {
		defer func() {
			if recovered := recover(); recovered != nil {
				log.ErrorContext(ctx, "handler panicked", "method", info.FullMethod, "panic", recovered)
				err = status.Error(codes.Internal, "internal error")
			}
		}()
		return handler(ctx, req)
	}
}
