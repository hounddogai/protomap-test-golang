package grpcx

import "context"

// Bearer is the credential a program calls the monolith with: its token and the tenant it acts for, which the
// monolith's authenticating interceptor checks with the identity service.
type Bearer struct {
	Token  string
	Tenant string
}

// GetRequestMetadata answers the headers every call carries.
func (b Bearer) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + b.Token, "x-tenant-id": b.Tenant}, nil
}

// RequireTransportSecurity is false: the mesh sidecar encrypts what leaves the host.
func (Bearer) RequireTransportSecurity() bool { return false }
