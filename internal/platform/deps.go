// Package platform holds what every domain module of the monolith is wired with.
package platform

import (
	"database/sql"
	"log/slog"

	"google.golang.org/grpc"

	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
)

// Flags are the feature flags the monolith started with.
type Flags map[string]bool

// Enabled reports whether a flag is on.
func (f Flags) Enabled(name string) bool { return f[name] }

// Deps is what a domain module needs to serve and to call other services.
type Deps struct {
	// Conn reaches every service of the mesh, the monolith's own included, through the sidecar.
	Conn    *grpc.ClientConn
	DB      *sql.DB
	Logger  *slog.Logger
	Bus     *eventbus.Bus
	Clients *registry.Registry
	Flags   Flags
	Region  string
}
