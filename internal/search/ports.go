package search

import (
	"context"
	"log/slog"

	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
)

// searchIndexExportPort is what the search domain needs of acme.search.v1.SearchIndexExportService.
type searchIndexExportPort interface {
	GetSearchIndexExport(ctx context.Context, request *searchv1.GetSearchIndexExportRequest) (*searchv1.GetSearchIndexExportResponse, error)
}

// grpcSearchIndexExportPort reaches acme.search.v1.SearchIndexExportService over gRPC.
type grpcSearchIndexExportPort struct {
	client searchv1.SearchIndexExportServiceClient
}

// sandboxSearchIndexExportPort answers in the sandbox environment, where acme.search.v1.SearchIndexExportService is not
// deployed.
type sandboxSearchIndexExportPort struct{ log *slog.Logger }

func (p grpcSearchIndexExportPort) GetSearchIndexExport(ctx context.Context, request *searchv1.GetSearchIndexExportRequest) (*searchv1.GetSearchIndexExportResponse, error) {
	return p.client.GetSearchIndexExport(ctx, request)
}

func (p sandboxSearchIndexExportPort) GetSearchIndexExport(ctx context.Context, request *searchv1.GetSearchIndexExportRequest) (*searchv1.GetSearchIndexExportResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers GetSearchIndexExport")
	return &searchv1.GetSearchIndexExportResponse{}, nil
}

func newSearchIndexExportPort(deps platform.Deps) searchIndexExportPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxSearchIndexExportPort{log: deps.Logger}
	}
	return grpcSearchIndexExportPort{client: searchv1.NewSearchIndexExportServiceClient(deps.Conn)}
}
