package search

import (
	"context"

	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"google.golang.org/grpc"
)

// Register registers every service of the search domain on the server.
func Register(server *grpc.Server, deps platform.Deps) {
	wireCalls(deps)
	deps.Clients.Put("acme.search.v1.SearchQueryHistoryService", searchv1.NewSearchQueryHistoryServiceClient(deps.Conn))
	deps.Clients.Put("acme.search.v1.SynonymSyncService", searchv1.NewSynonymSyncServiceClient(deps.Conn))
	searchv1.RegisterSearchIndexServiceServer(server, NewSearchIndexServer(deps))
	searchv1.RegisterSearchQueryQueryServiceServer(server, NewSearchQueryQueryServer(deps))
	searchv1.RegisterSynonymValidationServiceServer(server, NewSynonymValidationServer(deps))
	searchv1.RegisterSearchIndexQueryServiceServer(server, NewSearchIndexQueryServer(deps))
	searchv1.RegisterSearchQueryValidationServiceServer(server, NewSearchQueryValidationServer(deps))
	searchv1.RegisterSynonymHistoryServiceServer(server, NewSynonymHistoryServer(deps))
	searchv1.RegisterSearchIndexValidationServiceServer(server, NewSearchIndexValidationServer(deps))
	searchv1.RegisterSearchQueryHistoryServiceServer(server, NewSearchQueryHistoryServer(deps))
	searchv1.RegisterSynonymNotificationServiceServer(server, NewSynonymNotificationServer(deps))
	searchv1.RegisterSearchIndexHistoryServiceServer(server, NewSearchIndexHistoryServer(deps))
	searchv1.RegisterSearchQueryNotificationServiceServer(server, NewSearchQueryNotificationServer(deps))
	synonymAdminServer := NewSynonymAdminServer(deps)
	server.RegisterService(&searchv1.SynonymAdminService_ServiceDesc, synonymAdminServer)
	searchIndexNotificationServer := NewSearchIndexNotificationServer(deps)
	server.RegisterService(&searchv1.SearchIndexNotificationService_ServiceDesc, searchIndexNotificationServer)
	searchv1.RegisterSearchQueryAdminServiceServer(server, NewSearchQueryAdminServer(deps))
	searchv1.RegisterSynonymExportServiceServer(server, NewSynonymExportServer(deps))
	searchIndexAdminServer := NewSearchIndexAdminServer(deps)
	server.RegisterService(&searchv1.SearchIndexAdminService_ServiceDesc, searchIndexAdminServer)
	searchv1.RegisterSearchQueryExportServiceServer(server, NewSearchQueryExportServer(deps))
	searchv1.RegisterSynonymSyncServiceServer(server, NewSynonymSyncServer(deps))
	searchv1.RegisterSearchIndexExportServiceServer(server, NewSearchIndexExportServer(deps))
}

// The functions that call other services, which wireCalls sets when the module registers.
var (
	reconcileSynonymsCall func(context.Context, *searchv1.ReconcileSynonymsRequest, ...grpc.CallOption) (*searchv1.ReconcileSynonymsResponse, error)
)

// wireCalls points the package's call functions at the services they reach.
func wireCalls(deps platform.Deps) {
	synonymSyncForReconcileSynonyms := searchv1.NewSynonymSyncServiceClient(deps.Conn)
	reconcileSynonymsCall = synonymSyncForReconcileSynonyms.ReconcileSynonyms
}
