package warehousing

import (
	"context"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	warehousingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/warehousing/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
)

// StorageBinQueryServer implements `acme.warehousing.v1.StorageBinQueryService`.
// Answers read-only questions about storage bins, for screens and reports.
type StorageBinQueryServer struct {
	warehousingv1.UnimplementedStorageBinQueryServiceServer
	store                                   *Store
	log                                     *slog.Logger
	pickListValidationClient                warehousingv1.PickListValidationServiceClient
	conn                                    *grpc.ClientConn
	apiCredentialNotificationClient         identityv1.ApiCredentialNotificationServiceClient
	apiCredentialNotificationClientFallback identityv1.ApiCredentialNotificationServiceClient
	clients                                 *registry.Registry
	warehouseQueryClient                    warehousingv1.WarehouseQueryServiceClient
}

// NewStorageBinQueryServer wires a StorageBinQueryServer.
func NewStorageBinQueryServer(deps platform.Deps) *StorageBinQueryServer {
	return &StorageBinQueryServer{
		store:                                   NewStore(deps.DB),
		log:                                     deps.Logger.With("service", "acme.warehousing.v1.StorageBinQueryService"),
		pickListValidationClient:                warehousingv1.NewPickListValidationServiceClient(deps.Conn),
		conn:                                    deps.Conn,
		apiCredentialNotificationClient:         identityv1.NewApiCredentialNotificationServiceClient(deps.Conn),
		apiCredentialNotificationClientFallback: identityv1.NewApiCredentialNotificationServiceClient(deps.Conn),
		clients:                                 deps.Clients,
		warehouseQueryClient:                    warehousingv1.NewWarehouseQueryServiceClient(deps.Conn),
	}
}

// LookupStorageBin returns a storage bin.
func (s *StorageBinQueryServer) LookupStorageBin(ctx context.Context, req *warehousingv1.LookupStorageBinRequest) (*warehousingv1.LookupStorageBinResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "storage_bin", req.GetStorageBinId(), "lookup_storage_bin"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "lookup storage bin", "tenant", req.GetContext().GetTenantId())
	return &warehousingv1.LookupStorageBinResponse{
		StorageBin: &warehousingv1.StorageBin{StorageBinId: req.GetStorageBinId()},
		Result:     &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// BrowseStorageBins lists storage bins a page at a time.
func (s *StorageBinQueryServer) BrowseStorageBins(ctx context.Context, req *warehousingv1.BrowseStorageBinsRequest) (*warehousingv1.BrowseStorageBinsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "browse storage bins", "tenant", req.GetContext().GetTenantId())
	validatePickListRequest := &warehousingv1.ValidatePickListRequest{
		Context: req.GetContext(),
		PickList: &warehousingv1.PickList{
			BillingAddress: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
	}
	validatePickListReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*warehousingv1.ValidatePickListResponse, error) {
		return s.pickListValidationClient.ValidatePickList(ctx, validatePickListRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.PickListValidationService/ValidatePickList")
	}
	return &warehousingv1.BrowseStorageBinsResponse{
		StorageBins: []*warehousingv1.StorageBin{&warehousingv1.StorageBin{
			Children: []*warehousingv1.StorageBin{&warehousingv1.StorageBin{
				Audit: &commonv1.AuditInfo{
					UpdatedAt: validatePickListReply.GetPickList().GetAudit().GetUpdatedAt(),
				},
			}},
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// FindStorageBins lists storage bins a page at a time.
func (s *StorageBinQueryServer) FindStorageBins(ctx context.Context, req *warehousingv1.FindStorageBinsRequest) (*warehousingv1.FindStorageBinsResponse, error) {
	s.log.InfoContext(ctx, "find storage bins", "tenant", req.GetContext().GetTenantId())
	return &warehousingv1.FindStorageBinsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// ResolveStorageBins returns several storage bins at once.
func (s *StorageBinQueryServer) ResolveStorageBins(ctx context.Context, req *warehousingv1.ResolveStorageBinsRequest) (*warehousingv1.ResolveStorageBinsResponse, error) {
	s.log.InfoContext(ctx, "resolve storage bins", "tenant", req.GetContext().GetTenantId())
	return &warehousingv1.ResolveStorageBinsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// ExportStorageBins streams every matching storage bin.
func (s *StorageBinQueryServer) ExportStorageBins(req *warehousingv1.ExportStorageBinsRequest, stream grpc.ServerStreamingServer[warehousingv1.ExportStorageBinsResponse]) error {
	ctx := stream.Context()
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ExportStorageBins", "error", err)
		return err
	}
	s.log.InfoContext(ctx, "export storage bins", "tenant", req.GetContext().GetTenantId())
	labels := make(map[string]string, len(req.GetFilter().GetLabels()))
	for key, value := range req.GetFilter().GetLabels() {
		labels[key] = value
	}
	notifyTenantWatchersRequest := &identityv1.NotifyTenantWatchersRequest{
		Context: req.GetContext(),
		Details: &identityv1.NotifyTenantWatchersRequest_Details{
			Recipient: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Variables: labels,
		},
		TenantId: req.GetContext().GetTenantId(),
	}
	notifyTenantWatchersReply := new(identityv1.NotifyTenantWatchersResponse)
	if err := grpcx.Invoke(ctx, s.conn, identityv1.TenantNotificationService_NotifyTenantWatchers_FullMethodName, notifyTenantWatchersRequest, notifyTenantWatchersReply); err != nil {
		return grpcx.Wrap(err, "acme.identity.v1.TenantNotificationService/NotifyTenantWatchers")
	}
	routes := map[string]func(context.Context, *identityv1.SendApiCredentialTestNoticeRequest, ...grpc.CallOption) (*identityv1.SendApiCredentialTestNoticeResponse, error){
		"primary":  s.apiCredentialNotificationClient.SendApiCredentialTestNotice,
		"fallback": s.apiCredentialNotificationClientFallback.SendApiCredentialTestNotice,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	packed, err := anypb.New(req.GetFilter())
	if err != nil {
		return grpcx.Wrap(err, "pack payload")
	}
	sendApiCredentialTestNoticeRequest := &identityv1.SendApiCredentialTestNoticeRequest{
		Context: req.GetContext(),
		Details: &identityv1.SendApiCredentialTestNoticeRequest_Details{
			Recipient: &commonv1.ContactInfo{
				Email: notifyTenantWatchersReply.GetTenant().GetManager().GetEmail(),
			},
			Payload: packed,
		},
	}
	sendApiCredentialTestNoticeReply, err := route(ctx, sendApiCredentialTestNoticeRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.identity.v1.ApiCredentialNotificationService/SendApiCredentialTestNotice")
	}
	if err := stream.Send(&warehousingv1.ExportStorageBinsResponse{
		StorageBin: &warehousingv1.StorageBin{
			Children: []*warehousingv1.StorageBin{&warehousingv1.StorageBin{
				Children: []*warehousingv1.StorageBin{&warehousingv1.StorageBin{Name: notifyTenantWatchersReply.GetTenant().GetName()}},
				Manager: &identityv1.UserRef{
					Email: notifyTenantWatchersReply.GetTenant().GetManager().GetEmail(),
				},
				Audit: &commonv1.AuditInfo{
					UpdatedAt: sendApiCredentialTestNoticeReply.GetApiCredential().GetAudit().GetUpdatedAt(),
				},
			}},
			Location: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}); err != nil {
		return err
	}
	return nil
}

// SummarizeStorageBins summarizes a storage bin.
func (s *StorageBinQueryServer) SummarizeStorageBins(ctx context.Context, req *warehousingv1.SummarizeStorageBinsRequest) (*warehousingv1.SummarizeStorageBinsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "storage_bin", req.GetStorageBinId(), "summarize_storage_bins"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "summarize storage bins", "tenant", req.GetContext().GetTenantId())
	usageRecordCalculation, err := registry.Lookup[subscriptionsv1.UsageRecordCalculationServiceClient](s.clients, "acme.subscriptions.v1.UsageRecordCalculationService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	estimateUsageRecordRequest := &subscriptionsv1.EstimateUsageRecordRequest{
		Context:     req.GetContext(),
		Annotations: annotations,
		Reason:      req.GetReason(),
		UsageRecord: &subscriptionsv1.UsageRecord{
			Subject:    &customersv1.CustomerRef{Email: req.GetContext().GetActor().GetEmail()},
			Attributes: req.GetDetails().GetFilters(),
			Detail:     packed,
		},
	}
	estimateUsageRecordReply, err := usageRecordCalculation.EstimateUsageRecord(ctx, estimateUsageRecordRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.UsageRecordCalculationService/EstimateUsageRecord")
	}
	return &warehousingv1.SummarizeStorageBinsResponse{
		Result: &commonv1.OperationResult{
			Message:       estimateUsageRecordReply.GetResult().GetMessage(),
			CorrelationId: estimateUsageRecordReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// SuggestStorageBins lists storage bins a page at a time.
func (s *StorageBinQueryServer) SuggestStorageBins(ctx context.Context, req *warehousingv1.SuggestStorageBinsRequest) (*warehousingv1.SuggestStorageBinsResponse, error) {
	s.log.InfoContext(ctx, "suggest storage bins", "tenant", req.GetContext().GetTenantId())
	warehouseQuery, ok := s.clients.Get("acme.warehousing.v1.WarehouseQueryService").(warehousingv1.WarehouseQueryServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.warehousing.v1.WarehouseQueryService")
	}
	browseWarehousesRequest := &warehousingv1.BrowseWarehousesRequest{
		Context: req.GetContext(),
		Filter: &warehousingv1.BrowseWarehousesRequest_Filter{
			Labels:  req.GetFilter().GetLabels(),
			Query:   req.GetFilter().GetQuery(),
			OwnerId: req.GetFilter().GetOwnerId(),
		},
		Page: &commonv1.PageRequest{PageToken: req.GetPage().GetPageToken()},
	}
	browseWarehousesReply, err := warehouseQuery.BrowseWarehouses(ctx, browseWarehousesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.WarehouseQueryService/BrowseWarehouses")
	}
	sellerScorecardValidation, ok := s.clients.Get("acme.sellers.v1.SellerScorecardValidationService").(sellersv1.SellerScorecardValidationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.sellers.v1.SellerScorecardValidationService")
	}
	normalizeSellerScorecardRequest := &sellersv1.NormalizeSellerScorecardRequest{
		Context: req.GetContext(),
		SellerScorecard: &sellersv1.SellerScorecard{
			BillingAddress: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
			Counterparty:   &customersv1.CustomerRef{Email: req.GetContext().GetActor().GetEmail()},
			Audit:          &commonv1.AuditInfo{CreatedAt: req.GetFilter().GetCreatedAfter()},
		},
		Details: &sellersv1.NormalizeSellerScorecardRequest_Details{Locale: req.GetContext().GetLocale()},
	}
	normalizeSellerScorecardReply, err := sellerScorecardValidation.NormalizeSellerScorecard(ctx, normalizeSellerScorecardRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardValidationService/NormalizeSellerScorecard")
	}
	return &warehousingv1.SuggestStorageBinsResponse{
		Result: &commonv1.OperationResult{Status: browseWarehousesReply.GetResult().GetStatus()},
		StorageBins: []*warehousingv1.StorageBin{&warehousingv1.StorageBin{
			Children: []*warehousingv1.StorageBin{&warehousingv1.StorageBin{
				Children: []*warehousingv1.StorageBin{&warehousingv1.StorageBin{
					Location: &commonv1.PostalAddress{
						Region: normalizeSellerScorecardReply.GetSellerScorecard().GetBillingAddress().GetRegion(),
					},
				}},
			}},
		}},
	}, nil
}

// PeekStorageBin returns a storage bin.
func (s *StorageBinQueryServer) PeekStorageBin(ctx context.Context, req *warehousingv1.PeekStorageBinRequest) (*warehousingv1.PeekStorageBinResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected PeekStorageBin", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "storage_bin", req.GetStorageBinId(), "peek_storage_bin"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "peek storage bin", "tenant", req.GetContext().GetTenantId())
	baggage := make(map[string]string, len(req.GetContext().GetBaggage()))
	for key, value := range req.GetContext().GetBaggage() {
		baggage[key] = value
	}
	summarizeWarehousesRequest := newSummarizeWarehousesRequest(withSummarizeWarehousesContext(req.GetContext()), withSummarizeWarehousesDetails(&warehousingv1.SummarizeWarehousesRequest_Details{Filters: baggage}))
	summarizeWarehousesReply, err := s.warehouseQueryClient.SummarizeWarehouses(ctx, summarizeWarehousesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.WarehouseQueryService/SummarizeWarehouses")
	}
	validatePutawayTaskRequest := &warehousingv1.ValidatePutawayTaskRequest{
		Context: req.GetContext(),
		PutawayTask: &warehousingv1.PutawayTask{
			Origin: &warehousingv1.PutawayTask_Customer{Customer: &warehousingv1.StorageBinRef{StorageBinId: req.GetStorageBinId()}},
			Assignee: &identityv1.UserRef{
				UserId: req.GetContext().GetActor().GetUserId(),
				Email:  req.GetContext().GetActor().GetEmail(),
			},
		},
	}
	validatePutawayTaskReply := new(warehousingv1.ValidatePutawayTaskResponse)
	if err := grpc.Invoke(ctx, "/acme.warehousing.v1.PutawayTaskValidationService/ValidatePutawayTask", validatePutawayTaskRequest, validatePutawayTaskReply, s.conn); err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.PutawayTaskValidationService/ValidatePutawayTask")
	}
	return &warehousingv1.PeekStorageBinResponse{
		Result: &commonv1.OperationResult{
			Status:  summarizeWarehousesReply.GetResult().GetStatus(),
			Details: summarizeWarehousesReply.GetResult().GetDetails(),
		},
		StorageBin: &warehousingv1.StorageBin{
			Children: []*warehousingv1.StorageBin{&warehousingv1.StorageBin{
				Audit: &commonv1.AuditInfo{
					CreatedBy: validatePutawayTaskReply.GetPutawayTask().GetAudit().GetCreatedBy(),
				},
				Children: []*warehousingv1.StorageBin{&warehousingv1.StorageBin{
					Manager: &identityv1.UserRef{
						UserId: validatePutawayTaskReply.GetPutawayTask().GetAssignee().GetUserId(),
					},
				}},
			}},
			StorageBinId: req.GetStorageBinId(),
		},
	}, nil
}
