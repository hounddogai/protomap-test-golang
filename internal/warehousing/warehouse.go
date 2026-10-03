package warehousing

import (
	"context"
	"errors"
	"io"
	"log/slog"

	auditv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audit/v1"
	campaignsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/campaigns/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	documentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/documents/v1"
	fulfillmentv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fulfillment/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	warehousingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/warehousing/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// WarehouseServer implements `acme.warehousing.v1.WarehouseService`.
// Manages warehouses: creates, reads, changes, and lists them.
type WarehouseServer struct {
	warehousingv1.UnimplementedWarehouseServiceServer
	store                                *Store
	log                                  *slog.Logger
	fulfillmentExceptionValidationClient fulfillmentv1.FulfillmentExceptionValidationServiceClient
	warehouseQueryClient                 warehousingv1.WarehouseQueryServiceClient
	bus                                  *eventbus.Bus
	conn                                 *grpc.ClientConn
	storageBinQueryClient                warehousingv1.StorageBinQueryServiceClient
	parcelQueryClient                    shippingv1.ParcelQueryServiceClient
	sellerPayoutScheduleValidationClient sellersv1.SellerPayoutScheduleValidationServiceClient
	marketingCampaignClient              campaignsv1.MarketingCampaignServiceClient
	pickListValidationClient             warehousingv1.PickListValidationServiceClient
	searchIndexValidationClient          searchv1.SearchIndexValidationServiceClient
	meteredDocumentValidation            meteredDocumentValidation
}

// NewWarehouseServer wires a WarehouseServer.
func NewWarehouseServer(deps platform.Deps) *WarehouseServer {
	return &WarehouseServer{
		store:                                NewStore(deps.DB),
		log:                                  deps.Logger.With("service", "acme.warehousing.v1.WarehouseService"),
		fulfillmentExceptionValidationClient: fulfillmentv1.NewFulfillmentExceptionValidationServiceClient(deps.Conn),
		warehouseQueryClient:                 warehousingv1.NewWarehouseQueryServiceClient(deps.Conn),
		bus:                                  deps.Bus,
		conn:                                 deps.Conn,
		storageBinQueryClient:                warehousingv1.NewStorageBinQueryServiceClient(deps.Conn),
		parcelQueryClient:                    shippingv1.NewParcelQueryServiceClient(deps.Conn),
		sellerPayoutScheduleValidationClient: sellersv1.NewSellerPayoutScheduleValidationServiceClient(deps.Conn),
		marketingCampaignClient:              campaignsv1.NewMarketingCampaignServiceClient(deps.Conn),
		pickListValidationClient:             warehousingv1.NewPickListValidationServiceClient(deps.Conn),
		searchIndexValidationClient:          searchv1.NewSearchIndexValidationServiceClient(deps.Conn),
		meteredDocumentValidation:            meteredDocumentValidation{DocumentValidationServiceClient: documentsv1.NewDocumentValidationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
	}
}

// CreateWarehouse creates a warehouse.
func (s *WarehouseServer) CreateWarehouse(ctx context.Context, req *warehousingv1.CreateWarehouseRequest) (*warehousingv1.CreateWarehouseResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveWarehouse(ctx, req.GetContext().GetTenantId(), req.GetWarehouse()); err != nil {
		return nil, grpcx.Wrap(err, "save warehouse")
	}
	s.log.InfoContext(ctx, "create warehouse", "tenant", req.GetContext().GetTenantId(), "etag", req.GetWarehouse().GetAudit().GetEtag())
	peekWarehouseRequest := &warehousingv1.PeekWarehouseRequest{
		Context:     req.GetContext(),
		WarehouseId: req.GetWarehouse().GetWarehouseId(),
	}
	peekWarehouseReply, err := peekWarehouseCall(ctx, peekWarehouseRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.WarehouseQueryService/PeekWarehouse")
	}
	explainFulfillmentExceptionDecisionRequest := newExplainFulfillmentExceptionDecisionRequest(withExplainFulfillmentExceptionDecisionContext(req.GetContext()), withExplainFulfillmentExceptionDecisionFulfillmentException(&fulfillmentv1.FulfillmentException{
		Audit: &commonv1.AuditInfo{
			CreatedBy: req.GetWarehouse().GetAudit().GetCreatedBy(),
			CreatedAt: req.GetWarehouse().GetAudit().GetCreatedAt(),
		},
		Origin: &fulfillmentv1.FulfillmentException_Customer{Customer: &fulfillmentv1.PackingSlipRef{
			DisplayName: peekWarehouseReply.GetWarehouse().GetOwner().GetDisplayName(),
		}},
	}))
	explainFulfillmentExceptionDecisionReply, err := s.fulfillmentExceptionValidationClient.ExplainFulfillmentExceptionDecision(ctx, explainFulfillmentExceptionDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.FulfillmentExceptionValidationService/ExplainFulfillmentExceptionDecision")
	}
	recentWarehouses.Store(recordKey{req.GetContext().GetTenantId(), req.GetWarehouse().GetWarehouseId()}, req.GetWarehouse())
	return &warehousingv1.CreateWarehouseResponse{
		Warehouse: &warehousingv1.Warehouse{
			Tags: peekWarehouseReply.GetWarehouse().GetTags(),
			Audit: &commonv1.AuditInfo{
				UpdatedBy: peekWarehouseReply.GetWarehouse().GetAudit().GetUpdatedBy(),
			},
			Owner: &identityv1.UserRef{
				Email: explainFulfillmentExceptionDecisionReply.GetFulfillmentException().GetRequester().GetEmail(),
			},
			Name:     req.GetWarehouse().GetName(),
			Capacity: req.GetWarehouse().GetCapacity(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: explainFulfillmentExceptionDecisionReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// GetWarehouse returns a warehouse.
func (s *WarehouseServer) GetWarehouse(ctx context.Context, req *warehousingv1.GetWarehouseRequest) (*warehousingv1.GetWarehouseResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "warehouse", req.GetWarehouseId(), "get_warehouse"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get warehouse", "tenant", req.GetContext().GetTenantId())
	suggestWarehousesRequest := buildSuggestWarehousesRequestForGetWarehouse(req)
	suggestWarehousesReply, err := s.warehouseQueryClient.SuggestWarehouses(ctx, suggestWarehousesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.WarehouseQueryService/SuggestWarehouses")
	}
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "warehousing.get_warehouse.browse_warehouses",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"filter.owner_id":   req.GetWarehouseId(),
			"context.tenant_id": req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	return &warehousingv1.GetWarehouseResponse{
		Result: &commonv1.OperationResult{
			Message: suggestWarehousesReply.GetResult().GetMessage(),
			Status:  suggestWarehousesReply.GetResult().GetStatus(),
		},
		Warehouse: &warehousingv1.Warehouse{WarehouseId: req.GetWarehouseId()},
	}, nil
}

// UpdateWarehouse changes a warehouse.
func (s *WarehouseServer) UpdateWarehouse(ctx context.Context, req *warehousingv1.UpdateWarehouseRequest) (*warehousingv1.UpdateWarehouseResponse, error) {
	if err := s.store.SaveWarehouse(ctx, req.GetContext().GetTenantId(), req.GetWarehouse()); err != nil {
		return nil, grpcx.Wrap(err, "save warehouse")
	}
	s.log.InfoContext(ctx, "update warehouse", "tenant", req.GetContext().GetTenantId(), "etag", req.GetWarehouse().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetWarehouse()).(*warehousingv1.Warehouse)
	ctx = tenancy.With(ctx, "postal_code", req.GetWarehouse().GetAddress().GetPostalCode())
	explainMetricSeriesDecisionRequest := &metricsv1.ExplainMetricSeriesDecisionRequest{
		Context: req.GetContext(),
		MetricSeries: &metricsv1.MetricSeries{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: cloned.GetAudit().GetUpdatedBy(),
				CreatedBy: req.GetWarehouse().GetAudit().GetCreatedBy(),
			},
			Locator: &metricsv1.MetricSeries_Address{Address: &commonv1.PostalAddress{PostalCode: tenancy.Value(ctx, "postal_code")}},
		},
	}
	explainMetricSeriesDecisionReply := new(metricsv1.ExplainMetricSeriesDecisionResponse)
	if err := grpcx.Invoke(ctx, s.conn, metricsv1.MetricSeriesValidationService_ExplainMetricSeriesDecision_FullMethodName, explainMetricSeriesDecisionRequest, explainMetricSeriesDecisionReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.metrics.v1.MetricSeriesValidationService/ExplainMetricSeriesDecision")
	}
	normalizePickListRequest := &warehousingv1.NormalizePickListRequest{
		Context: req.GetContext(),
		PickList: &warehousingv1.PickList{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: explainMetricSeriesDecisionReply.GetMetricSeries().GetAudit().GetUpdatedAt(),
			},
			Counterparty: &customersv1.CustomerRef{
				Email: explainMetricSeriesDecisionReply.GetMetricSeries().GetOwner().GetEmail(),
			},
		},
	}
	normalizePickListReply := new(warehousingv1.NormalizePickListResponse)
	if err := s.conn.Invoke(ctx, warehousingPickListValidationServiceNormalizePickListMethod, normalizePickListRequest, normalizePickListReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.PickListValidationService/NormalizePickList")
	}
	browseStorageBinsRequest := &warehousingv1.BrowseStorageBinsRequest{
		Context: req.GetContext(),
		Filter: &warehousingv1.BrowseStorageBinsRequest_Filter{
			Region:       explainMetricSeriesDecisionReply.GetMetricSeries().GetAddress().GetRegion(),
			CreatedAfter: explainMetricSeriesDecisionReply.GetMetricSeries().GetAudit().GetCreatedAt(),
		},
	}
	var browseStorageBinsReply *warehousingv1.BrowseStorageBinsResponse
	if err := pipeline.Run(ctx, "acme.warehousing.v1.StorageBinQueryService/BrowseStorageBins", func(ctx context.Context) error {
		answer, err := s.storageBinQueryClient.BrowseStorageBins(ctx, browseStorageBinsRequest)
		browseStorageBinsReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.StorageBinQueryService/BrowseStorageBins")
	}
	listAuditEventsRequest := &auditv1.ListAuditEventsRequest{
		Context: req.GetContext(),
		Filter:  &auditv1.ListAuditEventsRequest_Filter{Region: req.GetWarehouse().GetAddress().GetRegion()},
	}
	listAuditEventsReply, err := listAuditEventsCall(ctx, listAuditEventsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.audit.v1.AuditEventService/ListAuditEvents")
	}
	_ = listAuditEventsReply
	return &warehousingv1.UpdateWarehouseResponse{
		Result: &commonv1.OperationResult{
			Message: explainMetricSeriesDecisionReply.GetOutcome().GetMessage(),
			Details: browseStorageBinsReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Warehouse: &warehousingv1.Warehouse{
			Audit: &commonv1.AuditInfo{
				CreatedBy: normalizePickListReply.GetPickList().GetAudit().GetCreatedBy(),
			},
			WarehouseId: req.GetWarehouse().GetWarehouseId(),
			Code:        req.GetWarehouse().GetCode(),
		},
	}, nil
}

// DeleteWarehouse deletes a warehouse.
func (s *WarehouseServer) DeleteWarehouse(ctx context.Context, req *warehousingv1.DeleteWarehouseRequest) (*warehousingv1.DeleteWarehouseResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected DeleteWarehouse", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "warehouse", req.GetWarehouseId(), "delete_warehouse"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "delete warehouse", "tenant", req.GetContext().GetTenantId())
	step := &warehouseServerDeleteWarehouseResolveParcelsStep{request: &shippingv1.ResolveParcelsRequest{Context: req.GetContext()}}
	if err := s.runDeleteWarehouseResolveParcels(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ParcelQueryService/ResolveParcels")
	}
	resolveParcelsReply := step.reply
	normalizeSellerPayoutScheduleRequest := &sellersv1.NormalizeSellerPayoutScheduleRequest{
		Context: req.GetContext(),
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
			Participants: []*sellersv1.SellerPayoutScheduleParticipant{&sellersv1.SellerPayoutScheduleParticipant{
				Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			}},
		},
	}
	normalizeSellerPayoutScheduleReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*sellersv1.NormalizeSellerPayoutScheduleResponse, error) {
		return s.sellerPayoutScheduleValidationClient.NormalizeSellerPayoutSchedule(ctx, normalizeSellerPayoutScheduleRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerPayoutScheduleValidationService/NormalizeSellerPayoutSchedule")
	}
	return &warehousingv1.DeleteWarehouseResponse{
		Result: &commonv1.OperationResult{
			Message:       resolveParcelsReply.GetResult().GetMessage(),
			Details:       resolveParcelsReply.GetResult().GetDetails(),
			CorrelationId: normalizeSellerPayoutScheduleReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// BatchGetWarehouses returns several warehouses at once.
func (s *WarehouseServer) BatchGetWarehouses(ctx context.Context, req *warehousingv1.BatchGetWarehousesRequest) (*warehousingv1.BatchGetWarehousesResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "batch get warehouses", "tenant", req.GetContext().GetTenantId())
	browseWarehousesRequest := &warehousingv1.BrowseWarehousesRequest{}
	browseWarehousesRequest.Context = req.GetContext()
	browseWarehousesRequest.Filter = &warehousingv1.BrowseWarehousesRequest_Filter{CreatedAfter: req.GetContext().GetDeadline()}
	browseWarehousesReply, err := s.warehouseQueryClient.BrowseWarehouses(ctx, browseWarehousesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.WarehouseQueryService/BrowseWarehouses")
	}
	cloned := proto.Clone(req.GetContext()).(*commonv1.RequestContext)
	ctx = tenancy.With(ctx, "locale", req.GetContext().GetLocale())
	normalizePutawayTaskRequest := &warehousingv1.NormalizePutawayTaskRequest{
		Context: req.GetContext(),
		PutawayTask: &warehousingv1.PutawayTask{
			Assignee: &identityv1.UserRef{
				Email:  cloned.GetActor().GetEmail(),
				UserId: req.GetContext().GetActor().GetUserId(),
			},
		},
		Details: &warehousingv1.NormalizePutawayTaskRequest_Details{Locale: tenancy.Value(ctx, "locale")},
	}
	normalizePutawayTaskReply := new(warehousingv1.NormalizePutawayTaskResponse)
	if err := grpc.Invoke(ctx, "/acme.warehousing.v1.PutawayTaskValidationService/NormalizePutawayTask", normalizePutawayTaskRequest, normalizePutawayTaskReply, s.conn); err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.PutawayTaskValidationService/NormalizePutawayTask")
	}
	return &warehousingv1.BatchGetWarehousesResponse{
		PageInfo: &commonv1.PageInfo{
			NextPageToken: browseWarehousesReply.GetPageInfo().GetNextPageToken(),
		},
		Result: &commonv1.OperationResult{Status: browseWarehousesReply.GetResult().GetStatus()},
		Warehouses: []*warehousingv1.Warehouse{&warehousingv1.Warehouse{
			Owner: &identityv1.UserRef{
				UserId: normalizePutawayTaskReply.GetPutawayTask().GetAssignee().GetUserId(),
			},
			Tags: normalizePutawayTaskReply.GetPutawayTask().GetTags(),
		}},
	}, nil
}

// SearchWarehouses lists warehouses a page at a time.
func (s *WarehouseServer) SearchWarehouses(ctx context.Context, req *warehousingv1.SearchWarehousesRequest) (*warehousingv1.SearchWarehousesResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "search warehouses", "tenant", req.GetContext().GetTenantId())
	labels := make(map[string]string, len(req.GetFilter().GetLabels()))
	for key, value := range req.GetFilter().GetLabels() {
		labels[key] = value
	}
	summarizeWarehousesReply, err := s.warehouseQueryClient.SummarizeWarehouses(ctx, &warehousingv1.SummarizeWarehousesRequest{
		Context:     req.GetContext(),
		Details:     &warehousingv1.SummarizeWarehousesRequest_Details{Filters: labels},
		Annotations: req.GetContext().GetBaggage(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.WarehouseQueryService/SummarizeWarehouses")
	}
	return &warehousingv1.SearchWarehousesResponse{
		Result: &commonv1.OperationResult{
			Details: summarizeWarehousesReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}

// WatchWarehouses streams the changes of warehouses as they happen.
func (s *WarehouseServer) WatchWarehouses(req *warehousingv1.WatchWarehousesRequest, stream grpc.ServerStreamingServer[warehousingv1.WatchWarehousesResponse]) error {
	ctx := stream.Context()
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected WatchWarehouses", "error", err)
		return err
	}
	s.log.InfoContext(ctx, "watch warehouses", "tenant", req.GetContext().GetTenantId())
	source := req.GetSubscriber().ProtoReflect()
	email := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("email"))).String()
	subscriberCache := cache.New[string, *commonv1.ContactInfo](1)
	subscriberCache.Put(req.GetContext().GetTraceId(), req.GetSubscriber())
	cachedSubscriber, _ := subscriberCache.Get(req.GetContext().GetTraceId())
	createMarketingCampaignReply, err := s.marketingCampaignClient.CreateMarketingCampaign(ctx, &campaignsv1.CreateMarketingCampaignRequest{
		Context: req.GetContext(),
		MarketingCampaign: &campaignsv1.MarketingCampaign{
			Participants: []*campaignsv1.MarketingCampaignParticipant{&campaignsv1.MarketingCampaignParticipant{
				Contact: &commonv1.ContactInfo{
					Email:            email,
					Name:             cachedSubscriber.GetName(),
					PreferredChannel: req.GetSubscriber().GetPreferredChannel(),
				},
			}},
			Location: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
	})
	if err != nil {
		return grpcx.Wrap(err, "acme.campaigns.v1.MarketingCampaignService/CreateMarketingCampaign")
	}
	step := &warehouseServerWatchWarehousesPreviewPickListStep{request: &warehousingv1.PreviewPickListRequest{
		Context: req.GetContext(),
		PickList: &warehousingv1.PickList{
			Audit: &commonv1.AuditInfo{
				CreatedAt: createMarketingCampaignReply.GetMarketingCampaign().GetAudit().GetCreatedAt(),
			},
			Counterparty: &customersv1.CustomerRef{
				DisplayName: createMarketingCampaignReply.GetMarketingCampaign().GetOrganizer().GetDisplayName(),
			},
		},
	}}
	if err := s.runWatchWarehousesPreviewPickList(ctx, step); err != nil {
		return grpcx.Wrap(err, "acme.warehousing.v1.PickListValidationService/PreviewPickList")
	}
	previewPickListReply := step.reply
	if err := stream.Send(&warehousingv1.WatchWarehousesResponse{
		Result: &commonv1.OperationResult{
			Message: createMarketingCampaignReply.GetResult().GetMessage(),
			Status:  createMarketingCampaignReply.GetResult().GetStatus(),
		},
		Warehouse: &warehousingv1.Warehouse{
			Owner: &identityv1.UserRef{
				DisplayName: previewPickListReply.GetPickList().GetCounterparty().GetDisplayName(),
				Email:       req.GetSubscriber().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedBy: previewPickListReply.GetPickList().GetAudit().GetCreatedBy(),
			},
		},
	}); err != nil {
		return err
	}
	return nil
}

// BatchCreateWarehouses takes a stream of warehouses and answers once at the end.
func (s *WarehouseServer) BatchCreateWarehouses(stream grpc.ClientStreamingServer[warehousingv1.BatchCreateWarehousesRequest, warehousingv1.BatchCreateWarehousesResponse]) error {
	ctx := stream.Context()
	var accepted int64
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		s.log.InfoContext(ctx, "batch create warehouses", "tenant", msg.GetContext().GetTenantId())
		normalizeSearchIndexRequest := &searchv1.NormalizeSearchIndexRequest{}
		normalizeSearchIndexRequest.Context = msg.GetContext()
		normalizeSearchIndexRequest.SearchIndex = &searchv1.SearchIndex{
			Owner:      &identityv1.UserRef{Email: msg.GetRecord().GetOwner().GetEmail()},
			Audit:      &commonv1.AuditInfo{Etag: msg.GetRecord().GetAudit().GetEtag()},
			Locator:    &searchv1.SearchIndex_Address{Address: &commonv1.PostalAddress{CountryCode: msg.GetRecord().GetAddress().GetCountryCode()}},
			Dimensions: &searchv1.SearchIndexDimensions{Width: msg.GetRecord().GetDimensions().GetWidth()},
		}
		if _, err := s.searchIndexValidationClient.NormalizeSearchIndex(ctx, normalizeSearchIndexRequest); err != nil {
			return grpcx.Wrap(err, "acme.search.v1.SearchIndexValidationService/NormalizeSearchIndex")
		}
		if _, err := s.parcelQueryClient.SummarizeParcels(ctx, &shippingv1.SummarizeParcelsRequest{
			Context: msg.GetContext(),
			Details: &shippingv1.SummarizeParcelsRequest_Details{
				Format:  msg.GetHeader().GetFormat(),
				Filters: msg.GetRecord().GetSpecs(),
			},
		}); err != nil {
			return grpcx.Wrap(err, "acme.shipping.v1.ParcelQueryService/SummarizeParcels")
		}
		browseWarehousesRequest := &warehousingv1.BrowseWarehousesRequest{Context: msg.GetContext()}
		if _, err := grpcx.Call(ctx, s.warehouseQueryClient.BrowseWarehouses, browseWarehousesRequest); err != nil {
			return grpcx.Wrap(err, "acme.warehousing.v1.WarehouseQueryService/BrowseWarehouses")
		}
		previewDocumentRequest := &documentsv1.PreviewDocumentRequest{
			Context: msg.GetContext(),
			Document: &documentsv1.Document{
				BillingAddress: &commonv1.PostalAddress{PostalCode: msg.GetRecord().GetAddress().GetPostalCode()},
			},
		}
		if _, err := s.meteredDocumentValidation.PreviewDocument(ctx, previewDocumentRequest); err != nil {
			return grpcx.Wrap(err, "acme.documents.v1.DocumentValidationService/PreviewDocument")
		}
		accepted++
	}
	return stream.SendAndClose(&warehousingv1.BatchCreateWarehousesResponse{Accepted: accepted})
}

// CloneWarehouse creates a warehouse.
func (s *WarehouseServer) CloneWarehouse(ctx context.Context, req *warehousingv1.CloneWarehouseRequest) (*warehousingv1.CloneWarehouseResponse, error) {
	if err := s.store.SaveWarehouse(ctx, req.GetContext().GetTenantId(), req.GetWarehouse()); err != nil {
		return nil, grpcx.Wrap(err, "save warehouse")
	}
	s.log.InfoContext(ctx, "clone warehouse", "tenant", req.GetContext().GetTenantId(), "etag", req.GetWarehouse().GetAudit().GetEtag())
	warehouseCache := cache.New[string, *warehousingv1.Warehouse](1)
	warehouseCache.Put(req.GetContext().GetTraceId(), req.GetWarehouse())
	cachedWarehouse, _ := warehouseCache.Get(req.GetContext().GetTraceId())
	peekWarehouseRequest := newPeekWarehouseRequest(withPeekWarehouseContext(req.GetContext()), withPeekWarehouseWarehouseId(cachedWarehouse.GetWarehouseId()))
	peekWarehouseReply, err := s.warehouseQueryClient.PeekWarehouse(ctx, peekWarehouseRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.WarehouseQueryService/PeekWarehouse")
	}
	return &warehousingv1.CloneWarehouseResponse{
		Warehouse: &warehousingv1.Warehouse{
			CategoryPath: peekWarehouseReply.GetWarehouse().GetCategoryPath(),
			Audit: &commonv1.AuditInfo{
				Etag: peekWarehouseReply.GetWarehouse().GetAudit().GetEtag(),
			},
			Capacity: req.GetWarehouse().GetCapacity(),
			Owner:    req.GetWarehouse().GetOwner(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// warehouseServerDeleteWarehouseResolveParcelsStep carries the request of one step of DeleteWarehouse and, once it ran,
// its reply.
type warehouseServerDeleteWarehouseResolveParcelsStep struct {
	request *shippingv1.ResolveParcelsRequest
	reply   *shippingv1.ResolveParcelsResponse
}

func (s *WarehouseServer) runDeleteWarehouseResolveParcels(ctx context.Context, step *warehouseServerDeleteWarehouseResolveParcelsStep) error {
	reply, err := s.parcelQueryClient.ResolveParcels(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}

// warehouseServerWatchWarehousesPreviewPickListStep carries the request of one step of WatchWarehouses and, once it
// ran, its reply.
type warehouseServerWatchWarehousesPreviewPickListStep struct {
	request *warehousingv1.PreviewPickListRequest
	reply   *warehousingv1.PreviewPickListResponse
}

func (s *WarehouseServer) runWatchWarehousesPreviewPickList(ctx context.Context, step *warehouseServerWatchWarehousesPreviewPickListStep) error {
	reply, err := s.pickListValidationClient.PreviewPickList(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
