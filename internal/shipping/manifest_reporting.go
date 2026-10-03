package shipping

import (
	"context"
	"errors"
	"io"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// ManifestReportingServer implements `acme.shipping.v1.ManifestReportingService`.
// Builds and schedules reports about manifests.
type ManifestReportingServer struct {
	shippingv1.UnimplementedManifestReportingServiceServer
	store                          *Store
	log                            *slog.Logger
	trackingEventReportingClient   shippingv1.TrackingEventReportingServiceClient
	customerCalculationClient      customersv1.CustomerCalculationServiceClient
	collectionApprovalClient       merchandisingv1.CollectionApprovalServiceClient
	conn                           *grpc.ClientConn
	tenantExportClient             identityv1.TenantExportServiceClient
	sellerApprovalClient           sellersv1.SellerApprovalServiceClient
	manifestCalculationClient      shippingv1.ManifestCalculationServiceClient
	synonymExportClient            searchv1.SynonymExportServiceClient
	consentRecordValidationClient  customersv1.ConsentRecordValidationServiceClient
	restockDecisionReportingClient returnsv1.RestockDecisionReportingServiceClient
	manifestSyncClient             shippingv1.ManifestSyncServiceClient
}

// NewManifestReportingServer wires a ManifestReportingServer.
func NewManifestReportingServer(deps platform.Deps) *ManifestReportingServer {
	return &ManifestReportingServer{
		store:                          NewStore(deps.DB),
		log:                            deps.Logger.With("service", "acme.shipping.v1.ManifestReportingService"),
		trackingEventReportingClient:   shippingv1.NewTrackingEventReportingServiceClient(deps.Conn),
		customerCalculationClient:      customersv1.NewCustomerCalculationServiceClient(deps.Conn),
		collectionApprovalClient:       merchandisingv1.NewCollectionApprovalServiceClient(deps.Conn),
		conn:                           deps.Conn,
		tenantExportClient:             identityv1.NewTenantExportServiceClient(deps.Conn),
		sellerApprovalClient:           sellersv1.NewSellerApprovalServiceClient(deps.Conn),
		manifestCalculationClient:      shippingv1.NewManifestCalculationServiceClient(deps.Conn),
		synonymExportClient:            searchv1.NewSynonymExportServiceClient(deps.Conn),
		consentRecordValidationClient:  customersv1.NewConsentRecordValidationServiceClient(deps.Conn),
		restockDecisionReportingClient: returnsv1.NewRestockDecisionReportingServiceClient(deps.Conn),
		manifestSyncClient:             shippingv1.NewManifestSyncServiceClient(deps.Conn),
	}
}

// GenerateManifestReport generates a manifest.
func (s *ManifestReportingServer) GenerateManifestReport(ctx context.Context, req *shippingv1.GenerateManifestReportRequest) (*shippingv1.GenerateManifestReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GenerateManifestReport", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "manifest", req.GetManifestId(), "generate_manifest_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "generate manifest report", "tenant", req.GetContext().GetTenantId())
	streamTrackingEventMetricsStream, err := s.trackingEventReportingClient.StreamTrackingEventMetrics(ctx, &shippingv1.StreamTrackingEventMetricsRequest{
		Context:    req.GetContext(),
		Subscriber: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		Filter:     &shippingv1.StreamTrackingEventMetricsRequest_Filter{Labels: req.GetDetails().GetFilters()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.TrackingEventReportingService/StreamTrackingEventMetrics")
	}
	for {
		received, err := streamTrackingEventMetricsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.shipping.v1.TrackingEventReportingService/StreamTrackingEventMetrics")
		}
		_ = received
	}
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := manifestReportingServerSimulateCustomerSteps[mode]
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	simulateCustomerRequest := &customersv1.SimulateCustomerRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			RelationshipManager: &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
			Contact:             &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
		Reason:      req.GetReason(),
		Annotations: annotations,
	}
	simulateCustomerReply, err := run(s, ctx, simulateCustomerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerCalculationService/SimulateCustomer")
	}
	return &shippingv1.GenerateManifestReportResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: simulateCustomerReply.GetResult().GetCorrelationId(),
			ReferenceIds:  simulateCustomerReply.GetResult().GetReferenceIds(),
			Status:        "ok",
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// FetchManifestReport returns a manifest.
func (s *ManifestReportingServer) FetchManifestReport(ctx context.Context, req *shippingv1.FetchManifestReportRequest) (*shippingv1.FetchManifestReportResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "manifest", req.GetManifestId(), "fetch_manifest_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "fetch manifest report", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "user_id", req.GetContext().GetActor().GetUserId())
	delegateCollectionApprovalRequest := newDelegateCollectionApprovalRequest(withDelegateCollectionApprovalContext(req.GetContext()), withDelegateCollectionApprovalCollection(&merchandisingv1.Collection{
		Manager: &identityv1.UserRef{Email: req.GetContext().GetActor().GetEmail()},
		Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
			Manager: &identityv1.UserRef{UserId: tenancy.Value(ctx, "user_id")},
			Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
				Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{ParentId: req.GetManifestId()}},
			}},
		}},
	}))
	delegateCollectionApprovalReply, err := s.collectionApprovalClient.DelegateCollectionApproval(ctx, delegateCollectionApprovalRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.CollectionApprovalService/DelegateCollectionApproval")
	}
	return &shippingv1.FetchManifestReportResponse{
		Manifest: &shippingv1.Manifest{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: delegateCollectionApprovalReply.GetCollection().GetAudit().GetUpdatedBy(),
				CreatedBy: delegateCollectionApprovalReply.GetCollection().GetAudit().GetCreatedBy(),
			},
			ManifestId: req.GetManifestId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ScheduleManifestReport schedules a manifest.
func (s *ManifestReportingServer) ScheduleManifestReport(ctx context.Context, req *shippingv1.ScheduleManifestReportRequest) (*shippingv1.ScheduleManifestReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ScheduleManifestReport", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "manifest", req.GetManifestId(), "schedule_manifest_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "schedule manifest report", "tenant", req.GetContext().GetTenantId())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	ctx = tenancy.With(ctx, "reason", req.GetReason())
	snapshot, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	reconcileManifestsRequest := &shippingv1.ReconcileManifestsRequest{
		Context:     req.GetContext(),
		Manifest:    &shippingv1.Manifest{ManifestId: req.GetManifestId()},
		Annotations: annotations,
		Reason:      tenancy.Value(ctx, "reason"),
		Details:     &shippingv1.ReconcileManifestsRequest_Details{Payload: snapshot},
	}
	reconcileManifestsReply, err := reconcileManifestsCall(ctx, reconcileManifestsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ManifestSyncService/ReconcileManifests")
	}
	resumeCollectionWorkflowRequest := &merchandisingv1.ResumeCollectionWorkflowRequest{
		Context: req.GetContext(),
		Collection: &merchandisingv1.Collection{
			Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
				Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
					Location: &commonv1.PostalAddress{
						Lines:  reconcileManifestsReply.GetManifest().GetBillingAddress().GetLines(),
						Region: reconcileManifestsReply.GetManifest().GetBillingAddress().GetRegion(),
					},
				}},
			}},
		},
	}
	resumeCollectionWorkflowReply := new(merchandisingv1.ResumeCollectionWorkflowResponse)
	if err := s.conn.Invoke(ctx, merchandisingCollectionWorkflowServiceResumeCollectionWorkflowMethod, resumeCollectionWorkflowRequest, resumeCollectionWorkflowReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.CollectionWorkflowService/ResumeCollectionWorkflow")
	}
	return &shippingv1.ScheduleManifestReportResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: reconcileManifestsReply.GetResult().GetCorrelationId(),
			Message:       reconcileManifestsReply.GetResult().GetMessage(),
			Status:        resumeCollectionWorkflowReply.GetResult().GetStatus(),
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// ListManifestReports lists manifests a page at a time.
func (s *ManifestReportingServer) ListManifestReports(ctx context.Context, req *shippingv1.ListManifestReportsRequest) (*shippingv1.ListManifestReportsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "list manifest reports", "tenant", req.GetContext().GetTenantId())
	streamTenantExportStream, err := s.tenantExportClient.StreamTenantExport(ctx, &identityv1.StreamTenantExportRequest{
		Context: req.GetContext(),
		Filter: &identityv1.StreamTenantExportRequest_Filter{
			Region:       req.GetFilter().GetRegion(),
			Labels:       req.GetFilter().GetLabels(),
			OwnerId:      req.GetFilter().GetOwnerId(),
			CreatedAfter: req.GetFilter().GetCreatedAfter(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantExportService/StreamTenantExport")
	}
	for {
		received, err := streamTenantExportStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.identity.v1.TenantExportService/StreamTenantExport")
		}
		_ = received
	}
	labels := make(map[string]string, len(req.GetFilter().GetLabels()))
	for key, value := range req.GetFilter().GetLabels() {
		labels[key] = value
	}
	ctx = tenancy.With(ctx, "query", req.GetFilter().GetQuery())
	listPendingSellersRequest := &sellersv1.ListPendingSellersRequest{
		Context: req.GetContext(),
		Page:    &commonv1.PageRequest{PageToken: req.GetPage().GetPageToken()},
		Filter: &sellersv1.ListPendingSellersRequest_Filter{
			CreatedAfter: req.GetFilter().GetCreatedAfter(),
			Labels:       labels,
			Query:        tenancy.Value(ctx, "query"),
		},
	}
	type listPendingSellersResult struct {
		reply *sellersv1.ListPendingSellersResponse
		err   error
	}
	listPendingSellersResults := make(chan listPendingSellersResult, 1)
	go func() {
		reply, err := s.sellerApprovalClient.ListPendingSellers(ctx, listPendingSellersRequest)
		listPendingSellersResults <- listPendingSellersResult{reply, err}
	}()
	var listPendingSellersReply *sellersv1.ListPendingSellersResponse
	select {
	case result := <-listPendingSellersResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.sellers.v1.SellerApprovalService/ListPendingSellers")
		}
		listPendingSellersReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &shippingv1.ListManifestReportsResponse{
		PageInfo: &commonv1.PageInfo{
			NextPageToken: listPendingSellersReply.GetPageInfo().GetNextPageToken(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: listPendingSellersReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// ShareManifestReport shares a manifest.
func (s *ManifestReportingServer) ShareManifestReport(ctx context.Context, req *shippingv1.ShareManifestReportRequest) (*shippingv1.ShareManifestReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "manifest", req.GetManifestId(), "share_manifest_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "share manifest report", "tenant", req.GetContext().GetTenantId())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	var envelope shippingv1.ShipmentRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	quoteManifestRequest := &shippingv1.QuoteManifestRequest{
		Context: req.GetContext(),
		Manifest: &shippingv1.Manifest{
			Counterparty: &customersv1.CustomerRef{Email: req.GetDetails().GetRecipient().GetEmail()},
		},
		Annotations: annotations,
		ManifestId:  envelope.GetDisplayName(),
	}
	send := s.quoteManifestSender(quoteManifestRequest)
	quoteManifestReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ManifestCalculationService/QuoteManifest")
	}
	var envelope2 shippingv1.ShipmentRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	estimateManifestRequest := &shippingv1.EstimateManifestRequest{
		Context: req.GetContext(),
		Manifest: &shippingv1.Manifest{
			Counterparty: &customersv1.CustomerRef{
				DisplayName: quoteManifestReply.GetManifest().GetCounterparty().GetDisplayName(),
			},
			Settlement: &shippingv1.Manifest_Bank{Bank: &shippingv1.ManifestBankSettlement{
				AccountHolder: quoteManifestReply.GetManifest().GetBank().GetAccountHolder(),
			}},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: quoteManifestReply.GetManifest().GetAudit().GetUpdatedAt(),
			},
		},
		ManifestId: envelope2.GetDisplayName(),
	}
	estimateManifestReply := new(shippingv1.EstimateManifestResponse)
	if err := grpcx.Invoke(ctx, s.conn, shippingv1.ManifestCalculationService_EstimateManifest_FullMethodName, estimateManifestRequest, estimateManifestReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ManifestCalculationService/EstimateManifest")
	}
	return &shippingv1.ShareManifestReportResponse{
		Manifest: &shippingv1.Manifest{
			BillingAddress: quoteManifestReply.GetManifest().GetBillingAddress(),
			Counterparty: &customersv1.CustomerRef{
				Email: estimateManifestReply.GetManifest().GetCounterparty().GetEmail(),
			},
			ManifestId: req.GetManifestId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CountManifestTotals counts manifests by status.
func (s *ManifestReportingServer) CountManifestTotals(ctx context.Context, req *shippingv1.CountManifestTotalsRequest) (*shippingv1.CountManifestTotalsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CountManifestTotals", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "count manifest totals", "tenant", req.GetContext().GetTenantId())
	streamSynonymExportStream, err := s.synonymExportClient.StreamSynonymExport(ctx, &searchv1.StreamSynonymExportRequest{
		Context: req.GetContext(),
		Filter:  &searchv1.StreamSynonymExportRequest_Filter{OwnerId: req.GetFilter().GetOwnerId()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymExportService/StreamSynonymExport")
	}
	for {
		received, err := streamSynonymExportStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.search.v1.SynonymExportService/StreamSynonymExport")
		}
		_ = received
	}
	source := req.GetContext().ProtoReflect()
	locale := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("locale"))).String()
	packed, err := anypb.New(req.GetPage())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	explainConsentRecordDecisionRequest := &customersv1.ExplainConsentRecordDecisionRequest{
		Context: req.GetContext(),
		Details: &customersv1.ExplainConsentRecordDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Locale:  locale,
		},
		ConsentRecord: &customersv1.ConsentRecord{
			Measurements:  []*customersv1.ConsentRecordMeasurement{&customersv1.ConsentRecordMeasurement{ObservedAt: req.GetFilter().GetCreatedAfter()}},
			CorrelationId: req.GetFilter().GetOwnerId(),
			Detail:        packed,
		},
	}
	type explainConsentRecordDecisionResult struct {
		reply *customersv1.ExplainConsentRecordDecisionResponse
		err   error
	}
	explainConsentRecordDecisionResults := make(chan explainConsentRecordDecisionResult, 1)
	go func() {
		reply, err := s.consentRecordValidationClient.ExplainConsentRecordDecision(ctx, explainConsentRecordDecisionRequest)
		explainConsentRecordDecisionResults <- explainConsentRecordDecisionResult{reply, err}
	}()
	var explainConsentRecordDecisionReply *customersv1.ExplainConsentRecordDecisionResponse
	select {
	case result := <-explainConsentRecordDecisionResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.customers.v1.ConsentRecordValidationService/ExplainConsentRecordDecision")
		}
		explainConsentRecordDecisionReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &shippingv1.CountManifestTotalsResponse{
		Result: &commonv1.OperationResult{
			Details: explainConsentRecordDecisionReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}

// DeleteManifestReport deletes a manifest.
func (s *ManifestReportingServer) DeleteManifestReport(ctx context.Context, req *shippingv1.DeleteManifestReportRequest) (*shippingv1.DeleteManifestReportResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "manifest", req.GetManifestId(), "delete_manifest_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "delete manifest report", "tenant", req.GetContext().GetTenantId())
	streamRestockDecisionMetricsStream, err := s.restockDecisionReportingClient.StreamRestockDecisionMetrics(ctx, &returnsv1.StreamRestockDecisionMetricsRequest{
		Context:    req.GetContext(),
		Subscriber: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		Filter:     &returnsv1.StreamRestockDecisionMetricsRequest_Filter{CreatedAfter: req.GetContext().GetDeadline()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.RestockDecisionReportingService/StreamRestockDecisionMetrics")
	}
	for {
		received, err := streamRestockDecisionMetricsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.returns.v1.RestockDecisionReportingService/StreamRestockDecisionMetrics")
		}
		_ = received
	}
	listManifestConflictsRequest := buildListManifestConflictsRequestForDeleteManifestReport(req)
	listManifestConflictsReply, err := s.manifestSyncClient.ListManifestConflicts(ctx, listManifestConflictsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ManifestSyncService/ListManifestConflicts")
	}
	return &shippingv1.DeleteManifestReportResponse{
		Result: &commonv1.OperationResult{
			Message:       listManifestConflictsReply.GetResult().GetMessage(),
			CorrelationId: listManifestConflictsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// manifestReportingServerSimulateCustomerSteps are the ways to run
// acme.customers.v1.CustomerCalculationService/SimulateCustomer, by name.
var manifestReportingServerSimulateCustomerSteps = map[string]func(*ManifestReportingServer, context.Context, *customersv1.SimulateCustomerRequest) (*customersv1.SimulateCustomerResponse, error){
	"default": (*ManifestReportingServer).simulateCustomerStep,
	"audited": (*ManifestReportingServer).simulateCustomerAuditedStep,
}

func (s *ManifestReportingServer) simulateCustomerStep(ctx context.Context, request *customersv1.SimulateCustomerRequest) (*customersv1.SimulateCustomerResponse, error) {
	return s.customerCalculationClient.SimulateCustomer(ctx, request)
}

func (s *ManifestReportingServer) simulateCustomerAuditedStep(ctx context.Context, request *customersv1.SimulateCustomerRequest) (*customersv1.SimulateCustomerResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.customers.v1.CustomerCalculationService/SimulateCustomer")
	return s.customerCalculationClient.SimulateCustomer(ctx, request)
}

// quoteManifestSender returns a function that sends one request to
// acme.shipping.v1.ManifestCalculationService/QuoteManifest.
func (s *ManifestReportingServer) quoteManifestSender(request *shippingv1.QuoteManifestRequest) func(context.Context) (*shippingv1.QuoteManifestResponse, error) {
	return func(ctx context.Context) (*shippingv1.QuoteManifestResponse, error) {
		return s.manifestCalculationClient.QuoteManifest(ctx, request)
	}
}
