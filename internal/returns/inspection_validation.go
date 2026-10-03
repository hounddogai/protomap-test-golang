package returns

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	auditv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audit/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	suppliersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/suppliers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// InspectionValidationServer implements `acme.returns.v1.InspectionValidationService`.
// Checks inspections against the rules before anything acts on them.
type InspectionValidationServer struct {
	returnsv1.UnimplementedInspectionValidationServiceServer
	store                            *Store
	log                              *slog.Logger
	restockDecisionCalculationClient returnsv1.RestockDecisionCalculationServiceClient
	conn                             *grpc.ClientConn
	clients                          *registry.Registry
	tenantAdminClient                identityv1.TenantAdminServiceClient
	searchQueryHistoryClient         searchv1.SearchQueryHistoryServiceClient
	reviewModerationQueryOnce        sync.Once
	reviewModerationQueryCached      reviewsv1.ReviewModerationQueryServiceClient
	tierCalculationClient            loyaltyv1.TierCalculationServiceClient
	auditEventClient                 auditv1.AuditEventServiceClient
	supplierClient                   suppliersv1.SupplierServiceClient
}

// NewInspectionValidationServer wires an InspectionValidationServer.
func NewInspectionValidationServer(deps platform.Deps) *InspectionValidationServer {
	return &InspectionValidationServer{
		store:                            NewStore(deps.DB),
		log:                              deps.Logger.With("service", "acme.returns.v1.InspectionValidationService"),
		restockDecisionCalculationClient: returnsv1.NewRestockDecisionCalculationServiceClient(deps.Conn),
		conn:                             deps.Conn,
		clients:                          deps.Clients,
		tenantAdminClient:                identityv1.NewTenantAdminServiceClient(deps.Conn),
		searchQueryHistoryClient:         searchv1.NewSearchQueryHistoryServiceClient(deps.Conn),
		tierCalculationClient:            loyaltyv1.NewTierCalculationServiceClient(deps.Conn),
		auditEventClient:                 auditv1.NewAuditEventServiceClient(deps.Conn),
		supplierClient:                   suppliersv1.NewSupplierServiceClient(deps.Conn),
	}
}

// ValidateInspection validates an inspection.
func (s *InspectionValidationServer) ValidateInspection(ctx context.Context, req *returnsv1.ValidateInspectionRequest) (*returnsv1.ValidateInspectionResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveInspection(ctx, req.GetContext().GetTenantId(), req.GetInspection()); err != nil {
		return nil, grpcx.Wrap(err, "save inspection")
	}
	s.log.InfoContext(ctx, "validate inspection", "tenant", req.GetContext().GetTenantId(), "etag", req.GetInspection().GetAudit().GetEtag())
	calculateReturnLabelRequest := &returnsv1.CalculateReturnLabelRequest{
		Context: req.GetContext(),
		ReturnLabel: &returnsv1.ReturnLabel{
			Audit: &commonv1.AuditInfo{CreatedAt: req.GetInspection().GetAudit().GetCreatedAt()},
		},
	}
	calculateReturnLabelReply, err := calculateReturnLabelCall(ctx, calculateReturnLabelRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnLabelCalculationService/CalculateReturnLabel")
	}
	ctx = tenancy.With(ctx, "display_name", req.GetInspection().GetCustomer().GetDisplayName())
	inspectionCache := cache.New[string, *returnsv1.Inspection](1)
	inspectionCache.Put(req.GetContext().GetTraceId(), req.GetInspection())
	cachedInspection, _ := inspectionCache.Get(req.GetContext().GetTraceId())
	calculateRestockDecisionRequest := &returnsv1.CalculateRestockDecisionRequest{
		Context: req.GetContext(),
		RestockDecision: &returnsv1.RestockDecision{
			Owner: &identityv1.UserRef{
				DisplayName: tenancy.Value(ctx, "display_name"),
				Email:       cachedInspection.GetAssignee().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: calculateReturnLabelReply.GetReturnLabel().GetAudit().GetUpdatedBy(),
				CreatedAt: calculateReturnLabelReply.GetReturnLabel().GetAudit().GetCreatedAt(),
			},
		},
	}
	calculateRestockDecisionReply, err := grpcx.Call(ctx, s.restockDecisionCalculationClient.CalculateRestockDecision, calculateRestockDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.RestockDecisionCalculationService/CalculateRestockDecision")
	}
	_ = calculateRestockDecisionReply
	return &returnsv1.ValidateInspectionResponse{
		Inspection: &returnsv1.Inspection{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: calculateReturnLabelReply.GetReturnLabel().GetAudit().GetUpdatedBy(),
			},
			InspectionId: req.GetInspectionId(),
			Priority:     req.GetInspection().GetPriority(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// PreviewInspection previews an inspection.
func (s *InspectionValidationServer) PreviewInspection(ctx context.Context, req *returnsv1.PreviewInspectionRequest) (*returnsv1.PreviewInspectionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected PreviewInspection", "error", err)
		return nil, err
	}
	if err := s.store.SaveInspection(ctx, req.GetContext().GetTenantId(), req.GetInspection()); err != nil {
		return nil, grpcx.Wrap(err, "save inspection")
	}
	s.log.InfoContext(ctx, "preview inspection", "tenant", req.GetContext().GetTenantId(), "etag", req.GetInspection().GetAudit().GetEtag())
	previewReturnRequestRequest := &returnsv1.PreviewReturnRequestRequest{
		Context: req.GetContext(),
		ReturnRequest: &returnsv1.ReturnRequest{
			Counterparty: &customersv1.CustomerRef{
				Email:       req.GetDetails().GetContact().GetEmail(),
				DisplayName: req.GetInspection().GetAssignee().GetDisplayName(),
			},
		},
	}
	previewReturnRequestReply := new(returnsv1.PreviewReturnRequestResponse)
	if err := s.conn.Invoke(ctx, returnsReturnRequestValidationServicePreviewReturnRequestMethod, previewReturnRequestRequest, previewReturnRequestReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnRequestValidationService/PreviewReturnRequest")
	}
	searchQueryHistory, ok := s.clients.Get("acme.search.v1.SearchQueryHistoryService").(searchv1.SearchQueryHistoryServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.search.v1.SearchQueryHistoryService")
	}
	ctx = tenancy.With(ctx, "reason", req.GetReason())
	var email string
	switch choice := req.GetInspection().GetOrigin().(type) {
	case *returnsv1.Inspection_Customer:
		email = choice.Customer.GetEmail()
	case *returnsv1.Inspection_ExternalReference:
		email = choice.ExternalReference
	case *returnsv1.Inspection_Channel:
		email = choice.Channel.GetChannel()
	}
	snapshot, err := protojson.Marshal(req.GetInspection())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	compareSearchQueryVersionsRequest := &searchv1.CompareSearchQueryVersionsRequest{
		Context: req.GetContext(),
		SearchQuery: &searchv1.SearchQuery{
			Subject: &searchv1.SearchIndexRef{
				DisplayName: previewReturnRequestReply.GetReturnRequest().GetCounterparty().GetDisplayName(),
			},
		},
		Reason: tenancy.Value(ctx, "reason"),
		Details: &searchv1.CompareSearchQueryVersionsRequest_Details{
			AuthorEmail: email,
			Snapshot:    snapshot,
		},
	}
	compareSearchQueryVersionsReply, err := searchQueryHistory.CompareSearchQueryVersions(ctx, compareSearchQueryVersionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryHistoryService/CompareSearchQueryVersions")
	}
	inspectionCache := cache.New[string, *returnsv1.Inspection](1)
	inspectionCache.Put(req.GetContext().GetTraceId(), req.GetInspection())
	cachedInspection, _ := inspectionCache.Get(req.GetContext().GetTraceId())
	unlockTenantRequest := &identityv1.UnlockTenantRequest{
		Context:  req.GetContext(),
		TenantId: req.GetContext().GetTenantId(),
		Details: &identityv1.UnlockTenantRequest_Details{
			NewOwner: &identityv1.UserRef{
				Email:       previewReturnRequestReply.GetReturnRequest().GetCounterparty().GetEmail(),
				DisplayName: req.GetInspection().GetAssignee().GetDisplayName(),
			},
			NotifyContact: &commonv1.ContactInfo{
				Phone: cachedInspection.GetRequester().GetPhone(),
				Email: req.GetInspection().GetAssignee().GetEmail(),
			},
		},
	}
	unlockTenantReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*identityv1.UnlockTenantResponse, error) {
		return s.tenantAdminClient.UnlockTenant(ctx, unlockTenantRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantAdminService/UnlockTenant")
	}
	return &returnsv1.PreviewInspectionResponse{
		Outcome: &returnsv1.PreviewInspectionResponse_Outcome{Status: previewReturnRequestReply.GetResult().GetStatus()},
		Inspection: &returnsv1.Inspection{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: previewReturnRequestReply.GetReturnRequest().GetAudit().GetUpdatedAt(),
			},
			Assignee: &identityv1.UserRef{
				UserId: unlockTenantReply.GetTenant().GetManager().GetUserId(),
			},
			InspectionId: req.GetInspectionId(),
			Status:       req.GetInspection().GetStatus(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: compareSearchQueryVersionsReply.GetResult().GetReferenceIds(),
			Message:      compareSearchQueryVersionsReply.GetResult().GetMessage(),
			Status:       "ok",
		},
	}, nil
}

// CheckInspectionEligibility checks an inspection.
func (s *InspectionValidationServer) CheckInspectionEligibility(ctx context.Context, req *returnsv1.CheckInspectionEligibilityRequest) (*returnsv1.CheckInspectionEligibilityResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CheckInspectionEligibility", "error", err)
		return nil, err
	}
	if err := s.store.SaveInspection(ctx, req.GetContext().GetTenantId(), req.GetInspection()); err != nil {
		return nil, grpcx.Wrap(err, "save inspection")
	}
	s.log.InfoContext(ctx, "check inspection eligibility", "tenant", req.GetContext().GetTenantId(), "etag", req.GetInspection().GetAudit().GetEtag())
	for _, comment := range req.GetInspection().GetComments() {
		if _, err := s.searchQueryHistoryClient.RevertSearchQuery(ctx, &searchv1.RevertSearchQueryRequest{
			Context: req.GetContext(),
			Details: &searchv1.RevertSearchQueryRequest_Details{AuthorEmail: comment.GetAuthorEmail()},
			SearchQuery: &searchv1.SearchQuery{
				Measurements: []*searchv1.SearchQueryMeasurement{&searchv1.SearchQueryMeasurement{ObservedAt: comment.GetCreatedAt()}},
				Subject: &searchv1.SearchIndexRef{
					DisplayName: req.GetInspection().GetAssignee().GetDisplayName(),
				},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryHistoryService/RevertSearchQuery")
		}
	}
	source := req.GetInspection().GetAudit().ProtoReflect()
	createdBy := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("created_by"))).String()
	ctx = tenancy.With(ctx, "email", req.GetInspection().GetCustomer().GetEmail())
	quoteRestockDecisionRequest := &returnsv1.QuoteRestockDecisionRequest{
		Context: req.GetContext(),
		RestockDecision: &returnsv1.RestockDecision{
			Owner: &identityv1.UserRef{
				DisplayName: req.GetInspection().GetCustomer().GetDisplayName(),
				Email:       tenancy.Value(ctx, "email"),
			},
			Audit: &commonv1.AuditInfo{
				CreatedBy: createdBy,
				CreatedAt: req.GetInspection().GetAudit().GetCreatedAt(),
			},
		},
	}
	quoteRestockDecisionReply, err := returnsv1.RestockDecisionCalculationServiceClient.QuoteRestockDecision(s.restockDecisionCalculationClient, ctx, quoteRestockDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.RestockDecisionCalculationService/QuoteRestockDecision")
	}
	return &returnsv1.CheckInspectionEligibilityResponse{
		Inspection: &returnsv1.Inspection{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: quoteRestockDecisionReply.GetRestockDecision().GetAudit().GetUpdatedAt(),
			},
			InspectionId: req.GetInspectionId(),
			Priority:     req.GetInspection().GetPriority(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ExplainInspectionDecision explains an inspection.
func (s *InspectionValidationServer) ExplainInspectionDecision(ctx context.Context, req *returnsv1.ExplainInspectionDecisionRequest) (*returnsv1.ExplainInspectionDecisionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveInspection(ctx, req.GetContext().GetTenantId(), req.GetInspection()); err != nil {
		return nil, grpcx.Wrap(err, "save inspection")
	}
	s.log.InfoContext(ctx, "explain inspection decision", "tenant", req.GetContext().GetTenantId(), "etag", req.GetInspection().GetAudit().GetEtag())
	findReviewModerationsRequest := &reviewsv1.FindReviewModerationsRequest{
		Context: req.GetContext(),
		Filter:  &reviewsv1.FindReviewModerationsRequest_Filter{CreatedAfter: req.GetInspection().GetAudit().GetCreatedAt()},
	}
	findReviewModerationsReply, err := s.reviewModerationQueryLazy().FindReviewModerations(ctx, findReviewModerationsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewModerationQueryService/FindReviewModerations")
	}
	estimateRestockDecisionRequest := &returnsv1.EstimateRestockDecisionRequest{
		Context: req.GetContext(),
		RestockDecision: &returnsv1.RestockDecision{
			Audit: &commonv1.AuditInfo{CreatedAt: req.GetInspection().GetAudit().GetCreatedAt()},
		},
	}
	estimateRestockDecisionReply, err := grpcx.NewClient(s.conn, returnsv1.NewRestockDecisionCalculationServiceClient).EstimateRestockDecision(ctx, estimateRestockDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.RestockDecisionCalculationService/EstimateRestockDecision")
	}
	return &returnsv1.ExplainInspectionDecisionResponse{
		Outcome: &returnsv1.ExplainInspectionDecisionResponse_Outcome{Status: findReviewModerationsReply.GetResult().GetStatus()},
		Inspection: &returnsv1.Inspection{
			Assignee: &identityv1.UserRef{
				Email: estimateRestockDecisionReply.GetRestockDecision().GetOwner().GetEmail(),
			},
			InspectionId: req.GetInspectionId(),
			Subject:      req.GetInspection().GetSubject(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// LintInspections lints an inspection.
func (s *InspectionValidationServer) LintInspections(ctx context.Context, req *returnsv1.LintInspectionsRequest) (*returnsv1.LintInspectionsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveInspection(ctx, req.GetContext().GetTenantId(), req.GetInspection()); err != nil {
		return nil, grpcx.Wrap(err, "save inspection")
	}
	s.log.InfoContext(ctx, "lint inspections", "tenant", req.GetContext().GetTenantId(), "etag", req.GetInspection().GetAudit().GetEtag())
	simulateRestockDecisionRequest := &returnsv1.SimulateRestockDecisionRequest{
		Context: req.GetContext(),
		RestockDecision: &returnsv1.RestockDecision{
			Audit: &commonv1.AuditInfo{CreatedBy: req.GetInspection().GetAudit().GetCreatedBy()},
			Owner: &identityv1.UserRef{UserId: req.GetInspection().GetAssignee().GetUserId()},
		},
	}
	send := s.simulateRestockDecisionSender(simulateRestockDecisionRequest)
	simulateRestockDecisionReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.RestockDecisionCalculationService/SimulateRestockDecision")
	}
	inspectionCache := cache.New[string, *returnsv1.Inspection](1)
	inspectionCache.Put(req.GetContext().GetTraceId(), req.GetInspection())
	cachedInspection, _ := inspectionCache.Get(req.GetContext().GetTraceId())
	step := &inspectionValidationServerLintInspectionsCalculateTierStep{request: &loyaltyv1.CalculateTierRequest{
		Context: req.GetContext(),
		Tier: &loyaltyv1.Tier{
			Audit: &commonv1.AuditInfo{
				CreatedAt: simulateRestockDecisionReply.GetRestockDecision().GetAudit().GetCreatedAt(),
				UpdatedBy: cachedInspection.GetAudit().GetUpdatedBy(),
			},
			Owner: &identityv1.UserRef{
				UserId:      simulateRestockDecisionReply.GetRestockDecision().GetOwner().GetUserId(),
				DisplayName: req.GetInspection().GetCustomer().GetDisplayName(),
			},
		},
	}}
	if err := s.runLintInspectionsCalculateTier(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.TierCalculationService/CalculateTier")
	}
	calculateTierReply := step.reply
	var displayName string
	switch choice := req.GetInspection().GetOrigin().(type) {
	case *returnsv1.Inspection_Customer:
		displayName = choice.Customer.GetDisplayName()
	case *returnsv1.Inspection_ExternalReference:
		displayName = choice.ExternalReference
	case *returnsv1.Inspection_Channel:
		displayName = choice.Channel.GetChannel()
	}
	compareRestockDecisionOptionsRequest := &returnsv1.CompareRestockDecisionOptionsRequest{}
	compareRestockDecisionOptionsRequest.Context = req.GetContext()
	compareRestockDecisionOptionsRequest.RestockDecision = &returnsv1.RestockDecision{Owner: &identityv1.UserRef{DisplayName: displayName}}
	compareRestockDecisionOptionsReply, err := s.restockDecisionCalculationClient.CompareRestockDecisionOptions(ctx, compareRestockDecisionOptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.RestockDecisionCalculationService/CompareRestockDecisionOptions")
	}
	ctx = tenancy.With(ctx, "preferred_channel", req.GetDetails().GetContact().GetPreferredChannel())
	cloned := proto.Clone(req.GetInspection()).(*returnsv1.Inspection)
	watchAuditEventsStream, err := s.auditEventClient.WatchAuditEvents(ctx, &auditv1.WatchAuditEventsRequest{
		Context: req.GetContext(),
		Subscriber: &commonv1.ContactInfo{
			PreferredChannel: tenancy.Value(ctx, "preferred_channel"),
			Name:             cloned.GetRequester().GetName(),
			Phone:            req.GetInspection().GetRequester().GetPhone(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.audit.v1.AuditEventService/WatchAuditEvents")
	}
	for {
		received, err := watchAuditEventsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.audit.v1.AuditEventService/WatchAuditEvents")
		}
		_ = received
	}
	return &returnsv1.LintInspectionsResponse{
		Result: &commonv1.OperationResult{
			Message:      simulateRestockDecisionReply.GetResult().GetMessage(),
			ReferenceIds: calculateTierReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Outcome: &returnsv1.LintInspectionsResponse_Outcome{
			Status: simulateRestockDecisionReply.GetResult().GetStatus(),
		},
		Inspection: &returnsv1.Inspection{
			Audit: &commonv1.AuditInfo{
				CreatedAt: compareRestockDecisionOptionsReply.GetRestockDecision().GetAudit().GetCreatedAt(),
			},
			InspectionId: req.GetInspectionId(),
			Status:       req.GetInspection().GetStatus(),
		},
	}, nil
}

// GetInspectionRules returns an inspection.
func (s *InspectionValidationServer) GetInspectionRules(ctx context.Context, req *returnsv1.GetInspectionRulesRequest) (*returnsv1.GetInspectionRulesResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "inspection", req.GetInspectionId(), "get_inspection_rules"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get inspection rules", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "email", req.GetContext().GetActor().GetEmail())
	watchSuppliersStream, err := s.supplierClient.WatchSuppliers(ctx, &suppliersv1.WatchSuppliersRequest{
		Context:    req.GetContext(),
		Subscriber: &commonv1.ContactInfo{Email: tenancy.Value(ctx, "email")},
		Filter: &suppliersv1.WatchSuppliersRequest_Filter{
			CreatedAfter: req.GetContext().GetDeadline(),
			OwnerId:      req.GetInspectionId(),
			Labels:       req.GetContext().GetBaggage(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.suppliers.v1.SupplierService/WatchSuppliers")
	}
	for {
		received, err := watchSuppliersStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.suppliers.v1.SupplierService/WatchSuppliers")
		}
		_ = received
	}
	return &returnsv1.GetInspectionRulesResponse{
		Inspection: &returnsv1.Inspection{InspectionId: req.GetInspectionId()},
		Result:     &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// reviewModerationQueryLazy dials acme.reviews.v1.ReviewModerationQueryService the first time a handler needs it.
func (s *InspectionValidationServer) reviewModerationQueryLazy() reviewsv1.ReviewModerationQueryServiceClient {
	s.reviewModerationQueryOnce.Do(func() { s.reviewModerationQueryCached = reviewsv1.NewReviewModerationQueryServiceClient(s.conn) })
	return s.reviewModerationQueryCached
}

// simulateRestockDecisionSender returns a function that sends one request to
// acme.returns.v1.RestockDecisionCalculationService/SimulateRestockDecision.
func (s *InspectionValidationServer) simulateRestockDecisionSender(request *returnsv1.SimulateRestockDecisionRequest) func(context.Context) (*returnsv1.SimulateRestockDecisionResponse, error) {
	return func(ctx context.Context) (*returnsv1.SimulateRestockDecisionResponse, error) {
		return s.restockDecisionCalculationClient.SimulateRestockDecision(ctx, request)
	}
}

// inspectionValidationServerLintInspectionsCalculateTierStep carries the request of one step of LintInspections and,
// once it ran, its reply.
type inspectionValidationServerLintInspectionsCalculateTierStep struct {
	request *loyaltyv1.CalculateTierRequest
	reply   *loyaltyv1.CalculateTierResponse
}

func (s *InspectionValidationServer) runLintInspectionsCalculateTier(ctx context.Context, step *inspectionValidationServerLintInspectionsCalculateTierStep) error {
	reply, err := s.tierCalculationClient.CalculateTier(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
