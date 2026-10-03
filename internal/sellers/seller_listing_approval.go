package sellers

import (
	"context"
	"fmt"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// SellerListingApprovalServer implements `acme.sellers.v1.SellerListingApprovalService`.
// Routes seller listings through approval, with escalation and delegation.
type SellerListingApprovalServer struct {
	*sellerListingApprovalHandlers
}

// sellerListingApprovalHandlers holds the handlers of SellerListingApprovalServer, with what they use.
type sellerListingApprovalHandlers struct {
	sellersv1.UnimplementedSellerListingApprovalServiceServer
	store                                *Store
	log                                  *slog.Logger
	conn                                 *grpc.ClientConn
	customerSegmentCalculationClient     customersv1.CustomerSegmentCalculationServiceClient
	consentRecordCalculationClient       customersv1.ConsentRecordCalculationServiceClient
	sellerApprovalClient                 sellersv1.SellerApprovalServiceClient
	searchQueryHistoryClient             searchv1.SearchQueryHistoryServiceClient
	sellerPayoutScheduleValidationClient sellersv1.SellerPayoutScheduleValidationServiceClient
	giftCardApprovalClient               giftcardsv1.GiftCardApprovalServiceClient
	clients                              *registry.Registry
	sellerListingReportingClient         sellersv1.SellerListingReportingServiceClient
	sellerListingReportingClientFallback sellersv1.SellerListingReportingServiceClient
	featureFlagValidationPort            featureFlagValidationPort
	searchQueryValidationClient          searchv1.SearchQueryValidationServiceClient
}

// NewSellerListingApprovalServer wires a SellerListingApprovalServer.
func NewSellerListingApprovalServer(deps platform.Deps) *SellerListingApprovalServer {
	return &SellerListingApprovalServer{sellerListingApprovalHandlers: &sellerListingApprovalHandlers{
		store:                                NewStore(deps.DB),
		log:                                  deps.Logger.With("service", "acme.sellers.v1.SellerListingApprovalService"),
		conn:                                 deps.Conn,
		customerSegmentCalculationClient:     customersv1.NewCustomerSegmentCalculationServiceClient(deps.Conn),
		consentRecordCalculationClient:       customersv1.NewConsentRecordCalculationServiceClient(deps.Conn),
		sellerApprovalClient:                 sellersv1.NewSellerApprovalServiceClient(deps.Conn),
		searchQueryHistoryClient:             searchv1.NewSearchQueryHistoryServiceClient(deps.Conn),
		sellerPayoutScheduleValidationClient: sellersv1.NewSellerPayoutScheduleValidationServiceClient(deps.Conn),
		giftCardApprovalClient:               giftcardsv1.NewGiftCardApprovalServiceClient(deps.Conn),
		clients:                              deps.Clients,
		sellerListingReportingClient:         sellersv1.NewSellerListingReportingServiceClient(deps.Conn),
		sellerListingReportingClientFallback: sellersv1.NewSellerListingReportingServiceClient(deps.Conn),
		featureFlagValidationPort:            newFeatureFlagValidationPort(deps),
		searchQueryValidationClient:          searchv1.NewSearchQueryValidationServiceClient(deps.Conn),
	}}
}

// SubmitSellerListing submits a seller listing.
func (s *sellerListingApprovalHandlers) SubmitSellerListing(ctx context.Context, req *sellersv1.SubmitSellerListingRequest) (*sellersv1.SubmitSellerListingResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveSellerListing(ctx, req.GetContext().GetTenantId(), req.GetSellerListing()); err != nil {
		return nil, grpcx.Wrap(err, "save seller listing")
	}
	s.log.InfoContext(ctx, "submit seller listing", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSellerListing().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "component_id", req.GetSellerListing().GetComponents()["primary"].GetComponentId())
	listPendingSellersRequest := &sellersv1.ListPendingSellersRequest{
		Context: req.GetContext(),
		Filter: &sellersv1.ListPendingSellersRequest_Filter{
			Region:       req.GetSellerListing().GetAddress().GetRegion(),
			CreatedAfter: req.GetSellerListing().GetAudit().GetUpdatedAt(),
			OwnerId:      tenancy.Value(ctx, "component_id"),
		},
	}
	listPendingSellersReply := new(sellersv1.ListPendingSellersResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", sellersSellerApprovalServiceName, "ListPendingSellers"), listPendingSellersRequest, listPendingSellersReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerApprovalService/ListPendingSellers")
	}
	var holder struct{ Etag string }
	if record := req.GetSellerListing().GetAudit(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Etag"); err != nil {
			s.log.WarnContext(ctx, "copy etag", "error", err)
		}
	}
	estimateCustomerSegmentRequest := &customersv1.EstimateCustomerSegmentRequest{}
	estimateCustomerSegmentRequest.Context = req.GetContext()
	estimateCustomerSegmentRequest.CustomerSegment = &customersv1.CustomerSegment{
		Owner: &identityv1.UserRef{
			DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
		},
		Audit:       &commonv1.AuditInfo{Etag: holder.Etag},
		Description: req.GetSellerListing().GetComponents()["primary"].GetDescription(),
	}
	estimateCustomerSegmentReply, err := s.customerSegmentCalculationClient.EstimateCustomerSegment(ctx, estimateCustomerSegmentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentCalculationService/EstimateCustomerSegment")
	}
	packed, err := anypb.New(req.GetSellerListing())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	calculateConsentRecordRequest := &customersv1.CalculateConsentRecordRequest{
		Context: req.GetContext(),
		Details: &customersv1.CalculateConsentRecordRequest_Details{
			Destination: &commonv1.PostalAddress{Lines: req.GetSellerListing().GetAddress().GetLines()},
		},
		ConsentRecord: &customersv1.ConsentRecord{Detail: packed},
	}
	send := s.calculateConsentRecordSender(calculateConsentRecordRequest)
	calculateConsentRecordReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordCalculationService/CalculateConsentRecord")
	}
	return &sellersv1.SubmitSellerListingResponse{
		Outcome: &sellersv1.SubmitSellerListingResponse_Outcome{Message: listPendingSellersReply.GetResult().GetMessage()},
		Result: &commonv1.OperationResult{
			CorrelationId: listPendingSellersReply.GetResult().GetCorrelationId(),
			Message:       calculateConsentRecordReply.GetResult().GetMessage(),
			Status:        "ok",
		},
		SellerListing: &sellersv1.SellerListing{
			Name: estimateCustomerSegmentReply.GetCustomerSegment().GetName(),
			Owner: &identityv1.UserRef{
				DisplayName: calculateConsentRecordReply.GetConsentRecord().GetSubject().GetDisplayName(),
			},
			SellerListingId: req.GetSellerListingId(),
			Status:          req.GetSellerListing().GetStatus(),
		},
	}, nil
}

// ApproveSellerListing approves a seller listing.
func (s *sellerListingApprovalHandlers) ApproveSellerListing(ctx context.Context, req *sellersv1.ApproveSellerListingRequest) (*sellersv1.ApproveSellerListingResponse, error) {
	if err := s.store.SaveSellerListing(ctx, req.GetContext().GetTenantId(), req.GetSellerListing()); err != nil {
		return nil, grpcx.Wrap(err, "save seller listing")
	}
	s.log.InfoContext(ctx, "approve seller listing", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSellerListing().GetAudit().GetEtag())
	escalateSellerRequest := &sellersv1.EscalateSellerRequest{
		Context: req.GetContext(),
		Seller: &sellersv1.Seller{
			RelationshipManager: &identityv1.UserRef{Email: req.GetSellerListing().GetOwner().GetEmail()},
			Audit:               &commonv1.AuditInfo{Etag: req.GetSellerListing().GetAudit().GetEtag()},
		},
		Details: &sellersv1.EscalateSellerRequest_Details{
			Approver: &identityv1.UserRef{
				DisplayName: req.GetSellerListing().GetOwner().GetDisplayName(),
			},
		},
	}
	escalateSellerReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*sellersv1.EscalateSellerResponse, error) {
		return s.sellerApprovalClient.EscalateSeller(ctx, escalateSellerRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerApprovalService/EscalateSeller")
	}
	ctx = tenancy.With(ctx, "component_id", req.GetSellerListing().GetComponents()["primary"].GetComponentId())
	listSearchQueryVersionsRequest := &searchv1.ListSearchQueryVersionsRequest{
		Context: req.GetContext(),
		Filter: &searchv1.ListSearchQueryVersionsRequest_Filter{
			Region:       escalateSellerReply.GetSeller().GetPrimaryAddress().GetRegion(),
			CreatedAfter: req.GetSellerListing().GetAudit().GetUpdatedAt(),
			OwnerId:      tenancy.Value(ctx, "component_id"),
		},
	}
	listSearchQueryVersionsReply, err := grpcx.Call(ctx, s.searchQueryHistoryClient.ListSearchQueryVersions, listSearchQueryVersionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryHistoryService/ListSearchQueryVersions")
	}
	return &sellersv1.ApproveSellerListingResponse{
		SellerListing: &sellersv1.SellerListing{
			Owner: &identityv1.UserRef{
				Email: escalateSellerReply.GetSeller().GetRelationshipManager().GetEmail(),
			},
			SellerListingId: req.GetSellerListingId(),
			Code:            req.GetSellerListing().GetCode(),
		},
		Outcome: &sellersv1.ApproveSellerListingResponse_Outcome{
			EffectiveAt: escalateSellerReply.GetOutcome().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{
			Message: listSearchQueryVersionsReply.GetResult().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// RejectSellerListing rejects a seller listing.
func (s *sellerListingApprovalHandlers) RejectSellerListing(ctx context.Context, req *sellersv1.RejectSellerListingRequest) (*sellersv1.RejectSellerListingResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveSellerListing(ctx, req.GetContext().GetTenantId(), req.GetSellerListing()); err != nil {
		return nil, grpcx.Wrap(err, "save seller listing")
	}
	s.log.InfoContext(ctx, "reject seller listing", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSellerListing().GetAudit().GetEtag())
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := sellerListingApprovalHandlersCheckSellerPayoutScheduleEligibilitySteps[mode]
	checkSellerPayoutScheduleEligibilityRequest := &sellersv1.CheckSellerPayoutScheduleEligibilityRequest{
		Context: req.GetContext(),
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
			Location: &commonv1.PostalAddress{
				CountryCode: req.GetSellerListing().GetAddress().GetCountryCode(),
			},
		},
		Details: &sellersv1.CheckSellerPayoutScheduleEligibilityRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetSellerListing().GetOwner().GetEmail()},
		},
	}
	checkSellerPayoutScheduleEligibilityReply, err := run(s, ctx, checkSellerPayoutScheduleEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerPayoutScheduleValidationService/CheckSellerPayoutScheduleEligibility")
	}
	return &sellersv1.RejectSellerListingResponse{
		Outcome: &sellersv1.RejectSellerListingResponse_Outcome{
			ReferenceId: checkSellerPayoutScheduleEligibilityReply.GetOutcome().GetReferenceId(),
		},
		SellerListing: &sellersv1.SellerListing{
			SellerListingId: req.GetSellerListingId(),
			Specs:           req.GetSellerListing().GetSpecs(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// EscalateSellerListing escalates a seller listing.
func (s *sellerListingApprovalHandlers) EscalateSellerListing(ctx context.Context, req *sellersv1.EscalateSellerListingRequest) (*sellersv1.EscalateSellerListingResponse, error) {
	if err := s.store.SaveSellerListing(ctx, req.GetContext().GetTenantId(), req.GetSellerListing()); err != nil {
		return nil, grpcx.Wrap(err, "save seller listing")
	}
	s.log.InfoContext(ctx, "escalate seller listing", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSellerListing().GetAudit().GetEtag())
	sellerListingCache := cache.New[string, *sellersv1.SellerListing](1)
	sellerListingCache.Put(req.GetContext().GetTraceId(), req.GetSellerListing())
	cachedSellerListing, _ := sellerListingCache.Get(req.GetContext().GetTraceId())
	approveGiftCardRequest := &giftcardsv1.ApproveGiftCardRequest{
		Context: req.GetContext(),
		GiftCard: &giftcardsv1.GiftCard{
			Limits: []*giftcardsv1.GiftCardLimit{&giftcardsv1.GiftCardLimit{Name: cachedSellerListing.GetName()}},
		},
	}
	send := s.approveGiftCardSender(approveGiftCardRequest)
	approveGiftCardReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardApprovalService/ApproveGiftCard")
	}
	return &sellersv1.EscalateSellerListingResponse{
		Outcome: &sellersv1.EscalateSellerListingResponse_Outcome{
			EffectiveAt: approveGiftCardReply.GetOutcome().GetEffectiveAt(),
		},
		SellerListing: &sellersv1.SellerListing{
			SellerListingId: req.GetSellerListingId(),
			Name:            req.GetSellerListing().GetName(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ListPendingSellerListings lists seller listings a page at a time.
func (s *sellerListingApprovalHandlers) ListPendingSellerListings(ctx context.Context, req *sellersv1.ListPendingSellerListingsRequest) (*sellersv1.ListPendingSellerListingsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "list pending seller listings", "tenant", req.GetContext().GetTenantId())
	return &sellersv1.ListPendingSellerListingsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// DelegateSellerListingApproval delegates a seller listing.
func (s *sellerListingApprovalHandlers) DelegateSellerListingApproval(ctx context.Context, req *sellersv1.DelegateSellerListingApprovalRequest) (*sellersv1.DelegateSellerListingApprovalResponse, error) {
	if err := s.store.SaveSellerListing(ctx, req.GetContext().GetTenantId(), req.GetSellerListing()); err != nil {
		return nil, grpcx.Wrap(err, "save seller listing")
	}
	s.log.InfoContext(ctx, "delegate seller listing approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSellerListing().GetAudit().GetEtag())
	subscription, err := registry.Lookup[subscriptionsv1.SubscriptionServiceClient](s.clients, "acme.subscriptions.v1.SubscriptionService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	var region string
	switch choice := req.GetSellerListing().GetLocator().(type) {
	case *sellersv1.SellerListing_Address:
		region = choice.Address.GetRegion()
	case *sellersv1.SellerListing_FacilityCode:
		region = choice.FacilityCode
	}
	searchSubscriptionsRequest := &subscriptionsv1.SearchSubscriptionsRequest{
		Context: req.GetContext(),
		Filter:  &subscriptionsv1.SearchSubscriptionsRequest_Filter{Region: region},
	}
	searchSubscriptionsReply, err := subscription.SearchSubscriptions(ctx, searchSubscriptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionService/SearchSubscriptions")
	}
	routes := map[string]func(context.Context, *sellersv1.GenerateSellerListingReportRequest, ...grpc.CallOption) (*sellersv1.GenerateSellerListingReportResponse, error){
		"primary":  s.sellerListingReportingClient.GenerateSellerListingReport,
		"fallback": s.sellerListingReportingClientFallback.GenerateSellerListingReport,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	generateSellerListingReportRequest := &sellersv1.GenerateSellerListingReportRequest{
		Context:         req.GetContext(),
		SellerListingId: req.GetSellerListing().GetSellerListingId(),
		Reason:          req.GetReason(),
		Annotations:     annotations,
		Details:         &sellersv1.GenerateSellerListingReportRequest_Details{Filters: searchSubscriptionsReply.GetResult().GetDetails()},
	}
	generateSellerListingReportReply, err := route(ctx, generateSellerListingReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingReportingService/GenerateSellerListingReport")
	}
	return &sellersv1.DelegateSellerListingApprovalResponse{
		Outcome: &sellersv1.DelegateSellerListingApprovalResponse_Outcome{Message: searchSubscriptionsReply.GetResult().GetMessage()},
		Result: &commonv1.OperationResult{
			ReferenceIds:  searchSubscriptionsReply.GetResult().GetReferenceIds(),
			CorrelationId: generateSellerListingReportReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		SellerListing: &sellersv1.SellerListing{
			SellerListingId: req.GetSellerListingId(),
			Owner:           req.GetSellerListing().GetOwner(),
		},
	}, nil
}

// RecallSellerListing recalls a seller listing.
func (s *sellerListingApprovalHandlers) RecallSellerListing(ctx context.Context, req *sellersv1.RecallSellerListingRequest) (*sellersv1.RecallSellerListingResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveSellerListing(ctx, req.GetContext().GetTenantId(), req.GetSellerListing()); err != nil {
		return nil, grpcx.Wrap(err, "save seller listing")
	}
	s.log.InfoContext(ctx, "recall seller listing", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSellerListing().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetDetails()).(*sellersv1.RecallSellerListingRequest_Details)
	explainFeatureFlagDecisionRequest := &featureflagsv1.ExplainFeatureFlagDecisionRequest{
		Context: req.GetContext(),
		FeatureFlag: &featureflagsv1.FeatureFlag{
			Audit: &commonv1.AuditInfo{UpdatedAt: req.GetSellerListing().GetAudit().GetUpdatedAt()},
			Owner: &identityv1.UserRef{UserId: req.GetSellerListing().GetOwner().GetUserId()},
		},
		Details: &featureflagsv1.ExplainFeatureFlagDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{Email: cloned.GetApprover().GetEmail()},
		},
	}
	explainFeatureFlagDecisionReply, err := s.featureFlagValidationPort.ExplainFeatureFlagDecision(ctx, explainFeatureFlagDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.featureflags.v1.FeatureFlagValidationService/ExplainFeatureFlagDecision")
	}
	encoded, err := protojson.Marshal(req.GetSellerListing())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded sellersv1.SellerListing
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	var countryCode string
	switch choice := req.GetSellerListing().GetLocator().(type) {
	case *sellersv1.SellerListing_Address:
		countryCode = choice.Address.GetCountryCode()
	case *sellersv1.SellerListing_FacilityCode:
		countryCode = choice.FacilityCode
	}
	simulateHouseholdRequest := &customersv1.SimulateHouseholdRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Audit: &commonv1.AuditInfo{
						CreatedAt: (&decoded).GetAudit().GetCreatedAt(),
						UpdatedBy: req.GetSellerListing().GetAudit().GetUpdatedBy(),
					},
					Location: &commonv1.PostalAddress{CountryCode: countryCode},
				}},
			}},
		},
	}
	simulateHouseholdReply, err := grpcx.NewClient(s.conn, customersv1.NewHouseholdCalculationServiceClient).SimulateHousehold(ctx, simulateHouseholdRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdCalculationService/SimulateHousehold")
	}
	packed, err := anypb.New(req.GetSellerListing())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	step := &sellerListingApprovalHandlersRecallSellerListingExplainSearchQueryDecisionStep{request: &searchv1.ExplainSearchQueryDecisionRequest{
		Context: req.GetContext(),
		SearchQuery: &searchv1.SearchQuery{
			Subject: &searchv1.SearchIndexRef{
				DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
			},
			CorrelationId: simulateHouseholdReply.GetResult().GetCorrelationId(),
			Detail:        packed,
		},
		Details: &searchv1.ExplainSearchQueryDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email: simulateHouseholdReply.GetHousehold().GetManager().GetEmail(),
			},
		},
	}}
	if err := s.runRecallSellerListingExplainSearchQueryDecision(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryValidationService/ExplainSearchQueryDecision")
	}
	explainSearchQueryDecisionReply := step.reply
	return &sellersv1.RecallSellerListingResponse{
		Outcome: &sellersv1.RecallSellerListingResponse_Outcome{
			Status: explainFeatureFlagDecisionReply.GetResult().GetStatus(),
		},
		SellerListing: &sellersv1.SellerListing{
			Owner: &identityv1.UserRef{
				DisplayName: explainFeatureFlagDecisionReply.GetFeatureFlag().GetOwner().GetDisplayName(),
				Email:       simulateHouseholdReply.GetHousehold().GetManager().GetEmail(),
			},
			SellerListingId: req.GetSellerListingId(),
			Specs:           req.GetSellerListing().GetSpecs(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: explainSearchQueryDecisionReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// GetSellerListingApprovalStatus returns a seller listing.
func (s *sellerListingApprovalHandlers) GetSellerListingApprovalStatus(ctx context.Context, req *sellersv1.GetSellerListingApprovalStatusRequest) (*sellersv1.GetSellerListingApprovalStatusResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "seller_listing", req.GetSellerListingId(), "get_seller_listing_approval_status"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get seller listing approval status", "tenant", req.GetContext().GetTenantId())
	return &sellersv1.GetSellerListingApprovalStatusResponse{
		SellerListing: &sellersv1.SellerListing{SellerListingId: req.GetSellerListingId()},
		Result:        &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CommentSellerListingApproval comments a seller listing.
func (s *sellerListingApprovalHandlers) CommentSellerListingApproval(ctx context.Context, req *sellersv1.CommentSellerListingApprovalRequest) (*sellersv1.CommentSellerListingApprovalResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveSellerListing(ctx, req.GetContext().GetTenantId(), req.GetSellerListing()); err != nil {
		return nil, grpcx.Wrap(err, "save seller listing")
	}
	s.log.InfoContext(ctx, "comment seller listing approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSellerListing().GetAudit().GetEtag())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	summarizeCustomersRequest := &customersv1.SummarizeCustomersRequest{
		Context:     req.GetContext(),
		Annotations: annotations,
	}
	summarizeCustomersReply := new(customersv1.SummarizeCustomersResponse)
	if err := grpcx.Invoke(ctx, s.conn, customersv1.CustomerQueryService_SummarizeCustomers_FullMethodName, summarizeCustomersRequest, summarizeCustomersReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerQueryService/SummarizeCustomers")
	}
	escalateGiftCardBatchRequest := &giftcardsv1.EscalateGiftCardBatchRequest{
		Context: req.GetContext(),
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			Audit: &commonv1.AuditInfo{CreatedAt: req.GetSellerListing().GetAudit().GetCreatedAt()},
			BillingAddress: &commonv1.PostalAddress{
				PostalCode: req.GetSellerListing().GetAddress().GetPostalCode(),
				Locality:   req.GetSellerListing().GetAddress().GetLocality(),
			},
		},
	}
	escalateGiftCardBatchReply := new(giftcardsv1.EscalateGiftCardBatchResponse)
	if err := s.conn.Invoke(ctx, giftcardsGiftCardBatchApprovalServiceEscalateGiftCardBatchMethod, escalateGiftCardBatchRequest, escalateGiftCardBatchReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchApprovalService/EscalateGiftCardBatch")
	}
	return &sellersv1.CommentSellerListingApprovalResponse{
		Result: &commonv1.OperationResult{
			Details:       summarizeCustomersReply.GetResult().GetDetails(),
			CorrelationId: summarizeCustomersReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Outcome: &sellersv1.CommentSellerListingApprovalResponse_Outcome{Status: escalateGiftCardBatchReply.GetOutcome().GetStatus()},
		SellerListing: &sellersv1.SellerListing{
			SellerListingId: req.GetSellerListingId(),
			Name:            req.GetSellerListing().GetName(),
		},
	}, nil
}

// calculateConsentRecordSender returns a function that sends one request to
// acme.customers.v1.ConsentRecordCalculationService/CalculateConsentRecord.
func (s *sellerListingApprovalHandlers) calculateConsentRecordSender(request *customersv1.CalculateConsentRecordRequest) func(context.Context) (*customersv1.CalculateConsentRecordResponse, error) {
	return func(ctx context.Context) (*customersv1.CalculateConsentRecordResponse, error) {
		return s.consentRecordCalculationClient.CalculateConsentRecord(ctx, request)
	}
}

// sellerListingApprovalHandlersCheckSellerPayoutScheduleEligibilitySteps are the ways to run
// acme.sellers.v1.SellerPayoutScheduleValidationService/CheckSellerPayoutScheduleEligibility, by name.
var sellerListingApprovalHandlersCheckSellerPayoutScheduleEligibilitySteps = map[string]func(*sellerListingApprovalHandlers, context.Context, *sellersv1.CheckSellerPayoutScheduleEligibilityRequest) (*sellersv1.CheckSellerPayoutScheduleEligibilityResponse, error){
	"default": (*sellerListingApprovalHandlers).checkSellerPayoutScheduleEligibilityStep,
	"audited": (*sellerListingApprovalHandlers).checkSellerPayoutScheduleEligibilityAuditedStep,
}

func (s *sellerListingApprovalHandlers) checkSellerPayoutScheduleEligibilityStep(ctx context.Context, request *sellersv1.CheckSellerPayoutScheduleEligibilityRequest) (*sellersv1.CheckSellerPayoutScheduleEligibilityResponse, error) {
	return s.sellerPayoutScheduleValidationClient.CheckSellerPayoutScheduleEligibility(ctx, request)
}

func (s *sellerListingApprovalHandlers) checkSellerPayoutScheduleEligibilityAuditedStep(ctx context.Context, request *sellersv1.CheckSellerPayoutScheduleEligibilityRequest) (*sellersv1.CheckSellerPayoutScheduleEligibilityResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.sellers.v1.SellerPayoutScheduleValidationService/CheckSellerPayoutScheduleEligibility")
	return s.sellerPayoutScheduleValidationClient.CheckSellerPayoutScheduleEligibility(ctx, request)
}

// approveGiftCardSender returns a function that sends one request to
// acme.giftcards.v1.GiftCardApprovalService/ApproveGiftCard.
func (s *sellerListingApprovalHandlers) approveGiftCardSender(request *giftcardsv1.ApproveGiftCardRequest) func(context.Context) (*giftcardsv1.ApproveGiftCardResponse, error) {
	return func(ctx context.Context) (*giftcardsv1.ApproveGiftCardResponse, error) {
		return s.giftCardApprovalClient.ApproveGiftCard(ctx, request)
	}
}

// sellerListingApprovalHandlersRecallSellerListingExplainSearchQueryDecisionStep carries the request of one step of
// RecallSellerListing and, once it ran, its reply.
type sellerListingApprovalHandlersRecallSellerListingExplainSearchQueryDecisionStep struct {
	request *searchv1.ExplainSearchQueryDecisionRequest
	reply   *searchv1.ExplainSearchQueryDecisionResponse
}

func (s *sellerListingApprovalHandlers) runRecallSellerListingExplainSearchQueryDecision(ctx context.Context, step *sellerListingApprovalHandlersRecallSellerListingExplainSearchQueryDecisionStep) error {
	reply, err := s.searchQueryValidationClient.ExplainSearchQueryDecision(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
