package fraud

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	auditv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audit/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
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
	"google.golang.org/protobuf/types/known/anypb"
)

// FraudRuleWorkflowServer implements `acme.fraud.v1.FraudRuleWorkflowService`.
// Runs the long-lived workflow that takes a fraud rule from start to finish.
type FraudRuleWorkflowServer struct {
	fraudv1.UnimplementedFraudRuleWorkflowServiceServer
	store                               *Store
	log                                 *slog.Logger
	fraudRuleApprovalClient             fraudv1.FraudRuleApprovalServiceClient
	customerSegmentValidationClient     customersv1.CustomerSegmentValidationServiceClient
	auditEventQueryClient               auditv1.AuditEventQueryServiceClient
	fraudRuleCalculationClient          fraudv1.FraudRuleCalculationServiceClient
	loyaltyMemberWorkflowClient         loyaltyv1.LoyaltyMemberWorkflowServiceClient
	recallSellerPayoutScheduleFn        func(context.Context, *sellersv1.RecallSellerPayoutScheduleRequest, ...grpc.CallOption) (*sellersv1.RecallSellerPayoutScheduleResponse, error)
	sellerListingWorkflowClient         sellersv1.SellerListingWorkflowServiceClient
	sellerListingWorkflowClientFallback sellersv1.SellerListingWorkflowServiceClient
	conn                                *grpc.ClientConn
	clients                             *registry.Registry
	riskAssessmentWorkflowClient        fraudv1.RiskAssessmentWorkflowServiceClient
	shipmentApprovalClient              shippingv1.ShipmentApprovalServiceClient
	watchlistReportingClient            fraudv1.WatchlistReportingServiceClient
	sellerWorkflowClient                sellersv1.SellerWorkflowServiceClient
	householdCalculationPort            householdCalculationPort
}

// NewFraudRuleWorkflowServer wires a FraudRuleWorkflowServer.
func NewFraudRuleWorkflowServer(deps platform.Deps) *FraudRuleWorkflowServer {
	sellerPayoutScheduleApproval := sellersv1.NewSellerPayoutScheduleApprovalServiceClient(deps.Conn)
	return &FraudRuleWorkflowServer{
		store:                               NewStore(deps.DB),
		log:                                 deps.Logger.With("service", "acme.fraud.v1.FraudRuleWorkflowService"),
		fraudRuleApprovalClient:             fraudv1.NewFraudRuleApprovalServiceClient(deps.Conn),
		customerSegmentValidationClient:     customersv1.NewCustomerSegmentValidationServiceClient(deps.Conn),
		auditEventQueryClient:               auditv1.NewAuditEventQueryServiceClient(deps.Conn),
		fraudRuleCalculationClient:          fraudv1.NewFraudRuleCalculationServiceClient(deps.Conn),
		loyaltyMemberWorkflowClient:         loyaltyv1.NewLoyaltyMemberWorkflowServiceClient(deps.Conn),
		recallSellerPayoutScheduleFn:        sellerPayoutScheduleApproval.RecallSellerPayoutSchedule,
		sellerListingWorkflowClient:         sellersv1.NewSellerListingWorkflowServiceClient(deps.Conn),
		sellerListingWorkflowClientFallback: sellersv1.NewSellerListingWorkflowServiceClient(deps.Conn),
		conn:                                deps.Conn,
		clients:                             deps.Clients,
		riskAssessmentWorkflowClient:        fraudv1.NewRiskAssessmentWorkflowServiceClient(deps.Conn),
		shipmentApprovalClient:              shippingv1.NewShipmentApprovalServiceClient(deps.Conn),
		watchlistReportingClient:            fraudv1.NewWatchlistReportingServiceClient(deps.Conn),
		sellerWorkflowClient:                sellersv1.NewSellerWorkflowServiceClient(deps.Conn),
		householdCalculationPort:            newHouseholdCalculationPort(deps),
	}
}

// StartFraudRuleWorkflow starts a fraud rule.
func (s *FraudRuleWorkflowServer) StartFraudRuleWorkflow(ctx context.Context, req *fraudv1.StartFraudRuleWorkflowRequest) (*fraudv1.StartFraudRuleWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveFraudRule(ctx, req.GetContext().GetTenantId(), req.GetFraudRule()); err != nil {
		return nil, grpcx.Wrap(err, "save fraud rule")
	}
	s.log.InfoContext(ctx, "start fraud rule workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetFraudRule().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetFraudRule()).(*fraudv1.FraudRule)
	submitFraudRuleRequest := &fraudv1.SubmitFraudRuleRequest{
		Context: req.GetContext(),
		FraudRule: &fraudv1.FraudRule{
			Audit: &commonv1.AuditInfo{UpdatedAt: cloned.GetAudit().GetUpdatedAt()},
			Owner: &identityv1.UserRef{Email: req.GetFraudRule().GetOwner().GetEmail()},
		},
	}
	type submitFraudRuleResult struct {
		reply *fraudv1.SubmitFraudRuleResponse
		err   error
	}
	submitFraudRuleResults := make(chan submitFraudRuleResult, 1)
	go func() {
		reply, err := s.fraudRuleApprovalClient.SubmitFraudRule(ctx, submitFraudRuleRequest)
		submitFraudRuleResults <- submitFraudRuleResult{reply, err}
	}()
	var submitFraudRuleReply *fraudv1.SubmitFraudRuleResponse
	select {
	case result := <-submitFraudRuleResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.fraud.v1.FraudRuleApprovalService/SubmitFraudRule")
		}
		submitFraudRuleReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &fraudv1.StartFraudRuleWorkflowResponse{
		Outcome: &fraudv1.StartFraudRuleWorkflowResponse_Outcome{Status: submitFraudRuleReply.GetResult().GetStatus()},
		Result:  &commonv1.OperationResult{Status: submitFraudRuleReply.GetOutcome().GetStatus()},
		FraudRule: &fraudv1.FraudRule{
			FraudRuleId: req.GetFraudRuleId(),
			Description: req.GetFraudRule().GetDescription(),
		},
	}, nil
}

// AdvanceFraudRuleWorkflow advances a fraud rule.
func (s *FraudRuleWorkflowServer) AdvanceFraudRuleWorkflow(ctx context.Context, req *fraudv1.AdvanceFraudRuleWorkflowRequest) (*fraudv1.AdvanceFraudRuleWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveFraudRule(ctx, req.GetContext().GetTenantId(), req.GetFraudRule()); err != nil {
		return nil, grpcx.Wrap(err, "save fraud rule")
	}
	s.log.InfoContext(ctx, "advance fraud rule workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetFraudRule().GetAudit().GetEtag())
	for _, condition := range req.GetFraudRule().GetConditions() {
		if _, err := s.customerSegmentValidationClient.PreviewCustomerSegment(ctx, &customersv1.PreviewCustomerSegmentRequest{
			Context: req.GetContext(),
			CustomerSegment: &customersv1.CustomerSegment{
				Conditions: []*customersv1.CustomerSegmentCondition{&customersv1.CustomerSegmentCondition{
					Operator:  condition.GetOperator(),
					Attribute: condition.GetAttribute(),
					Value:     condition.GetValue(),
				}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentValidationService/PreviewCustomerSegment")
		}
	}
	return &fraudv1.AdvanceFraudRuleWorkflowResponse{
		FraudRule: &fraudv1.FraudRule{
			FraudRuleId: req.GetFraudRuleId(),
			Effective:   req.GetFraudRule().GetEffective(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CancelFraudRuleWorkflow cancels a fraud rule.
func (s *FraudRuleWorkflowServer) CancelFraudRuleWorkflow(ctx context.Context, req *fraudv1.CancelFraudRuleWorkflowRequest) (*fraudv1.CancelFraudRuleWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveFraudRule(ctx, req.GetContext().GetTenantId(), req.GetFraudRule()); err != nil {
		return nil, grpcx.Wrap(err, "save fraud rule")
	}
	s.log.InfoContext(ctx, "cancel fraud rule workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetFraudRule().GetAudit().GetEtag())
	findAuditEventsRequest := newFindAuditEventsRequest(withFindAuditEventsContext(req.GetContext()), withFindAuditEventsFilter(&auditv1.FindAuditEventsRequest_Filter{
		CreatedAfter: req.GetFraudRule().GetAudit().GetCreatedAt(),
		OwnerId:      req.GetFraudRule().GetOwner().GetUserId(),
	}))
	findAuditEventsReply, err := s.auditEventQueryClient.FindAuditEvents(ctx, findAuditEventsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.audit.v1.AuditEventQueryService/FindAuditEvents")
	}
	compareFraudRuleOptionsRequest := buildCompareFraudRuleOptionsRequestForCancelFraudRuleWorkflow(req)
	compareFraudRuleOptionsReply, err := s.fraudRuleCalculationClient.CompareFraudRuleOptions(ctx, compareFraudRuleOptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.FraudRuleCalculationService/CompareFraudRuleOptions")
	}
	resumeLoyaltyMemberWorkflowRequest := buildResumeLoyaltyMemberWorkflowRequestForCancelFraudRuleWorkflow(req, compareFraudRuleOptionsReply)
	resumeLoyaltyMemberWorkflowReply, err := s.loyaltyMemberWorkflowClient.ResumeLoyaltyMemberWorkflow(ctx, resumeLoyaltyMemberWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberWorkflowService/ResumeLoyaltyMemberWorkflow")
	}
	return &fraudv1.CancelFraudRuleWorkflowResponse{
		Result: &commonv1.OperationResult{
			Message: findAuditEventsReply.GetResult().GetMessage(),
			Details: compareFraudRuleOptionsReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		FraudRule: &fraudv1.FraudRule{
			Owner: &identityv1.UserRef{
				DisplayName: compareFraudRuleOptionsReply.GetFraudRule().GetOwner().GetDisplayName(),
				Email:       resumeLoyaltyMemberWorkflowReply.GetLoyaltyMember().GetContact().GetEmail(),
			},
			FraudRuleId: req.GetFraudRuleId(),
			Effective:   req.GetFraudRule().GetEffective(),
		},
		Outcome: &fraudv1.CancelFraudRuleWorkflowResponse_Outcome{
			NextSteps: resumeLoyaltyMemberWorkflowReply.GetOutcome().GetNextSteps(),
		},
	}, nil
}

// RetryFraudRuleWorkflow retrys a fraud rule.
func (s *FraudRuleWorkflowServer) RetryFraudRuleWorkflow(ctx context.Context, req *fraudv1.RetryFraudRuleWorkflowRequest) (*fraudv1.RetryFraudRuleWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RetryFraudRuleWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SaveFraudRule(ctx, req.GetContext().GetTenantId(), req.GetFraudRule()); err != nil {
		return nil, grpcx.Wrap(err, "save fraud rule")
	}
	s.log.InfoContext(ctx, "retry fraud rule workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetFraudRule().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "email", req.GetDetails().GetNotify().GetEmail())
	encoded, err := protojson.Marshal(req.GetFraudRule())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded fraudv1.FraudRule
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	recallSellerPayoutScheduleRequest := &sellersv1.RecallSellerPayoutScheduleRequest{
		Context: req.GetContext(),
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
			Organizer: &identityv1.UserRef{Email: tenancy.Value(ctx, "email")},
			Participants: []*sellersv1.SellerPayoutScheduleParticipant{&sellersv1.SellerPayoutScheduleParticipant{
				Contact: &commonv1.ContactInfo{Phone: req.GetDetails().GetNotify().GetPhone()},
			}},
		},
		Details: &sellersv1.RecallSellerPayoutScheduleRequest_Details{
			Approver: &identityv1.UserRef{DisplayName: (&decoded).GetOwner().GetDisplayName()},
		},
	}
	recallSellerPayoutScheduleReply, err := s.recallSellerPayoutScheduleFn(ctx, recallSellerPayoutScheduleRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerPayoutScheduleApprovalService/RecallSellerPayoutSchedule")
	}
	routes := map[string]func(context.Context, *sellersv1.AdvanceSellerListingWorkflowRequest, ...grpc.CallOption) (*sellersv1.AdvanceSellerListingWorkflowResponse, error){
		"primary":  s.sellerListingWorkflowClient.AdvanceSellerListingWorkflow,
		"fallback": s.sellerListingWorkflowClientFallback.AdvanceSellerListingWorkflow,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	advanceSellerListingWorkflowRequest := &sellersv1.AdvanceSellerListingWorkflowRequest{
		Context: req.GetContext(),
		Details: &sellersv1.AdvanceSellerListingWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Name: req.GetDetails().GetNotify().GetName()},
		},
		SellerListing: &sellersv1.SellerListing{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: recallSellerPayoutScheduleReply.GetSellerPayoutSchedule().GetAudit().GetUpdatedBy(),
			},
		},
	}
	advanceSellerListingWorkflowReply, err := route(ctx, advanceSellerListingWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingWorkflowService/AdvanceSellerListingWorkflow")
	}
	cloned := proto.Clone(req.GetFraudRule()).(*fraudv1.FraudRule)
	delegateRiskAssessmentApprovalRequest := &fraudv1.DelegateRiskAssessmentApprovalRequest{
		Context: req.GetContext(),
		RiskAssessment: &fraudv1.RiskAssessment{
			Origin:   &fraudv1.RiskAssessment_Customer{Customer: &customersv1.CustomerRef{Email: req.GetFraudRule().GetOwner().GetEmail()}},
			Comments: []*fraudv1.RiskAssessmentComment{&fraudv1.RiskAssessmentComment{CreatedAt: cloned.GetAudit().GetCreatedAt()}},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: recallSellerPayoutScheduleReply.GetSellerPayoutSchedule().GetAudit().GetUpdatedAt(),
			},
			Assignee: &identityv1.UserRef{
				Email: recallSellerPayoutScheduleReply.GetSellerPayoutSchedule().GetOrganizer().GetEmail(),
			},
		},
	}
	delegateRiskAssessmentApprovalReply := new(fraudv1.DelegateRiskAssessmentApprovalResponse)
	if err := grpcx.Invoke(ctx, s.conn, fraudv1.RiskAssessmentApprovalService_DelegateRiskAssessmentApproval_FullMethodName, delegateRiskAssessmentApprovalRequest, delegateRiskAssessmentApprovalReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.RiskAssessmentApprovalService/DelegateRiskAssessmentApproval")
	}
	customerSegmentValidation, err := registry.Lookup[customersv1.CustomerSegmentValidationServiceClient](s.clients, "acme.customers.v1.CustomerSegmentValidationService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	checkCustomerSegmentEligibilityRequest := &customersv1.CheckCustomerSegmentEligibilityRequest{
		Context: req.GetContext(),
		CustomerSegment: &customersv1.CustomerSegment{
			Action: &customersv1.CustomerSegment_Discount{Discount: &customersv1.CustomerSegmentDiscountAction{Percent: req.GetFraudRule().GetDiscount().GetPercent()}},
		},
	}
	checkCustomerSegmentEligibilityReply, err := customerSegmentValidation.CheckCustomerSegmentEligibility(ctx, checkCustomerSegmentEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentValidationService/CheckCustomerSegmentEligibility")
	}
	return &fraudv1.RetryFraudRuleWorkflowResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: recallSellerPayoutScheduleReply.GetResult().GetCorrelationId(),
			ReferenceIds:  delegateRiskAssessmentApprovalReply.GetResult().GetReferenceIds(),
			Status:        checkCustomerSegmentEligibilityReply.GetOutcome().GetStatus(),
		},
		Outcome: &fraudv1.RetryFraudRuleWorkflowResponse_Outcome{
			Status:    advanceSellerListingWorkflowReply.GetOutcome().GetStatus(),
			NextSteps: delegateRiskAssessmentApprovalReply.GetOutcome().GetNextSteps(),
		},
		FraudRule: &fraudv1.FraudRule{
			Audit: &commonv1.AuditInfo{
				Etag: advanceSellerListingWorkflowReply.GetSellerListing().GetAudit().GetEtag(),
			},
			FraudRuleId: req.GetFraudRuleId(),
			Owner:       req.GetFraudRule().GetOwner(),
		},
	}, nil
}

// PauseFraudRuleWorkflow pauses a fraud rule.
func (s *FraudRuleWorkflowServer) PauseFraudRuleWorkflow(ctx context.Context, req *fraudv1.PauseFraudRuleWorkflowRequest) (*fraudv1.PauseFraudRuleWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveFraudRule(ctx, req.GetContext().GetTenantId(), req.GetFraudRule()); err != nil {
		return nil, grpcx.Wrap(err, "save fraud rule")
	}
	s.log.InfoContext(ctx, "pause fraud rule workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetFraudRule().GetAudit().GetEtag())
	encoded, err := protojson.Marshal(req.GetFraudRule())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded fraudv1.FraudRule
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	pauseRiskAssessmentWorkflowRequest := &fraudv1.PauseRiskAssessmentWorkflowRequest{
		Context: req.GetContext(),
		RiskAssessment: &fraudv1.RiskAssessment{
			Audit: &commonv1.AuditInfo{Etag: (&decoded).GetAudit().GetEtag()},
		},
		Details: &fraudv1.PauseRiskAssessmentWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetNotify().GetPreferredChannel(),
				Email:            req.GetFraudRule().GetOwner().GetEmail(),
			},
		},
	}
	pauseRiskAssessmentWorkflowReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*fraudv1.PauseRiskAssessmentWorkflowResponse, error) {
		return s.riskAssessmentWorkflowClient.PauseRiskAssessmentWorkflow(ctx, pauseRiskAssessmentWorkflowRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.RiskAssessmentWorkflowService/PauseRiskAssessmentWorkflow")
	}
	listPendingShipmentsRequest := buildListPendingShipmentsRequestForPauseFraudRuleWorkflow(req, pauseRiskAssessmentWorkflowReply)
	listPendingShipmentsReply, err := s.shipmentApprovalClient.ListPendingShipments(ctx, listPendingShipmentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ShipmentApprovalService/ListPendingShipments")
	}
	return &fraudv1.PauseFraudRuleWorkflowResponse{
		FraudRule: &fraudv1.FraudRule{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: pauseRiskAssessmentWorkflowReply.GetRiskAssessment().GetAudit().GetUpdatedBy(),
			},
			Priority:    pauseRiskAssessmentWorkflowReply.GetRiskAssessment().GetPriority(),
			FraudRuleId: req.GetFraudRuleId(),
			Description: req.GetFraudRule().GetDescription(),
		},
		Outcome: &fraudv1.PauseFraudRuleWorkflowResponse_Outcome{Status: listPendingShipmentsReply.GetResult().GetStatus()},
		Result: &commonv1.OperationResult{
			Details: listPendingShipmentsReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}

// ResumeFraudRuleWorkflow resumes a fraud rule.
func (s *FraudRuleWorkflowServer) ResumeFraudRuleWorkflow(ctx context.Context, req *fraudv1.ResumeFraudRuleWorkflowRequest) (*fraudv1.ResumeFraudRuleWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ResumeFraudRuleWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SaveFraudRule(ctx, req.GetContext().GetTenantId(), req.GetFraudRule()); err != nil {
		return nil, grpcx.Wrap(err, "save fraud rule")
	}
	s.log.InfoContext(ctx, "resume fraud rule workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetFraudRule().GetAudit().GetEtag())
	streamWatchlistMetricsStream, err := s.watchlistReportingClient.StreamWatchlistMetrics(ctx, &fraudv1.StreamWatchlistMetricsRequest{
		Context: req.GetContext(),
		Subscriber: &commonv1.ContactInfo{
			Email: req.GetDetails().GetNotify().GetEmail(),
			Phone: req.GetDetails().GetNotify().GetPhone(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.WatchlistReportingService/StreamWatchlistMetrics")
	}
	for {
		received, err := streamWatchlistMetricsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.fraud.v1.WatchlistReportingService/StreamWatchlistMetrics")
		}
		_ = received
	}
	scheduleWatchlistExpiryRequest := &fraudv1.ScheduleWatchlistExpiryRequest{
		Context: req.GetContext(),
		Watchlist: &fraudv1.Watchlist{
			Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
				Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
					Manager: &identityv1.UserRef{
						DisplayName: req.GetFraudRule().GetOwner().GetDisplayName(),
						Email:       req.GetDetails().GetNotify().GetEmail(),
					},
				}},
			}},
		},
	}
	scheduleWatchlistExpiryReply, err := grpcx.NewClient(s.conn, fraudv1.NewWatchlistLifecycleServiceClient).ScheduleWatchlistExpiry(ctx, scheduleWatchlistExpiryRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.WatchlistLifecycleService/ScheduleWatchlistExpiry")
	}
	fraudRuleCache := cache.New[string, *fraudv1.FraudRule](1)
	fraudRuleCache.Put(req.GetContext().GetTraceId(), req.GetFraudRule())
	cachedFraudRule, _ := fraudRuleCache.Get(req.GetContext().GetTraceId())
	packed, err := anypb.New(req.GetFraudRule())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	signalReviewVoteWorkflowRequest := &reviewsv1.SignalReviewVoteWorkflowRequest{
		Context: req.GetContext(),
		Details: &reviewsv1.SignalReviewVoteWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Email: cachedFraudRule.GetOwner().GetEmail()},
		},
		ReviewVote: &reviewsv1.ReviewVote{Detail: packed},
	}
	signalReviewVoteWorkflowReply := new(reviewsv1.SignalReviewVoteWorkflowResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", reviewsReviewVoteWorkflowServiceName, "SignalReviewVoteWorkflow"), signalReviewVoteWorkflowRequest, signalReviewVoteWorkflowReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewVoteWorkflowService/SignalReviewVoteWorkflow")
	}
	return &fraudv1.ResumeFraudRuleWorkflowResponse{
		FraudRule: &fraudv1.FraudRule{
			Audit: &commonv1.AuditInfo{
				Etag: scheduleWatchlistExpiryReply.GetWatchlist().GetAudit().GetEtag(),
			},
			FraudRuleId: req.GetFraudRuleId(),
			Status:      req.GetFraudRule().GetStatus(),
		},
		Result: &commonv1.OperationResult{
			Message: signalReviewVoteWorkflowReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Outcome: &fraudv1.ResumeFraudRuleWorkflowResponse_Outcome{
			Message: signalReviewVoteWorkflowReply.GetOutcome().GetMessage(),
		},
	}, nil
}

// InspectFraudRuleWorkflow returns a fraud rule.
func (s *FraudRuleWorkflowServer) InspectFraudRuleWorkflow(ctx context.Context, req *fraudv1.InspectFraudRuleWorkflowRequest) (*fraudv1.InspectFraudRuleWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "fraud_rule", req.GetFraudRuleId(), "inspect_fraud_rule_workflow"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "inspect fraud rule workflow", "tenant", req.GetContext().GetTenantId())
	quoteFraudRuleRequest := &fraudv1.QuoteFraudRuleRequest{
		Context:     req.GetContext(),
		FraudRuleId: req.GetFraudRuleId(),
		FraudRule: &fraudv1.FraudRule{
			Owner: &identityv1.UserRef{
				UserId: req.GetContext().GetActor().GetUserId(),
				Email:  req.GetContext().GetActor().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{CreatedAt: req.GetContext().GetDeadline()},
		},
	}
	quoteFraudRuleReply := new(fraudv1.QuoteFraudRuleResponse)
	if err := grpcx.Invoke(ctx, s.conn, fraudv1.FraudRuleCalculationService_QuoteFraudRule_FullMethodName, quoteFraudRuleRequest, quoteFraudRuleReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.FraudRuleCalculationService/QuoteFraudRule")
	}
	return &fraudv1.InspectFraudRuleWorkflowResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: quoteFraudRuleReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		FraudRule: &fraudv1.FraudRule{
			Owner: &identityv1.UserRef{
				Email: quoteFraudRuleReply.GetFraudRule().GetOwner().GetEmail(),
			},
			FraudRuleId: req.GetFraudRuleId(),
		},
	}, nil
}

// ListFraudRuleWorkflows lists fraud rules a page at a time.
func (s *FraudRuleWorkflowServer) ListFraudRuleWorkflows(ctx context.Context, req *fraudv1.ListFraudRuleWorkflowsRequest) (*fraudv1.ListFraudRuleWorkflowsResponse, error) {
	s.log.InfoContext(ctx, "list fraud rule workflows", "tenant", req.GetContext().GetTenantId())
	return &fraudv1.ListFraudRuleWorkflowsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// SignalFraudRuleWorkflow signals a fraud rule.
func (s *FraudRuleWorkflowServer) SignalFraudRuleWorkflow(ctx context.Context, req *fraudv1.SignalFraudRuleWorkflowRequest) (*fraudv1.SignalFraudRuleWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SignalFraudRuleWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SaveFraudRule(ctx, req.GetContext().GetTenantId(), req.GetFraudRule()); err != nil {
		return nil, grpcx.Wrap(err, "save fraud rule")
	}
	s.log.InfoContext(ctx, "signal fraud rule workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetFraudRule().GetAudit().GetEtag())
	encoded, err := protojson.Marshal(req.GetFraudRule())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded fraudv1.FraudRule
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	parameters := make(map[string]string, len(req.GetDetails().GetParameters()))
	for key, value := range req.GetDetails().GetParameters() {
		parameters[key] = value
	}
	encoded2, err := protojson.Marshal(req.GetFraudRule())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded2 fraudv1.FraudRule
	if err := protojson.Unmarshal(encoded2, &decoded2); err != nil {
		return nil, err
	}
	listSellerWorkflowsRequest := &sellersv1.ListSellerWorkflowsRequest{
		Context: req.GetContext(),
		Filter: &sellersv1.ListSellerWorkflowsRequest_Filter{
			CreatedAfter: (&decoded).GetAudit().GetCreatedAt(),
			Labels:       parameters,
			OwnerId:      (&decoded2).GetFraudRuleId(),
		},
	}
	listSellerWorkflowsReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*sellersv1.ListSellerWorkflowsResponse, error) {
		return s.sellerWorkflowClient.ListSellerWorkflows(ctx, listSellerWorkflowsRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerWorkflowService/ListSellerWorkflows")
	}
	estimateHouseholdRequest := &customersv1.EstimateHouseholdRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Manager: &identityv1.UserRef{Email: req.GetFraudRule().GetOwner().GetEmail()},
					Audit:   &commonv1.AuditInfo{Etag: req.GetFraudRule().GetAudit().GetEtag()},
				}},
			}},
		},
	}
	estimateHouseholdReply, err := s.householdCalculationPort.EstimateHousehold(ctx, estimateHouseholdRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdCalculationService/EstimateHousehold")
	}
	_ = estimateHouseholdReply
	return &fraudv1.SignalFraudRuleWorkflowResponse{
		Outcome: &fraudv1.SignalFraudRuleWorkflowResponse_Outcome{Message: listSellerWorkflowsReply.GetResult().GetMessage()},
		Result: &commonv1.OperationResult{
			Message: listSellerWorkflowsReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		FraudRule: &fraudv1.FraudRule{
			FraudRuleId: req.GetFraudRuleId(),
			Version:     req.GetFraudRule().GetVersion(),
		},
	}, nil
}
