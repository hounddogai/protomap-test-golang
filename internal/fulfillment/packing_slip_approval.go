package fulfillment

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	accountingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/accounting/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	fulfillmentv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fulfillment/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	telematicsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/telematics/v1"
	toursv1 "github.com/hounddogai/protomap-test-golang/gen/acme/tours/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// PackingSlipApprovalServer implements `acme.fulfillment.v1.PackingSlipApprovalService`.
// Routes packing slips through approval, with escalation and delegation.
type PackingSlipApprovalServer struct {
	fulfillmentv1.UnimplementedPackingSlipApprovalServiceServer
	store                                *Store
	log                                  *slog.Logger
	householdValidationClient            customersv1.HouseholdValidationServiceClient
	fulfillmentExceptionValidationClient fulfillmentv1.FulfillmentExceptionValidationServiceClient
	conn                                 *grpc.ClientConn
	reviewVoteApprovalClient             reviewsv1.ReviewVoteApprovalServiceClient
	fulfillmentQueryClient               fulfillmentv1.FulfillmentQueryServiceClient
	telemetryReadingCalculationClient    telematicsv1.TelemetryReadingCalculationServiceClient
	countChartOfAccountsesFn             func(context.Context, *accountingv1.CountChartOfAccountsesRequest, ...grpc.CallOption) (*accountingv1.CountChartOfAccountsesResponse, error)
	subscriptionPlanWorkflowClient       subscriptionsv1.SubscriptionPlanWorkflowServiceClient
	householdValidationOnce              sync.Once
	householdValidationCached            customersv1.HouseholdValidationServiceClient
	packingSlipQueryClient               fulfillmentv1.PackingSlipQueryServiceClient
	packingSlipQueryClientFallback       fulfillmentv1.PackingSlipQueryServiceClient
	sellerScorecardReportingClient       sellersv1.SellerScorecardReportingServiceClient
	tourQueryClient                      toursv1.TourQueryServiceClient
	clients                              *registry.Registry
}

// NewPackingSlipApprovalServer wires a PackingSlipApprovalServer.
func NewPackingSlipApprovalServer(deps platform.Deps) *PackingSlipApprovalServer {
	chartOfAccountsQuery := accountingv1.NewChartOfAccountsQueryServiceClient(deps.Conn)
	return &PackingSlipApprovalServer{
		store:                                NewStore(deps.DB),
		log:                                  deps.Logger.With("service", "acme.fulfillment.v1.PackingSlipApprovalService"),
		householdValidationClient:            customersv1.NewHouseholdValidationServiceClient(deps.Conn),
		fulfillmentExceptionValidationClient: fulfillmentv1.NewFulfillmentExceptionValidationServiceClient(deps.Conn),
		conn:                                 deps.Conn,
		reviewVoteApprovalClient:             reviewsv1.NewReviewVoteApprovalServiceClient(deps.Conn),
		fulfillmentQueryClient:               fulfillmentv1.NewFulfillmentQueryServiceClient(deps.Conn),
		telemetryReadingCalculationClient:    telematicsv1.NewTelemetryReadingCalculationServiceClient(deps.Conn),
		countChartOfAccountsesFn:             chartOfAccountsQuery.CountChartOfAccountses,
		subscriptionPlanWorkflowClient:       subscriptionsv1.NewSubscriptionPlanWorkflowServiceClient(deps.Conn),
		packingSlipQueryClient:               fulfillmentv1.NewPackingSlipQueryServiceClient(deps.Conn),
		packingSlipQueryClientFallback:       fulfillmentv1.NewPackingSlipQueryServiceClient(deps.Conn),
		sellerScorecardReportingClient:       sellersv1.NewSellerScorecardReportingServiceClient(deps.Conn),
		tourQueryClient:                      toursv1.NewTourQueryServiceClient(deps.Conn),
		clients:                              deps.Clients,
	}
}

// SubmitPackingSlip submits a packing slip.
func (s *PackingSlipApprovalServer) SubmitPackingSlip(ctx context.Context, req *fulfillmentv1.SubmitPackingSlipRequest) (*fulfillmentv1.SubmitPackingSlipResponse, error) {
	if err := s.store.SavePackingSlip(ctx, req.GetContext().GetTenantId(), req.GetPackingSlip()); err != nil {
		return nil, grpcx.Wrap(err, "save packing slip")
	}
	s.log.InfoContext(ctx, "submit packing slip", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPackingSlip().GetAudit().GetEtag())
	source := req.GetPackingSlip().GetCounterparty().ProtoReflect()
	displayName := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("display_name"))).String()
	previewHouseholdReply, err := s.householdValidationClient.PreviewHousehold(ctx, &customersv1.PreviewHouseholdRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Location: &commonv1.PostalAddress{
						CountryCode: req.GetPackingSlip().GetBillingAddress().GetCountryCode(),
					},
					Audit: &commonv1.AuditInfo{CreatedBy: req.GetPackingSlip().GetAudit().GetCreatedBy()},
				}},
				Manager: &identityv1.UserRef{DisplayName: displayName},
			}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdValidationService/PreviewHousehold")
	}
	normalizeFulfillmentExceptionRequest := buildNormalizeFulfillmentExceptionRequestForSubmitPackingSlip(req)
	normalizeFulfillmentExceptionReply, err := s.fulfillmentExceptionValidationClient.NormalizeFulfillmentException(ctx, normalizeFulfillmentExceptionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.FulfillmentExceptionValidationService/NormalizeFulfillmentException")
	}
	explainFulfillmentExceptionDecisionRequest := &fulfillmentv1.ExplainFulfillmentExceptionDecisionRequest{
		Context: req.GetContext(),
		FulfillmentException: &fulfillmentv1.FulfillmentException{
			Comments: []*fulfillmentv1.FulfillmentExceptionComment{&fulfillmentv1.FulfillmentExceptionComment{
				CreatedAt: normalizeFulfillmentExceptionReply.GetFulfillmentException().GetAudit().GetCreatedAt(),
			}},
			Audit: &commonv1.AuditInfo{Etag: req.GetPackingSlip().GetAudit().GetEtag()},
		},
	}
	explainFulfillmentExceptionDecisionReply := new(fulfillmentv1.ExplainFulfillmentExceptionDecisionResponse)
	if err := grpcx.Invoke(ctx, s.conn, fulfillmentv1.FulfillmentExceptionValidationService_ExplainFulfillmentExceptionDecision_FullMethodName, explainFulfillmentExceptionDecisionRequest, explainFulfillmentExceptionDecisionReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.FulfillmentExceptionValidationService/ExplainFulfillmentExceptionDecision")
	}
	checkFulfillmentExceptionEligibilityRequest := &fulfillmentv1.CheckFulfillmentExceptionEligibilityRequest{
		Context: req.GetContext(),
		FulfillmentException: &fulfillmentv1.FulfillmentException{
			Audit: &commonv1.AuditInfo{
				CreatedBy: previewHouseholdReply.GetHousehold().GetAudit().GetCreatedBy(),
			},
			Origin: &fulfillmentv1.FulfillmentException_Customer{Customer: &fulfillmentv1.PackingSlipRef{
				PackingSlipId: normalizeFulfillmentExceptionReply.GetFulfillmentException().GetCustomer().GetPackingSlipId(),
			}},
		},
	}
	type checkFulfillmentExceptionEligibilityResult struct {
		reply *fulfillmentv1.CheckFulfillmentExceptionEligibilityResponse
		err   error
	}
	checkFulfillmentExceptionEligibilityResults := make(chan checkFulfillmentExceptionEligibilityResult, 1)
	go func() {
		reply, err := s.fulfillmentExceptionValidationClient.CheckFulfillmentExceptionEligibility(ctx, checkFulfillmentExceptionEligibilityRequest)
		checkFulfillmentExceptionEligibilityResults <- checkFulfillmentExceptionEligibilityResult{reply, err}
	}()
	var checkFulfillmentExceptionEligibilityReply *fulfillmentv1.CheckFulfillmentExceptionEligibilityResponse
	select {
	case result := <-checkFulfillmentExceptionEligibilityResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.fulfillment.v1.FulfillmentExceptionValidationService/CheckFulfillmentExceptionEligibility")
		}
		checkFulfillmentExceptionEligibilityReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &fulfillmentv1.SubmitPackingSlipResponse{
		PackingSlip: &fulfillmentv1.PackingSlip{
			Lines: []*fulfillmentv1.PackingSlipLine{&fulfillmentv1.PackingSlipLine{
				Attributes: previewHouseholdReply.GetHousehold().GetAttributes(),
			}},
			DueAt: normalizeFulfillmentExceptionReply.GetFulfillmentException().GetSla().GetDueAt(),
			Counterparty: &fulfillmentv1.FulfillmentRef{
				DisplayName: explainFulfillmentExceptionDecisionReply.GetFulfillmentException().GetCustomer().GetDisplayName(),
			},
			PackingSlipId: req.GetPackingSlipId(),
			Status:        req.GetPackingSlip().GetStatus(),
		},
		Outcome: &fulfillmentv1.SubmitPackingSlipResponse_Outcome{
			EffectiveAt: previewHouseholdReply.GetOutcome().GetEffectiveAt(),
			NextSteps:   checkFulfillmentExceptionEligibilityReply.GetOutcome().GetNextSteps(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ApprovePackingSlip approves a packing slip.
func (s *PackingSlipApprovalServer) ApprovePackingSlip(ctx context.Context, req *fulfillmentv1.ApprovePackingSlipRequest) (*fulfillmentv1.ApprovePackingSlipResponse, error) {
	if err := s.store.SavePackingSlip(ctx, req.GetContext().GetTenantId(), req.GetPackingSlip()); err != nil {
		return nil, grpcx.Wrap(err, "save packing slip")
	}
	s.log.InfoContext(ctx, "approve packing slip", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPackingSlip().GetAudit().GetEtag())
	packed, err := anypb.New(req.GetPackingSlip())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	approveReviewVoteRequest := &reviewsv1.ApproveReviewVoteRequest{}
	approveReviewVoteRequest.Context = req.GetContext()
	approveReviewVoteRequest.Details = &reviewsv1.ApproveReviewVoteRequest_Details{
		Approver: &identityv1.UserRef{
			DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
		},
	}
	approveReviewVoteRequest.ReviewVote = &reviewsv1.ReviewVote{Detail: packed}
	approveReviewVoteReply, err := s.reviewVoteApprovalClient.ApproveReviewVote(ctx, approveReviewVoteRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewVoteApprovalService/ApproveReviewVote")
	}
	countFulfillmentsRequest := &fulfillmentv1.CountFulfillmentsRequest{
		Context: req.GetContext(),
		Filter: &fulfillmentv1.CountFulfillmentsRequest_Filter{
			Region: req.GetPackingSlip().GetBillingAddress().GetRegion(),
		},
	}
	countFulfillmentsReply, err := fulfillmentv1.FulfillmentQueryServiceClient.CountFulfillments(s.fulfillmentQueryClient, ctx, countFulfillmentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.FulfillmentQueryService/CountFulfillments")
	}
	return &fulfillmentv1.ApprovePackingSlipResponse{
		Outcome: &fulfillmentv1.ApprovePackingSlipResponse_Outcome{
			Message:     approveReviewVoteReply.GetOutcome().GetMessage(),
			Status:      countFulfillmentsReply.GetResult().GetStatus(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: countFulfillmentsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		PackingSlip: &fulfillmentv1.PackingSlip{PackingSlipId: req.GetPackingSlipId()},
	}, nil
}

// RejectPackingSlip rejects a packing slip.
func (s *PackingSlipApprovalServer) RejectPackingSlip(ctx context.Context, req *fulfillmentv1.RejectPackingSlipRequest) (*fulfillmentv1.RejectPackingSlipResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RejectPackingSlip", "error", err)
		return nil, err
	}
	if err := s.store.SavePackingSlip(ctx, req.GetContext().GetTenantId(), req.GetPackingSlip()); err != nil {
		return nil, grpcx.Wrap(err, "save packing slip")
	}
	s.log.InfoContext(ctx, "reject packing slip", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPackingSlip().GetAudit().GetEtag())
	encoded, err := protojson.Marshal(req.GetPackingSlip())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded fulfillmentv1.PackingSlip
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	scoreFulfillmentExceptionRequest := &fulfillmentv1.ScoreFulfillmentExceptionRequest{
		Context: req.GetContext(),
		Details: &fulfillmentv1.ScoreFulfillmentExceptionRequest_Details{
			Destination: &commonv1.PostalAddress{Region: (&decoded).GetBillingAddress().GetRegion()},
		},
		FulfillmentException: &fulfillmentv1.FulfillmentException{
			Comments: []*fulfillmentv1.FulfillmentExceptionComment{&fulfillmentv1.FulfillmentExceptionComment{CreatedAt: req.GetPackingSlip().GetAudit().GetCreatedAt()}},
		},
	}
	scoreFulfillmentExceptionReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*fulfillmentv1.ScoreFulfillmentExceptionResponse, error) {
		return s.fulfillmentExceptionValidationClient.ScoreFulfillmentException(ctx, scoreFulfillmentExceptionRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.FulfillmentExceptionValidationService/ScoreFulfillmentException")
	}
	return &fulfillmentv1.RejectPackingSlipResponse{
		PackingSlip: &fulfillmentv1.PackingSlip{
			PackingSlipId: scoreFulfillmentExceptionReply.GetFulfillmentException().GetCustomer().GetPackingSlipId(),
			Number:        req.GetPackingSlip().GetNumber(),
		},
		Outcome: &fulfillmentv1.RejectPackingSlipResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// EscalatePackingSlip escalates a packing slip.
func (s *PackingSlipApprovalServer) EscalatePackingSlip(ctx context.Context, req *fulfillmentv1.EscalatePackingSlipRequest) (*fulfillmentv1.EscalatePackingSlipResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePackingSlip(ctx, req.GetContext().GetTenantId(), req.GetPackingSlip()); err != nil {
		return nil, grpcx.Wrap(err, "save packing slip")
	}
	s.log.InfoContext(ctx, "escalate packing slip", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPackingSlip().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "display_name", req.GetPackingSlip().GetCounterparty().GetDisplayName())
	estimateTelemetryReadingReply, err := s.telemetryReadingCalculationClient.EstimateTelemetryReading(ctx, &telematicsv1.EstimateTelemetryReadingRequest{
		Context: req.GetContext(),
		TelemetryReading: &telematicsv1.TelemetryReading{
			Subject: &telematicsv1.GeoFenceRef{DisplayName: tenancy.Value(ctx, "display_name")},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.telematics.v1.TelemetryReadingCalculationService/EstimateTelemetryReading")
	}
	simulatePreferenceRequest := &customersv1.SimulatePreferenceRequest{
		Context: req.GetContext(),
		Preference: &customersv1.Preference{
			Owner: &identityv1.UserRef{
				DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
			},
		},
		Details: &customersv1.SimulatePreferenceRequest_Details{
			Destination: &commonv1.PostalAddress{
				Recipient:   req.GetPackingSlip().GetBillingAddress().GetRecipient(),
				CountryCode: req.GetPackingSlip().GetBillingAddress().GetCountryCode(),
			},
		},
	}
	simulatePreferenceReply, err := simulatePreferenceCall(ctx, simulatePreferenceRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceCalculationService/SimulatePreference")
	}
	countChartOfAccountsesRequest := &accountingv1.CountChartOfAccountsesRequest{
		Context: req.GetContext(),
		Filter: &accountingv1.CountChartOfAccountsesRequest_Filter{
			Region: req.GetPackingSlip().GetBillingAddress().GetRegion(),
		},
	}
	countChartOfAccountsesReply, err := s.countChartOfAccountsesFn(ctx, countChartOfAccountsesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.accounting.v1.ChartOfAccountsQueryService/CountChartOfAccountses")
	}
	return &fulfillmentv1.EscalatePackingSlipResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: estimateTelemetryReadingReply.GetResult().GetCorrelationId(),
			ReferenceIds:  estimateTelemetryReadingReply.GetResult().GetReferenceIds(),
			Message:       countChartOfAccountsesReply.GetResult().GetMessage(),
			Status:        "ok",
		},
		PackingSlip: &fulfillmentv1.PackingSlip{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: simulatePreferenceReply.GetPreference().GetAudit().GetUpdatedAt(),
			},
			PackingSlipId: req.GetPackingSlipId(),
			Metadata:      req.GetPackingSlip().GetMetadata(),
		},
	}, nil
}

// ListPendingPackingSlips lists packing slips a page at a time.
func (s *PackingSlipApprovalServer) ListPendingPackingSlips(ctx context.Context, req *fulfillmentv1.ListPendingPackingSlipsRequest) (*fulfillmentv1.ListPendingPackingSlipsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "list pending packing slips", "tenant", req.GetContext().GetTenantId())
	return &fulfillmentv1.ListPendingPackingSlipsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// DelegatePackingSlipApproval delegates a packing slip.
func (s *PackingSlipApprovalServer) DelegatePackingSlipApproval(ctx context.Context, req *fulfillmentv1.DelegatePackingSlipApprovalRequest) (*fulfillmentv1.DelegatePackingSlipApprovalResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected DelegatePackingSlipApproval", "error", err)
		return nil, err
	}
	if err := s.store.SavePackingSlip(ctx, req.GetContext().GetTenantId(), req.GetPackingSlip()); err != nil {
		return nil, grpcx.Wrap(err, "save packing slip")
	}
	s.log.InfoContext(ctx, "delegate packing slip approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPackingSlip().GetAudit().GetEtag())
	signalSubscriptionPlanWorkflowRequest := buildSignalSubscriptionPlanWorkflowRequestForDelegatePackingSlipApproval(req)
	signalSubscriptionPlanWorkflowReply, err := s.subscriptionPlanWorkflowClient.SignalSubscriptionPlanWorkflow(ctx, signalSubscriptionPlanWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionPlanWorkflowService/SignalSubscriptionPlanWorkflow")
	}
	scoreHouseholdRequest := &customersv1.ScoreHouseholdRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Location: &commonv1.PostalAddress{Lines: req.GetPackingSlip().GetBillingAddress().GetLines()},
					Manager: &identityv1.UserRef{
						DisplayName: signalSubscriptionPlanWorkflowReply.GetSubscriptionPlan().GetOwner().GetDisplayName(),
					},
				}},
			}},
		},
	}
	scoreHouseholdReply, err := s.householdValidationLazy().ScoreHousehold(ctx, scoreHouseholdRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdValidationService/ScoreHousehold")
	}
	return &fulfillmentv1.DelegatePackingSlipApprovalResponse{
		PackingSlip: &fulfillmentv1.PackingSlip{
			Audit: &commonv1.AuditInfo{
				CreatedBy: signalSubscriptionPlanWorkflowReply.GetSubscriptionPlan().GetAudit().GetCreatedBy(),
			},
			PackingSlipId: req.GetPackingSlipId(),
			Counterparty:  &fulfillmentv1.FulfillmentRef{Number: req.GetPackingSlip().GetNumber()},
		},
		Outcome: &fulfillmentv1.DelegatePackingSlipApprovalResponse_Outcome{Status: scoreHouseholdReply.GetResult().GetStatus()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RecallPackingSlip recalls a packing slip.
func (s *PackingSlipApprovalServer) RecallPackingSlip(ctx context.Context, req *fulfillmentv1.RecallPackingSlipRequest) (*fulfillmentv1.RecallPackingSlipResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SavePackingSlip(ctx, req.GetContext().GetTenantId(), req.GetPackingSlip()); err != nil {
		return nil, grpcx.Wrap(err, "save packing slip")
	}
	s.log.InfoContext(ctx, "recall packing slip", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPackingSlip().GetAudit().GetEtag())
	routes := map[string]func(context.Context, *fulfillmentv1.FindPackingSlipsRequest, ...grpc.CallOption) (*fulfillmentv1.FindPackingSlipsResponse, error){
		"primary":  s.packingSlipQueryClient.FindPackingSlips,
		"fallback": s.packingSlipQueryClientFallback.FindPackingSlips,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	cloned := proto.Clone(req.GetDetails()).(*fulfillmentv1.RecallPackingSlipRequest_Details)
	metadata2 := make(map[string]string, len(req.GetPackingSlip().GetMetadata()))
	for key, value := range req.GetPackingSlip().GetMetadata() {
		metadata2[key] = value
	}
	findPackingSlipsRequest := &fulfillmentv1.FindPackingSlipsRequest{
		Context: req.GetContext(),
		Filter: &fulfillmentv1.FindPackingSlipsRequest_Filter{
			Region:       req.GetPackingSlip().GetBillingAddress().GetRegion(),
			CreatedAfter: req.GetPackingSlip().GetIssuedAt(),
			OwnerId:      cloned.GetApprover().GetUserId(),
			Labels:       metadata2,
		},
	}
	findPackingSlipsReply, err := route(ctx, findPackingSlipsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.PackingSlipQueryService/FindPackingSlips")
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*sellersv1.ShareSellerScorecardReportResponse, len(req.GetPackingSlip().GetLines()))
	for i, line := range req.GetPackingSlip().GetLines() {
		group.Go(func() error {
			reply, err := s.sellerScorecardReportingClient.ShareSellerScorecardReport(groupCtx, &sellersv1.ShareSellerScorecardReportRequest{
				Context: req.GetContext(),
				Details: &sellersv1.ShareSellerScorecardReportRequest_Details{
					Recipient: &commonv1.ContactInfo{Email: req.GetDetails().GetApprover().GetEmail()},
					Variables: line.GetAttributes(),
				},
				Reason: req.GetReason(),
			})
			if err != nil {
				return err
			}
			replies[i] = reply
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardReportingService/ShareSellerScorecardReport")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	findFulfillmentsRequest := &fulfillmentv1.FindFulfillmentsRequest{
		Context: req.GetContext(),
		Filter: &fulfillmentv1.FindFulfillmentsRequest_Filter{
			Region:       req.GetPackingSlip().GetBillingAddress().GetRegion(),
			CreatedAfter: req.GetPackingSlip().GetAudit().GetCreatedAt(),
		},
	}
	findFulfillmentsReply := new(fulfillmentv1.FindFulfillmentsResponse)
	if err := grpcx.Invoke(ctx, s.conn, fulfillmentv1.FulfillmentQueryService_FindFulfillments_FullMethodName, findFulfillmentsRequest, findFulfillmentsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.FulfillmentQueryService/FindFulfillments")
	}
	exportToursStream, err := s.tourQueryClient.ExportTours(ctx, &toursv1.ExportToursRequest{
		Context: req.GetContext(),
		Filter: &toursv1.ExportToursRequest_Filter{
			Region: req.GetPackingSlip().GetBillingAddress().GetRegion(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.tours.v1.TourQueryService/ExportTours")
	}
	for {
		received, err := exportToursStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.tours.v1.TourQueryService/ExportTours")
		}
		_ = received
	}
	return &fulfillmentv1.RecallPackingSlipResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: findPackingSlipsReply.GetResult().GetReferenceIds(),
			Message:      findFulfillmentsReply.GetResult().GetMessage(),
			Status:       "ok",
		},
		Outcome: &fulfillmentv1.RecallPackingSlipResponse_Outcome{Message: findFulfillmentsReply.GetResult().GetMessage()},
		PackingSlip: &fulfillmentv1.PackingSlip{
			PackingSlipId: req.GetPackingSlipId(),
			Number:        req.GetPackingSlip().GetNumber(),
		},
	}, nil
}

// GetPackingSlipApprovalStatus returns a packing slip.
func (s *PackingSlipApprovalServer) GetPackingSlipApprovalStatus(ctx context.Context, req *fulfillmentv1.GetPackingSlipApprovalStatusRequest) (*fulfillmentv1.GetPackingSlipApprovalStatusResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "packing_slip", req.GetPackingSlipId(), "get_packing_slip_approval_status"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get packing slip approval status", "tenant", req.GetContext().GetTenantId())
	return &fulfillmentv1.GetPackingSlipApprovalStatusResponse{
		PackingSlip: &fulfillmentv1.PackingSlip{PackingSlipId: req.GetPackingSlipId()},
		Result:      &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CommentPackingSlipApproval comments a packing slip.
func (s *PackingSlipApprovalServer) CommentPackingSlipApproval(ctx context.Context, req *fulfillmentv1.CommentPackingSlipApprovalRequest) (*fulfillmentv1.CommentPackingSlipApprovalResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePackingSlip(ctx, req.GetContext().GetTenantId(), req.GetPackingSlip()); err != nil {
		return nil, grpcx.Wrap(err, "save packing slip")
	}
	s.log.InfoContext(ctx, "comment packing slip approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPackingSlip().GetAudit().GetEtag())
	customerValidation, ok := s.clients.Get("acme.customers.v1.CustomerValidationService").(customersv1.CustomerValidationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.customers.v1.CustomerValidationService")
	}
	ctx = tenancy.With(ctx, "display_name", req.GetDetails().GetApprover().GetDisplayName())
	explainCustomerDecisionRequest := &customersv1.ExplainCustomerDecisionRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			RelationshipManager: &identityv1.UserRef{DisplayName: tenancy.Value(ctx, "display_name")},
		},
	}
	explainCustomerDecisionReply, err := customerValidation.ExplainCustomerDecision(ctx, explainCustomerDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerValidationService/ExplainCustomerDecision")
	}
	return &fulfillmentv1.CommentPackingSlipApprovalResponse{
		Outcome: &fulfillmentv1.CommentPackingSlipApprovalResponse_Outcome{
			Message: explainCustomerDecisionReply.GetResult().GetMessage(),
		},
		PackingSlip: &fulfillmentv1.PackingSlip{
			Counterparty: &fulfillmentv1.FulfillmentRef{
				DisplayName: explainCustomerDecisionReply.GetCustomer().GetRelationshipManager().GetDisplayName(),
			},
			PackingSlipId: req.GetPackingSlipId(),
			Status:        req.GetPackingSlip().GetStatus(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// householdValidationLazy dials acme.customers.v1.HouseholdValidationService the first time a handler needs it.
func (s *PackingSlipApprovalServer) householdValidationLazy() customersv1.HouseholdValidationServiceClient {
	s.householdValidationOnce.Do(func() { s.householdValidationCached = customersv1.NewHouseholdValidationServiceClient(s.conn) })
	return s.householdValidationCached
}
