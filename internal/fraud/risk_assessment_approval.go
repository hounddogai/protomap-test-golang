package fraud

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/anypb"
)

// RiskAssessmentApprovalServer implements `acme.fraud.v1.RiskAssessmentApprovalService`.
// Routes risk assessments through approval, with escalation and delegation.
type RiskAssessmentApprovalServer struct {
	fraudv1.UnimplementedRiskAssessmentApprovalServiceServer
	store                          *Store
	log                            *slog.Logger
	conn                           *grpc.ClientConn
	riskAssessmentValidationClient fraudv1.RiskAssessmentValidationServiceClient
	fraudRuleCalculationClient     fraudv1.FraudRuleCalculationServiceClient
	customerValidationClient       customersv1.CustomerValidationServiceClient
	sellerScorecardApprovalClient  sellersv1.SellerScorecardApprovalServiceClient
	productReviewWorkflowOnce      sync.Once
	productReviewWorkflowCached    reviewsv1.ProductReviewWorkflowServiceClient
	fraudRuleLifecycleClient       fraudv1.FraudRuleLifecycleServiceClient
	customerOnce                   sync.Once
	customerCached                 customersv1.CustomerServiceClient
	riskAssessmentClient           fraudv1.RiskAssessmentServiceClient
	riskAssessmentClientFallback   fraudv1.RiskAssessmentServiceClient
	riskAssessmentQueryClient      fraudv1.RiskAssessmentQueryServiceClient
	userValidationClient           identityv1.UserValidationServiceClient
	returnRequestApprovalClient    returnsv1.ReturnRequestApprovalServiceClient
	fraudSignalValidationPort      fraudSignalValidationPort
	bus                            *eventbus.Bus
	riskAssessmentQueryPort        riskAssessmentQueryPort
}

// NewRiskAssessmentApprovalServer wires a RiskAssessmentApprovalServer.
func NewRiskAssessmentApprovalServer(deps platform.Deps) *RiskAssessmentApprovalServer {
	return &RiskAssessmentApprovalServer{
		store:                          NewStore(deps.DB),
		log:                            deps.Logger.With("service", "acme.fraud.v1.RiskAssessmentApprovalService"),
		conn:                           deps.Conn,
		riskAssessmentValidationClient: fraudv1.NewRiskAssessmentValidationServiceClient(deps.Conn),
		fraudRuleCalculationClient:     fraudv1.NewFraudRuleCalculationServiceClient(deps.Conn),
		customerValidationClient:       customersv1.NewCustomerValidationServiceClient(deps.Conn),
		sellerScorecardApprovalClient:  sellersv1.NewSellerScorecardApprovalServiceClient(deps.Conn),
		fraudRuleLifecycleClient:       fraudv1.NewFraudRuleLifecycleServiceClient(deps.Conn),
		riskAssessmentClient:           fraudv1.NewRiskAssessmentServiceClient(deps.Conn),
		riskAssessmentClientFallback:   fraudv1.NewRiskAssessmentServiceClient(deps.Conn),
		riskAssessmentQueryClient:      fraudv1.NewRiskAssessmentQueryServiceClient(deps.Conn),
		userValidationClient:           identityv1.NewUserValidationServiceClient(deps.Conn),
		returnRequestApprovalClient:    returnsv1.NewReturnRequestApprovalServiceClient(deps.Conn),
		fraudSignalValidationPort:      newFraudSignalValidationPort(deps),
		bus:                            deps.Bus,
		riskAssessmentQueryPort:        newRiskAssessmentQueryPort(deps),
	}
}

// SubmitRiskAssessment submits a risk assessment.
func (s *RiskAssessmentApprovalServer) SubmitRiskAssessment(ctx context.Context, req *fraudv1.SubmitRiskAssessmentRequest) (*fraudv1.SubmitRiskAssessmentResponse, error) {
	if err := s.store.SaveRiskAssessment(ctx, req.GetContext().GetTenantId(), req.GetRiskAssessment()); err != nil {
		return nil, grpcx.Wrap(err, "save risk assessment")
	}
	s.log.InfoContext(ctx, "submit risk assessment", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRiskAssessment().GetAudit().GetEtag())
	lintCustomersRequest := &customersv1.LintCustomersRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			Audit:               &commonv1.AuditInfo{Etag: req.GetRiskAssessment().GetAudit().GetEtag()},
			RelationshipManager: &identityv1.UserRef{Email: req.GetRiskAssessment().GetRequester().GetEmail()},
		},
		Details: &customersv1.LintCustomersRequest_Details{
			Contact: &commonv1.ContactInfo{Name: req.GetRiskAssessment().GetRequester().GetName()},
		},
	}
	lintCustomersReply := new(customersv1.LintCustomersResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", customersCustomerValidationServiceName, "LintCustomers"), lintCustomersRequest, lintCustomersReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerValidationService/LintCustomers")
	}
	return &fraudv1.SubmitRiskAssessmentResponse{
		RiskAssessment: &fraudv1.RiskAssessment{
			Audit: &commonv1.AuditInfo{
				CreatedAt: lintCustomersReply.GetCustomer().GetAudit().GetCreatedAt(),
			},
			RiskAssessmentId: req.GetRiskAssessmentId(),
		},
		Outcome: &fraudv1.SubmitRiskAssessmentResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ApproveRiskAssessment approves a risk assessment.
func (s *RiskAssessmentApprovalServer) ApproveRiskAssessment(ctx context.Context, req *fraudv1.ApproveRiskAssessmentRequest) (*fraudv1.ApproveRiskAssessmentResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ApproveRiskAssessment", "error", err)
		return nil, err
	}
	if err := s.store.SaveRiskAssessment(ctx, req.GetContext().GetTenantId(), req.GetRiskAssessment()); err != nil {
		return nil, grpcx.Wrap(err, "save risk assessment")
	}
	s.log.InfoContext(ctx, "approve risk assessment", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRiskAssessment().GetAudit().GetEtag())
	packed, err := anypb.New(req.GetRiskAssessment())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	scoreConsentRecordRequest := &customersv1.ScoreConsentRecordRequest{
		Context: req.GetContext(),
		ConsentRecord: &customersv1.ConsentRecord{
			Subject: &customersv1.HouseholdRef{
				DisplayName: req.GetRiskAssessment().GetAssignee().GetDisplayName(),
			},
			Measurements: []*customersv1.ConsentRecordMeasurement{&customersv1.ConsentRecordMeasurement{
				ObservedAt: req.GetRiskAssessment().GetAudit().GetUpdatedAt(),
			}},
			Detail: packed,
		},
		Reason: req.GetReason(),
	}
	scoreConsentRecordReply, err := grpcx.NewClient(s.conn, customersv1.NewConsentRecordValidationServiceClient).ScoreConsentRecord(ctx, scoreConsentRecordRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordValidationService/ScoreConsentRecord")
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*fraudv1.LintRiskAssessmentsResponse, len(req.GetRiskAssessment().GetComments()))
	for i, comment := range req.GetRiskAssessment().GetComments() {
		group.Go(func() error {
			reply, err := s.riskAssessmentValidationClient.LintRiskAssessments(groupCtx, &fraudv1.LintRiskAssessmentsRequest{
				Context: req.GetContext(),
				RiskAssessment: &fraudv1.RiskAssessment{
					Comments: []*fraudv1.RiskAssessmentComment{&fraudv1.RiskAssessmentComment{CommentId: comment.GetCommentId()}},
				},
			})
			if err != nil {
				return err
			}
			replies[i] = reply
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.RiskAssessmentValidationService/LintRiskAssessments")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	calculateFraudRuleRequest := &fraudv1.CalculateFraudRuleRequest{Context: req.GetContext()}
	type calculateFraudRuleResult struct {
		reply *fraudv1.CalculateFraudRuleResponse
		err   error
	}
	calculateFraudRuleResults := make(chan calculateFraudRuleResult, 1)
	go func() {
		reply, err := s.fraudRuleCalculationClient.CalculateFraudRule(ctx, calculateFraudRuleRequest)
		calculateFraudRuleResults <- calculateFraudRuleResult{reply, err}
	}()
	var calculateFraudRuleReply *fraudv1.CalculateFraudRuleResponse
	select {
	case result := <-calculateFraudRuleResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.fraud.v1.FraudRuleCalculationService/CalculateFraudRule")
		}
		calculateFraudRuleReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &fraudv1.ApproveRiskAssessmentResponse{
		RiskAssessment: &fraudv1.RiskAssessment{
			Assignee: &identityv1.UserRef{
				DisplayName: scoreConsentRecordReply.GetConsentRecord().GetSubject().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedBy: calculateFraudRuleReply.GetFraudRule().GetAudit().GetCreatedBy(),
			},
			Requester: &commonv1.ContactInfo{
				Email: calculateFraudRuleReply.GetFraudRule().GetOwner().GetEmail(),
			},
			RiskAssessmentId: req.GetRiskAssessmentId(),
			Status:           req.GetRiskAssessment().GetStatus(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: scoreConsentRecordReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// RejectRiskAssessment rejects a risk assessment.
func (s *RiskAssessmentApprovalServer) RejectRiskAssessment(ctx context.Context, req *fraudv1.RejectRiskAssessmentRequest) (*fraudv1.RejectRiskAssessmentResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveRiskAssessment(ctx, req.GetContext().GetTenantId(), req.GetRiskAssessment()); err != nil {
		return nil, grpcx.Wrap(err, "save risk assessment")
	}
	s.log.InfoContext(ctx, "reject risk assessment", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRiskAssessment().GetAudit().GetEtag())
	var holder struct{ TenantId string }
	if record := req.GetContext(); record != nil {
		if err := mapper.CopyFields(&holder, record, "TenantId"); err != nil {
			s.log.WarnContext(ctx, "copy tenant_id", "error", err)
		}
	}
	getTenantPreferencesRequest := &identityv1.GetTenantPreferencesRequest{
		Context:  req.GetContext(),
		TenantId: holder.TenantId,
	}
	getTenantPreferencesReply, err := getTenantPreferencesCall(ctx, getTenantPreferencesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantNotificationService/GetTenantPreferences")
	}
	step := &riskAssessmentApprovalServerRejectRiskAssessmentScoreCustomerStep{request: &customersv1.ScoreCustomerRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: getTenantPreferencesReply.GetTenant().GetAudit().GetUpdatedBy(),
				Etag:      getTenantPreferencesReply.GetTenant().GetAudit().GetEtag(),
			},
		},
	}}
	if err := s.runRejectRiskAssessmentScoreCustomer(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerValidationService/ScoreCustomer")
	}
	scoreCustomerReply := step.reply
	listPointsLedgerWorkflowsRequest := &loyaltyv1.ListPointsLedgerWorkflowsRequest{
		Context: req.GetContext(),
		Filter: &loyaltyv1.ListPointsLedgerWorkflowsRequest_Filter{
			Region: getTenantPreferencesReply.GetTenant().GetLocation().GetRegion(),
		},
	}
	listPointsLedgerWorkflowsReply := new(loyaltyv1.ListPointsLedgerWorkflowsResponse)
	if err := grpcx.Invoke(ctx, s.conn, loyaltyv1.PointsLedgerWorkflowService_ListPointsLedgerWorkflows_FullMethodName, listPointsLedgerWorkflowsRequest, listPointsLedgerWorkflowsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerWorkflowService/ListPointsLedgerWorkflows")
	}
	for comment := range iterx.Each(req.GetRiskAssessment().GetComments(), func(candidate *fraudv1.RiskAssessmentComment) bool { return candidate != nil }) {
		if _, err := s.sellerScorecardApprovalClient.RecallSellerScorecard(ctx, &sellersv1.RecallSellerScorecardRequest{
			Context: req.GetContext(),
			Details: &sellersv1.RecallSellerScorecardRequest_Details{
				Approver: &identityv1.UserRef{Email: comment.GetAuthorEmail()},
			},
			SellerScorecard: &sellersv1.SellerScorecard{
				Audit: &commonv1.AuditInfo{CreatedAt: comment.GetCreatedAt()},
				BillingAddress: &commonv1.PostalAddress{
					Locality: scoreCustomerReply.GetCustomer().GetPrimaryAddress().GetLocality(),
				},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardApprovalService/RecallSellerScorecard")
		}
	}
	return &fraudv1.RejectRiskAssessmentResponse{
		RiskAssessment: &fraudv1.RiskAssessment{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: getTenantPreferencesReply.GetTenant().GetAudit().GetUpdatedAt(),
				Etag:      scoreCustomerReply.GetCustomer().GetAudit().GetEtag(),
			},
			Assignee: &identityv1.UserRef{
				Email:       getTenantPreferencesReply.GetTenant().GetManager().GetEmail(),
				DisplayName: scoreCustomerReply.GetCustomer().GetRelationshipManager().GetDisplayName(),
			},
			RiskAssessmentId: req.GetRiskAssessmentId(),
			Subject:          req.GetRiskAssessment().GetSubject(),
		},
		Result: &commonv1.OperationResult{
			Details: listPointsLedgerWorkflowsReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}

// EscalateRiskAssessment escalates a risk assessment.
func (s *RiskAssessmentApprovalServer) EscalateRiskAssessment(ctx context.Context, req *fraudv1.EscalateRiskAssessmentRequest) (*fraudv1.EscalateRiskAssessmentResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected EscalateRiskAssessment", "error", err)
		return nil, err
	}
	if err := s.store.SaveRiskAssessment(ctx, req.GetContext().GetTenantId(), req.GetRiskAssessment()); err != nil {
		return nil, grpcx.Wrap(err, "save risk assessment")
	}
	s.log.InfoContext(ctx, "escalate risk assessment", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRiskAssessment().GetAudit().GetEtag())
	encoded, err := protojson.Marshal(req.GetRiskAssessment())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded fraudv1.RiskAssessment
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	cancelProductReviewWorkflowRequest := &reviewsv1.CancelProductReviewWorkflowRequest{
		Context: req.GetContext(),
		ProductReview: &reviewsv1.ProductReview{
			Counterparty: &customersv1.CustomerRef{Email: (&decoded).GetAssignee().GetEmail()},
		},
		Details: &reviewsv1.CancelProductReviewWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				PreferredChannel: req.GetRiskAssessment().GetRequester().GetPreferredChannel(),
			},
		},
	}
	cancelProductReviewWorkflowReply, err := s.productReviewWorkflowLazy().CancelProductReviewWorkflow(ctx, cancelProductReviewWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ProductReviewWorkflowService/CancelProductReviewWorkflow")
	}
	scheduleFraudRuleExpiryRequest := &fraudv1.ScheduleFraudRuleExpiryRequest{
		Context: req.GetContext(),
		FraudRule: &fraudv1.FraudRule{
			Owner: &identityv1.UserRef{
				DisplayName: cancelProductReviewWorkflowReply.GetProductReview().GetCounterparty().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedBy: cancelProductReviewWorkflowReply.GetProductReview().GetAudit().GetCreatedBy(),
			},
		},
	}
	type scheduleFraudRuleExpiryResult struct {
		reply *fraudv1.ScheduleFraudRuleExpiryResponse
		err   error
	}
	scheduleFraudRuleExpiryResults := make(chan scheduleFraudRuleExpiryResult, 1)
	go func() {
		reply, err := s.fraudRuleLifecycleClient.ScheduleFraudRuleExpiry(ctx, scheduleFraudRuleExpiryRequest)
		scheduleFraudRuleExpiryResults <- scheduleFraudRuleExpiryResult{reply, err}
	}()
	var scheduleFraudRuleExpiryReply *fraudv1.ScheduleFraudRuleExpiryResponse
	select {
	case result := <-scheduleFraudRuleExpiryResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.fraud.v1.FraudRuleLifecycleService/ScheduleFraudRuleExpiry")
		}
		scheduleFraudRuleExpiryReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	updateCustomerRequest := &customersv1.UpdateCustomerRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: cancelProductReviewWorkflowReply.GetProductReview().GetAudit().GetUpdatedAt(),
			},
			RelationshipManager: &identityv1.UserRef{
				Email: cancelProductReviewWorkflowReply.GetProductReview().GetCounterparty().GetEmail(),
			},
		},
	}
	updateCustomerReply, err := s.customerLazy().UpdateCustomer(ctx, updateCustomerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerService/UpdateCustomer")
	}
	return &fraudv1.EscalateRiskAssessmentResponse{
		Result: &commonv1.OperationResult{
			Status:        cancelProductReviewWorkflowReply.GetResult().GetStatus(),
			CorrelationId: scheduleFraudRuleExpiryReply.GetResult().GetCorrelationId(),
		},
		RiskAssessment: &fraudv1.RiskAssessment{
			Assignee: &identityv1.UserRef{
				Email: cancelProductReviewWorkflowReply.GetProductReview().GetCounterparty().GetEmail(),
			},
			Requester: &commonv1.ContactInfo{
				Email: updateCustomerReply.GetCustomer().GetContact().GetEmail(),
			},
			RiskAssessmentId: req.GetRiskAssessmentId(),
			Subject:          req.GetRiskAssessment().GetSubject(),
		},
	}, nil
}

// DelegateRiskAssessmentApproval delegates a risk assessment.
func (s *RiskAssessmentApprovalServer) DelegateRiskAssessmentApproval(ctx context.Context, req *fraudv1.DelegateRiskAssessmentApprovalRequest) (*fraudv1.DelegateRiskAssessmentApprovalResponse, error) {
	if err := s.store.SaveRiskAssessment(ctx, req.GetContext().GetTenantId(), req.GetRiskAssessment()); err != nil {
		return nil, grpcx.Wrap(err, "save risk assessment")
	}
	s.log.InfoContext(ctx, "delegate risk assessment approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRiskAssessment().GetAudit().GetEtag())
	routes := map[string]func(context.Context, *fraudv1.BatchGetRiskAssessmentsRequest, ...grpc.CallOption) (*fraudv1.BatchGetRiskAssessmentsResponse, error){
		"primary":  s.riskAssessmentClient.BatchGetRiskAssessments,
		"fallback": s.riskAssessmentClientFallback.BatchGetRiskAssessments,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	batchGetRiskAssessmentsRequest := &fraudv1.BatchGetRiskAssessmentsRequest{Context: req.GetContext()}
	batchGetRiskAssessmentsReply, err := route(ctx, batchGetRiskAssessmentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.RiskAssessmentService/BatchGetRiskAssessments")
	}
	summarizeRiskAssessmentsRequest := &fraudv1.SummarizeRiskAssessmentsRequest{
		Context:          req.GetContext(),
		RiskAssessmentId: req.GetRiskAssessment().GetRiskAssessmentId(),
		Reason:           req.GetReason(),
		Annotations:      req.GetAnnotations(),
	}
	summarizeRiskAssessmentsReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*fraudv1.SummarizeRiskAssessmentsResponse, error) {
		return s.riskAssessmentQueryClient.SummarizeRiskAssessments(ctx, summarizeRiskAssessmentsRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.RiskAssessmentQueryService/SummarizeRiskAssessments")
	}
	validateSearchQueryRequest := &searchv1.ValidateSearchQueryRequest{
		Context: req.GetContext(),
		Details: &searchv1.ValidateSearchQueryRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email: req.GetRiskAssessment().GetAssignee().GetEmail(),
				Phone: req.GetRiskAssessment().GetRequester().GetPhone(),
				Name:  req.GetRiskAssessment().GetRequester().GetName(),
			},
		},
		SearchQuery: &searchv1.SearchQuery{
			Subject: &searchv1.SearchIndexRef{
				DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
			},
		},
	}
	validateSearchQueryReply := new(searchv1.ValidateSearchQueryResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", searchSearchQueryValidationServiceName, "ValidateSearchQuery"), validateSearchQueryRequest, validateSearchQueryReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryValidationService/ValidateSearchQuery")
	}
	recallSellerScorecardRequest := &sellersv1.RecallSellerScorecardRequest{
		Context: req.GetContext(),
		Details: &sellersv1.RecallSellerScorecardRequest_Details{
			Approver: &identityv1.UserRef{
				DisplayName: req.GetRiskAssessment().GetCustomer().GetDisplayName(),
			},
		},
	}
	recallSellerScorecardReply := new(sellersv1.RecallSellerScorecardResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", sellersSellerScorecardApprovalServiceName, "RecallSellerScorecard"), recallSellerScorecardRequest, recallSellerScorecardReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardApprovalService/RecallSellerScorecard")
	}
	return &fraudv1.DelegateRiskAssessmentApprovalResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: batchGetRiskAssessmentsReply.GetResult().GetReferenceIds(),
			Status:       summarizeRiskAssessmentsReply.GetResult().GetStatus(),
		},
		Outcome: &fraudv1.DelegateRiskAssessmentApprovalResponse_Outcome{Message: validateSearchQueryReply.GetOutcome().GetMessage()},
		RiskAssessment: &fraudv1.RiskAssessment{
			Sla: &fraudv1.RiskAssessmentSla{
				DueAt: recallSellerScorecardReply.GetSellerScorecard().GetDueAt(),
			},
			RiskAssessmentId: req.GetRiskAssessmentId(),
			CustomFields:     req.GetRiskAssessment().GetCustomFields(),
		},
	}, nil
}

// RecallRiskAssessment recalls a risk assessment.
func (s *RiskAssessmentApprovalServer) RecallRiskAssessment(ctx context.Context, req *fraudv1.RecallRiskAssessmentRequest) (*fraudv1.RecallRiskAssessmentResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RecallRiskAssessment", "error", err)
		return nil, err
	}
	if err := s.store.SaveRiskAssessment(ctx, req.GetContext().GetTenantId(), req.GetRiskAssessment()); err != nil {
		return nil, grpcx.Wrap(err, "save risk assessment")
	}
	s.log.InfoContext(ctx, "recall risk assessment", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRiskAssessment().GetAudit().GetEtag())
	encoded, err := protojson.Marshal(req.GetRiskAssessment())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded fraudv1.RiskAssessment
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	listWatchlistReportsRequest := &fraudv1.ListWatchlistReportsRequest{
		Context: req.GetContext(),
		Filter: &fraudv1.ListWatchlistReportsRequest_Filter{
			CreatedAfter: (&decoded).GetSla().GetDueAt(),
			OwnerId:      req.GetRiskAssessment().GetSla().GetPolicyId(),
		},
	}
	listWatchlistReportsReply := new(fraudv1.ListWatchlistReportsResponse)
	if err := grpcx.Invoke(ctx, s.conn, fraudv1.WatchlistReportingService_ListWatchlistReports_FullMethodName, listWatchlistReportsRequest, listWatchlistReportsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.WatchlistReportingService/ListWatchlistReports")
	}
	lintUsersRequest := &identityv1.LintUsersRequest{
		Context: req.GetContext(),
		User: &identityv1.User{
			Contact: &commonv1.ContactInfo{
				Email:            req.GetRiskAssessment().GetCustomer().GetEmail(),
				PreferredChannel: req.GetRiskAssessment().GetRequester().GetPreferredChannel(),
				Name:             req.GetRiskAssessment().GetRequester().GetName(),
			},
			Audit: &commonv1.AuditInfo{Etag: req.GetRiskAssessment().GetAudit().GetEtag()},
		},
	}
	lintUsersReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*identityv1.LintUsersResponse, error) {
		return s.userValidationClient.LintUsers(ctx, lintUsersRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserValidationService/LintUsers")
	}
	return &fraudv1.RecallRiskAssessmentResponse{
		Result: &commonv1.OperationResult{
			Message: listWatchlistReportsReply.GetResult().GetMessage(),
			Status:  lintUsersReply.GetOutcome().GetStatus(),
		},
		Outcome: &fraudv1.RecallRiskAssessmentResponse_Outcome{Message: listWatchlistReportsReply.GetResult().GetMessage()},
		RiskAssessment: &fraudv1.RiskAssessment{
			RiskAssessmentId: req.GetRiskAssessmentId(),
			CustomFields:     req.GetRiskAssessment().GetCustomFields(),
		},
	}, nil
}

// GetRiskAssessmentApprovalStatus returns a risk assessment.
func (s *RiskAssessmentApprovalServer) GetRiskAssessmentApprovalStatus(ctx context.Context, req *fraudv1.GetRiskAssessmentApprovalStatusRequest) (*fraudv1.GetRiskAssessmentApprovalStatusResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "risk_assessment", req.GetRiskAssessmentId(), "get_risk_assessment_approval_status"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get risk assessment approval status", "tenant", req.GetContext().GetTenantId())
	return &fraudv1.GetRiskAssessmentApprovalStatusResponse{
		RiskAssessment: &fraudv1.RiskAssessment{RiskAssessmentId: req.GetRiskAssessmentId()},
		Result:         &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CommentRiskAssessmentApproval comments a risk assessment.
func (s *RiskAssessmentApprovalServer) CommentRiskAssessmentApproval(ctx context.Context, req *fraudv1.CommentRiskAssessmentApprovalRequest) (*fraudv1.CommentRiskAssessmentApprovalResponse, error) {
	if err := s.store.SaveRiskAssessment(ctx, req.GetContext().GetTenantId(), req.GetRiskAssessment()); err != nil {
		return nil, grpcx.Wrap(err, "save risk assessment")
	}
	s.log.InfoContext(ctx, "comment risk assessment approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRiskAssessment().GetAudit().GetEtag())
	delegateReturnRequestApprovalReply, err := s.returnRequestApprovalClient.DelegateReturnRequestApproval(ctx, &returnsv1.DelegateReturnRequestApprovalRequest{
		Context: req.GetContext(),
		ReturnRequest: &returnsv1.ReturnRequest{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: req.GetRiskAssessment().GetAudit().GetUpdatedAt(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnRequestApprovalService/DelegateReturnRequestApproval")
	}
	packed, err := anypb.New(req.GetRiskAssessment())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	previewFraudSignalRequest := &fraudv1.PreviewFraudSignalRequest{
		Context: req.GetContext(),
		FraudSignal: &fraudv1.FraudSignal{
			Subject: &fraudv1.FraudRuleRef{
				DisplayName: req.GetRiskAssessment().GetCustomer().GetDisplayName(),
			},
			Detail: packed,
		},
		Details: &fraudv1.PreviewFraudSignalRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetRiskAssessment().GetAssignee().GetEmail()},
		},
	}
	previewFraudSignalReply, err := s.fraudSignalValidationPort.PreviewFraudSignal(ctx, previewFraudSignalRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.FraudSignalValidationService/PreviewFraudSignal")
	}
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "fraud.comment_risk_assessment_approval.submit_seller_scorecard",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"details.approver.email":                     req.GetDetails().GetApprover().GetEmail(),
			"seller_scorecard.counterparty.display_name": req.GetRiskAssessment().GetCustomer().GetDisplayName(),
			"context.tenant_id":                          req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	resolveRiskAssessmentsRequest := &fraudv1.ResolveRiskAssessmentsRequest{Context: req.GetContext()}
	resolveRiskAssessmentsReply, err := s.riskAssessmentQueryPort.ResolveRiskAssessments(ctx, resolveRiskAssessmentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.RiskAssessmentQueryService/ResolveRiskAssessments")
	}
	return &fraudv1.CommentRiskAssessmentApprovalResponse{
		RiskAssessment: &fraudv1.RiskAssessment{
			Assignee: &identityv1.UserRef{
				DisplayName: delegateReturnRequestApprovalReply.GetReturnRequest().GetCounterparty().GetDisplayName(),
			},
			RiskAssessmentId: req.GetRiskAssessmentId(),
			Priority:         req.GetRiskAssessment().GetPriority(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: previewFraudSignalReply.GetFraudSignal().GetCorrelationId(),
			Status:        "ok",
		},
		Outcome: &fraudv1.CommentRiskAssessmentApprovalResponse_Outcome{
			Message: resolveRiskAssessmentsReply.GetResult().GetMessage(),
		},
	}, nil
}

// riskAssessmentApprovalServerRejectRiskAssessmentScoreCustomerStep carries the request of one step of
// RejectRiskAssessment and, once it ran, its reply.
type riskAssessmentApprovalServerRejectRiskAssessmentScoreCustomerStep struct {
	request *customersv1.ScoreCustomerRequest
	reply   *customersv1.ScoreCustomerResponse
}

func (s *RiskAssessmentApprovalServer) runRejectRiskAssessmentScoreCustomer(ctx context.Context, step *riskAssessmentApprovalServerRejectRiskAssessmentScoreCustomerStep) error {
	reply, err := s.customerValidationClient.ScoreCustomer(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}

// productReviewWorkflowLazy dials acme.reviews.v1.ProductReviewWorkflowService the first time a handler needs it.
func (s *RiskAssessmentApprovalServer) productReviewWorkflowLazy() reviewsv1.ProductReviewWorkflowServiceClient {
	s.productReviewWorkflowOnce.Do(func() { s.productReviewWorkflowCached = reviewsv1.NewProductReviewWorkflowServiceClient(s.conn) })
	return s.productReviewWorkflowCached
}

// customerLazy dials acme.customers.v1.CustomerService the first time a handler needs it.
func (s *RiskAssessmentApprovalServer) customerLazy() customersv1.CustomerServiceClient {
	s.customerOnce.Do(func() { s.customerCached = customersv1.NewCustomerServiceClient(s.conn) })
	return s.customerCached
}
