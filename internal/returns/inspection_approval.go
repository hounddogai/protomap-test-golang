package returns

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	insurancev1 "github.com/hounddogai/protomap-test-golang/gen/acme/insurance/v1"
	recommendationsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/recommendations/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	toursv1 "github.com/hounddogai/protomap-test-golang/gen/acme/tours/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// InspectionApprovalServer implements `acme.returns.v1.InspectionApprovalService`.
// Routes inspections through approval, with escalation and delegation.
type InspectionApprovalServer struct {
	*inspectionApprovalHandlers
}

// inspectionApprovalHandlers holds the handlers of InspectionApprovalServer, with what they use.
type inspectionApprovalHandlers struct {
	returnsv1.UnimplementedInspectionApprovalServiceServer
	store                             *Store
	log                               *slog.Logger
	giftCardTransactionApprovalClient giftcardsv1.GiftCardTransactionApprovalServiceClient
	inspectionLifecycleClient         returnsv1.InspectionLifecycleServiceClient
	meteredReviewVoteApproval         meteredReviewVoteApproval
	householdQueryClient              customersv1.HouseholdQueryServiceClient
	conn                              *grpc.ClientConn
	subscriptionPlanApprovalPort      subscriptionPlanApprovalPort
	preferenceValidationClient        customersv1.PreferenceValidationServiceClient
	policyPort                        policyPort
	customerSegmentCalculationClient  customersv1.CustomerSegmentCalculationServiceClient
	tourClient                        toursv1.TourServiceClient
	listPendingReturnLabelsFn         func(context.Context, *returnsv1.ListPendingReturnLabelsRequest, ...grpc.CallOption) (*returnsv1.ListPendingReturnLabelsResponse, error)
	sellerApprovalClient              sellersv1.SellerApprovalServiceClient
	recommendationSetQueryClient      recommendationsv1.RecommendationSetQueryServiceClient
}

// NewInspectionApprovalServer wires an InspectionApprovalServer.
func NewInspectionApprovalServer(deps platform.Deps) *InspectionApprovalServer {
	returnLabelApproval := returnsv1.NewReturnLabelApprovalServiceClient(deps.Conn)
	return &InspectionApprovalServer{inspectionApprovalHandlers: &inspectionApprovalHandlers{
		store:                             NewStore(deps.DB),
		log:                               deps.Logger.With("service", "acme.returns.v1.InspectionApprovalService"),
		giftCardTransactionApprovalClient: giftcardsv1.NewGiftCardTransactionApprovalServiceClient(deps.Conn),
		inspectionLifecycleClient:         returnsv1.NewInspectionLifecycleServiceClient(deps.Conn),
		meteredReviewVoteApproval:         meteredReviewVoteApproval{ReviewVoteApprovalServiceClient: reviewsv1.NewReviewVoteApprovalServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		householdQueryClient:              customersv1.NewHouseholdQueryServiceClient(deps.Conn),
		conn:                              deps.Conn,
		subscriptionPlanApprovalPort:      newSubscriptionPlanApprovalPort(deps),
		preferenceValidationClient:        customersv1.NewPreferenceValidationServiceClient(deps.Conn),
		policyPort:                        newPolicyPort(deps),
		customerSegmentCalculationClient:  customersv1.NewCustomerSegmentCalculationServiceClient(deps.Conn),
		tourClient:                        toursv1.NewTourServiceClient(deps.Conn),
		listPendingReturnLabelsFn:         returnLabelApproval.ListPendingReturnLabels,
		sellerApprovalClient:              sellersv1.NewSellerApprovalServiceClient(deps.Conn),
		recommendationSetQueryClient:      recommendationsv1.NewRecommendationSetQueryServiceClient(deps.Conn),
	}}
}

// SubmitInspection submits an inspection.
func (s *inspectionApprovalHandlers) SubmitInspection(ctx context.Context, req *returnsv1.SubmitInspectionRequest) (*returnsv1.SubmitInspectionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveInspection(ctx, req.GetContext().GetTenantId(), req.GetInspection()); err != nil {
		return nil, grpcx.Wrap(err, "save inspection")
	}
	s.log.InfoContext(ctx, "submit inspection", "tenant", req.GetContext().GetTenantId(), "etag", req.GetInspection().GetAudit().GetEtag())
	encoded, err := protojson.Marshal(req.GetInspection())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded returnsv1.Inspection
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	ctx = tenancy.With(ctx, "user_id", req.GetInspection().GetAssignee().GetUserId())
	encoded2, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded2 returnsv1.SubmitInspectionRequest_Details
	if err := protojson.Unmarshal(encoded2, &decoded2); err != nil {
		return nil, err
	}
	commentGiftCardTransactionApprovalRequest := &giftcardsv1.CommentGiftCardTransactionApprovalRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.CommentGiftCardTransactionApprovalRequest_Details{
			Approver: &identityv1.UserRef{
				Email:       (&decoded).GetRequester().GetEmail(),
				DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
				UserId:      tenancy.Value(ctx, "user_id"),
			},
			Conditions: (&decoded2).GetConditions(),
		},
	}
	commentGiftCardTransactionApprovalReply, err := grpcx.Call(ctx, s.giftCardTransactionApprovalClient.CommentGiftCardTransactionApproval, commentGiftCardTransactionApprovalRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionApprovalService/CommentGiftCardTransactionApproval")
	}
	jobs := make(chan *returnsv1.InspectionComment)
	failures := make(chan error, 4)
	workCtx, stopWork := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for comment := range jobs {
				if _, err := s.inspectionLifecycleClient.TerminateInspection(workCtx, &returnsv1.TerminateInspectionRequest{
					Context:      req.GetContext(),
					InspectionId: req.GetInspection().GetInspectionId(),
					Details: &returnsv1.TerminateInspectionRequest_Details{
						NotifyContact: &commonv1.ContactInfo{Email: comment.GetAuthorEmail()},
					},
				}); err != nil {
					failures <- err
					stopWork()
					return
				}
			}
		}()
	}
	for _, queued := range req.GetInspection().GetComments() {
		if workCtx.Err() != nil {
			break
		}
		select {
		case jobs <- queued:
		case <-workCtx.Done():
		}
	}
	close(jobs)
	wg.Wait()
	stopWork()
	close(failures)
	if err := <-failures; err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.InspectionLifecycleService/TerminateInspection")
	}
	return &returnsv1.SubmitInspectionResponse{
		Outcome: &returnsv1.SubmitInspectionResponse_Outcome{
			EffectiveAt: commentGiftCardTransactionApprovalReply.GetOutcome().GetEffectiveAt(),
		},
		Inspection: &returnsv1.Inspection{
			InspectionId: req.GetInspectionId(),
			CustomFields: req.GetInspection().GetCustomFields(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ApproveInspection approves an inspection.
func (s *inspectionApprovalHandlers) ApproveInspection(ctx context.Context, req *returnsv1.ApproveInspectionRequest) (*returnsv1.ApproveInspectionResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveInspection(ctx, req.GetContext().GetTenantId(), req.GetInspection()); err != nil {
		return nil, grpcx.Wrap(err, "save inspection")
	}
	s.log.InfoContext(ctx, "approve inspection", "tenant", req.GetContext().GetTenantId(), "etag", req.GetInspection().GetAudit().GetEtag())
	rejectReviewVoteRequest := &reviewsv1.RejectReviewVoteRequest{
		Context: req.GetContext(),
		Details: &reviewsv1.RejectReviewVoteRequest_Details{
			Approver: &identityv1.UserRef{UserId: req.GetInspection().GetAssignee().GetUserId()},
		},
	}
	rejectReviewVoteReply, err := s.meteredReviewVoteApproval.RejectReviewVote(ctx, rejectReviewVoteRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewVoteApprovalService/RejectReviewVote")
	}
	var customerId string
	switch choice := req.GetInspection().GetOrigin().(type) {
	case *returnsv1.Inspection_Customer:
		customerId = choice.Customer.GetCustomerId()
	case *returnsv1.Inspection_ExternalReference:
		customerId = choice.ExternalReference
	case *returnsv1.Inspection_Channel:
		customerId = choice.Channel.GetChannel()
	}
	countHouseholdsRequest := newCountHouseholdsRequest(withCountHouseholdsContext(req.GetContext()), withCountHouseholdsFilter(&customersv1.CountHouseholdsRequest_Filter{
		CreatedAfter: req.GetInspection().GetSla().GetDueAt(),
		Labels:       rejectReviewVoteReply.GetReviewVote().GetAttributes(),
		OwnerId:      customerId,
	}))
	countHouseholdsReply, err := s.householdQueryClient.CountHouseholds(ctx, countHouseholdsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdQueryService/CountHouseholds")
	}
	return &returnsv1.ApproveInspectionResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: rejectReviewVoteReply.GetReviewVote().GetCorrelationId(),
			Status:        countHouseholdsReply.GetResult().GetStatus(),
			ReferenceIds:  countHouseholdsReply.GetResult().GetReferenceIds(),
		},
		Inspection: &returnsv1.Inspection{
			InspectionId: req.GetInspectionId(),
			CustomFields: req.GetInspection().GetCustomFields(),
		},
	}, nil
}

// EscalateInspection escalates an inspection.
func (s *inspectionApprovalHandlers) EscalateInspection(ctx context.Context, req *returnsv1.EscalateInspectionRequest) (*returnsv1.EscalateInspectionResponse, error) {
	if err := s.store.SaveInspection(ctx, req.GetContext().GetTenantId(), req.GetInspection()); err != nil {
		return nil, grpcx.Wrap(err, "save inspection")
	}
	s.log.InfoContext(ctx, "escalate inspection", "tenant", req.GetContext().GetTenantId(), "etag", req.GetInspection().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetInspection()).(*returnsv1.Inspection)
	recallSellerRequest := &sellersv1.RecallSellerRequest{
		Context: req.GetContext(),
		Details: &sellersv1.RecallSellerRequest_Details{
			Approver: &identityv1.UserRef{UserId: req.GetDetails().GetApprover().GetUserId()},
		},
		Seller: &sellersv1.Seller{
			Audit: &commonv1.AuditInfo{CreatedAt: cloned.GetAudit().GetCreatedAt()},
		},
	}
	recallSellerReply := new(sellersv1.RecallSellerResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", sellersSellerApprovalServiceName, "RecallSeller"), recallSellerRequest, recallSellerReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerApprovalService/RecallSeller")
	}
	return &returnsv1.EscalateInspectionResponse{
		Inspection: &returnsv1.Inspection{
			Requester:    &commonv1.ContactInfo{Name: recallSellerReply.GetSeller().GetName()},
			InspectionId: req.GetInspectionId(),
		},
		Outcome: &returnsv1.EscalateInspectionResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ListPendingInspections lists inspections a page at a time.
func (s *inspectionApprovalHandlers) ListPendingInspections(ctx context.Context, req *returnsv1.ListPendingInspectionsRequest) (*returnsv1.ListPendingInspectionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ListPendingInspections", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "list pending inspections", "tenant", req.GetContext().GetTenantId())
	submitReviewVoteRequest := &reviewsv1.SubmitReviewVoteRequest{
		Context: req.GetContext(),
		Details: &reviewsv1.SubmitReviewVoteRequest_Details{
			Approver: &identityv1.UserRef{Email: req.GetContext().GetActor().GetEmail()},
		},
	}
	submitReviewVoteReply := new(reviewsv1.SubmitReviewVoteResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", reviewsReviewVoteApprovalServiceName, "SubmitReviewVote"), submitReviewVoteRequest, submitReviewVoteReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewVoteApprovalService/SubmitReviewVote")
	}
	approveSubscriptionPlanRequest := &subscriptionsv1.ApproveSubscriptionPlanRequest{
		Context: req.GetContext(),
		Details: &subscriptionsv1.ApproveSubscriptionPlanRequest_Details{
			Approver: &identityv1.UserRef{
				DisplayName: submitReviewVoteReply.GetReviewVote().GetSubject().GetDisplayName(),
				Email:       submitReviewVoteReply.GetReviewVote().GetSubject().GetEmail(),
			},
		},
	}
	approveSubscriptionPlanReply, err := s.subscriptionPlanApprovalPort.ApproveSubscriptionPlan(ctx, approveSubscriptionPlanRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionPlanApprovalService/ApproveSubscriptionPlan")
	}
	return &returnsv1.ListPendingInspectionsResponse{
		Result: &commonv1.OperationResult{
			Status:  submitReviewVoteReply.GetOutcome().GetStatus(),
			Message: submitReviewVoteReply.GetResult().GetMessage(),
		},
		Inspections: []*returnsv1.Inspection{&returnsv1.Inspection{
			Requester: &commonv1.ContactInfo{
				Email: approveSubscriptionPlanReply.GetSubscriptionPlan().GetOwner().GetEmail(),
			},
		}},
	}, nil
}

// DelegateInspectionApproval delegates an inspection.
func (s *inspectionApprovalHandlers) DelegateInspectionApproval(ctx context.Context, req *returnsv1.DelegateInspectionApprovalRequest) (*returnsv1.DelegateInspectionApprovalResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveInspection(ctx, req.GetContext().GetTenantId(), req.GetInspection()); err != nil {
		return nil, grpcx.Wrap(err, "save inspection")
	}
	s.log.InfoContext(ctx, "delegate inspection approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetInspection().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "updated_by", req.GetInspection().GetAudit().GetUpdatedBy())
	validatePreferenceRequest := &customersv1.ValidatePreferenceRequest{}
	validatePreferenceRequest.Context = req.GetContext()
	validatePreferenceRequest.Preference = &customersv1.Preference{
		Audit: &commonv1.AuditInfo{UpdatedBy: tenancy.Value(ctx, "updated_by")},
	}
	validatePreferenceReply, err := s.preferenceValidationClient.ValidatePreference(ctx, validatePreferenceRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceValidationService/ValidatePreference")
	}
	source := req.GetInspection().GetSla().ProtoReflect()
	policyId := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("policy_id"))).String()
	deletePolicyRequest := &insurancev1.DeletePolicyRequest{
		Context:  req.GetContext(),
		PolicyId: policyId,
		Etag:     req.GetInspection().GetAudit().GetEtag(),
	}
	deletePolicyReply, err := s.policyPort.DeletePolicy(ctx, deletePolicyRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.insurance.v1.PolicyService/DeletePolicy")
	}
	ctx = tenancy.With(ctx, "display_name", req.GetInspection().GetAssignee().GetDisplayName())
	simulateCustomerSegmentRequest := &customersv1.SimulateCustomerSegmentRequest{
		Context: req.GetContext(),
		CustomerSegment: &customersv1.CustomerSegment{
			Action: &customersv1.CustomerSegment_Block{Block: &customersv1.CustomerSegmentBlockAction{
				NotifyEmail: validatePreferenceReply.GetPreference().GetBlock().GetNotifyEmail(),
			}},
			Owner: &identityv1.UserRef{DisplayName: tenancy.Value(ctx, "display_name")},
			Audit: &commonv1.AuditInfo{UpdatedBy: req.GetInspection().GetAudit().GetUpdatedBy()},
		},
	}
	send := s.simulateCustomerSegmentSender(simulateCustomerSegmentRequest)
	simulateCustomerSegmentReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentCalculationService/SimulateCustomerSegment")
	}
	_ = simulateCustomerSegmentReply
	return &returnsv1.DelegateInspectionApprovalResponse{
		Inspection: &returnsv1.Inspection{
			Assignee: &identityv1.UserRef{
				UserId: validatePreferenceReply.GetPreference().GetOwner().GetUserId(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: validatePreferenceReply.GetPreference().GetAudit().GetUpdatedAt(),
			},
			InspectionId: req.GetInspectionId(),
			Requester:    req.GetInspection().GetRequester(),
		},
		Outcome: &returnsv1.DelegateInspectionApprovalResponse_Outcome{
			Status:  deletePolicyReply.GetResult().GetStatus(),
			Message: deletePolicyReply.GetResult().GetMessage(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RecallInspection recalls an inspection.
func (s *inspectionApprovalHandlers) RecallInspection(ctx context.Context, req *returnsv1.RecallInspectionRequest) (*returnsv1.RecallInspectionResponse, error) {
	if err := s.store.SaveInspection(ctx, req.GetContext().GetTenantId(), req.GetInspection()); err != nil {
		return nil, grpcx.Wrap(err, "save inspection")
	}
	s.log.InfoContext(ctx, "recall inspection", "tenant", req.GetContext().GetTenantId(), "etag", req.GetInspection().GetAudit().GetEtag())
	calculateCustomerSegmentRequest := newCalculateCustomerSegmentRequest(withCalculateCustomerSegmentContext(req.GetContext()), withCalculateCustomerSegmentCustomerSegment(&customersv1.CustomerSegment{
		Owner: &identityv1.UserRef{
			DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
		},
		Audit: &commonv1.AuditInfo{Etag: req.GetInspection().GetAudit().GetEtag()},
	}))
	calculateCustomerSegmentReply, err := s.customerSegmentCalculationClient.CalculateCustomerSegment(ctx, calculateCustomerSegmentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentCalculationService/CalculateCustomerSegment")
	}
	watchToursStream, err := s.tourClient.WatchTours(ctx, &toursv1.WatchToursRequest{
		Context: req.GetContext(),
		Subscriber: &commonv1.ContactInfo{
			Email:            req.GetInspection().GetAssignee().GetEmail(),
			PreferredChannel: req.GetInspection().GetRequester().GetPreferredChannel(),
			Phone:            req.GetInspection().GetRequester().GetPhone(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.tours.v1.TourService/WatchTours")
	}
	for {
		received, err := watchToursStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.tours.v1.TourService/WatchTours")
		}
		_ = received
	}
	return &returnsv1.RecallInspectionResponse{
		Inspection: &returnsv1.Inspection{
			Audit: &commonv1.AuditInfo{
				Etag: calculateCustomerSegmentReply.GetCustomerSegment().GetAudit().GetEtag(),
			},
			InspectionId: req.GetInspectionId(),
			CustomFields: req.GetInspection().GetCustomFields(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetInspectionApprovalStatus returns an inspection.
func (s *inspectionApprovalHandlers) GetInspectionApprovalStatus(ctx context.Context, req *returnsv1.GetInspectionApprovalStatusRequest) (*returnsv1.GetInspectionApprovalStatusResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "inspection", req.GetInspectionId(), "get_inspection_approval_status"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get inspection approval status", "tenant", req.GetContext().GetTenantId())
	listPendingReturnLabelsRequest := &returnsv1.ListPendingReturnLabelsRequest{
		Context: req.GetContext(),
		Filter:  &returnsv1.ListPendingReturnLabelsRequest_Filter{OwnerId: req.GetInspectionId()},
	}
	listPendingReturnLabelsReply, err := s.listPendingReturnLabelsFn(ctx, listPendingReturnLabelsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnLabelApprovalService/ListPendingReturnLabels")
	}
	return &returnsv1.GetInspectionApprovalStatusResponse{
		Result: &commonv1.OperationResult{
			Message: listPendingReturnLabelsReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Inspection: &returnsv1.Inspection{InspectionId: req.GetInspectionId()},
	}, nil
}

// CommentInspectionApproval comments an inspection.
func (s *inspectionApprovalHandlers) CommentInspectionApproval(ctx context.Context, req *returnsv1.CommentInspectionApprovalRequest) (*returnsv1.CommentInspectionApprovalResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CommentInspectionApproval", "error", err)
		return nil, err
	}
	if err := s.store.SaveInspection(ctx, req.GetContext().GetTenantId(), req.GetInspection()); err != nil {
		return nil, grpcx.Wrap(err, "save inspection")
	}
	s.log.InfoContext(ctx, "comment inspection approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetInspection().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "email", req.GetInspection().GetAssignee().GetEmail())
	approveSellerRequest := &sellersv1.ApproveSellerRequest{}
	approveSellerRequest.Context = req.GetContext()
	approveSellerRequest.Seller = &sellersv1.Seller{
		Contact: &commonv1.ContactInfo{Email: tenancy.Value(ctx, "email")},
	}
	approveSellerReply, err := s.sellerApprovalClient.ApproveSeller(ctx, approveSellerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerApprovalService/ApproveSeller")
	}
	suggestRecommendationSetsRequest := &recommendationsv1.SuggestRecommendationSetsRequest{
		Context: req.GetContext(),
		Filter: &recommendationsv1.SuggestRecommendationSetsRequest_Filter{
			Region:       approveSellerReply.GetSeller().GetPrimaryAddress().GetRegion(),
			CreatedAfter: approveSellerReply.GetSeller().GetAudit().GetCreatedAt(),
		},
	}
	var suggestRecommendationSetsReply *recommendationsv1.SuggestRecommendationSetsResponse
	if err := pipeline.Run(ctx, "acme.recommendations.v1.RecommendationSetQueryService/SuggestRecommendationSets", func(ctx context.Context) error {
		answer, err := s.recommendationSetQueryClient.SuggestRecommendationSets(ctx, suggestRecommendationSetsRequest)
		suggestRecommendationSetsReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.recommendations.v1.RecommendationSetQueryService/SuggestRecommendationSets")
	}
	var email string
	switch choice := req.GetInspection().GetOrigin().(type) {
	case *returnsv1.Inspection_Customer:
		email = choice.Customer.GetEmail()
	case *returnsv1.Inspection_ExternalReference:
		email = choice.ExternalReference
	case *returnsv1.Inspection_Channel:
		email = choice.Channel.GetChannel()
	}
	source := req.GetInspection().GetAssignee().ProtoReflect()
	displayName := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("display_name"))).String()
	packed, err := anypb.New(req.GetInspection())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	approveReviewVoteRequest := &reviewsv1.ApproveReviewVoteRequest{
		Context: req.GetContext(),
		Details: &reviewsv1.ApproveReviewVoteRequest_Details{
			Approver: &identityv1.UserRef{
				Email:       req.GetInspection().GetRequester().GetEmail(),
				DisplayName: displayName,
			},
		},
		ReviewVote: &reviewsv1.ReviewVote{
			Subject: &customersv1.CustomerRef{Email: email},
			Detail:  packed,
		},
	}
	approveReviewVoteReply := new(reviewsv1.ApproveReviewVoteResponse)
	if err := grpcx.LegacyInvoke(ctx, s.conn, "/acme.reviews.v1.ReviewVoteApprovalService/ApproveReviewVote", approveReviewVoteRequest, approveReviewVoteReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewVoteApprovalService/ApproveReviewVote")
	}
	return &returnsv1.CommentInspectionApprovalResponse{
		Inspection: &returnsv1.Inspection{
			Audit:        &commonv1.AuditInfo{Etag: approveSellerReply.GetSeller().GetAudit().GetEtag()},
			InspectionId: req.GetInspectionId(),
			Assignee:     req.GetInspection().GetAssignee(),
		},
		Outcome: &returnsv1.CommentInspectionApprovalResponse_Outcome{
			Message: suggestRecommendationSetsReply.GetResult().GetMessage(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: approveReviewVoteReply.GetReviewVote().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// simulateCustomerSegmentSender returns a function that sends one request to
// acme.customers.v1.CustomerSegmentCalculationService/SimulateCustomerSegment.
func (s *inspectionApprovalHandlers) simulateCustomerSegmentSender(request *customersv1.SimulateCustomerSegmentRequest) func(context.Context) (*customersv1.SimulateCustomerSegmentResponse, error) {
	return func(ctx context.Context) (*customersv1.SimulateCustomerSegmentResponse, error) {
		return s.customerSegmentCalculationClient.SimulateCustomerSegment(ctx, request)
	}
}
