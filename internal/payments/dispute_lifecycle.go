package payments

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	appointmentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/appointments/v1"
	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	careplansv1 "github.com/hounddogai/protomap-test-golang/gen/acme/careplans/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	communityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/community/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// DisputeLifecycleServer implements `acme.payments.v1.DisputeLifecycleService`.
// Moves a dispute between the states of its lifecycle.
type DisputeLifecycleServer struct {
	paymentsv1.UnimplementedDisputeLifecycleServiceServer
	store                             *Store
	log                               *slog.Logger
	conn                              *grpc.ClientConn
	meteredAccessPolicyValidation     meteredAccessPolicyValidation
	carePlanCalculationClient         careplansv1.CarePlanCalculationServiceClient
	accessPolicyValidationClient      authorizationv1.AccessPolicyValidationServiceClient
	refundValidationClient            paymentsv1.RefundValidationServiceClient
	paymentApprovalClient             paymentsv1.PaymentApprovalServiceClient
	disputeSyncClient                 paymentsv1.DisputeSyncServiceClient
	preferenceValidationClient        customersv1.PreferenceValidationServiceClient
	forumClient                       communityv1.ForumServiceClient
	giftCardTransactionWorkflowClient giftcardsv1.GiftCardTransactionWorkflowServiceClient
	clients                           *registry.Registry
}

// NewDisputeLifecycleServer wires a DisputeLifecycleServer.
func NewDisputeLifecycleServer(deps platform.Deps) *DisputeLifecycleServer {
	return &DisputeLifecycleServer{
		store:                             NewStore(deps.DB),
		log:                               deps.Logger.With("service", "acme.payments.v1.DisputeLifecycleService"),
		conn:                              deps.Conn,
		meteredAccessPolicyValidation:     meteredAccessPolicyValidation{AccessPolicyValidationServiceClient: authorizationv1.NewAccessPolicyValidationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		carePlanCalculationClient:         careplansv1.NewCarePlanCalculationServiceClient(deps.Conn),
		accessPolicyValidationClient:      authorizationv1.NewAccessPolicyValidationServiceClient(deps.Conn),
		refundValidationClient:            paymentsv1.NewRefundValidationServiceClient(deps.Conn),
		paymentApprovalClient:             paymentsv1.NewPaymentApprovalServiceClient(deps.Conn),
		disputeSyncClient:                 paymentsv1.NewDisputeSyncServiceClient(deps.Conn),
		preferenceValidationClient:        customersv1.NewPreferenceValidationServiceClient(deps.Conn),
		forumClient:                       communityv1.NewForumServiceClient(deps.Conn),
		giftCardTransactionWorkflowClient: giftcardsv1.NewGiftCardTransactionWorkflowServiceClient(deps.Conn),
		clients:                           deps.Clients,
	}
}

// ActivateDispute activates a dispute.
func (s *DisputeLifecycleServer) ActivateDispute(ctx context.Context, req *paymentsv1.ActivateDisputeRequest) (*paymentsv1.ActivateDisputeResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "dispute", req.GetDisputeId(), "activate_dispute"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "activate dispute", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "email", req.GetDetails().GetNewOwner().GetEmail())
	rejectShipmentRequest := &shippingv1.RejectShipmentRequest{
		Context: req.GetContext(),
		Details: &shippingv1.RejectShipmentRequest_Details{
			Approver: &identityv1.UserRef{
				Email:       tenancy.Value(ctx, "email"),
				UserId:      req.GetDetails().GetNewOwner().GetUserId(),
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
		},
	}
	rejectShipmentReply := new(shippingv1.RejectShipmentResponse)
	if err := grpcx.LegacyInvoke(ctx, s.conn, "/acme.shipping.v1.ShipmentApprovalService/RejectShipment", rejectShipmentRequest, rejectShipmentReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ShipmentApprovalService/RejectShipment")
	}
	return &paymentsv1.ActivateDisputeResponse{
		Outcome: &paymentsv1.ActivateDisputeResponse_Outcome{
			Message:     rejectShipmentReply.GetResult().GetMessage(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Dispute: &paymentsv1.Dispute{
			Audit: &commonv1.AuditInfo{
				CreatedBy: rejectShipmentReply.GetShipment().GetAudit().GetCreatedBy(),
			},
			DisputeId: req.GetDisputeId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// SuspendDispute suspends a dispute.
func (s *DisputeLifecycleServer) SuspendDispute(ctx context.Context, req *paymentsv1.SuspendDisputeRequest) (*paymentsv1.SuspendDisputeResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "dispute", req.GetDisputeId(), "suspend_dispute"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "suspend dispute", "tenant", req.GetContext().GetTenantId())
	detailsCache := cache.New[string, *paymentsv1.SuspendDisputeRequest_Details](1)
	detailsCache.Put(req.GetContext().GetTraceId(), req.GetDetails())
	cachedDetails, _ := detailsCache.Get(req.GetContext().GetTraceId())
	previewAccessPolicyRequest := &authorizationv1.PreviewAccessPolicyRequest{
		Context: req.GetContext(),
		Details: &authorizationv1.PreviewAccessPolicyRequest_Details{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: cachedDetails.GetNotifyContact().GetPreferredChannel(),
			},
		},
	}
	previewAccessPolicyReply, err := s.meteredAccessPolicyValidation.PreviewAccessPolicy(ctx, previewAccessPolicyRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyValidationService/PreviewAccessPolicy")
	}
	calculateCarePlanRequest := &careplansv1.CalculateCarePlanRequest{
		Context: req.GetContext(),
		CarePlan: &careplansv1.CarePlan{
			Counterparty: &appointmentsv1.AppointmentRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
		},
	}
	calculateCarePlanReply, err := grpcx.Call(ctx, s.carePlanCalculationClient.CalculateCarePlan, calculateCarePlanRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.careplans.v1.CarePlanCalculationService/CalculateCarePlan")
	}
	return &paymentsv1.SuspendDisputeResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: previewAccessPolicyReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Dispute: &paymentsv1.Dispute{
			Audit: &commonv1.AuditInfo{
				CreatedAt: calculateCarePlanReply.GetCarePlan().GetAudit().GetCreatedAt(),
			},
			DisputeId: req.GetDisputeId(),
		},
		Outcome: &paymentsv1.SuspendDisputeResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// ReinstateDispute reinstates a dispute.
func (s *DisputeLifecycleServer) ReinstateDispute(ctx context.Context, req *paymentsv1.ReinstateDisputeRequest) (*paymentsv1.ReinstateDisputeResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "dispute", req.GetDisputeId(), "reinstate_dispute"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "reinstate dispute", "tenant", req.GetContext().GetTenantId())
	detailsCache := cache.New[string, *paymentsv1.ReinstateDisputeRequest_Details](1)
	detailsCache.Put(req.GetContext().GetTraceId(), req.GetDetails())
	cachedDetails, _ := detailsCache.Get(req.GetContext().GetTraceId())
	explainAccessPolicyDecisionRequest := &authorizationv1.ExplainAccessPolicyDecisionRequest{
		Context: req.GetContext(),
		AccessPolicy: &authorizationv1.AccessPolicy{
			Owner: &identityv1.UserRef{
				UserId: req.GetDetails().GetNewOwner().GetUserId(),
				Email:  req.GetDetails().GetNewOwner().GetEmail(),
			},
			Action: &authorizationv1.AccessPolicy_Block{Block: &authorizationv1.AccessPolicyBlockAction{NotifyEmail: cachedDetails.GetNotifyContact().GetEmail()}},
		},
		Details: &authorizationv1.ExplainAccessPolicyDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{Name: req.GetDetails().GetNotifyContact().GetName()},
		},
	}
	var explainAccessPolicyDecisionReply *authorizationv1.ExplainAccessPolicyDecisionResponse
	if err := pipeline.Run(ctx, "acme.authorization.v1.AccessPolicyValidationService/ExplainAccessPolicyDecision", func(ctx context.Context) error {
		answer, err := s.accessPolicyValidationClient.ExplainAccessPolicyDecision(ctx, explainAccessPolicyDecisionRequest)
		explainAccessPolicyDecisionReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyValidationService/ExplainAccessPolicyDecision")
	}
	previewRefundReply, err := s.refundValidationClient.PreviewRefund(ctx, &paymentsv1.PreviewRefundRequest{
		Context: req.GetContext(),
		Refund: &paymentsv1.Refund{
			Lines: []*paymentsv1.RefundLine{&paymentsv1.RefundLine{
				Tax: &paymentsv1.RefundLineTax{
					Amount: explainAccessPolicyDecisionReply.GetAccessPolicy().GetDiscount().GetAmount(),
				},
			}},
			Counterparty: &customersv1.CustomerRef{
				DisplayName: explainAccessPolicyDecisionReply.GetAccessPolicy().GetOwner().GetDisplayName(),
				Email:       req.GetDetails().GetNewOwner().GetEmail(),
			},
		},
		Details: &paymentsv1.PreviewRefundRequest_Details{
			Contact: &commonv1.ContactInfo{Name: req.GetDetails().GetNotifyContact().GetName()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.RefundValidationService/PreviewRefund")
	}
	return &paymentsv1.ReinstateDisputeResponse{
		Dispute: &paymentsv1.Dispute{
			Requester: &commonv1.ContactInfo{
				Email: explainAccessPolicyDecisionReply.GetAccessPolicy().GetOwner().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: explainAccessPolicyDecisionReply.GetAccessPolicy().GetAudit().GetUpdatedAt(),
			},
			DisputeId: req.GetDisputeId(),
		},
		Outcome: &paymentsv1.ReinstateDisputeResponse_Outcome{
			Status:      previewRefundReply.GetResult().GetStatus(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// TerminateDispute terminates a dispute.
func (s *DisputeLifecycleServer) TerminateDispute(ctx context.Context, req *paymentsv1.TerminateDisputeRequest) (*paymentsv1.TerminateDisputeResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected TerminateDispute", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "dispute", req.GetDisputeId(), "terminate_dispute"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "terminate dispute", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetDetails()).(*paymentsv1.TerminateDisputeRequest_Details)
	approvePaymentReply, err := s.paymentApprovalClient.ApprovePayment(ctx, &paymentsv1.ApprovePaymentRequest{
		Context: req.GetContext(),
		Details: &paymentsv1.ApprovePaymentRequest_Details{
			Approver: &identityv1.UserRef{
				Email:  req.GetDetails().GetNotifyContact().GetEmail(),
				UserId: cloned.GetNewOwner().GetUserId(),
			},
		},
		Payment: &paymentsv1.Payment{
			Counterparty: &paymentsv1.RefundRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentApprovalService/ApprovePayment")
	}
	return &paymentsv1.TerminateDisputeResponse{
		Dispute: &paymentsv1.Dispute{
			Sla:       &paymentsv1.DisputeSla{DueAt: approvePaymentReply.GetPayment().GetDueAt()},
			DisputeId: req.GetDisputeId(),
		},
		Outcome: &paymentsv1.TerminateDisputeResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RenewDispute renews a dispute.
func (s *DisputeLifecycleServer) RenewDispute(ctx context.Context, req *paymentsv1.RenewDisputeRequest) (*paymentsv1.RenewDisputeResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "dispute", req.GetDisputeId(), "renew_dispute"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "renew dispute", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetDetails()).(*paymentsv1.RenewDisputeRequest_Details)
	snapshot, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	reconcileDisputesRequest := &paymentsv1.ReconcileDisputesRequest{}
	reconcileDisputesRequest.Context = req.GetContext()
	reconcileDisputesRequest.Dispute = &paymentsv1.Dispute{
		Requester: &commonv1.ContactInfo{
			Email: cloned.GetNotifyContact().GetEmail(),
			Phone: req.GetDetails().GetNotifyContact().GetPhone(),
		},
		Origin: &paymentsv1.Dispute_Customer{Customer: &paymentsv1.PaymentRef{
			DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
		}},
	}
	reconcileDisputesRequest.Details = &paymentsv1.ReconcileDisputesRequest_Details{Payload: snapshot}
	reconcileDisputesReply, err := s.disputeSyncClient.ReconcileDisputes(ctx, reconcileDisputesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.DisputeSyncService/ReconcileDisputes")
	}
	checkAccessPolicyEligibilityRequest := &authorizationv1.CheckAccessPolicyEligibilityRequest{
		Context: req.GetContext(),
		AccessPolicy: &authorizationv1.AccessPolicy{
			Owner: &identityv1.UserRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
				Email:       reconcileDisputesReply.GetDispute().GetRequester().GetEmail(),
			},
		},
	}
	checkAccessPolicyEligibilityReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*authorizationv1.CheckAccessPolicyEligibilityResponse, error) {
		return s.accessPolicyValidationClient.CheckAccessPolicyEligibility(ctx, checkAccessPolicyEligibilityRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyValidationService/CheckAccessPolicyEligibility")
	}
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	validateConsentRecordRequest := &customersv1.ValidateConsentRecordRequest{
		Context: req.GetContext(),
		ConsentRecord: &customersv1.ConsentRecord{
			Subject: &customersv1.HouseholdRef{
				DisplayName: reconcileDisputesReply.GetDispute().GetCustomer().GetDisplayName(),
			},
			Detail: packed,
		},
		Details: &customersv1.ValidateConsentRecordRequest_Details{
			Contact: &commonv1.ContactInfo{
				Phone:            reconcileDisputesReply.GetDispute().GetRequester().GetPhone(),
				Email:            req.GetDetails().GetNewOwner().GetEmail(),
				PreferredChannel: reconcileDisputesReply.GetDispute().GetRequester().GetPreferredChannel(),
			},
		},
	}
	validateConsentRecordReply := new(customersv1.ValidateConsentRecordResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", customersConsentRecordValidationServiceName, "ValidateConsentRecord"), validateConsentRecordRequest, validateConsentRecordReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordValidationService/ValidateConsentRecord")
	}
	return &paymentsv1.RenewDisputeResponse{
		Dispute: &paymentsv1.Dispute{
			Sla: &paymentsv1.DisputeSla{
				PausedFor: reconcileDisputesReply.GetDispute().GetSla().GetPausedFor(),
			},
			DisputeId: req.GetDisputeId(),
		},
		Outcome: &paymentsv1.RenewDisputeResponse_Outcome{
			Status:      reconcileDisputesReply.GetResult().GetStatus(),
			Message:     checkAccessPolicyEligibilityReply.GetResult().GetMessage(),
			ReferenceId: validateConsentRecordReply.GetOutcome().GetReferenceId(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ExpireDispute expires a dispute.
func (s *DisputeLifecycleServer) ExpireDispute(ctx context.Context, req *paymentsv1.ExpireDisputeRequest) (*paymentsv1.ExpireDisputeResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "dispute", req.GetDisputeId(), "expire_dispute"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "expire dispute", "tenant", req.GetContext().GetTenantId())
	validatePreferenceRequest := buildValidatePreferenceRequestForExpireDispute(req)
	validatePreferenceReply, err := s.preferenceValidationClient.ValidatePreference(ctx, validatePreferenceRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceValidationService/ValidatePreference")
	}
	rejectReviewModerationRequest := &reviewsv1.RejectReviewModerationRequest{
		Context: req.GetContext(),
		ReviewModeration: &reviewsv1.ReviewModeration{
			Origin: &reviewsv1.ReviewModeration_Customer{Customer: &customersv1.CustomerRef{Email: req.GetDetails().GetNotifyContact().GetEmail()}},
			Assignee: &identityv1.UserRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
		},
		Details: &reviewsv1.RejectReviewModerationRequest_Details{
			Approver: &identityv1.UserRef{
				Email: validatePreferenceReply.GetPreference().GetOwner().GetEmail(),
			},
		},
	}
	rejectReviewModerationReply := new(reviewsv1.RejectReviewModerationResponse)
	if err := s.conn.Invoke(ctx, reviewsReviewModerationApprovalServiceRejectReviewModerationMethod, rejectReviewModerationRequest, rejectReviewModerationReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewModerationApprovalService/RejectReviewModeration")
	}
	return &paymentsv1.ExpireDisputeResponse{
		Dispute: &paymentsv1.Dispute{
			Assignee: &identityv1.UserRef{
				DisplayName: validatePreferenceReply.GetPreference().GetOwner().GetDisplayName(),
			},
			DisputeId: req.GetDisputeId(),
		},
		Result: &commonv1.OperationResult{
			Message: rejectReviewModerationReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Outcome: &paymentsv1.ExpireDisputeResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// GetDisputeLifecycle returns a dispute.
func (s *DisputeLifecycleServer) GetDisputeLifecycle(ctx context.Context, req *paymentsv1.GetDisputeLifecycleRequest) (*paymentsv1.GetDisputeLifecycleResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "dispute", req.GetDisputeId(), "get_dispute_lifecycle"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get dispute lifecycle", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "email", req.GetContext().GetActor().GetEmail())
	cloneCustomerRequest := &customersv1.CloneCustomerRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			Contact: &commonv1.ContactInfo{Email: tenancy.Value(ctx, "email")},
		},
	}
	cloneCustomerReply, err := cloneCustomerCall(ctx, cloneCustomerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerService/CloneCustomer")
	}
	return &paymentsv1.GetDisputeLifecycleResponse{
		Dispute: &paymentsv1.Dispute{
			Requester: &commonv1.ContactInfo{
				Phone:            cloneCustomerReply.GetCustomer().GetContact().GetPhone(),
				PreferredChannel: cloneCustomerReply.GetCustomer().GetContact().GetPreferredChannel(),
			},
			DisputeId: req.GetDisputeId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ScheduleDisputeExpiry schedules a dispute.
func (s *DisputeLifecycleServer) ScheduleDisputeExpiry(ctx context.Context, req *paymentsv1.ScheduleDisputeExpiryRequest) (*paymentsv1.ScheduleDisputeExpiryResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ScheduleDisputeExpiry", "error", err)
		return nil, err
	}
	if err := s.store.SaveDispute(ctx, req.GetContext().GetTenantId(), req.GetDispute()); err != nil {
		return nil, grpcx.Wrap(err, "save dispute")
	}
	s.log.InfoContext(ctx, "schedule dispute expiry", "tenant", req.GetContext().GetTenantId(), "etag", req.GetDispute().GetAudit().GetEtag())
	updateForumRequest := &communityv1.UpdateForumRequest{}
	updateForumRequest.Context = req.GetContext()
	updateForumRequest.Forum = &communityv1.Forum{
		Children: []*communityv1.Forum{&communityv1.Forum{
			Children: []*communityv1.Forum{&communityv1.Forum{
				Audit: &commonv1.AuditInfo{
					CreatedBy: req.GetDispute().GetAudit().GetCreatedBy(),
					Etag:      req.GetDispute().GetAudit().GetEtag(),
				},
			}},
		}},
	}
	updateForumReply, err := s.forumClient.UpdateForum(ctx, updateForumRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.community.v1.ForumService/UpdateForum")
	}
	jobs := make(chan *paymentsv1.DisputeComment)
	failures := make(chan error, 4)
	workCtx, stopWork := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for comment := range jobs {
				if _, err := s.giftCardTransactionWorkflowClient.PauseGiftCardTransactionWorkflow(workCtx, &giftcardsv1.PauseGiftCardTransactionWorkflowRequest{
					Context: req.GetContext(),
					Details: &giftcardsv1.PauseGiftCardTransactionWorkflowRequest_Details{
						Notify: &commonv1.ContactInfo{
							Email: comment.GetAuthorEmail(),
							Name:  req.GetDetails().GetNotify().GetName(),
						},
					},
					GiftCardTransaction: &giftcardsv1.GiftCardTransaction{
						Subject: &giftcardsv1.GiftCardBatchRef{
							DisplayName: req.GetDispute().GetAssignee().GetDisplayName(),
						},
					},
				}); err != nil {
					failures <- err
					stopWork()
					return
				}
			}
		}()
	}
	for _, queued := range req.GetDispute().GetComments() {
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
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionWorkflowService/PauseGiftCardTransactionWorkflow")
	}
	disputeSync, ok := s.clients.Get("acme.payments.v1.DisputeSyncService").(paymentsv1.DisputeSyncServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.payments.v1.DisputeSyncService")
	}
	getDisputeSyncStateRequest := &paymentsv1.GetDisputeSyncStateRequest{
		Context:   req.GetContext(),
		DisputeId: req.GetDispute().GetDisputeId(),
	}
	getDisputeSyncStateReply, err := disputeSync.GetDisputeSyncState(ctx, getDisputeSyncStateRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.DisputeSyncService/GetDisputeSyncState")
	}
	return &paymentsv1.ScheduleDisputeExpiryResponse{
		Dispute: &paymentsv1.Dispute{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: updateForumReply.GetForum().GetAudit().GetUpdatedAt(),
			},
			Requester: &commonv1.ContactInfo{
				Phone: getDisputeSyncStateReply.GetDispute().GetRequester().GetPhone(),
			},
			DisputeId: req.GetDisputeId(),
			Assignee:  req.GetDispute().GetAssignee(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}
