package payments

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	messagingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/messaging/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// PayoutLifecycleServer implements `acme.payments.v1.PayoutLifecycleService`.
// Moves a payout between the states of its lifecycle.
type PayoutLifecycleServer struct {
	paymentsv1.UnimplementedPayoutLifecycleServiceServer
	store                            *Store
	log                              *slog.Logger
	payoutSyncClient                 paymentsv1.PayoutSyncServiceClient
	payoutSyncClientFallback         paymentsv1.PayoutSyncServiceClient
	fraudSignalWorkflowClient        fraudv1.FraudSignalWorkflowServiceClient
	householdValidationClient        customersv1.HouseholdValidationServiceClient
	conversationQueryClient          messagingv1.ConversationQueryServiceClient
	conn                             *grpc.ClientConn
	paymentInstrumentReportingClient paymentsv1.PaymentInstrumentReportingServiceClient
	refundReportingClient            paymentsv1.RefundReportingServiceClient
	reviewVoteApprovalPort           reviewVoteApprovalPort
	payoutSyncPort                   payoutSyncPort
	sellerWorkflowClient             sellersv1.SellerWorkflowServiceClient
}

// NewPayoutLifecycleServer wires a PayoutLifecycleServer.
func NewPayoutLifecycleServer(deps platform.Deps) *PayoutLifecycleServer {
	return &PayoutLifecycleServer{
		store:                            NewStore(deps.DB),
		log:                              deps.Logger.With("service", "acme.payments.v1.PayoutLifecycleService"),
		payoutSyncClient:                 paymentsv1.NewPayoutSyncServiceClient(deps.Conn),
		payoutSyncClientFallback:         paymentsv1.NewPayoutSyncServiceClient(deps.Conn),
		fraudSignalWorkflowClient:        fraudv1.NewFraudSignalWorkflowServiceClient(deps.Conn),
		householdValidationClient:        customersv1.NewHouseholdValidationServiceClient(deps.Conn),
		conversationQueryClient:          messagingv1.NewConversationQueryServiceClient(deps.Conn),
		conn:                             deps.Conn,
		paymentInstrumentReportingClient: paymentsv1.NewPaymentInstrumentReportingServiceClient(deps.Conn),
		refundReportingClient:            paymentsv1.NewRefundReportingServiceClient(deps.Conn),
		reviewVoteApprovalPort:           newReviewVoteApprovalPort(deps),
		payoutSyncPort:                   newPayoutSyncPort(deps),
		sellerWorkflowClient:             sellersv1.NewSellerWorkflowServiceClient(deps.Conn),
	}
}

// ActivatePayout activates a payout.
func (s *PayoutLifecycleServer) ActivatePayout(ctx context.Context, req *paymentsv1.ActivatePayoutRequest) (*paymentsv1.ActivatePayoutResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ActivatePayout", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "payout", req.GetPayoutId(), "activate_payout"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "activate payout", "tenant", req.GetContext().GetTenantId())
	routes := map[string]func(context.Context, *paymentsv1.PullPayoutsRequest, ...grpc.CallOption) (*paymentsv1.PullPayoutsResponse, error){
		"primary":  s.payoutSyncClient.PullPayouts,
		"fallback": s.payoutSyncClientFallback.PullPayouts,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	pullPayoutsRequest := &paymentsv1.PullPayoutsRequest{
		Context:  req.GetContext(),
		PayoutId: req.GetPayoutId(),
		Payout: &paymentsv1.Payout{
			Counterparty: &paymentsv1.RefundRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
		},
		Reason: req.GetReason(),
	}
	pullPayoutsReply, err := route(ctx, pullPayoutsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PayoutSyncService/PullPayouts")
	}
	cloned := proto.Clone(req.GetDetails()).(*paymentsv1.ActivatePayoutRequest_Details)
	signalFraudSignalWorkflowRequest := &fraudv1.SignalFraudSignalWorkflowRequest{
		Context: req.GetContext(),
		FraudSignal: &fraudv1.FraudSignal{
			Subject: &fraudv1.FraudRuleRef{
				DisplayName: pullPayoutsReply.GetPayout().GetCounterparty().GetDisplayName(),
			},
		},
		Details: &fraudv1.SignalFraudSignalWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				Email: req.GetDetails().GetNewOwner().GetEmail(),
				Name:  cloned.GetNotifyContact().GetName(),
			},
		},
	}
	signalFraudSignalWorkflowReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*fraudv1.SignalFraudSignalWorkflowResponse, error) {
		return s.fraudSignalWorkflowClient.SignalFraudSignalWorkflow(ctx, signalFraudSignalWorkflowRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.FraudSignalWorkflowService/SignalFraudSignalWorkflow")
	}
	checkHouseholdEligibilityRequest := &customersv1.CheckHouseholdEligibilityRequest{}
	checkHouseholdEligibilityRequest.Context = req.GetContext()
	checkHouseholdEligibilityRequest.Household = &customersv1.Household{
		Children: []*customersv1.Household{&customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Location: &commonv1.PostalAddress{
					Lines: pullPayoutsReply.GetPayout().GetBillingAddress().GetLines(),
				},
			}},
		}},
	}
	checkHouseholdEligibilityReply, err := s.householdValidationClient.CheckHouseholdEligibility(ctx, checkHouseholdEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdValidationService/CheckHouseholdEligibility")
	}
	resolveConversationsRequest := &messagingv1.ResolveConversationsRequest{}
	resolveConversationsRequest.Context = req.GetContext()
	resolveConversationsReply, err := s.conversationQueryClient.ResolveConversations(ctx, resolveConversationsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.messaging.v1.ConversationQueryService/ResolveConversations")
	}
	return &paymentsv1.ActivatePayoutResponse{
		Result: &commonv1.OperationResult{
			Status:  pullPayoutsReply.GetOutcome().GetStatus(),
			Message: resolveConversationsReply.GetResult().GetMessage(),
		},
		Payout: &paymentsv1.Payout{
			Counterparty: &paymentsv1.RefundRef{
				Number: pullPayoutsReply.GetPayout().GetCounterparty().GetNumber(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedBy: checkHouseholdEligibilityReply.GetHousehold().GetAudit().GetCreatedBy(),
			},
			PayoutId: req.GetPayoutId(),
		},
		Outcome: &paymentsv1.ActivatePayoutResponse_Outcome{
			NextSteps:   signalFraudSignalWorkflowReply.GetOutcome().GetNextSteps(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
	}, nil
}

// SuspendPayout suspends a payout.
func (s *PayoutLifecycleServer) SuspendPayout(ctx context.Context, req *paymentsv1.SuspendPayoutRequest) (*paymentsv1.SuspendPayoutResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "payout", req.GetPayoutId(), "suspend_payout"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "suspend payout", "tenant", req.GetContext().GetTenantId())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	estimateConsentRecordRequest := &customersv1.EstimateConsentRecordRequest{
		Context: req.GetContext(),
		ConsentRecord: &customersv1.ConsentRecord{
			Subject: &customersv1.HouseholdRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
			Detail: packed,
		},
		Annotations: annotations,
		Reason:      req.GetReason(),
	}
	estimateConsentRecordReply := new(customersv1.EstimateConsentRecordResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", customersConsentRecordCalculationServiceName, "EstimateConsentRecord"), estimateConsentRecordRequest, estimateConsentRecordReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordCalculationService/EstimateConsentRecord")
	}
	return &paymentsv1.SuspendPayoutResponse{
		Payout: &paymentsv1.Payout{
			Counterparty: &paymentsv1.RefundRef{
				DisplayName: estimateConsentRecordReply.GetConsentRecord().GetSubject().GetDisplayName(),
			},
			PayoutId: req.GetPayoutId(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: estimateConsentRecordReply.GetConsentRecord().GetCorrelationId(),
			Status:        "ok",
		},
		Outcome: &paymentsv1.SuspendPayoutResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// ReinstatePayout reinstates a payout.
func (s *PayoutLifecycleServer) ReinstatePayout(ctx context.Context, req *paymentsv1.ReinstatePayoutRequest) (*paymentsv1.ReinstatePayoutResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "payout", req.GetPayoutId(), "reinstate_payout"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "reinstate payout", "tenant", req.GetContext().GetTenantId())
	delegateTierApprovalRequest := &loyaltyv1.DelegateTierApprovalRequest{
		Context: req.GetContext(),
		Tier: &loyaltyv1.Tier{
			Owner:  &identityv1.UserRef{Email: req.GetDetails().GetNotifyContact().GetEmail()},
			Action: &loyaltyv1.Tier_Block{Block: &loyaltyv1.TierBlockAction{Reason: req.GetReason()}},
		},
		Details: &loyaltyv1.DelegateTierApprovalRequest_Details{
			Approver: &identityv1.UserRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
		},
	}
	delegateTierApprovalReply := new(loyaltyv1.DelegateTierApprovalResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", loyaltyTierApprovalServiceName, "DelegateTierApproval"), delegateTierApprovalRequest, delegateTierApprovalReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.TierApprovalService/DelegateTierApproval")
	}
	return &paymentsv1.ReinstatePayoutResponse{
		Result: &commonv1.OperationResult{
			Details: delegateTierApprovalReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Payout:  &paymentsv1.Payout{PayoutId: req.GetPayoutId()},
		Outcome: &paymentsv1.ReinstatePayoutResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// RenewPayout renews a payout.
func (s *PayoutLifecycleServer) RenewPayout(ctx context.Context, req *paymentsv1.RenewPayoutRequest) (*paymentsv1.RenewPayoutResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "payout", req.GetPayoutId(), "renew_payout"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "renew payout", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetDetails()).(*paymentsv1.RenewPayoutRequest_Details)
	snapshot, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	pullPayoutsRequest := &paymentsv1.PullPayoutsRequest{
		Context: req.GetContext(),
		Payout: &paymentsv1.Payout{
			Counterparty: &paymentsv1.RefundRef{DisplayName: cloned.GetNewOwner().GetDisplayName()},
			PayoutId:     req.GetPayoutId(),
		},
		Annotations: req.GetAnnotations(),
		Reason:      req.GetReason(),
		Details:     &paymentsv1.PullPayoutsRequest_Details{Payload: snapshot},
	}
	pullPayoutsReply := new(paymentsv1.PullPayoutsResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", paymentsPayoutSyncServiceName, "PullPayouts"), pullPayoutsRequest, pullPayoutsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PayoutSyncService/PullPayouts")
	}
	step := &payoutLifecycleServerRenewPayoutCountPaymentInstrumentTotalsStep{request: &paymentsv1.CountPaymentInstrumentTotalsRequest{
		Context: req.GetContext(),
		Filter: &paymentsv1.CountPaymentInstrumentTotalsRequest_Filter{
			Region:       pullPayoutsReply.GetPayout().GetBillingAddress().GetRegion(),
			CreatedAfter: pullPayoutsReply.GetPayout().GetAudit().GetCreatedAt(),
			OwnerId:      pullPayoutsReply.GetOutcome().GetReferenceId(),
			Labels:       pullPayoutsReply.GetResult().GetDetails(),
		},
	}}
	if err := s.runRenewPayoutCountPaymentInstrumentTotals(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentInstrumentReportingService/CountPaymentInstrumentTotals")
	}
	countPaymentInstrumentTotalsReply := step.reply
	return &paymentsv1.RenewPayoutResponse{
		Payout: &paymentsv1.Payout{
			Counterparty: pullPayoutsReply.GetPayout().GetCounterparty(),
			PayoutId:     req.GetPayoutId(),
		},
		Outcome: &paymentsv1.RenewPayoutResponse_Outcome{
			Message:     countPaymentInstrumentTotalsReply.GetResult().GetMessage(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: countPaymentInstrumentTotalsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// ExpirePayout expires a payout.
func (s *PayoutLifecycleServer) ExpirePayout(ctx context.Context, req *paymentsv1.ExpirePayoutRequest) (*paymentsv1.ExpirePayoutResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ExpirePayout", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "payout", req.GetPayoutId(), "expire_payout"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "expire payout", "tenant", req.GetContext().GetTenantId())
	streamRefundMetricsStream, err := s.refundReportingClient.StreamRefundMetrics(ctx, &paymentsv1.StreamRefundMetricsRequest{
		Context: req.GetContext(),
		Subscriber: &commonv1.ContactInfo{
			PreferredChannel: req.GetDetails().GetNotifyContact().GetPreferredChannel(),
			Email:            req.GetDetails().GetNotifyContact().GetEmail(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.RefundReportingService/StreamRefundMetrics")
	}
	for {
		received, err := streamRefundMetricsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.payments.v1.RefundReportingService/StreamRefundMetrics")
		}
		_ = received
	}
	return &paymentsv1.ExpirePayoutResponse{
		Payout:  &paymentsv1.Payout{PayoutId: req.GetPayoutId()},
		Outcome: &paymentsv1.ExpirePayoutResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetPayoutLifecycle returns a payout.
func (s *PayoutLifecycleServer) GetPayoutLifecycle(ctx context.Context, req *paymentsv1.GetPayoutLifecycleRequest) (*paymentsv1.GetPayoutLifecycleResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "payout", req.GetPayoutId(), "get_payout_lifecycle"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get payout lifecycle", "tenant", req.GetContext().GetTenantId())
	var holder struct{ Email string }
	if record := req.GetContext().GetActor(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Email"); err != nil {
			s.log.WarnContext(ctx, "copy email", "error", err)
		}
	}
	encoded, err := protojson.Marshal(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded commonv1.RequestContext
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	packed, err := anypb.New(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	escalateReviewVoteRequest := &reviewsv1.EscalateReviewVoteRequest{
		Context: req.GetContext(),
		ReviewVote: &reviewsv1.ReviewVote{
			Subject:      &customersv1.CustomerRef{Email: holder.Email},
			Measurements: []*reviewsv1.ReviewVoteMeasurement{&reviewsv1.ReviewVoteMeasurement{ObservedAt: (&decoded).GetDeadline()}},
			Detail:       packed,
		},
		Details: &reviewsv1.EscalateReviewVoteRequest_Details{
			Approver: &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
		},
	}
	escalateReviewVoteReply, err := s.reviewVoteApprovalPort.EscalateReviewVote(ctx, escalateReviewVoteRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewVoteApprovalService/EscalateReviewVote")
	}
	return &paymentsv1.GetPayoutLifecycleResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: escalateReviewVoteReply.GetResult().GetCorrelationId(),
			ReferenceIds:  escalateReviewVoteReply.GetResult().GetReferenceIds(),
			Status:        "ok",
		},
		Payout: &paymentsv1.Payout{PayoutId: req.GetPayoutId()},
	}, nil
}

// SchedulePayoutExpiry schedules a payout.
func (s *PayoutLifecycleServer) SchedulePayoutExpiry(ctx context.Context, req *paymentsv1.SchedulePayoutExpiryRequest) (*paymentsv1.SchedulePayoutExpiryResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SchedulePayoutExpiry", "error", err)
		return nil, err
	}
	if err := s.store.SavePayout(ctx, req.GetContext().GetTenantId(), req.GetPayout()); err != nil {
		return nil, grpcx.Wrap(err, "save payout")
	}
	s.log.InfoContext(ctx, "schedule payout expiry", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPayout().GetAudit().GetEtag())
	encoded, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded paymentsv1.SchedulePayoutExpiryRequest_Details
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	packed, err := anypb.New(req.GetPayout())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	previewConsentRecordRequest := &customersv1.PreviewConsentRecordRequest{
		Context: req.GetContext(),
		Details: &customersv1.PreviewConsentRecordRequest_Details{
			Contact: &commonv1.ContactInfo{Phone: (&decoded).GetNotify().GetPhone()},
		},
		ConsentRecord: &customersv1.ConsentRecord{Detail: packed},
	}
	previewConsentRecordReply, err := previewConsentRecordCall(ctx, previewConsentRecordRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordValidationService/PreviewConsentRecord")
	}
	syncPayoutsRequest := &paymentsv1.SyncPayoutsRequest{
		Context: req.GetContext(),
		Payout: &paymentsv1.Payout{
			Audit: &commonv1.AuditInfo{UpdatedBy: req.GetPayout().GetAudit().GetUpdatedBy()},
			BillingAddress: &commonv1.PostalAddress{
				PostalCode: req.GetPayout().GetBillingAddress().GetPostalCode(),
				Lines:      req.GetPayout().GetBillingAddress().GetLines(),
			},
		},
	}
	syncPayoutsReply, err := s.payoutSyncPort.SyncPayouts(ctx, syncPayoutsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PayoutSyncService/SyncPayouts")
	}
	encoded2, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded2 paymentsv1.SchedulePayoutExpiryRequest_Details
	if err := protojson.Unmarshal(encoded2, &decoded2); err != nil {
		return nil, err
	}
	startSellerWorkflowReply, err := s.sellerWorkflowClient.StartSellerWorkflow(ctx, &sellersv1.StartSellerWorkflowRequest{
		Context: req.GetContext(),
		Seller: &sellersv1.Seller{
			Contact: &commonv1.ContactInfo{Email: (&decoded2).GetNotify().GetEmail()},
			Audit:   &commonv1.AuditInfo{CreatedAt: req.GetPayout().GetAudit().GetCreatedAt()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerWorkflowService/StartSellerWorkflow")
	}
	return &paymentsv1.SchedulePayoutExpiryResponse{
		Payout: &paymentsv1.Payout{
			Counterparty: &paymentsv1.RefundRef{
				DisplayName: previewConsentRecordReply.GetConsentRecord().GetSubject().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: syncPayoutsReply.GetPayout().GetAudit().GetUpdatedBy(),
				Etag:      startSellerWorkflowReply.GetSeller().GetAudit().GetEtag(),
			},
			PayoutId: req.GetPayoutId(),
			Status:   req.GetPayout().GetStatus(),
		},
		Outcome: &paymentsv1.SchedulePayoutExpiryResponse_Outcome{Message: startSellerWorkflowReply.GetResult().GetMessage()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// payoutLifecycleServerRenewPayoutCountPaymentInstrumentTotalsStep carries the request of one step of RenewPayout and,
// once it ran, its reply.
type payoutLifecycleServerRenewPayoutCountPaymentInstrumentTotalsStep struct {
	request *paymentsv1.CountPaymentInstrumentTotalsRequest
	reply   *paymentsv1.CountPaymentInstrumentTotalsResponse
}

func (s *PayoutLifecycleServer) runRenewPayoutCountPaymentInstrumentTotals(ctx context.Context, step *payoutLifecycleServerRenewPayoutCountPaymentInstrumentTotalsStep) error {
	reply, err := s.paymentInstrumentReportingClient.CountPaymentInstrumentTotals(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
