package partnerapi

import (
	"context"
	"errors"
	"io"
	"log/slog"

	checkoutv1 "github.com/hounddogai/protomap-test-golang/gen/acme/checkout/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	contractsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/contracts/v1"
	driverappv1 "github.com/hounddogai/protomap-test-golang/gen/acme/driverapp/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	partnerapiv1 "github.com/hounddogai/protomap-test-golang/gen/acme/partnerapi/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/anypb"
)

// PartnerIntegrationNotificationServer implements `acme.partnerapi.v1.PartnerIntegrationNotificationService`.
// Tells the people who follow a partner integration what changed.
type PartnerIntegrationNotificationServer struct {
	partnerapiv1.UnsafePartnerIntegrationNotificationServiceServer
	store                               *Store
	log                                 *slog.Logger
	partnerWebhookWorkflowClient        partnerapiv1.PartnerWebhookWorkflowServiceClient
	meteredPartnerOrderWorkflow         meteredPartnerOrderWorkflow
	commentPartnerWebhookApprovalFn     func(context.Context, *partnerapiv1.CommentPartnerWebhookApprovalRequest, ...grpc.CallOption) (*partnerapiv1.CommentPartnerWebhookApprovalResponse, error)
	subscriptionPlanApprovalClient      subscriptionsv1.SubscriptionPlanApprovalServiceClient
	conn                                *grpc.ClientConn
	shippingQuoteWorkflowClient         checkoutv1.ShippingQuoteWorkflowServiceClient
	clients                             *registry.Registry
	partnerOrderApprovalClient          partnerapiv1.PartnerOrderApprovalServiceClient
	partnerWebhookValidationClient      partnerapiv1.PartnerWebhookValidationServiceClient
	sellerWorkflowClient                sellersv1.SellerWorkflowServiceClient
	partnerIntegrationIntegrationClient partnerapiv1.PartnerIntegrationIntegrationServiceClient
	roleNotificationClient              identityv1.RoleNotificationServiceClient
	partnerIntegrationReportingClient   partnerapiv1.PartnerIntegrationReportingServiceClient
	sellerScorecardReportingClient      sellersv1.SellerScorecardReportingServiceClient
	sellerPayoutScheduleReportingClient sellersv1.SellerPayoutScheduleReportingServiceClient
	driverHomeClient                    driverappv1.DriverHomeServiceClient
}

// NewPartnerIntegrationNotificationServer wires a PartnerIntegrationNotificationServer.
func NewPartnerIntegrationNotificationServer(deps platform.Deps) *PartnerIntegrationNotificationServer {
	partnerWebhookApproval := partnerapiv1.NewPartnerWebhookApprovalServiceClient(deps.Conn)
	return &PartnerIntegrationNotificationServer{
		store:                               NewStore(deps.DB),
		log:                                 deps.Logger.With("service", "acme.partnerapi.v1.PartnerIntegrationNotificationService"),
		partnerWebhookWorkflowClient:        partnerapiv1.NewPartnerWebhookWorkflowServiceClient(deps.Conn),
		meteredPartnerOrderWorkflow:         meteredPartnerOrderWorkflow{PartnerOrderWorkflowServiceClient: partnerapiv1.NewPartnerOrderWorkflowServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		commentPartnerWebhookApprovalFn:     partnerWebhookApproval.CommentPartnerWebhookApproval,
		subscriptionPlanApprovalClient:      subscriptionsv1.NewSubscriptionPlanApprovalServiceClient(deps.Conn),
		conn:                                deps.Conn,
		shippingQuoteWorkflowClient:         checkoutv1.NewShippingQuoteWorkflowServiceClient(deps.Conn),
		clients:                             deps.Clients,
		partnerOrderApprovalClient:          partnerapiv1.NewPartnerOrderApprovalServiceClient(deps.Conn),
		partnerWebhookValidationClient:      partnerapiv1.NewPartnerWebhookValidationServiceClient(deps.Conn),
		sellerWorkflowClient:                sellersv1.NewSellerWorkflowServiceClient(deps.Conn),
		partnerIntegrationIntegrationClient: partnerapiv1.NewPartnerIntegrationIntegrationServiceClient(deps.Conn),
		roleNotificationClient:              identityv1.NewRoleNotificationServiceClient(deps.Conn),
		partnerIntegrationReportingClient:   partnerapiv1.NewPartnerIntegrationReportingServiceClient(deps.Conn),
		sellerScorecardReportingClient:      sellersv1.NewSellerScorecardReportingServiceClient(deps.Conn),
		sellerPayoutScheduleReportingClient: sellersv1.NewSellerPayoutScheduleReportingServiceClient(deps.Conn),
		driverHomeClient:                    driverappv1.NewDriverHomeServiceClient(deps.Conn),
	}
}

// SubscribePartnerIntegrationUpdates subscribes a partner integration.
func (s *PartnerIntegrationNotificationServer) SubscribePartnerIntegrationUpdates(ctx context.Context, req *partnerapiv1.SubscribePartnerIntegrationUpdatesRequest) (*partnerapiv1.SubscribePartnerIntegrationUpdatesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SubscribePartnerIntegrationUpdates", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "partner_integration", req.GetPartnerIntegrationId(), "subscribe_partner_integration_updates"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "subscribe partner integration updates", "tenant", req.GetContext().GetTenantId())
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	cancelPartnerWebhookWorkflowRequest := &partnerapiv1.CancelPartnerWebhookWorkflowRequest{
		Context: req.GetContext(),
		Details: &partnerapiv1.CancelPartnerWebhookWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				Name:  req.GetDetails().GetRecipient().GetName(),
				Email: req.GetDetails().GetRecipient().GetEmail(),
			},
		},
		PartnerWebhook: &partnerapiv1.PartnerWebhook{Detail: packed},
	}
	var cancelPartnerWebhookWorkflowReply *partnerapiv1.CancelPartnerWebhookWorkflowResponse
	if err := pipeline.Run(ctx, "acme.partnerapi.v1.PartnerWebhookWorkflowService/CancelPartnerWebhookWorkflow", func(ctx context.Context) error {
		answer, err := s.partnerWebhookWorkflowClient.CancelPartnerWebhookWorkflow(ctx, cancelPartnerWebhookWorkflowRequest)
		cancelPartnerWebhookWorkflowReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookWorkflowService/CancelPartnerWebhookWorkflow")
	}
	var envelope partnerapiv1.PartnerIntegrationRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	cancelPartnerOrderWorkflowRequest := &partnerapiv1.CancelPartnerOrderWorkflowRequest{
		Context: req.GetContext(),
		PartnerOrder: &partnerapiv1.PartnerOrder{
			Counterparty: &sellersv1.SellerRef{
				DisplayName: cancelPartnerWebhookWorkflowReply.GetPartnerWebhook().GetSubject().GetDisplayName(),
			},
		},
		Details: &partnerapiv1.CancelPartnerOrderWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				Phone:            req.GetDetails().GetRecipient().GetPhone(),
				PreferredChannel: req.GetDetails().GetRecipient().GetPreferredChannel(),
			},
		},
		PartnerOrderId: envelope.GetDisplayName(),
	}
	cancelPartnerOrderWorkflowReply, err := s.meteredPartnerOrderWorkflow.CancelPartnerOrderWorkflow(ctx, cancelPartnerOrderWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerOrderWorkflowService/CancelPartnerOrderWorkflow")
	}
	var envelope2 partnerapiv1.PartnerIntegrationRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	commentPartnerWebhookApprovalRequest := &partnerapiv1.CommentPartnerWebhookApprovalRequest{
		Context: req.GetContext(),
		Details: &partnerapiv1.CommentPartnerWebhookApprovalRequest_Details{
			Approver: &identityv1.UserRef{
				DisplayName: cancelPartnerWebhookWorkflowReply.GetPartnerWebhook().GetSubject().GetDisplayName(),
				Email:       cancelPartnerOrderWorkflowReply.GetPartnerOrder().GetCounterparty().GetEmail(),
			},
		},
		PartnerWebhook: &partnerapiv1.PartnerWebhook{
			Kind:   cancelPartnerWebhookWorkflowReply.GetPartnerWebhook().GetKind(),
			Source: cancelPartnerWebhookWorkflowReply.GetPartnerWebhook().GetSource(),
		},
		PartnerWebhookId: envelope2.GetDisplayName(),
	}
	commentPartnerWebhookApprovalReply, err := s.commentPartnerWebhookApprovalFn(ctx, commentPartnerWebhookApprovalRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookApprovalService/CommentPartnerWebhookApproval")
	}
	delegateSubscriptionPlanApprovalRequest := &subscriptionsv1.DelegateSubscriptionPlanApprovalRequest{
		Context: req.GetContext(),
		SubscriptionPlan: &subscriptionsv1.SubscriptionPlan{
			Owner: &identityv1.UserRef{
				DisplayName: cancelPartnerOrderWorkflowReply.GetPartnerOrder().GetCounterparty().GetDisplayName(),
			},
		},
	}
	type delegateSubscriptionPlanApprovalResult struct {
		reply *subscriptionsv1.DelegateSubscriptionPlanApprovalResponse
		err   error
	}
	delegateSubscriptionPlanApprovalResults := make(chan delegateSubscriptionPlanApprovalResult, 1)
	go func() {
		reply, err := s.subscriptionPlanApprovalClient.DelegateSubscriptionPlanApproval(ctx, delegateSubscriptionPlanApprovalRequest)
		delegateSubscriptionPlanApprovalResults <- delegateSubscriptionPlanApprovalResult{reply, err}
	}()
	var delegateSubscriptionPlanApprovalReply *subscriptionsv1.DelegateSubscriptionPlanApprovalResponse
	select {
	case result := <-delegateSubscriptionPlanApprovalResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.subscriptions.v1.SubscriptionPlanApprovalService/DelegateSubscriptionPlanApproval")
		}
		delegateSubscriptionPlanApprovalReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &partnerapiv1.SubscribePartnerIntegrationUpdatesResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: cancelPartnerWebhookWorkflowReply.GetResult().GetReferenceIds(),
			Message:      cancelPartnerWebhookWorkflowReply.GetOutcome().GetMessage(),
			Status:       "ok",
		},
		PartnerIntegration: &partnerapiv1.PartnerIntegration{
			Audit: &commonv1.AuditInfo{
				CreatedAt: cancelPartnerOrderWorkflowReply.GetPartnerOrder().GetAudit().GetCreatedAt(),
				UpdatedAt: cancelPartnerOrderWorkflowReply.GetPartnerOrder().GetAudit().GetUpdatedAt(),
			},
			Contact: &commonv1.ContactInfo{
				Email: delegateSubscriptionPlanApprovalReply.GetSubscriptionPlan().GetOwner().GetEmail(),
				Phone: req.GetDetails().GetRecipient().GetPhone(),
			},
			PartnerIntegrationId: req.GetPartnerIntegrationId(),
		},
		Outcome: &partnerapiv1.SubscribePartnerIntegrationUpdatesResponse_Outcome{
			NextSteps: commentPartnerWebhookApprovalReply.GetOutcome().GetNextSteps(),
		},
	}, nil
}

// UnsubscribePartnerIntegrationUpdates unsubscribes a partner integration.
func (s *PartnerIntegrationNotificationServer) UnsubscribePartnerIntegrationUpdates(ctx context.Context, req *partnerapiv1.UnsubscribePartnerIntegrationUpdatesRequest) (*partnerapiv1.UnsubscribePartnerIntegrationUpdatesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected UnsubscribePartnerIntegrationUpdates", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "partner_integration", req.GetPartnerIntegrationId(), "unsubscribe_partner_integration_updates"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "unsubscribe partner integration updates", "tenant", req.GetContext().GetTenantId())
	var envelope partnerapiv1.PartnerIntegrationRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	commentPartnerOrderApprovalRequest := &partnerapiv1.CommentPartnerOrderApprovalRequest{
		Context: req.GetContext(),
		PartnerOrder: &partnerapiv1.PartnerOrder{
			Counterparty: &sellersv1.SellerRef{Email: req.GetDetails().GetRecipient().GetEmail()},
		},
		PartnerOrderId: envelope.GetDisplayName(),
	}
	commentPartnerOrderApprovalReply := new(partnerapiv1.CommentPartnerOrderApprovalResponse)
	if err := s.conn.Invoke(ctx, partnerapiPartnerOrderApprovalServiceCommentPartnerOrderApprovalMethod, commentPartnerOrderApprovalRequest, commentPartnerOrderApprovalReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerOrderApprovalService/CommentPartnerOrderApproval")
	}
	encoded, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded partnerapiv1.UnsubscribePartnerIntegrationUpdatesRequest_Details
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	var envelope2 partnerapiv1.PartnerIntegrationRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	step := &partnerIntegrationNotificationServerUnsubscribePartnerIntegrationUpdatesSignalShippingQuoteWorkflowStep{request: &checkoutv1.SignalShippingQuoteWorkflowRequest{
		Context: req.GetContext(),
		ShippingQuote: &checkoutv1.ShippingQuote{
			Audit: &commonv1.AuditInfo{
				CreatedAt: commentPartnerOrderApprovalReply.GetPartnerOrder().GetAudit().GetCreatedAt(),
			},
			BillingAddress: &commonv1.PostalAddress{
				Locality: commentPartnerOrderApprovalReply.GetPartnerOrder().GetBillingAddress().GetLocality(),
				Lines:    commentPartnerOrderApprovalReply.GetPartnerOrder().GetBillingAddress().GetLines(),
			},
		},
		Details: &checkoutv1.SignalShippingQuoteWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				PreferredChannel: (&decoded).GetRecipient().GetPreferredChannel(),
			},
		},
		ShippingQuoteId: envelope2.GetDisplayName(),
	}}
	if err := s.runUnsubscribePartnerIntegrationUpdatesSignalShippingQuoteWorkflow(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.checkout.v1.ShippingQuoteWorkflowService/SignalShippingQuoteWorkflow")
	}
	signalShippingQuoteWorkflowReply := step.reply
	partnerOrderApproval, ok := s.clients.Get("acme.partnerapi.v1.PartnerOrderApprovalService").(partnerapiv1.PartnerOrderApprovalServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.partnerapi.v1.PartnerOrderApprovalService")
	}
	var envelope3 partnerapiv1.PartnerIntegrationRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope3); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	recallPartnerOrderRequest := &partnerapiv1.RecallPartnerOrderRequest{
		Context: req.GetContext(),
		PartnerOrder: &partnerapiv1.PartnerOrder{
			Settlement: &partnerapiv1.PartnerOrder_Card{Card: &partnerapiv1.PartnerOrderCardSettlement{
				HolderName: commentPartnerOrderApprovalReply.GetPartnerOrder().GetCard().GetHolderName(),
				Token:      signalShippingQuoteWorkflowReply.GetShippingQuote().GetCard().GetToken(),
				Network:    signalShippingQuoteWorkflowReply.GetShippingQuote().GetCard().GetNetwork(),
			}},
			Audit: &commonv1.AuditInfo{
				CreatedBy: signalShippingQuoteWorkflowReply.GetShippingQuote().GetAudit().GetCreatedBy(),
			},
		},
		PartnerOrderId: envelope3.GetDisplayName(),
	}
	recallPartnerOrderReply, err := partnerOrderApproval.RecallPartnerOrder(ctx, recallPartnerOrderRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerOrderApprovalService/RecallPartnerOrder")
	}
	return &partnerapiv1.UnsubscribePartnerIntegrationUpdatesResponse{
		Result: &commonv1.OperationResult{
			Status: commentPartnerOrderApprovalReply.GetOutcome().GetStatus(),
		},
		PartnerIntegration: &partnerapiv1.PartnerIntegration{
			Audit: &commonv1.AuditInfo{
				CreatedBy: commentPartnerOrderApprovalReply.GetPartnerOrder().GetAudit().GetCreatedBy(),
			},
			PartnerIntegrationId: req.GetPartnerIntegrationId(),
			Contact: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetRecipient().GetPreferredChannel(),
			},
		},
		Outcome: &partnerapiv1.UnsubscribePartnerIntegrationUpdatesResponse_Outcome{
			EffectiveAt: signalShippingQuoteWorkflowReply.GetOutcome().GetEffectiveAt(),
			Message:     recallPartnerOrderReply.GetOutcome().GetMessage(),
		},
	}, nil
}

// NotifyPartnerIntegrationWatchers notifys a partner integration.
func (s *PartnerIntegrationNotificationServer) NotifyPartnerIntegrationWatchers(ctx context.Context, req *partnerapiv1.NotifyPartnerIntegrationWatchersRequest) (*partnerapiv1.NotifyPartnerIntegrationWatchersResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "partner_integration", req.GetPartnerIntegrationId(), "notify_partner_integration_watchers"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "notify partner integration watchers", "tenant", req.GetContext().GetTenantId())
	approvePartnerOrderRequest := &partnerapiv1.ApprovePartnerOrderRequest{
		Context: req.GetContext(),
		Details: &partnerapiv1.ApprovePartnerOrderRequest_Details{
			Approver: &identityv1.UserRef{Email: req.GetDetails().GetRecipient().GetEmail()},
		},
		Reason: req.GetReason(),
		PartnerOrder: &partnerapiv1.PartnerOrder{
			Counterparty: &sellersv1.SellerRef{Email: req.GetContext().GetActor().GetEmail()},
		},
		Annotations: req.GetAnnotations(),
	}
	approvePartnerOrderReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*partnerapiv1.ApprovePartnerOrderResponse, error) {
		return s.partnerOrderApprovalClient.ApprovePartnerOrder(ctx, approvePartnerOrderRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerOrderApprovalService/ApprovePartnerOrder")
	}
	listPartnerWebhookWorkflowsRequest := &partnerapiv1.ListPartnerWebhookWorkflowsRequest{
		Context: req.GetContext(),
		Filter: &partnerapiv1.ListPartnerWebhookWorkflowsRequest_Filter{
			Region:       approvePartnerOrderReply.GetPartnerOrder().GetBillingAddress().GetRegion(),
			CreatedAfter: approvePartnerOrderReply.GetPartnerOrder().GetDueAt(),
			Labels:       approvePartnerOrderReply.GetPartnerOrder().GetMetadata(),
		},
	}
	listPartnerWebhookWorkflowsReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*partnerapiv1.ListPartnerWebhookWorkflowsResponse, error) {
		return s.partnerWebhookWorkflowClient.ListPartnerWebhookWorkflows(ctx, listPartnerWebhookWorkflowsRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookWorkflowService/ListPartnerWebhookWorkflows")
	}
	normalizePartnerWebhookRequest := &partnerapiv1.NormalizePartnerWebhookRequest{
		Context: req.GetContext(),
		Details: &partnerapiv1.NormalizePartnerWebhookRequest_Details{
			Contact: &commonv1.ContactInfo{
				Name:  req.GetDetails().GetRecipient().GetName(),
				Email: approvePartnerOrderReply.GetPartnerOrder().GetCounterparty().GetEmail(),
				Phone: req.GetDetails().GetRecipient().GetPhone(),
			},
		},
		PartnerWebhook: &partnerapiv1.PartnerWebhook{
			CorrelationId: approvePartnerOrderReply.GetResult().GetCorrelationId(),
		},
	}
	normalizePartnerWebhookReply, err := grpcx.Call(ctx, s.partnerWebhookValidationClient.NormalizePartnerWebhook, normalizePartnerWebhookRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookValidationService/NormalizePartnerWebhook")
	}
	return &partnerapiv1.NotifyPartnerIntegrationWatchersResponse{
		PartnerIntegration: &partnerapiv1.PartnerIntegration{
			Holder: &partnerapiv1.PartnerWebhookRef{
				DisplayName: approvePartnerOrderReply.GetPartnerOrder().GetCounterparty().GetDisplayName(),
				Kind:        normalizePartnerWebhookReply.GetPartnerWebhook().GetKind(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: approvePartnerOrderReply.GetPartnerOrder().GetAudit().GetUpdatedAt(),
			},
			PartnerIntegrationId: req.GetPartnerIntegrationId(),
			Contact:              &commonv1.ContactInfo{Name: req.GetDetails().GetRecipient().GetName()},
		},
		Result: &commonv1.OperationResult{
			Status: listPartnerWebhookWorkflowsReply.GetResult().GetStatus(),
		},
	}, nil
}

// ListPartnerIntegrationSubscriptions lists partner integrations a page at a time.
func (s *PartnerIntegrationNotificationServer) ListPartnerIntegrationSubscriptions(ctx context.Context, req *partnerapiv1.ListPartnerIntegrationSubscriptionsRequest) (*partnerapiv1.ListPartnerIntegrationSubscriptionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "list partner integration subscriptions", "tenant", req.GetContext().GetTenantId())
	pauseSellerWorkflowRequest := &sellersv1.PauseSellerWorkflowRequest{
		Context: req.GetContext(),
		Seller: &sellersv1.Seller{
			RelationshipManager: &identityv1.UserRef{
				Email:  req.GetContext().GetActor().GetEmail(),
				UserId: req.GetContext().GetActor().GetUserId(),
			},
			PrimaryAddress: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
	}
	type pauseSellerWorkflowResult struct {
		reply *sellersv1.PauseSellerWorkflowResponse
		err   error
	}
	pauseSellerWorkflowResults := make(chan pauseSellerWorkflowResult, 1)
	go func() {
		reply, err := s.sellerWorkflowClient.PauseSellerWorkflow(ctx, pauseSellerWorkflowRequest)
		pauseSellerWorkflowResults <- pauseSellerWorkflowResult{reply, err}
	}()
	var pauseSellerWorkflowReply *sellersv1.PauseSellerWorkflowResponse
	select {
	case result := <-pauseSellerWorkflowResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.sellers.v1.SellerWorkflowService/PauseSellerWorkflow")
		}
		pauseSellerWorkflowReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	updateContractRequest := &contractsv1.UpdateContractRequest{
		Context: req.GetContext(),
		Contract: &contractsv1.Contract{
			BillingAddress: &commonv1.PostalAddress{
				Recipient: pauseSellerWorkflowReply.GetSeller().GetPrimaryAddress().GetRecipient(),
			},
			Counterparty: &contractsv1.RenewalRef{
				DisplayName: pauseSellerWorkflowReply.GetSeller().GetRelationshipManager().GetDisplayName(),
			},
		},
	}
	updateContractReply := new(contractsv1.UpdateContractResponse)
	if err := s.conn.Invoke(ctx, contractsContractServiceUpdateContractMethod, updateContractRequest, updateContractReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.contracts.v1.ContractService/UpdateContract")
	}
	return &partnerapiv1.ListPartnerIntegrationSubscriptionsResponse{
		PartnerIntegrations: []*partnerapiv1.PartnerIntegration{&partnerapiv1.PartnerIntegration{
			Audit: &commonv1.AuditInfo{
				CreatedAt: pauseSellerWorkflowReply.GetSeller().GetAudit().GetCreatedAt(),
			},
		}},
		Result: &commonv1.OperationResult{
			ReferenceIds: pauseSellerWorkflowReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// SendPartnerIntegrationTestNotice sends a partner integration.
func (s *PartnerIntegrationNotificationServer) SendPartnerIntegrationTestNotice(ctx context.Context, req *partnerapiv1.SendPartnerIntegrationTestNoticeRequest) (*partnerapiv1.SendPartnerIntegrationTestNoticeResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "partner_integration", req.GetPartnerIntegrationId(), "send_partner_integration_test_notice"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "send partner integration test notice", "tenant", req.GetContext().GetTenantId())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	var holder struct{ TemplateId string }
	if record := req.GetDetails(); record != nil {
		if err := mapper.CopyFields(&holder, record, "TemplateId"); err != nil {
			s.log.WarnContext(ctx, "copy template_id", "error", err)
		}
	}
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	var envelope partnerapiv1.PartnerIntegrationRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	replayPartnerIntegrationEventsRequest := &partnerapiv1.ReplayPartnerIntegrationEventsRequest{}
	replayPartnerIntegrationEventsRequest.Context = req.GetContext()
	replayPartnerIntegrationEventsRequest.PartnerIntegrationId = req.GetPartnerIntegrationId()
	replayPartnerIntegrationEventsRequest.Annotations = annotations
	replayPartnerIntegrationEventsRequest.Reason = req.GetReason()
	replayPartnerIntegrationEventsRequest.Details = &partnerapiv1.ReplayPartnerIntegrationEventsRequest_Details{
		MessageId: holder.TemplateId,
		Envelope:  packed,
		Endpoint:  envelope.GetDisplayName(),
	}
	replayPartnerIntegrationEventsReply, err := s.partnerIntegrationIntegrationClient.ReplayPartnerIntegrationEvents(ctx, replayPartnerIntegrationEventsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerIntegrationIntegrationService/ReplayPartnerIntegrationEvents")
	}
	streamRoleNoticesStream, err := s.roleNotificationClient.StreamRoleNotices(ctx, &identityv1.StreamRoleNoticesRequest{
		Context: req.GetContext(),
		Subscriber: &commonv1.ContactInfo{
			PreferredChannel: replayPartnerIntegrationEventsReply.GetPartnerIntegration().GetContact().GetPreferredChannel(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.RoleNotificationService/StreamRoleNotices")
	}
	for {
		received, err := streamRoleNoticesStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.identity.v1.RoleNotificationService/StreamRoleNotices")
		}
		_ = received
	}
	fetchPartnerIntegrationReportRequest := &partnerapiv1.FetchPartnerIntegrationReportRequest{}
	fetchPartnerIntegrationReportRequest.Context = req.GetContext()
	fetchPartnerIntegrationReportRequest.PartnerIntegrationId = req.GetPartnerIntegrationId()
	fetchPartnerIntegrationReportReply, err := s.partnerIntegrationReportingClient.FetchPartnerIntegrationReport(ctx, fetchPartnerIntegrationReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerIntegrationReportingService/FetchPartnerIntegrationReport")
	}
	return &partnerapiv1.SendPartnerIntegrationTestNoticeResponse{
		Outcome: &partnerapiv1.SendPartnerIntegrationTestNoticeResponse_Outcome{
			ReferenceId: replayPartnerIntegrationEventsReply.GetOutcome().GetReferenceId(),
		},
		PartnerIntegration: &partnerapiv1.PartnerIntegration{
			Holder:               fetchPartnerIntegrationReportReply.GetPartnerIntegration().GetHolder(),
			PartnerIntegrationId: req.GetPartnerIntegrationId(),
			Contact:              &commonv1.ContactInfo{Email: req.GetDetails().GetRecipient().GetEmail()},
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetPartnerIntegrationPreferences returns a partner integration.
func (s *PartnerIntegrationNotificationServer) GetPartnerIntegrationPreferences(ctx context.Context, req *partnerapiv1.GetPartnerIntegrationPreferencesRequest) (*partnerapiv1.GetPartnerIntegrationPreferencesResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "partner_integration", req.GetPartnerIntegrationId(), "get_partner_integration_preferences"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get partner integration preferences", "tenant", req.GetContext().GetTenantId())
	return &partnerapiv1.GetPartnerIntegrationPreferencesResponse{
		PartnerIntegration: &partnerapiv1.PartnerIntegration{PartnerIntegrationId: req.GetPartnerIntegrationId()},
		Result:             &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// StreamPartnerIntegrationNotices streams the changes of partner integrations as they happen.
func (s *PartnerIntegrationNotificationServer) StreamPartnerIntegrationNotices(req *partnerapiv1.StreamPartnerIntegrationNoticesRequest, stream grpc.ServerStreamingServer[partnerapiv1.StreamPartnerIntegrationNoticesResponse]) error {
	ctx := stream.Context()
	if req.GetContext().GetTenantId() == "" {
		return status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "stream partner integration notices", "tenant", req.GetContext().GetTenantId())
	generateSellerScorecardReportRequest := &sellersv1.GenerateSellerScorecardReportRequest{
		Context:     req.GetContext(),
		Details:     &sellersv1.GenerateSellerScorecardReportRequest_Details{Filters: req.GetFilter().GetLabels()},
		Annotations: req.GetContext().GetBaggage(),
	}
	generateSellerScorecardReportReply, err := sellersv1.SellerScorecardReportingServiceClient.GenerateSellerScorecardReport(s.sellerScorecardReportingClient, ctx, generateSellerScorecardReportRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardReportingService/GenerateSellerScorecardReport")
	}
	step := &partnerIntegrationNotificationServerStreamPartnerIntegrationNoticesGenerateSellerPayoutScheduleReportStep{request: &sellersv1.GenerateSellerPayoutScheduleReportRequest{
		Context:     req.GetContext(),
		Details:     &sellersv1.GenerateSellerPayoutScheduleReportRequest_Details{Period: generateSellerScorecardReportReply.GetPeriod()},
		Annotations: generateSellerScorecardReportReply.GetResult().GetDetails(),
	}}
	if err := s.runStreamPartnerIntegrationNoticesGenerateSellerPayoutScheduleReport(ctx, step); err != nil {
		return grpcx.Wrap(err, "acme.sellers.v1.SellerPayoutScheduleReportingService/GenerateSellerPayoutScheduleReport")
	}
	generateSellerPayoutScheduleReportReply := step.reply
	listDriverHomesRequest := &driverappv1.ListDriverHomesRequest{}
	listDriverHomesRequest.Context = req.GetContext()
	listDriverHomesRequest.Filter = &driverappv1.ListDriverHomesRequest_Filter{OwnerId: req.GetFilter().GetOwnerId()}
	listDriverHomesReply, err := s.driverHomeClient.ListDriverHomes(ctx, listDriverHomesRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.driverapp.v1.DriverHomeService/ListDriverHomes")
	}
	if err := stream.Send(&partnerapiv1.StreamPartnerIntegrationNoticesResponse{
		Result: &commonv1.OperationResult{
			Message:      generateSellerScorecardReportReply.GetResult().GetMessage(),
			Details:      generateSellerPayoutScheduleReportReply.GetResult().GetDetails(),
			ReferenceIds: listDriverHomesReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		PartnerIntegration: &partnerapiv1.PartnerIntegration{
			Labels:  req.GetFilter().GetLabels(),
			Contact: &commonv1.ContactInfo{Email: req.GetSubscriber().GetEmail()},
		},
	}); err != nil {
		return err
	}
	return nil
}

// partnerIntegrationNotificationServerUnsubscribePartnerIntegrationUpdatesSignalShippingQuoteWorkflowStep carries the
// request of one step of UnsubscribePartnerIntegrationUpdates and, once it ran, its reply.
type partnerIntegrationNotificationServerUnsubscribePartnerIntegrationUpdatesSignalShippingQuoteWorkflowStep struct {
	request *checkoutv1.SignalShippingQuoteWorkflowRequest
	reply   *checkoutv1.SignalShippingQuoteWorkflowResponse
}

func (s *PartnerIntegrationNotificationServer) runUnsubscribePartnerIntegrationUpdatesSignalShippingQuoteWorkflow(ctx context.Context, step *partnerIntegrationNotificationServerUnsubscribePartnerIntegrationUpdatesSignalShippingQuoteWorkflowStep) error {
	reply, err := s.shippingQuoteWorkflowClient.SignalShippingQuoteWorkflow(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}

// partnerIntegrationNotificationServerStreamPartnerIntegrationNoticesGenerateSellerPayoutScheduleReportStep carries the
// request of one step of StreamPartnerIntegrationNotices and, once it ran, its reply.
type partnerIntegrationNotificationServerStreamPartnerIntegrationNoticesGenerateSellerPayoutScheduleReportStep struct {
	request *sellersv1.GenerateSellerPayoutScheduleReportRequest
	reply   *sellersv1.GenerateSellerPayoutScheduleReportResponse
}

func (s *PartnerIntegrationNotificationServer) runStreamPartnerIntegrationNoticesGenerateSellerPayoutScheduleReport(ctx context.Context, step *partnerIntegrationNotificationServerStreamPartnerIntegrationNoticesGenerateSellerPayoutScheduleReportStep) error {
	reply, err := s.sellerPayoutScheduleReportingClient.GenerateSellerPayoutScheduleReport(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
