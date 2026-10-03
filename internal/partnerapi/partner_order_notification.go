package partnerapi

import (
	"context"
	"errors"
	"io"
	"log/slog"

	checkoutv1 "github.com/hounddogai/protomap-test-golang/gen/acme/checkout/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	employeeportalv1 "github.com/hounddogai/protomap-test-golang/gen/acme/employeeportal/v1"
	financeportalv1 "github.com/hounddogai/protomap-test-golang/gen/acme/financeportal/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	partnerapiv1 "github.com/hounddogai/protomap-test-golang/gen/acme/partnerapi/v1"
	pricingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/pricing/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
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

// PartnerOrderNotificationServer implements `acme.partnerapi.v1.PartnerOrderNotificationService`.
// Tells the people who follow a partner order what changed.
type PartnerOrderNotificationServer struct {
	partnerapiv1.UnimplementedPartnerOrderNotificationServiceServer
	store                                *Store
	log                                  *slog.Logger
	conn                                 *grpc.ClientConn
	financeWorkspaceClient               financeportalv1.FinanceWorkspaceServiceClient
	clients                              *registry.Registry
	priceListWorkflowClient              pricingv1.PriceListWorkflowServiceClient
	restockDecisionReportingClient       returnsv1.RestockDecisionReportingServiceClient
	portalHomePort                       portalHomePort
	partnerOrderReportingClient          partnerapiv1.PartnerOrderReportingServiceClient
	partnerIntegrationNotificationClient partnerapiv1.PartnerIntegrationNotificationServiceClient
	partnerIntegrationIntegrationPort    partnerIntegrationIntegrationPort
	partnerWebhookReportingClient        partnerapiv1.PartnerWebhookReportingServiceClient
	partnerWebhookApprovalClient         partnerapiv1.PartnerWebhookApprovalServiceClient
	checkoutSessionApprovalClient        checkoutv1.CheckoutSessionApprovalServiceClient
}

// NewPartnerOrderNotificationServer wires a PartnerOrderNotificationServer.
func NewPartnerOrderNotificationServer(deps platform.Deps) *PartnerOrderNotificationServer {
	return &PartnerOrderNotificationServer{
		store:                                NewStore(deps.DB),
		log:                                  deps.Logger.With("service", "acme.partnerapi.v1.PartnerOrderNotificationService"),
		conn:                                 deps.Conn,
		financeWorkspaceClient:               financeportalv1.NewFinanceWorkspaceServiceClient(deps.Conn),
		clients:                              deps.Clients,
		priceListWorkflowClient:              pricingv1.NewPriceListWorkflowServiceClient(deps.Conn),
		restockDecisionReportingClient:       returnsv1.NewRestockDecisionReportingServiceClient(deps.Conn),
		portalHomePort:                       newPortalHomePort(deps),
		partnerOrderReportingClient:          partnerapiv1.NewPartnerOrderReportingServiceClient(deps.Conn),
		partnerIntegrationNotificationClient: partnerapiv1.NewPartnerIntegrationNotificationServiceClient(deps.Conn),
		partnerIntegrationIntegrationPort:    newPartnerIntegrationIntegrationPort(deps),
		partnerWebhookReportingClient:        partnerapiv1.NewPartnerWebhookReportingServiceClient(deps.Conn),
		partnerWebhookApprovalClient:         partnerapiv1.NewPartnerWebhookApprovalServiceClient(deps.Conn),
		checkoutSessionApprovalClient:        checkoutv1.NewCheckoutSessionApprovalServiceClient(deps.Conn),
	}
}

// SubscribePartnerOrderUpdates subscribes a partner order.
func (s *PartnerOrderNotificationServer) SubscribePartnerOrderUpdates(ctx context.Context, req *partnerapiv1.SubscribePartnerOrderUpdatesRequest) (*partnerapiv1.SubscribePartnerOrderUpdatesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "partner_order", req.GetPartnerOrderId(), "subscribe_partner_order_updates"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "subscribe partner order updates", "tenant", req.GetContext().GetTenantId())
	encoded, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded partnerapiv1.SubscribePartnerOrderUpdatesRequest_Details
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	cloned := proto.Clone(req.GetDetails()).(*partnerapiv1.SubscribePartnerOrderUpdatesRequest_Details)
	cloned2 := proto.Clone(req.GetDetails()).(*partnerapiv1.SubscribePartnerOrderUpdatesRequest_Details)
	var envelope partnerapiv1.PartnerIntegrationRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	pauseSellerListingWorkflowRequest := &sellersv1.PauseSellerListingWorkflowRequest{
		Context: req.GetContext(),
		Details: &sellersv1.PauseSellerListingWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				Name:             (&decoded).GetRecipient().GetName(),
				Phone:            cloned.GetRecipient().GetPhone(),
				PreferredChannel: cloned2.GetRecipient().GetPreferredChannel(),
			},
		},
		SellerListing: &sellersv1.SellerListing{
			Owner: &identityv1.UserRef{Email: req.GetDetails().GetRecipient().GetEmail()},
		},
		SellerListingId: envelope.GetDisplayName(),
	}
	pauseSellerListingWorkflowReply := new(sellersv1.PauseSellerListingWorkflowResponse)
	if err := s.conn.Invoke(ctx, sellersSellerListingWorkflowServicePauseSellerListingWorkflowMethod, pauseSellerListingWorkflowRequest, pauseSellerListingWorkflowReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingWorkflowService/PauseSellerListingWorkflow")
	}
	batchGetFinanceWorkspacesRequest := &financeportalv1.BatchGetFinanceWorkspacesRequest{Context: req.GetContext()}
	batchGetFinanceWorkspacesReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*financeportalv1.BatchGetFinanceWorkspacesResponse, error) {
		return s.financeWorkspaceClient.BatchGetFinanceWorkspaces(ctx, batchGetFinanceWorkspacesRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.financeportal.v1.FinanceWorkspaceService/BatchGetFinanceWorkspaces")
	}
	return &partnerapiv1.SubscribePartnerOrderUpdatesResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: pauseSellerListingWorkflowReply.GetResult().GetCorrelationId(),
			Details:       batchGetFinanceWorkspacesReply.GetResult().GetDetails(),
			Status:        "ok",
		},
		PartnerOrder: &partnerapiv1.PartnerOrder{
			Counterparty: &sellersv1.SellerRef{
				DisplayName: pauseSellerListingWorkflowReply.GetSellerListing().GetOwner().GetDisplayName(),
				Email:       req.GetDetails().GetRecipient().GetEmail(),
			},
			PartnerOrderId: req.GetPartnerOrderId(),
		},
	}, nil
}

// UnsubscribePartnerOrderUpdates unsubscribes a partner order.
func (s *PartnerOrderNotificationServer) UnsubscribePartnerOrderUpdates(ctx context.Context, req *partnerapiv1.UnsubscribePartnerOrderUpdatesRequest) (*partnerapiv1.UnsubscribePartnerOrderUpdatesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected UnsubscribePartnerOrderUpdates", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "partner_order", req.GetPartnerOrderId(), "unsubscribe_partner_order_updates"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "unsubscribe partner order updates", "tenant", req.GetContext().GetTenantId())
	partnerOrderIntegration, ok := s.clients.Get("acme.partnerapi.v1.PartnerOrderIntegrationService").(partnerapiv1.PartnerOrderIntegrationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.partnerapi.v1.PartnerOrderIntegrationService")
	}
	var envelope partnerapiv1.PartnerIntegrationRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	listPartnerOrderSourcesRequest := &partnerapiv1.ListPartnerOrderSourcesRequest{
		Context: req.GetContext(),
		Filter: &partnerapiv1.ListPartnerOrderSourcesRequest_Filter{
			OwnerId:      req.GetDetails().GetTemplateId(),
			CreatedAfter: req.GetContext().GetDeadline(),
			Labels:       req.GetAnnotations(),
		},
		Page: &commonv1.PageRequest{PageToken: envelope.GetDisplayName()},
	}
	listPartnerOrderSourcesReply, err := partnerOrderIntegration.ListPartnerOrderSources(ctx, listPartnerOrderSourcesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerOrderIntegrationService/ListPartnerOrderSources")
	}
	source := req.GetDetails().GetRecipient().ProtoReflect()
	email := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("email"))).String()
	var envelope2 partnerapiv1.PartnerIntegrationRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	startPriceListWorkflowReply, err := s.priceListWorkflowClient.StartPriceListWorkflow(ctx, &pricingv1.StartPriceListWorkflowRequest{
		Context:   req.GetContext(),
		PriceList: &pricingv1.PriceList{Owner: &identityv1.UserRef{Email: email}},
		Details: &pricingv1.StartPriceListWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Name: req.GetDetails().GetRecipient().GetName()},
		},
		PriceListId: envelope2.GetDisplayName(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceListWorkflowService/StartPriceListWorkflow")
	}
	return &partnerapiv1.UnsubscribePartnerOrderUpdatesResponse{
		Outcome: &partnerapiv1.UnsubscribePartnerOrderUpdatesResponse_Outcome{
			Status: listPartnerOrderSourcesReply.GetResult().GetStatus(),
		},
		Result: &commonv1.OperationResult{
			Message: startPriceListWorkflowReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		PartnerOrder: &partnerapiv1.PartnerOrder{
			PartnerOrderId: req.GetPartnerOrderId(),
			Counterparty:   &sellersv1.SellerRef{Email: req.GetDetails().GetRecipient().GetEmail()},
		},
	}, nil
}

// NotifyPartnerOrderWatchers notifys a partner order.
func (s *PartnerOrderNotificationServer) NotifyPartnerOrderWatchers(ctx context.Context, req *partnerapiv1.NotifyPartnerOrderWatchersRequest) (*partnerapiv1.NotifyPartnerOrderWatchersResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "partner_order", req.GetPartnerOrderId(), "notify_partner_order_watchers"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "notify partner order watchers", "tenant", req.GetContext().GetTenantId())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	var envelope partnerapiv1.PartnerIntegrationRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	generateRestockDecisionReportRequest := &returnsv1.GenerateRestockDecisionReportRequest{}
	generateRestockDecisionReportRequest.Context = req.GetContext()
	generateRestockDecisionReportRequest.Annotations = annotations
	generateRestockDecisionReportRequest.Reason = req.GetReason()
	generateRestockDecisionReportRequest.RestockDecisionId = envelope.GetDisplayName()
	generateRestockDecisionReportReply, err := s.restockDecisionReportingClient.GenerateRestockDecisionReport(ctx, generateRestockDecisionReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.RestockDecisionReportingService/GenerateRestockDecisionReport")
	}
	var envelope2 partnerapiv1.PartnerIntegrationRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	clonePortalHomeRequest := &employeeportalv1.ClonePortalHomeRequest{
		Context: req.GetContext(),
		PortalHome: &employeeportalv1.PortalHome{
			Requester:    &commonv1.ContactInfo{Phone: req.GetDetails().GetRecipient().GetPhone()},
			PortalHomeId: envelope2.GetDisplayName(),
		},
	}
	clonePortalHomeReply, err := s.portalHomePort.ClonePortalHome(ctx, clonePortalHomeRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.employeeportal.v1.PortalHomeService/ClonePortalHome")
	}
	step := &partnerOrderNotificationServerNotifyPartnerOrderWatchersDeletePartnerOrderReportStep{request: &partnerapiv1.DeletePartnerOrderReportRequest{
		Context:        req.GetContext(),
		PartnerOrderId: req.GetPartnerOrderId(),
		Etag:           clonePortalHomeReply.GetPortalHome().GetAudit().GetEtag(),
		Reason:         req.GetReason(),
	}}
	if err := s.runNotifyPartnerOrderWatchersDeletePartnerOrderReport(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerOrderReportingService/DeletePartnerOrderReport")
	}
	deletePartnerOrderReportReply := step.reply
	return &partnerapiv1.NotifyPartnerOrderWatchersResponse{
		Outcome: &partnerapiv1.NotifyPartnerOrderWatchersResponse_Outcome{
			Message: generateRestockDecisionReportReply.GetResult().GetMessage(),
		},
		Result: &commonv1.OperationResult{
			Details: generateRestockDecisionReportReply.GetResult().GetDetails(),
			Message: deletePartnerOrderReportReply.GetResult().GetMessage(),
			Status:  deletePartnerOrderReportReply.GetResult().GetStatus(),
		},
		PartnerOrder: &partnerapiv1.PartnerOrder{
			Counterparty: &sellersv1.SellerRef{
				DisplayName: clonePortalHomeReply.GetPortalHome().GetCustomer().GetDisplayName(),
				Email:       req.GetDetails().GetRecipient().GetEmail(),
			},
			PartnerOrderId: req.GetPartnerOrderId(),
		},
	}, nil
}

// ListPartnerOrderSubscriptions lists partner orders a page at a time.
func (s *PartnerOrderNotificationServer) ListPartnerOrderSubscriptions(ctx context.Context, req *partnerapiv1.ListPartnerOrderSubscriptionsRequest) (*partnerapiv1.ListPartnerOrderSubscriptionsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "list partner order subscriptions", "tenant", req.GetContext().GetTenantId())
	step := &partnerOrderNotificationServerListPartnerOrderSubscriptionsUnsubscribePartnerIntegrationUpdatesStep{request: &partnerapiv1.UnsubscribePartnerIntegrationUpdatesRequest{
		Context: req.GetContext(),
		Details: &partnerapiv1.UnsubscribePartnerIntegrationUpdatesRequest_Details{
			Recipient: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Variables: req.GetFilter().GetLabels(),
		},
		Annotations: req.GetContext().GetBaggage(),
	}}
	if err := s.runListPartnerOrderSubscriptionsUnsubscribePartnerIntegrationUpdates(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerIntegrationNotificationService/UnsubscribePartnerIntegrationUpdates")
	}
	unsubscribePartnerIntegrationUpdatesReply := step.reply
	return &partnerapiv1.ListPartnerOrderSubscriptionsResponse{
		Result: &commonv1.OperationResult{
			Message: unsubscribePartnerIntegrationUpdatesReply.GetOutcome().GetMessage(),
			Status:  "ok",
		},
		PartnerOrders: []*partnerapiv1.PartnerOrder{&partnerapiv1.PartnerOrder{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: unsubscribePartnerIntegrationUpdatesReply.GetPartnerIntegration().GetAudit().GetUpdatedBy(),
			},
		}},
	}, nil
}

// GetPartnerOrderPreferences returns a partner order.
func (s *PartnerOrderNotificationServer) GetPartnerOrderPreferences(ctx context.Context, req *partnerapiv1.GetPartnerOrderPreferencesRequest) (*partnerapiv1.GetPartnerOrderPreferencesResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "partner_order", req.GetPartnerOrderId(), "get_partner_order_preferences"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get partner order preferences", "tenant", req.GetContext().GetTenantId())
	listPartnerIntegrationSourcesRequest := &partnerapiv1.ListPartnerIntegrationSourcesRequest{
		Context: req.GetContext(),
		Filter: &partnerapiv1.ListPartnerIntegrationSourcesRequest_Filter{
			CreatedAfter: req.GetContext().GetDeadline(),
			OwnerId:      req.GetPartnerOrderId(),
		},
	}
	listPartnerIntegrationSourcesReply, err := s.partnerIntegrationIntegrationPort.ListPartnerIntegrationSources(ctx, listPartnerIntegrationSourcesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerIntegrationIntegrationService/ListPartnerIntegrationSources")
	}
	return &partnerapiv1.GetPartnerOrderPreferencesResponse{
		Result: &commonv1.OperationResult{
			Status:  listPartnerIntegrationSourcesReply.GetResult().GetStatus(),
			Details: listPartnerIntegrationSourcesReply.GetResult().GetDetails(),
		},
		PartnerOrder: &partnerapiv1.PartnerOrder{PartnerOrderId: req.GetPartnerOrderId()},
	}, nil
}

// StreamPartnerOrderNotices streams the changes of partner orders as they happen.
func (s *PartnerOrderNotificationServer) StreamPartnerOrderNotices(req *partnerapiv1.StreamPartnerOrderNoticesRequest, stream grpc.ServerStreamingServer[partnerapiv1.StreamPartnerOrderNoticesResponse]) error {
	ctx := stream.Context()
	if req.GetContext().GetTenantId() == "" {
		return status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "stream partner order notices", "tenant", req.GetContext().GetTenantId())
	streamPartnerIntegrationNoticesStream, err := s.partnerIntegrationNotificationClient.StreamPartnerIntegrationNotices(ctx, &partnerapiv1.StreamPartnerIntegrationNoticesRequest{
		Context:    req.GetContext(),
		Subscriber: &commonv1.ContactInfo{Phone: req.GetSubscriber().GetPhone()},
	})
	if err != nil {
		return grpcx.Wrap(err, "acme.partnerapi.v1.PartnerIntegrationNotificationService/StreamPartnerIntegrationNotices")
	}
	for {
		received, err := streamPartnerIntegrationNoticesStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return grpcx.Wrap(err, "acme.partnerapi.v1.PartnerIntegrationNotificationService/StreamPartnerIntegrationNotices")
		}
		_ = received
	}
	ctx = tenancy.With(ctx, "preferred_channel", req.GetSubscriber().GetPreferredChannel())
	encoded, err := protojson.Marshal(req.GetFilter())
	if err != nil {
		return grpcx.Wrap(err, "encode request")
	}
	var decoded partnerapiv1.StreamPartnerOrderNoticesRequest_Filter
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return err
	}
	streamPartnerWebhookMetricsStream, err := s.partnerWebhookReportingClient.StreamPartnerWebhookMetrics(ctx, &partnerapiv1.StreamPartnerWebhookMetricsRequest{
		Context:    req.GetContext(),
		Subscriber: &commonv1.ContactInfo{PreferredChannel: tenancy.Value(ctx, "preferred_channel")},
		Filter: &partnerapiv1.StreamPartnerWebhookMetricsRequest_Filter{
			OwnerId: (&decoded).GetOwnerId(),
			Query:   req.GetFilter().GetQuery(),
			Region:  req.GetFilter().GetRegion(),
		},
	})
	if err != nil {
		return grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookReportingService/StreamPartnerWebhookMetrics")
	}
	for {
		received2, err := streamPartnerWebhookMetricsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookReportingService/StreamPartnerWebhookMetrics")
		}
		_ = received2
	}
	delegatePartnerWebhookApprovalRequest := &partnerapiv1.DelegatePartnerWebhookApprovalRequest{
		Context: req.GetContext(),
		Details: &partnerapiv1.DelegatePartnerWebhookApprovalRequest_Details{
			Approver: &identityv1.UserRef{Email: req.GetSubscriber().GetEmail()},
		},
	}
	delegatePartnerWebhookApprovalReply, err := grpcx.Call(ctx, s.partnerWebhookApprovalClient.DelegatePartnerWebhookApproval, delegatePartnerWebhookApprovalRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookApprovalService/DelegatePartnerWebhookApproval")
	}
	encoded2, err := protojson.Marshal(req.GetSubscriber())
	if err != nil {
		return grpcx.Wrap(err, "encode request")
	}
	var decoded2 commonv1.ContactInfo
	if err := protojson.Unmarshal(encoded2, &decoded2); err != nil {
		return err
	}
	ctx = tenancy.With(ctx, "region", req.GetFilter().GetRegion())
	submitCheckoutSessionRequest := &checkoutv1.SubmitCheckoutSessionRequest{
		Context: req.GetContext(),
		Details: &checkoutv1.SubmitCheckoutSessionRequest_Details{
			Approver: &identityv1.UserRef{Email: (&decoded2).GetEmail()},
		},
		CheckoutSession: &checkoutv1.CheckoutSession{
			BillingAddress: &commonv1.PostalAddress{Region: tenancy.Value(ctx, "region")},
		},
	}
	submitCheckoutSessionReply, err := checkoutv1.CheckoutSessionApprovalServiceClient.SubmitCheckoutSession(s.checkoutSessionApprovalClient, ctx, submitCheckoutSessionRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.checkout.v1.CheckoutSessionApprovalService/SubmitCheckoutSession")
	}
	if err := stream.Send(&partnerapiv1.StreamPartnerOrderNoticesResponse{
		PartnerOrder: &partnerapiv1.PartnerOrder{
			Counterparty: &sellersv1.SellerRef{
				DisplayName: delegatePartnerWebhookApprovalReply.GetPartnerWebhook().GetSubject().GetDisplayName(),
				Email:       req.GetSubscriber().GetEmail(),
			},
			Lines: []*partnerapiv1.PartnerOrderLine{&partnerapiv1.PartnerOrderLine{
				Attributes: delegatePartnerWebhookApprovalReply.GetPartnerWebhook().GetAttributes(),
			}},
			BillingAddress: &commonv1.PostalAddress{
				Recipient: submitCheckoutSessionReply.GetCheckoutSession().GetBillingAddress().GetRecipient(),
				Lines:     submitCheckoutSessionReply.GetCheckoutSession().GetBillingAddress().GetLines(),
				Region:    req.GetFilter().GetRegion(),
			},
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}); err != nil {
		return err
	}
	return nil
}

// partnerOrderNotificationServerNotifyPartnerOrderWatchersDeletePartnerOrderReportStep carries the request of one step
// of NotifyPartnerOrderWatchers and, once it ran, its reply.
type partnerOrderNotificationServerNotifyPartnerOrderWatchersDeletePartnerOrderReportStep struct {
	request *partnerapiv1.DeletePartnerOrderReportRequest
	reply   *partnerapiv1.DeletePartnerOrderReportResponse
}

func (s *PartnerOrderNotificationServer) runNotifyPartnerOrderWatchersDeletePartnerOrderReport(ctx context.Context, step *partnerOrderNotificationServerNotifyPartnerOrderWatchersDeletePartnerOrderReportStep) error {
	reply, err := s.partnerOrderReportingClient.DeletePartnerOrderReport(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}

// partnerOrderNotificationServerListPartnerOrderSubscriptionsUnsubscribePartnerIntegrationUpdatesStep carries the
// request of one step of ListPartnerOrderSubscriptions and, once it ran, its reply.
type partnerOrderNotificationServerListPartnerOrderSubscriptionsUnsubscribePartnerIntegrationUpdatesStep struct {
	request *partnerapiv1.UnsubscribePartnerIntegrationUpdatesRequest
	reply   *partnerapiv1.UnsubscribePartnerIntegrationUpdatesResponse
}

func (s *PartnerOrderNotificationServer) runListPartnerOrderSubscriptionsUnsubscribePartnerIntegrationUpdates(ctx context.Context, step *partnerOrderNotificationServerListPartnerOrderSubscriptionsUnsubscribePartnerIntegrationUpdatesStep) error {
	reply, err := s.partnerIntegrationNotificationClient.UnsubscribePartnerIntegrationUpdates(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
