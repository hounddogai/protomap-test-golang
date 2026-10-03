package partnerapi

import (
	"context"
	"fmt"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	partnerapiv1 "github.com/hounddogai/protomap-test-golang/gen/acme/partnerapi/v1"
	pricingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/pricing/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
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

// PartnerIntegrationWorkflowServer implements `acme.partnerapi.v1.PartnerIntegrationWorkflowService`.
// Runs the long-lived workflow that takes a partner integration from start to finish.
type PartnerIntegrationWorkflowServer struct {
	partnerapiv1.UnimplementedPartnerIntegrationWorkflowServiceServer
	store                               *Store
	log                                 *slog.Logger
	conn                                *grpc.ClientConn
	searchQueryValidationClient         searchv1.SearchQueryValidationServiceClient
	partnerIntegrationReportingClient   partnerapiv1.PartnerIntegrationReportingServiceClient
	shipmentWorkflowClient              shippingv1.ShipmentWorkflowServiceClient
	meteredPartnerWebhookValidation     meteredPartnerWebhookValidation
	priceListWorkflowClient             pricingv1.PriceListWorkflowServiceClient
	sellerPayoutScheduleLifecycleClient sellersv1.SellerPayoutScheduleLifecycleServiceClient
	partnerIntegrationReportingPort     partnerIntegrationReportingPort
	searchQueryValidationClientFallback searchv1.SearchQueryValidationServiceClient
	clients                             *registry.Registry
	partnerWebhookValidationClient      partnerapiv1.PartnerWebhookValidationServiceClient
	apiCredentialHistoryClient          identityv1.ApiCredentialHistoryServiceClient
	subscriptionPlanApprovalClient      subscriptionsv1.SubscriptionPlanApprovalServiceClient
}

// NewPartnerIntegrationWorkflowServer wires a PartnerIntegrationWorkflowServer.
func NewPartnerIntegrationWorkflowServer(deps platform.Deps) *PartnerIntegrationWorkflowServer {
	return &PartnerIntegrationWorkflowServer{
		store:                               NewStore(deps.DB),
		log:                                 deps.Logger.With("service", "acme.partnerapi.v1.PartnerIntegrationWorkflowService"),
		conn:                                deps.Conn,
		searchQueryValidationClient:         searchv1.NewSearchQueryValidationServiceClient(deps.Conn),
		partnerIntegrationReportingClient:   partnerapiv1.NewPartnerIntegrationReportingServiceClient(deps.Conn),
		shipmentWorkflowClient:              shippingv1.NewShipmentWorkflowServiceClient(deps.Conn),
		meteredPartnerWebhookValidation:     meteredPartnerWebhookValidation{PartnerWebhookValidationServiceClient: partnerapiv1.NewPartnerWebhookValidationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		priceListWorkflowClient:             pricingv1.NewPriceListWorkflowServiceClient(deps.Conn),
		sellerPayoutScheduleLifecycleClient: sellersv1.NewSellerPayoutScheduleLifecycleServiceClient(deps.Conn),
		partnerIntegrationReportingPort:     newPartnerIntegrationReportingPort(deps),
		searchQueryValidationClientFallback: searchv1.NewSearchQueryValidationServiceClient(deps.Conn),
		clients:                             deps.Clients,
		partnerWebhookValidationClient:      partnerapiv1.NewPartnerWebhookValidationServiceClient(deps.Conn),
		apiCredentialHistoryClient:          identityv1.NewApiCredentialHistoryServiceClient(deps.Conn),
		subscriptionPlanApprovalClient:      subscriptionsv1.NewSubscriptionPlanApprovalServiceClient(deps.Conn),
	}
}

// StartPartnerIntegrationWorkflow starts a partner integration.
func (s *PartnerIntegrationWorkflowServer) StartPartnerIntegrationWorkflow(ctx context.Context, req *partnerapiv1.StartPartnerIntegrationWorkflowRequest) (*partnerapiv1.StartPartnerIntegrationWorkflowResponse, error) {
	if err := s.store.SavePartnerIntegration(ctx, req.GetContext().GetTenantId(), req.GetPartnerIntegration()); err != nil {
		return nil, grpcx.Wrap(err, "save partner integration")
	}
	s.log.InfoContext(ctx, "start partner integration workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPartnerIntegration().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetPartnerIntegration()).(*partnerapiv1.PartnerIntegration)
	rejectPartnerOrderRequest := &partnerapiv1.RejectPartnerOrderRequest{
		Context: req.GetContext(),
		PartnerOrder: &partnerapiv1.PartnerOrder{
			Settlement: &partnerapiv1.PartnerOrder_Bank{Bank: &partnerapiv1.PartnerOrderBankSettlement{
				Iban:          req.GetPartnerIntegration().GetBank().GetIban(),
				AccountHolder: req.GetPartnerIntegration().GetBank().GetAccountHolder(),
			}},
			Counterparty: &sellersv1.SellerRef{Email: req.GetPartnerIntegration().GetContact().GetEmail()},
			Audit:        &commonv1.AuditInfo{UpdatedBy: cloned.GetAudit().GetUpdatedBy()},
		},
	}
	rejectPartnerOrderReply := new(partnerapiv1.RejectPartnerOrderResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", partnerapiPartnerOrderApprovalServiceName, "RejectPartnerOrder"), rejectPartnerOrderRequest, rejectPartnerOrderReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerOrderApprovalService/RejectPartnerOrder")
	}
	detailsCache := cache.New[string, *partnerapiv1.StartPartnerIntegrationWorkflowRequest_Details](1)
	detailsCache.Put(req.GetContext().GetTraceId(), req.GetDetails())
	cachedDetails, _ := detailsCache.Get(req.GetContext().GetTraceId())
	packed, err := anypb.New(req.GetPartnerIntegration())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	explainSearchQueryDecisionRequest := &searchv1.ExplainSearchQueryDecisionRequest{
		Context: req.GetContext(),
		Details: &searchv1.ExplainSearchQueryDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{
				Phone: cachedDetails.GetNotify().GetPhone(),
				Email: rejectPartnerOrderReply.GetPartnerOrder().GetCounterparty().GetEmail(),
				Name:  req.GetDetails().GetNotify().GetName(),
			},
		},
		SearchQuery: &searchv1.SearchQuery{Detail: packed},
	}
	explainSearchQueryDecisionReply, err := grpcx.Call(ctx, s.searchQueryValidationClient.ExplainSearchQueryDecision, explainSearchQueryDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryValidationService/ExplainSearchQueryDecision")
	}
	schedulePartnerIntegrationReportRequest := &partnerapiv1.SchedulePartnerIntegrationReportRequest{
		Context: req.GetContext(),
		Reason:  req.GetReason(),
	}
	schedulePartnerIntegrationReportReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*partnerapiv1.SchedulePartnerIntegrationReportResponse, error) {
		return s.partnerIntegrationReportingClient.SchedulePartnerIntegrationReport(ctx, schedulePartnerIntegrationReportRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerIntegrationReportingService/SchedulePartnerIntegrationReport")
	}
	for limit := range iterx.Each(req.GetPartnerIntegration().GetLimits(), func(candidate *partnerapiv1.PartnerIntegrationLimit) bool { return candidate != nil }) {
		if _, err := s.shipmentWorkflowClient.CancelShipmentWorkflow(ctx, &shippingv1.CancelShipmentWorkflowRequest{
			Context: req.GetContext(),
			Shipment: &shippingv1.Shipment{
				Lines: []*shippingv1.ShipmentLine{&shippingv1.ShipmentLine{Tax: &shippingv1.ShipmentLineTax{Amount: limit.GetAmount()}}},
				Settlement: &shippingv1.Shipment_Card{Card: &shippingv1.ShipmentCardSettlement{
					Network: rejectPartnerOrderReply.GetPartnerOrder().GetCard().GetNetwork(),
				}},
			},
			Details: &shippingv1.CancelShipmentWorkflowRequest_Details{
				Notify: &commonv1.ContactInfo{
					PreferredChannel: req.GetDetails().GetNotify().GetPreferredChannel(),
				},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.shipping.v1.ShipmentWorkflowService/CancelShipmentWorkflow")
		}
	}
	return &partnerapiv1.StartPartnerIntegrationWorkflowResponse{
		Result: &commonv1.OperationResult{
			Status:  rejectPartnerOrderReply.GetOutcome().GetStatus(),
			Details: explainSearchQueryDecisionReply.GetResult().GetDetails(),
		},
		PartnerIntegration: &partnerapiv1.PartnerIntegration{
			Audit: &commonv1.AuditInfo{
				Etag: rejectPartnerOrderReply.GetPartnerOrder().GetAudit().GetEtag(),
			},
			PartnerIntegrationId: req.GetPartnerIntegrationId(),
			OpenedAt:             req.GetPartnerIntegration().GetOpenedAt(),
		},
		Outcome: &partnerapiv1.StartPartnerIntegrationWorkflowResponse_Outcome{
			Message: schedulePartnerIntegrationReportReply.GetResult().GetMessage(),
		},
	}, nil
}

// AdvancePartnerIntegrationWorkflow advances a partner integration.
func (s *PartnerIntegrationWorkflowServer) AdvancePartnerIntegrationWorkflow(ctx context.Context, req *partnerapiv1.AdvancePartnerIntegrationWorkflowRequest) (*partnerapiv1.AdvancePartnerIntegrationWorkflowResponse, error) {
	if err := s.store.SavePartnerIntegration(ctx, req.GetContext().GetTenantId(), req.GetPartnerIntegration()); err != nil {
		return nil, grpcx.Wrap(err, "save partner integration")
	}
	s.log.InfoContext(ctx, "advance partner integration workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPartnerIntegration().GetAudit().GetEtag())
	packed, err := anypb.New(req.GetPartnerIntegration())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	explainPartnerWebhookDecisionRequest := &partnerapiv1.ExplainPartnerWebhookDecisionRequest{
		Context: req.GetContext(),
		Details: &partnerapiv1.ExplainPartnerWebhookDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{
				Name:             req.GetDetails().GetNotify().GetName(),
				PreferredChannel: req.GetDetails().GetNotify().GetPreferredChannel(),
			},
		},
		PartnerWebhook: &partnerapiv1.PartnerWebhook{Detail: packed},
	}
	explainPartnerWebhookDecisionReply, err := s.meteredPartnerWebhookValidation.ExplainPartnerWebhookDecision(ctx, explainPartnerWebhookDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookValidationService/ExplainPartnerWebhookDecision")
	}
	startPriceListWorkflowRequest := &pricingv1.StartPriceListWorkflowRequest{
		Context: req.GetContext(),
		PriceList: &pricingv1.PriceList{
			Audit: &commonv1.AuditInfo{
				CreatedAt: req.GetPartnerIntegration().GetAudit().GetCreatedAt(),
			},
		},
	}
	startPriceListWorkflowReply, err := grpcx.Call(ctx, s.priceListWorkflowClient.StartPriceListWorkflow, startPriceListWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceListWorkflowService/StartPriceListWorkflow")
	}
	return &partnerapiv1.AdvancePartnerIntegrationWorkflowResponse{
		PartnerIntegration: &partnerapiv1.PartnerIntegration{
			CurrencyCode:         explainPartnerWebhookDecisionReply.GetPartnerWebhook().GetSubject().GetCurrencyCode(),
			PartnerIntegrationId: req.GetPartnerIntegrationId(),
			OpenedAt:             req.GetPartnerIntegration().GetOpenedAt(),
		},
		Outcome: &partnerapiv1.AdvancePartnerIntegrationWorkflowResponse_Outcome{
			Status: startPriceListWorkflowReply.GetOutcome().GetStatus(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CancelPartnerIntegrationWorkflow cancels a partner integration.
func (s *PartnerIntegrationWorkflowServer) CancelPartnerIntegrationWorkflow(ctx context.Context, req *partnerapiv1.CancelPartnerIntegrationWorkflowRequest) (*partnerapiv1.CancelPartnerIntegrationWorkflowResponse, error) {
	if err := s.store.SavePartnerIntegration(ctx, req.GetContext().GetTenantId(), req.GetPartnerIntegration()); err != nil {
		return nil, grpcx.Wrap(err, "save partner integration")
	}
	s.log.InfoContext(ctx, "cancel partner integration workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPartnerIntegration().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetPartnerIntegration()).(*partnerapiv1.PartnerIntegration)
	scheduleSellerPayoutScheduleExpiryRequest := &sellersv1.ScheduleSellerPayoutScheduleExpiryRequest{
		Context: req.GetContext(),
		Details: &sellersv1.ScheduleSellerPayoutScheduleExpiryRequest_Details{
			Notify: &commonv1.ContactInfo{Email: cloned.GetContact().GetEmail()},
		},
	}
	scheduleSellerPayoutScheduleExpiryReply, err := grpcx.Call(ctx, s.sellerPayoutScheduleLifecycleClient.ScheduleSellerPayoutScheduleExpiry, scheduleSellerPayoutScheduleExpiryRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerPayoutScheduleLifecycleService/ScheduleSellerPayoutScheduleExpiry")
	}
	deletePartnerIntegrationReportRequest := &partnerapiv1.DeletePartnerIntegrationReportRequest{
		Context:              req.GetContext(),
		PartnerIntegrationId: req.GetPartnerIntegrationId(),
		Etag:                 scheduleSellerPayoutScheduleExpiryReply.GetSellerPayoutSchedule().GetAudit().GetEtag(),
	}
	deletePartnerIntegrationReportReply, err := s.partnerIntegrationReportingPort.DeletePartnerIntegrationReport(ctx, deletePartnerIntegrationReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerIntegrationReportingService/DeletePartnerIntegrationReport")
	}
	return &partnerapiv1.CancelPartnerIntegrationWorkflowResponse{
		Result: &commonv1.OperationResult{
			Status:  scheduleSellerPayoutScheduleExpiryReply.GetOutcome().GetStatus(),
			Details: deletePartnerIntegrationReportReply.GetResult().GetDetails(),
		},
		PartnerIntegration: &partnerapiv1.PartnerIntegration{
			PartnerIntegrationId: req.GetPartnerIntegrationId(),
			Available:            req.GetPartnerIntegration().GetAvailable(),
		},
	}, nil
}

// RetryPartnerIntegrationWorkflow retrys a partner integration.
func (s *PartnerIntegrationWorkflowServer) RetryPartnerIntegrationWorkflow(ctx context.Context, req *partnerapiv1.RetryPartnerIntegrationWorkflowRequest) (*partnerapiv1.RetryPartnerIntegrationWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SavePartnerIntegration(ctx, req.GetContext().GetTenantId(), req.GetPartnerIntegration()); err != nil {
		return nil, grpcx.Wrap(err, "save partner integration")
	}
	s.log.InfoContext(ctx, "retry partner integration workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPartnerIntegration().GetAudit().GetEtag())
	routes := map[string]func(context.Context, *searchv1.LintSearchQueriesRequest, ...grpc.CallOption) (*searchv1.LintSearchQueriesResponse, error){
		"primary":  s.searchQueryValidationClient.LintSearchQueries,
		"fallback": s.searchQueryValidationClientFallback.LintSearchQueries,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	encoded, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded partnerapiv1.RetryPartnerIntegrationWorkflowRequest_Details
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	packed, err := anypb.New(req.GetPartnerIntegration())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	lintSearchQueriesRequest := &searchv1.LintSearchQueriesRequest{
		Context: req.GetContext(),
		Details: &searchv1.LintSearchQueriesRequest_Details{
			Contact: &commonv1.ContactInfo{
				Phone:            (&decoded).GetNotify().GetPhone(),
				PreferredChannel: req.GetDetails().GetNotify().GetPreferredChannel(),
			},
		},
		SearchQuery: &searchv1.SearchQuery{Detail: packed},
	}
	lintSearchQueriesReply, err := route(ctx, lintSearchQueriesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryValidationService/LintSearchQueries")
	}
	cloned := proto.Clone(req.GetPartnerIntegration()).(*partnerapiv1.PartnerIntegration)
	previewPartnerWebhookRequest := &partnerapiv1.PreviewPartnerWebhookRequest{
		Context: req.GetContext(),
		Details: &partnerapiv1.PreviewPartnerWebhookRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email:            cloned.GetContact().GetEmail(),
				Phone:            req.GetDetails().GetNotify().GetPhone(),
				PreferredChannel: req.GetDetails().GetNotify().GetPreferredChannel(),
			},
		},
	}
	previewPartnerWebhookReply := new(partnerapiv1.PreviewPartnerWebhookResponse)
	if err := s.conn.Invoke(ctx, partnerapiPartnerWebhookValidationServicePreviewPartnerWebhookMethod, previewPartnerWebhookRequest, previewPartnerWebhookReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookValidationService/PreviewPartnerWebhook")
	}
	apiCredentialNotification, err := registry.Lookup[identityv1.ApiCredentialNotificationServiceClient](s.clients, "acme.identity.v1.ApiCredentialNotificationService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	detailsCache := cache.New[string, *partnerapiv1.RetryPartnerIntegrationWorkflowRequest_Details](1)
	detailsCache.Put(req.GetContext().GetTraceId(), req.GetDetails())
	cachedDetails, _ := detailsCache.Get(req.GetContext().GetTraceId())
	unsubscribeApiCredentialUpdatesRequest := &identityv1.UnsubscribeApiCredentialUpdatesRequest{
		Context: req.GetContext(),
		Details: &identityv1.UnsubscribeApiCredentialUpdatesRequest_Details{
			Recipient: &commonv1.ContactInfo{
				Name:  cachedDetails.GetNotify().GetName(),
				Phone: req.GetDetails().GetNotify().GetPhone(),
			},
		},
	}
	unsubscribeApiCredentialUpdatesReply, err := apiCredentialNotification.UnsubscribeApiCredentialUpdates(ctx, unsubscribeApiCredentialUpdatesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.ApiCredentialNotificationService/UnsubscribeApiCredentialUpdates")
	}
	packed2, err := anypb.New(req.GetPartnerIntegration())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	explainPartnerWebhookDecisionRequest := &partnerapiv1.ExplainPartnerWebhookDecisionRequest{
		Context: req.GetContext(),
		Details: &partnerapiv1.ExplainPartnerWebhookDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email:            req.GetPartnerIntegration().GetContact().GetEmail(),
				Name:             unsubscribeApiCredentialUpdatesReply.GetApiCredential().GetContact().GetName(),
				PreferredChannel: unsubscribeApiCredentialUpdatesReply.GetApiCredential().GetContact().GetPreferredChannel(),
			},
		},
		PartnerWebhook: &partnerapiv1.PartnerWebhook{Detail: packed2},
	}
	explainPartnerWebhookDecisionReply := new(partnerapiv1.ExplainPartnerWebhookDecisionResponse)
	if err := grpcx.Invoke(ctx, s.conn, partnerapiv1.PartnerWebhookValidationService_ExplainPartnerWebhookDecision_FullMethodName, explainPartnerWebhookDecisionRequest, explainPartnerWebhookDecisionReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookValidationService/ExplainPartnerWebhookDecision")
	}
	return &partnerapiv1.RetryPartnerIntegrationWorkflowResponse{
		Outcome: &partnerapiv1.RetryPartnerIntegrationWorkflowResponse_Outcome{
			NextSteps: lintSearchQueriesReply.GetOutcome().GetNextSteps(),
		},
		PartnerIntegration: &partnerapiv1.PartnerIntegration{
			Holder: &partnerapiv1.PartnerWebhookRef{
				Kind:             previewPartnerWebhookReply.GetPartnerWebhook().GetKind(),
				PartnerWebhookId: explainPartnerWebhookDecisionReply.GetPartnerWebhook().GetPartnerWebhookId(),
			},
			PartnerIntegrationId: req.GetPartnerIntegrationId(),
			Available:            req.GetPartnerIntegration().GetAvailable(),
		},
		Result: &commonv1.OperationResult{
			Status: unsubscribeApiCredentialUpdatesReply.GetOutcome().GetStatus(),
		},
	}, nil
}

// PausePartnerIntegrationWorkflow pauses a partner integration.
func (s *PartnerIntegrationWorkflowServer) PausePartnerIntegrationWorkflow(ctx context.Context, req *partnerapiv1.PausePartnerIntegrationWorkflowRequest) (*partnerapiv1.PausePartnerIntegrationWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected PausePartnerIntegrationWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SavePartnerIntegration(ctx, req.GetContext().GetTenantId(), req.GetPartnerIntegration()); err != nil {
		return nil, grpcx.Wrap(err, "save partner integration")
	}
	s.log.InfoContext(ctx, "pause partner integration workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPartnerIntegration().GetAudit().GetEtag())
	sharePartnerIntegrationReportRequest := buildSharePartnerIntegrationReportRequestForPausePartnerIntegrationWorkflow(req)
	sharePartnerIntegrationReportReply, err := s.partnerIntegrationReportingClient.SharePartnerIntegrationReport(ctx, sharePartnerIntegrationReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerIntegrationReportingService/SharePartnerIntegrationReport")
	}
	for _, limit := range req.GetPartnerIntegration().GetLimits() {
		if _, err := s.shipmentWorkflowClient.StartShipmentWorkflow(ctx, &shippingv1.StartShipmentWorkflowRequest{
			Context: req.GetContext(),
			Shipment: &shippingv1.Shipment{
				Lines: []*shippingv1.ShipmentLine{&shippingv1.ShipmentLine{Tax: &shippingv1.ShipmentLineTax{Amount: limit.GetAmount()}}},
				Settlement: &shippingv1.Shipment_Card{Card: &shippingv1.ShipmentCardSettlement{
					Network: sharePartnerIntegrationReportReply.GetPartnerIntegration().GetCard().GetNetwork(),
				}},
			},
			Details: &shippingv1.StartShipmentWorkflowRequest_Details{
				Notify: &commonv1.ContactInfo{
					Name: sharePartnerIntegrationReportReply.GetPartnerIntegration().GetContact().GetName(),
				},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.shipping.v1.ShipmentWorkflowService/StartShipmentWorkflow")
		}
	}
	ctx = tenancy.With(ctx, "reason", req.GetReason())
	deletePartnerIntegrationReportReply, err := s.partnerIntegrationReportingClient.DeletePartnerIntegrationReport(ctx, &partnerapiv1.DeletePartnerIntegrationReportRequest{
		Context:              req.GetContext(),
		PartnerIntegrationId: req.GetPartnerIntegration().GetPartnerIntegrationId(),
		Reason:               tenancy.Value(ctx, "reason"),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerIntegrationReportingService/DeletePartnerIntegrationReport")
	}
	fetchPartnerIntegrationReportRequest := &partnerapiv1.FetchPartnerIntegrationReportRequest{
		Context:              req.GetContext(),
		PartnerIntegrationId: req.GetPartnerIntegration().GetPartnerIntegrationId(),
	}
	var fetchPartnerIntegrationReportReply *partnerapiv1.FetchPartnerIntegrationReportResponse
	if err := pipeline.Run(ctx, "acme.partnerapi.v1.PartnerIntegrationReportingService/FetchPartnerIntegrationReport", func(ctx context.Context) error {
		answer, err := s.partnerIntegrationReportingClient.FetchPartnerIntegrationReport(ctx, fetchPartnerIntegrationReportRequest)
		fetchPartnerIntegrationReportReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerIntegrationReportingService/FetchPartnerIntegrationReport")
	}
	return &partnerapiv1.PausePartnerIntegrationWorkflowResponse{
		PartnerIntegration: &partnerapiv1.PartnerIntegration{
			Contact: &commonv1.ContactInfo{
				Email: sharePartnerIntegrationReportReply.GetPartnerIntegration().GetContact().GetEmail(),
			},
			Status:               fetchPartnerIntegrationReportReply.GetPartnerIntegration().GetStatus(),
			PartnerIntegrationId: req.GetPartnerIntegrationId(),
			Balance:              req.GetPartnerIntegration().GetBalance(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: deletePartnerIntegrationReportReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Outcome: &partnerapiv1.PausePartnerIntegrationWorkflowResponse_Outcome{
			EffectiveAt: deletePartnerIntegrationReportReply.GetEffectiveAt(),
		},
	}, nil
}

// ResumePartnerIntegrationWorkflow resumes a partner integration.
func (s *PartnerIntegrationWorkflowServer) ResumePartnerIntegrationWorkflow(ctx context.Context, req *partnerapiv1.ResumePartnerIntegrationWorkflowRequest) (*partnerapiv1.ResumePartnerIntegrationWorkflowResponse, error) {
	if err := s.store.SavePartnerIntegration(ctx, req.GetContext().GetTenantId(), req.GetPartnerIntegration()); err != nil {
		return nil, grpcx.Wrap(err, "save partner integration")
	}
	s.log.InfoContext(ctx, "resume partner integration workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPartnerIntegration().GetAudit().GetEtag())
	partnerIntegrationCache := cache.New[string, *partnerapiv1.PartnerIntegration](1)
	partnerIntegrationCache.Put(req.GetContext().GetTraceId(), req.GetPartnerIntegration())
	cachedPartnerIntegration, _ := partnerIntegrationCache.Get(req.GetContext().GetTraceId())
	packed, err := anypb.New(req.GetPartnerIntegration())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	explainPartnerWebhookDecisionReply, err := s.partnerWebhookValidationClient.ExplainPartnerWebhookDecision(ctx, &partnerapiv1.ExplainPartnerWebhookDecisionRequest{
		Context: req.GetContext(),
		Details: &partnerapiv1.ExplainPartnerWebhookDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email: req.GetDetails().GetNotify().GetEmail(),
				Phone: cachedPartnerIntegration.GetContact().GetPhone(),
			},
		},
		PartnerWebhook: &partnerapiv1.PartnerWebhook{Detail: packed},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookValidationService/ExplainPartnerWebhookDecision")
	}
	for _, limit := range req.GetPartnerIntegration().GetLimits() {
		if _, err := s.apiCredentialHistoryClient.AnnotateApiCredentialVersion(ctx, &identityv1.AnnotateApiCredentialVersionRequest{
			Context: req.GetContext(),
			ApiCredential: &identityv1.ApiCredential{
				Limits: []*identityv1.ApiCredentialLimit{&identityv1.ApiCredentialLimit{
					Amount: limit.GetAmount(),
					Period: limit.GetPeriod(),
					Name:   limit.GetName(),
				}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.identity.v1.ApiCredentialHistoryService/AnnotateApiCredentialVersion")
		}
	}
	return &partnerapiv1.ResumePartnerIntegrationWorkflowResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: explainPartnerWebhookDecisionReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Outcome: &partnerapiv1.ResumePartnerIntegrationWorkflowResponse_Outcome{
			Status: explainPartnerWebhookDecisionReply.GetResult().GetStatus(),
		},
		PartnerIntegration: &partnerapiv1.PartnerIntegration{
			PartnerIntegrationId: req.GetPartnerIntegrationId(),
			Holder:               req.GetPartnerIntegration().GetHolder(),
		},
	}, nil
}

// InspectPartnerIntegrationWorkflow returns a partner integration.
func (s *PartnerIntegrationWorkflowServer) InspectPartnerIntegrationWorkflow(ctx context.Context, req *partnerapiv1.InspectPartnerIntegrationWorkflowRequest) (*partnerapiv1.InspectPartnerIntegrationWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "partner_integration", req.GetPartnerIntegrationId(), "inspect_partner_integration_workflow"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "inspect partner integration workflow", "tenant", req.GetContext().GetTenantId())
	return &partnerapiv1.InspectPartnerIntegrationWorkflowResponse{
		PartnerIntegration: &partnerapiv1.PartnerIntegration{PartnerIntegrationId: req.GetPartnerIntegrationId()},
		Result:             &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ListPartnerIntegrationWorkflows lists partner integrations a page at a time.
func (s *PartnerIntegrationWorkflowServer) ListPartnerIntegrationWorkflows(ctx context.Context, req *partnerapiv1.ListPartnerIntegrationWorkflowsRequest) (*partnerapiv1.ListPartnerIntegrationWorkflowsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "list partner integration workflows", "tenant", req.GetContext().GetTenantId())
	return &partnerapiv1.ListPartnerIntegrationWorkflowsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// SignalPartnerIntegrationWorkflow signals a partner integration.
func (s *PartnerIntegrationWorkflowServer) SignalPartnerIntegrationWorkflow(ctx context.Context, req *partnerapiv1.SignalPartnerIntegrationWorkflowRequest) (*partnerapiv1.SignalPartnerIntegrationWorkflowResponse, error) {
	if err := s.store.SavePartnerIntegration(ctx, req.GetContext().GetTenantId(), req.GetPartnerIntegration()); err != nil {
		return nil, grpcx.Wrap(err, "save partner integration")
	}
	s.log.InfoContext(ctx, "signal partner integration workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPartnerIntegration().GetAudit().GetEtag())
	getPartnerWebhookRulesRequest := newGetPartnerWebhookRulesRequest(withGetPartnerWebhookRulesContext(req.GetContext()), withGetPartnerWebhookRulesPartnerWebhookId(req.GetPartnerIntegration().GetHolder().GetPartnerWebhookId()))
	getPartnerWebhookRulesReply, err := s.partnerWebhookValidationClient.GetPartnerWebhookRules(ctx, getPartnerWebhookRulesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookValidationService/GetPartnerWebhookRules")
	}
	approveSubscriptionPlanRequest := newApproveSubscriptionPlanRequest(withApproveSubscriptionPlanContext(req.GetContext()), withApproveSubscriptionPlanDetails(&subscriptionsv1.ApproveSubscriptionPlanRequest_Details{
		Approver: &identityv1.UserRef{
			DisplayName: getPartnerWebhookRulesReply.GetPartnerWebhook().GetSubject().GetDisplayName(),
			Email:       req.GetPartnerIntegration().GetContact().GetEmail(),
		},
	}))
	approveSubscriptionPlanReply, err := s.subscriptionPlanApprovalClient.ApproveSubscriptionPlan(ctx, approveSubscriptionPlanRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionPlanApprovalService/ApproveSubscriptionPlan")
	}
	cancelPriceListWorkflowRequest := &pricingv1.CancelPriceListWorkflowRequest{
		Context: req.GetContext(),
		PriceList: &pricingv1.PriceList{
			Action: &pricingv1.PriceList_Route{Route: &pricingv1.PriceListRouteAction{
				Target: approveSubscriptionPlanReply.GetSubscriptionPlan().GetRoute().GetTarget(),
			}},
		},
	}
	type cancelPriceListWorkflowResult struct {
		reply *pricingv1.CancelPriceListWorkflowResponse
		err   error
	}
	cancelPriceListWorkflowResults := make(chan cancelPriceListWorkflowResult, 1)
	go func() {
		reply, err := s.priceListWorkflowClient.CancelPriceListWorkflow(ctx, cancelPriceListWorkflowRequest)
		cancelPriceListWorkflowResults <- cancelPriceListWorkflowResult{reply, err}
	}()
	var cancelPriceListWorkflowReply *pricingv1.CancelPriceListWorkflowResponse
	select {
	case result := <-cancelPriceListWorkflowResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.pricing.v1.PriceListWorkflowService/CancelPriceListWorkflow")
		}
		cancelPriceListWorkflowReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &partnerapiv1.SignalPartnerIntegrationWorkflowResponse{
		PartnerIntegration: &partnerapiv1.PartnerIntegration{
			PartnerIntegrationId: getPartnerWebhookRulesReply.GetPartnerWebhook().GetSubject().GetPartnerIntegrationId(),
			Available:            req.GetPartnerIntegration().GetAvailable(),
			Balance:              req.GetPartnerIntegration().GetBalance(),
		},
		Outcome: &partnerapiv1.SignalPartnerIntegrationWorkflowResponse_Outcome{
			EffectiveAt: approveSubscriptionPlanReply.GetOutcome().GetEffectiveAt(),
			Status:      approveSubscriptionPlanReply.GetResult().GetStatus(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: cancelPriceListWorkflowReply.GetResult().GetReferenceIds(),
			Status:       cancelPriceListWorkflowReply.GetResult().GetStatus(),
		},
	}, nil
}
