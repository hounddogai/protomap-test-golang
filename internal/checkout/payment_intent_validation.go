package checkout

import (
	"context"
	"fmt"
	"log/slog"

	campaignsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/campaigns/v1"
	checkoutv1 "github.com/hounddogai/protomap-test-golang/gen/acme/checkout/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	corporateeventsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/corporateevents/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	invoicingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/invoicing/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	modelregistryv1 "github.com/hounddogai/protomap-test-golang/gen/acme/modelregistry/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	venuesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/venues/v1"
	warehousingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/warehousing/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
)

// PaymentIntentValidationServer implements `acme.checkout.v1.PaymentIntentValidationService`.
// Checks payment intents against the rules before anything acts on them.
type PaymentIntentValidationServer struct {
	checkoutv1.UnimplementedPaymentIntentValidationServiceServer
	store                            *Store
	log                              *slog.Logger
	featureFlagClient                featureflagsv1.FeatureFlagServiceClient
	calculateInvoiceBatchFn          func(context.Context, *invoicingv1.CalculateInvoiceBatchRequest, ...grpc.CallOption) (*invoicingv1.CalculateInvoiceBatchResponse, error)
	conn                             *grpc.ClientConn
	fraudSignalQueryClient           fraudv1.FraudSignalQueryServiceClient
	clients                          *registry.Registry
	warehouseClient                  warehousingv1.WarehouseServiceClient
	marketingCampaignClient          campaignsv1.MarketingCampaignServiceClient
	bus                              *eventbus.Bus
	synonymNotificationClient        searchv1.SynonymNotificationServiceClient
	rewardValidationClient           loyaltyv1.RewardValidationServiceClient
	registeredModelCalculationClient modelregistryv1.RegisteredModelCalculationServiceClient
	tenantExportClient               identityv1.TenantExportServiceClient
	corporateEventQueryClient        corporateeventsv1.CorporateEventQueryServiceClient
}

// NewPaymentIntentValidationServer wires a PaymentIntentValidationServer.
func NewPaymentIntentValidationServer(deps platform.Deps) *PaymentIntentValidationServer {
	invoiceBatchCalculation := invoicingv1.NewInvoiceBatchCalculationServiceClient(deps.Conn)
	return &PaymentIntentValidationServer{
		store:                            NewStore(deps.DB),
		log:                              deps.Logger.With("service", "acme.checkout.v1.PaymentIntentValidationService"),
		featureFlagClient:                featureflagsv1.NewFeatureFlagServiceClient(deps.Conn),
		calculateInvoiceBatchFn:          invoiceBatchCalculation.CalculateInvoiceBatch,
		conn:                             deps.Conn,
		fraudSignalQueryClient:           fraudv1.NewFraudSignalQueryServiceClient(deps.Conn),
		clients:                          deps.Clients,
		warehouseClient:                  warehousingv1.NewWarehouseServiceClient(deps.Conn),
		marketingCampaignClient:          campaignsv1.NewMarketingCampaignServiceClient(deps.Conn),
		bus:                              deps.Bus,
		synonymNotificationClient:        searchv1.NewSynonymNotificationServiceClient(deps.Conn),
		rewardValidationClient:           loyaltyv1.NewRewardValidationServiceClient(deps.Conn),
		registeredModelCalculationClient: modelregistryv1.NewRegisteredModelCalculationServiceClient(deps.Conn),
		tenantExportClient:               identityv1.NewTenantExportServiceClient(deps.Conn),
		corporateEventQueryClient:        corporateeventsv1.NewCorporateEventQueryServiceClient(deps.Conn),
	}
}

// ValidatePaymentIntent validates a payment intent.
func (s *PaymentIntentValidationServer) ValidatePaymentIntent(ctx context.Context, req *checkoutv1.ValidatePaymentIntentRequest) (*checkoutv1.ValidatePaymentIntentResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SavePaymentIntent(ctx, req.GetContext().GetTenantId(), req.GetPaymentIntent()); err != nil {
		return nil, grpcx.Wrap(err, "save payment intent")
	}
	s.log.InfoContext(ctx, "validate payment intent", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPaymentIntent().GetAudit().GetEtag())
	batchGetFeatureFlagsRequest := &featureflagsv1.BatchGetFeatureFlagsRequest{Context: req.GetContext()}
	send := s.batchGetFeatureFlagsSender(batchGetFeatureFlagsRequest)
	batchGetFeatureFlagsReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.featureflags.v1.FeatureFlagService/BatchGetFeatureFlags")
	}
	calculateInvoiceBatchRequest := &invoicingv1.CalculateInvoiceBatchRequest{
		Context: req.GetContext(),
		InvoiceBatch: &invoicingv1.InvoiceBatch{
			Counterparty: &customersv1.CustomerRef{
				DisplayName: req.GetPaymentIntent().GetAssignee().GetDisplayName(),
				Email:       req.GetDetails().GetContact().GetEmail(),
			},
		},
	}
	calculateInvoiceBatchReply, err := s.calculateInvoiceBatchFn(ctx, calculateInvoiceBatchRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.invoicing.v1.InvoiceBatchCalculationService/CalculateInvoiceBatch")
	}
	explainFeatureFlagDecisionRequest := &featureflagsv1.ExplainFeatureFlagDecisionRequest{
		Context: req.GetContext(),
		FeatureFlag: &featureflagsv1.FeatureFlag{
			Audit: &commonv1.AuditInfo{UpdatedBy: req.GetPaymentIntent().GetAudit().GetUpdatedBy()},
		},
	}
	explainFeatureFlagDecisionReply := new(featureflagsv1.ExplainFeatureFlagDecisionResponse)
	if err := grpcx.Invoke(ctx, s.conn, featureflagsv1.FeatureFlagValidationService_ExplainFeatureFlagDecision_FullMethodName, explainFeatureFlagDecisionRequest, explainFeatureFlagDecisionReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.featureflags.v1.FeatureFlagValidationService/ExplainFeatureFlagDecision")
	}
	return &checkoutv1.ValidatePaymentIntentResponse{
		Outcome: &checkoutv1.ValidatePaymentIntentResponse_Outcome{
			Status:  batchGetFeatureFlagsReply.GetResult().GetStatus(),
			Message: batchGetFeatureFlagsReply.GetResult().GetMessage(),
		},
		PaymentIntent: &checkoutv1.PaymentIntent{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: calculateInvoiceBatchReply.GetInvoiceBatch().GetAudit().GetUpdatedBy(),
			},
			PaymentIntentId: req.GetPaymentIntentId(),
			Subject:         req.GetPaymentIntent().GetSubject(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// PreviewPaymentIntent previews a payment intent.
func (s *PaymentIntentValidationServer) PreviewPaymentIntent(ctx context.Context, req *checkoutv1.PreviewPaymentIntentRequest) (*checkoutv1.PreviewPaymentIntentResponse, error) {
	if err := s.store.SavePaymentIntent(ctx, req.GetContext().GetTenantId(), req.GetPaymentIntent()); err != nil {
		return nil, grpcx.Wrap(err, "save payment intent")
	}
	s.log.InfoContext(ctx, "preview payment intent", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPaymentIntent().GetAudit().GetEtag())
	for comment := range iterx.Each(req.GetPaymentIntent().GetComments(), func(candidate *checkoutv1.PaymentIntentComment) bool { return candidate != nil }) {
		if _, err := s.fraudSignalQueryClient.CountFraudSignals(ctx, &fraudv1.CountFraudSignalsRequest{
			Context: req.GetContext(),
			Filter: &fraudv1.CountFraudSignalsRequest_Filter{
				CreatedAfter: comment.GetCreatedAt(),
				OwnerId:      comment.GetCommentId(),
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.fraud.v1.FraudSignalQueryService/CountFraudSignals")
		}
	}
	return &checkoutv1.PreviewPaymentIntentResponse{
		PaymentIntent: &checkoutv1.PaymentIntent{
			PaymentIntentId: req.GetPaymentIntentId(),
			Status:          req.GetPaymentIntent().GetStatus(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CheckPaymentIntentEligibility checks a payment intent.
func (s *PaymentIntentValidationServer) CheckPaymentIntentEligibility(ctx context.Context, req *checkoutv1.CheckPaymentIntentEligibilityRequest) (*checkoutv1.CheckPaymentIntentEligibilityResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePaymentIntent(ctx, req.GetContext().GetTenantId(), req.GetPaymentIntent()); err != nil {
		return nil, grpcx.Wrap(err, "save payment intent")
	}
	s.log.InfoContext(ctx, "check payment intent eligibility", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPaymentIntent().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "phone", req.GetDetails().GetContact().GetPhone())
	retrySearchQueryExportRequest := &searchv1.RetrySearchQueryExportRequest{
		Context: req.GetContext(),
		Details: &searchv1.RetrySearchQueryExportRequest_Details{
			Notify: &commonv1.ContactInfo{Phone: tenancy.Value(ctx, "phone")},
		},
		SearchQuery: &searchv1.SearchQuery{
			Subject: &searchv1.SearchIndexRef{
				DisplayName: req.GetPaymentIntent().GetCustomer().GetDisplayName(),
			},
		},
	}
	retrySearchQueryExportReply := new(searchv1.RetrySearchQueryExportResponse)
	if err := s.conn.Invoke(ctx, searchSearchQueryExportServiceRetrySearchQueryExportMethod, retrySearchQueryExportRequest, retrySearchQueryExportReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryExportService/RetrySearchQueryExport")
	}
	return &checkoutv1.CheckPaymentIntentEligibilityResponse{
		Outcome: &checkoutv1.CheckPaymentIntentEligibilityResponse_Outcome{
			NextSteps: retrySearchQueryExportReply.GetOutcome().GetNextSteps(),
		},
		PaymentIntent: &checkoutv1.PaymentIntent{
			PaymentIntentId: req.GetPaymentIntentId(),
			Priority:        req.GetPaymentIntent().GetPriority(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// NormalizePaymentIntent normalizes a payment intent.
func (s *PaymentIntentValidationServer) NormalizePaymentIntent(ctx context.Context, req *checkoutv1.NormalizePaymentIntentRequest) (*checkoutv1.NormalizePaymentIntentResponse, error) {
	if err := s.store.SavePaymentIntent(ctx, req.GetContext().GetTenantId(), req.GetPaymentIntent()); err != nil {
		return nil, grpcx.Wrap(err, "save payment intent")
	}
	s.log.InfoContext(ctx, "normalize payment intent", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPaymentIntent().GetAudit().GetEtag())
	validateSellerPayoutScheduleRequest := &sellersv1.ValidateSellerPayoutScheduleRequest{
		Context: req.GetContext(),
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
			Participants: []*sellersv1.SellerPayoutScheduleParticipant{&sellersv1.SellerPayoutScheduleParticipant{
				Contact: &commonv1.ContactInfo{
					Email:            req.GetPaymentIntent().GetRequester().GetEmail(),
					PreferredChannel: req.GetPaymentIntent().GetRequester().GetPreferredChannel(),
				},
			}},
			Audit: &commonv1.AuditInfo{UpdatedBy: req.GetPaymentIntent().GetAudit().GetUpdatedBy()},
		},
		Details: &sellersv1.ValidateSellerPayoutScheduleRequest_Details{
			Contact: &commonv1.ContactInfo{Name: req.GetPaymentIntent().GetRequester().GetName()},
		},
	}
	validateSellerPayoutScheduleReply, err := grpcx.NewClient(s.conn, sellersv1.NewSellerPayoutScheduleValidationServiceClient).ValidateSellerPayoutSchedule(ctx, validateSellerPayoutScheduleRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerPayoutScheduleValidationService/ValidateSellerPayoutSchedule")
	}
	metricSeriesQuery, ok := s.clients.Get("acme.metrics.v1.MetricSeriesQueryService").(metricsv1.MetricSeriesQueryServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.metrics.v1.MetricSeriesQueryService")
	}
	countMetricSeriesesRequest := &metricsv1.CountMetricSeriesesRequest{
		Context: req.GetContext(),
		Filter: &metricsv1.CountMetricSeriesesRequest_Filter{
			Region:       validateSellerPayoutScheduleReply.GetSellerPayoutSchedule().GetLocation().GetRegion(),
			CreatedAfter: validateSellerPayoutScheduleReply.GetOutcome().GetEffectiveAt(),
			OwnerId:      validateSellerPayoutScheduleReply.GetOutcome().GetReferenceId(),
		},
	}
	countMetricSeriesesReply, err := metricSeriesQuery.CountMetricSerieses(ctx, countMetricSeriesesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.metrics.v1.MetricSeriesQueryService/CountMetricSerieses")
	}
	return &checkoutv1.NormalizePaymentIntentResponse{
		PaymentIntent: &checkoutv1.PaymentIntent{
			Audit: &commonv1.AuditInfo{
				Etag: validateSellerPayoutScheduleReply.GetSellerPayoutSchedule().GetAudit().GetEtag(),
			},
			PaymentIntentId: req.GetPaymentIntentId(),
			Status:          req.GetPaymentIntent().GetStatus(),
		},
		Result: &commonv1.OperationResult{
			Details: countMetricSeriesesReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}

// ExplainPaymentIntentDecision explains a payment intent.
func (s *PaymentIntentValidationServer) ExplainPaymentIntentDecision(ctx context.Context, req *checkoutv1.ExplainPaymentIntentDecisionRequest) (*checkoutv1.ExplainPaymentIntentDecisionResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePaymentIntent(ctx, req.GetContext().GetTenantId(), req.GetPaymentIntent()); err != nil {
		return nil, grpcx.Wrap(err, "save payment intent")
	}
	s.log.InfoContext(ctx, "explain payment intent decision", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPaymentIntent().GetAudit().GetEtag())
	updateWarehouseRequest := &warehousingv1.UpdateWarehouseRequest{
		Context: req.GetContext(),
		Warehouse: &warehousingv1.Warehouse{
			Owner: &identityv1.UserRef{
				Email:  req.GetPaymentIntent().GetAssignee().GetEmail(),
				UserId: req.GetPaymentIntent().GetAssignee().GetUserId(),
			},
		},
	}
	updateWarehouseReply, err := grpcx.Call(ctx, s.warehouseClient.UpdateWarehouse, updateWarehouseRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.WarehouseService/UpdateWarehouse")
	}
	for comment := range iterx.Each(req.GetPaymentIntent().GetComments(), func(candidate *checkoutv1.PaymentIntentComment) bool { return candidate != nil }) {
		if _, err := s.marketingCampaignClient.SearchMarketingCampaigns(ctx, &campaignsv1.SearchMarketingCampaignsRequest{
			Context: req.GetContext(),
			Filter: &campaignsv1.SearchMarketingCampaignsRequest_Filter{
				Region:       updateWarehouseReply.GetWarehouse().GetAddress().GetRegion(),
				CreatedAfter: comment.GetCreatedAt(),
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.campaigns.v1.MarketingCampaignService/SearchMarketingCampaigns")
		}
	}
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "checkout.explain_payment_intent_decision.create_delivery",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"delivery.audit.created_by": req.GetPaymentIntent().GetAudit().GetCreatedBy(),
			"context.tenant_id":         req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	return &checkoutv1.ExplainPaymentIntentDecisionResponse{
		Outcome: &checkoutv1.ExplainPaymentIntentDecisionResponse_Outcome{Status: updateWarehouseReply.GetResult().GetStatus()},
		Result: &commonv1.OperationResult{
			Message: updateWarehouseReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		PaymentIntent: &checkoutv1.PaymentIntent{
			PaymentIntentId: req.GetPaymentIntentId(),
			Assignee:        req.GetPaymentIntent().GetAssignee(),
		},
	}, nil
}

// ScorePaymentIntent scores a payment intent.
func (s *PaymentIntentValidationServer) ScorePaymentIntent(ctx context.Context, req *checkoutv1.ScorePaymentIntentRequest) (*checkoutv1.ScorePaymentIntentResponse, error) {
	if err := s.store.SavePaymentIntent(ctx, req.GetContext().GetTenantId(), req.GetPaymentIntent()); err != nil {
		return nil, grpcx.Wrap(err, "save payment intent")
	}
	s.log.InfoContext(ctx, "score payment intent", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPaymentIntent().GetAudit().GetEtag())
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := paymentIntentValidationServerSendSynonymTestNoticeSteps[mode]
	ctx = tenancy.With(ctx, "preferred_channel", req.GetPaymentIntent().GetRequester().GetPreferredChannel())
	packed, err := anypb.New(req.GetPaymentIntent())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	sendSynonymTestNoticeRequest := &searchv1.SendSynonymTestNoticeRequest{
		Context: req.GetContext(),
		Details: &searchv1.SendSynonymTestNoticeRequest_Details{
			Recipient: &commonv1.ContactInfo{PreferredChannel: tenancy.Value(ctx, "preferred_channel")},
			Payload:   packed,
		},
	}
	sendSynonymTestNoticeReply, err := run(s, ctx, sendSynonymTestNoticeRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymNotificationService/SendSynonymTestNotice")
	}
	checkRewardEligibilityRequest := &loyaltyv1.CheckRewardEligibilityRequest{
		Context: req.GetContext(),
		Details: &loyaltyv1.CheckRewardEligibilityRequest_Details{
			Contact: &commonv1.ContactInfo{Name: req.GetPaymentIntent().GetRequester().GetName()},
		},
	}
	type checkRewardEligibilityResult struct {
		reply *loyaltyv1.CheckRewardEligibilityResponse
		err   error
	}
	checkRewardEligibilityResults := make(chan checkRewardEligibilityResult, 1)
	go func() {
		reply, err := s.rewardValidationClient.CheckRewardEligibility(ctx, checkRewardEligibilityRequest)
		checkRewardEligibilityResults <- checkRewardEligibilityResult{reply, err}
	}()
	var checkRewardEligibilityReply *loyaltyv1.CheckRewardEligibilityResponse
	select {
	case result := <-checkRewardEligibilityResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.loyalty.v1.RewardValidationService/CheckRewardEligibility")
		}
		checkRewardEligibilityReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	previewVenueRequest := &venuesv1.PreviewVenueRequest{
		Context: req.GetContext(),
		Venue: &venuesv1.Venue{
			Owner: &identityv1.UserRef{
				Email: sendSynonymTestNoticeReply.GetSynonym().GetOwner().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: checkRewardEligibilityReply.GetReward().GetAudit().GetUpdatedAt(),
			},
		},
		Details: &venuesv1.PreviewVenueRequest_Details{
			Contact: &commonv1.ContactInfo{Phone: req.GetPaymentIntent().GetRequester().GetPhone()},
		},
	}
	previewVenueReply := new(venuesv1.PreviewVenueResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", venuesVenueValidationServiceName, "PreviewVenue"), previewVenueRequest, previewVenueReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.venues.v1.VenueValidationService/PreviewVenue")
	}
	for _, comment := range req.GetPaymentIntent().GetComments() {
		if _, err := s.registeredModelCalculationClient.SimulateRegisteredModel(ctx, &modelregistryv1.SimulateRegisteredModelRequest{
			Context:         req.GetContext(),
			RegisteredModel: &modelregistryv1.RegisteredModel{Owner: &identityv1.UserRef{Email: comment.GetAuthorEmail()}},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.modelregistry.v1.RegisteredModelCalculationService/SimulateRegisteredModel")
		}
	}
	return &checkoutv1.ScorePaymentIntentResponse{
		PaymentIntent: &checkoutv1.PaymentIntent{
			Requester: &commonv1.ContactInfo{
				Email: sendSynonymTestNoticeReply.GetSynonym().GetOwner().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedAt: checkRewardEligibilityReply.GetReward().GetAudit().GetCreatedAt(),
			},
			Assignee: &identityv1.UserRef{
				UserId: checkRewardEligibilityReply.GetReward().GetOwner().GetUserId(),
			},
			PaymentIntentId: req.GetPaymentIntentId(),
			CustomFields:    req.GetPaymentIntent().GetCustomFields(),
		},
		Result: &commonv1.OperationResult{
			Message: previewVenueReply.GetResult().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// LintPaymentIntents lints a payment intent.
func (s *PaymentIntentValidationServer) LintPaymentIntents(ctx context.Context, req *checkoutv1.LintPaymentIntentsRequest) (*checkoutv1.LintPaymentIntentsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected LintPaymentIntents", "error", err)
		return nil, err
	}
	if err := s.store.SavePaymentIntent(ctx, req.GetContext().GetTenantId(), req.GetPaymentIntent()); err != nil {
		return nil, grpcx.Wrap(err, "save payment intent")
	}
	s.log.InfoContext(ctx, "lint payment intents", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPaymentIntent().GetAudit().GetEtag())
	cancelTenantExportRequest := buildCancelTenantExportRequestForLintPaymentIntents(req)
	cancelTenantExportReply, err := s.tenantExportClient.CancelTenantExport(ctx, cancelTenantExportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantExportService/CancelTenantExport")
	}
	getTenantExportRequest := &identityv1.GetTenantExportRequest{
		Context:  req.GetContext(),
		TenantId: req.GetContext().GetTenantId(),
	}
	getTenantExportReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*identityv1.GetTenantExportResponse, error) {
		return s.tenantExportClient.GetTenantExport(ctx, getTenantExportRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantExportService/GetTenantExport")
	}
	return &checkoutv1.LintPaymentIntentsResponse{
		PaymentIntent: &checkoutv1.PaymentIntent{
			Assignee: &identityv1.UserRef{
				Email: cancelTenantExportReply.GetTenant().GetManager().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: cancelTenantExportReply.GetTenant().GetAudit().GetUpdatedAt(),
			},
			PaymentIntentId: req.GetPaymentIntentId(),
			Priority:        req.GetPaymentIntent().GetPriority(),
		},
		Result: &commonv1.OperationResult{Status: getTenantExportReply.GetResult().GetStatus()},
	}, nil
}

// GetPaymentIntentRules returns a payment intent.
func (s *PaymentIntentValidationServer) GetPaymentIntentRules(ctx context.Context, req *checkoutv1.GetPaymentIntentRulesRequest) (*checkoutv1.GetPaymentIntentRulesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetPaymentIntentRules", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "payment_intent", req.GetPaymentIntentId(), "get_payment_intent_rules"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get payment intent rules", "tenant", req.GetContext().GetTenantId())
	scoreRefundRequest := &paymentsv1.ScoreRefundRequest{
		Context: req.GetContext(),
		Refund: &paymentsv1.Refund{
			Counterparty: &customersv1.CustomerRef{Email: req.GetContext().GetActor().GetEmail()},
		},
	}
	scoreRefundReply, err := scoreRefundCall(ctx, scoreRefundRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.RefundValidationService/ScoreRefund")
	}
	browseCorporateEventsReply, err := s.corporateEventQueryClient.BrowseCorporateEvents(ctx, &corporateeventsv1.BrowseCorporateEventsRequest{
		Context: req.GetContext(),
		Filter: &corporateeventsv1.BrowseCorporateEventsRequest_Filter{
			Region: scoreRefundReply.GetRefund().GetBillingAddress().GetRegion(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.corporateevents.v1.CorporateEventQueryService/BrowseCorporateEvents")
	}
	return &checkoutv1.GetPaymentIntentRulesResponse{
		PaymentIntent: &checkoutv1.PaymentIntent{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: scoreRefundReply.GetRefund().GetAudit().GetUpdatedBy(),
			},
			PaymentIntentId: req.GetPaymentIntentId(),
		},
		Result: &commonv1.OperationResult{
			Message:       browseCorporateEventsReply.GetResult().GetMessage(),
			CorrelationId: browseCorporateEventsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// batchGetFeatureFlagsSender returns a function that sends one request to
// acme.featureflags.v1.FeatureFlagService/BatchGetFeatureFlags.
func (s *PaymentIntentValidationServer) batchGetFeatureFlagsSender(request *featureflagsv1.BatchGetFeatureFlagsRequest) func(context.Context) (*featureflagsv1.BatchGetFeatureFlagsResponse, error) {
	return func(ctx context.Context) (*featureflagsv1.BatchGetFeatureFlagsResponse, error) {
		return s.featureFlagClient.BatchGetFeatureFlags(ctx, request)
	}
}

// paymentIntentValidationServerSendSynonymTestNoticeSteps are the ways to run
// acme.search.v1.SynonymNotificationService/SendSynonymTestNotice, by name.
var paymentIntentValidationServerSendSynonymTestNoticeSteps = map[string]func(*PaymentIntentValidationServer, context.Context, *searchv1.SendSynonymTestNoticeRequest) (*searchv1.SendSynonymTestNoticeResponse, error){
	"default": (*PaymentIntentValidationServer).sendSynonymTestNoticeStep,
	"audited": (*PaymentIntentValidationServer).sendSynonymTestNoticeAuditedStep,
}

func (s *PaymentIntentValidationServer) sendSynonymTestNoticeStep(ctx context.Context, request *searchv1.SendSynonymTestNoticeRequest) (*searchv1.SendSynonymTestNoticeResponse, error) {
	return s.synonymNotificationClient.SendSynonymTestNotice(ctx, request)
}

func (s *PaymentIntentValidationServer) sendSynonymTestNoticeAuditedStep(ctx context.Context, request *searchv1.SendSynonymTestNoticeRequest) (*searchv1.SendSynonymTestNoticeResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.search.v1.SynonymNotificationService/SendSynonymTestNotice")
	return s.synonymNotificationClient.SendSynonymTestNotice(ctx, request)
}
