package pricing

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	driversv1 "github.com/hounddogai/protomap-test-golang/gen/acme/drivers/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	modelregistryv1 "github.com/hounddogai/protomap-test-golang/gen/acme/modelregistry/v1"
	pricingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/pricing/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// CurrencyRateLifecycleServer implements `acme.pricing.v1.CurrencyRateLifecycleService`.
// Moves a currency rate between the states of its lifecycle.
type CurrencyRateLifecycleServer struct {
	pricingv1.UnimplementedCurrencyRateLifecycleServiceServer
	store                            *Store
	log                              *slog.Logger
	conn                             *grpc.ClientConn
	priceOverrideCalculationOnce     sync.Once
	priceOverrideCalculationCached   pricingv1.PriceOverrideCalculationServiceClient
	priceQuoteValidationClient       pricingv1.PriceQuoteValidationServiceClient
	priceQuoteCalculationClient      pricingv1.PriceQuoteCalculationServiceClient
	priceOverrideCalculationClient   pricingv1.PriceOverrideCalculationServiceClient
	priceQuoteQueryClient            pricingv1.PriceQuoteQueryServiceClient
	registeredModelCalculationClient modelregistryv1.RegisteredModelCalculationServiceClient
	clients                          *registry.Registry
	inspectionApprovalClient         returnsv1.InspectionApprovalServiceClient
	giftCardBatchWorkflowClient      giftcardsv1.GiftCardBatchWorkflowServiceClient
	currencyRateReportingClient      pricingv1.CurrencyRateReportingServiceClient
	apiCredentialHistoryClient       identityv1.ApiCredentialHistoryServiceClient
	householdQueryPort               householdQueryPort
	priceOverrideValidationClient    pricingv1.PriceOverrideValidationServiceClient
}

// NewCurrencyRateLifecycleServer wires a CurrencyRateLifecycleServer.
func NewCurrencyRateLifecycleServer(deps platform.Deps) *CurrencyRateLifecycleServer {
	return &CurrencyRateLifecycleServer{
		store:                            NewStore(deps.DB),
		log:                              deps.Logger.With("service", "acme.pricing.v1.CurrencyRateLifecycleService"),
		conn:                             deps.Conn,
		priceQuoteValidationClient:       pricingv1.NewPriceQuoteValidationServiceClient(deps.Conn),
		priceQuoteCalculationClient:      pricingv1.NewPriceQuoteCalculationServiceClient(deps.Conn),
		priceOverrideCalculationClient:   pricingv1.NewPriceOverrideCalculationServiceClient(deps.Conn),
		priceQuoteQueryClient:            pricingv1.NewPriceQuoteQueryServiceClient(deps.Conn),
		registeredModelCalculationClient: modelregistryv1.NewRegisteredModelCalculationServiceClient(deps.Conn),
		clients:                          deps.Clients,
		inspectionApprovalClient:         returnsv1.NewInspectionApprovalServiceClient(deps.Conn),
		giftCardBatchWorkflowClient:      giftcardsv1.NewGiftCardBatchWorkflowServiceClient(deps.Conn),
		currencyRateReportingClient:      pricingv1.NewCurrencyRateReportingServiceClient(deps.Conn),
		apiCredentialHistoryClient:       identityv1.NewApiCredentialHistoryServiceClient(deps.Conn),
		householdQueryPort:               newHouseholdQueryPort(deps),
		priceOverrideValidationClient:    pricingv1.NewPriceOverrideValidationServiceClient(deps.Conn),
	}
}

// ActivateCurrencyRate activates a currency rate.
func (s *CurrencyRateLifecycleServer) ActivateCurrencyRate(ctx context.Context, req *pricingv1.ActivateCurrencyRateRequest) (*pricingv1.ActivateCurrencyRateResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "currency_rate", req.GetCurrencyRateId(), "activate_currency_rate"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "activate currency rate", "tenant", req.GetContext().GetTenantId())
	recalculatePriceOverridesRequest := &pricingv1.RecalculatePriceOverridesRequest{
		Context: req.GetContext(),
		PriceOverride: &pricingv1.PriceOverride{
			Owner: &identityv1.UserRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
				Email:       req.GetDetails().GetNotifyContact().GetEmail(),
			},
		},
	}
	recalculatePriceOverridesReply, err := s.priceOverrideCalculationLazy().RecalculatePriceOverrides(ctx, recalculatePriceOverridesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceOverrideCalculationService/RecalculatePriceOverrides")
	}
	lintPriceQuotesRequest := &pricingv1.LintPriceQuotesRequest{
		Context: req.GetContext(),
		PriceQuote: &pricingv1.PriceQuote{
			Counterparty: &pricingv1.PriceListRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
		},
		Details: &pricingv1.LintPriceQuotesRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetNotifyContact().GetEmail()},
		},
	}
	lintPriceQuotesReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*pricingv1.LintPriceQuotesResponse, error) {
		return s.priceQuoteValidationClient.LintPriceQuotes(ctx, lintPriceQuotesRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceQuoteValidationService/LintPriceQuotes")
	}
	return &pricingv1.ActivateCurrencyRateResponse{
		Outcome: &pricingv1.ActivateCurrencyRateResponse_Outcome{
			Status:      recalculatePriceOverridesReply.GetResult().GetStatus(),
			Message:     lintPriceQuotesReply.GetResult().GetMessage(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		CurrencyRate: &pricingv1.CurrencyRate{CurrencyRateId: req.GetCurrencyRateId()},
		Result:       &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// SuspendCurrencyRate suspends a currency rate.
func (s *CurrencyRateLifecycleServer) SuspendCurrencyRate(ctx context.Context, req *pricingv1.SuspendCurrencyRateRequest) (*pricingv1.SuspendCurrencyRateResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "currency_rate", req.GetCurrencyRateId(), "suspend_currency_rate"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "suspend currency rate", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetDetails()).(*pricingv1.SuspendCurrencyRateRequest_Details)
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	calculatePriceQuoteReply, err := s.priceQuoteCalculationClient.CalculatePriceQuote(ctx, &pricingv1.CalculatePriceQuoteRequest{
		Context: req.GetContext(),
		PriceQuote: &pricingv1.PriceQuote{
			Counterparty: &pricingv1.PriceListRef{DisplayName: cloned.GetNewOwner().GetDisplayName()},
		},
		Reason:      req.GetReason(),
		Annotations: annotations,
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceQuoteCalculationService/CalculatePriceQuote")
	}
	step := &currencyRateLifecycleServerSuspendCurrencyRateRecalculatePriceOverridesStep{request: &pricingv1.RecalculatePriceOverridesRequest{
		Context: req.GetContext(),
		Details: &pricingv1.RecalculatePriceOverridesRequest_Details{
			Destination: &commonv1.PostalAddress{
				Lines:  calculatePriceQuoteReply.GetPriceQuote().GetBillingAddress().GetLines(),
				Region: calculatePriceQuoteReply.GetPriceQuote().GetBillingAddress().GetRegion(),
			},
		},
		PriceOverride: &pricingv1.PriceOverride{
			Audit: &commonv1.AuditInfo{
				CreatedAt: calculatePriceQuoteReply.GetPriceQuote().GetAudit().GetCreatedAt(),
			},
		},
	}}
	if err := s.runSuspendCurrencyRateRecalculatePriceOverrides(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceOverrideCalculationService/RecalculatePriceOverrides")
	}
	recalculatePriceOverridesReply := step.reply
	resolvePriceQuotesRequest := &pricingv1.ResolvePriceQuotesRequest{Context: req.GetContext()}
	resolvePriceQuotesReply, err := pricingv1.PriceQuoteQueryServiceClient.ResolvePriceQuotes(s.priceQuoteQueryClient, ctx, resolvePriceQuotesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceQuoteQueryService/ResolvePriceQuotes")
	}
	return &pricingv1.SuspendCurrencyRateResponse{
		Result: &commonv1.OperationResult{
			Message:      calculatePriceQuoteReply.GetResult().GetMessage(),
			ReferenceIds: resolvePriceQuotesReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		CurrencyRate: &pricingv1.CurrencyRate{
			Subject: &customersv1.CustomerRef{
				DisplayName: recalculatePriceOverridesReply.GetPriceOverride().GetOwner().GetDisplayName(),
			},
			CurrencyRateId: req.GetCurrencyRateId(),
		},
		Outcome: &pricingv1.SuspendCurrencyRateResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// ReinstateCurrencyRate reinstates a currency rate.
func (s *CurrencyRateLifecycleServer) ReinstateCurrencyRate(ctx context.Context, req *pricingv1.ReinstateCurrencyRateRequest) (*pricingv1.ReinstateCurrencyRateResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "currency_rate", req.GetCurrencyRateId(), "reinstate_currency_rate"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "reinstate currency rate", "tenant", req.GetContext().GetTenantId())
	calculateRegisteredModelReply, err := s.registeredModelCalculationClient.CalculateRegisteredModel(ctx, &modelregistryv1.CalculateRegisteredModelRequest{
		Context: req.GetContext(),
		RegisteredModel: &modelregistryv1.RegisteredModel{
			Owner: &identityv1.UserRef{
				UserId:      req.GetDetails().GetNewOwner().GetUserId(),
				Email:       req.GetDetails().GetNotifyContact().GetEmail(),
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
		},
		Reason: req.GetReason(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.modelregistry.v1.RegisteredModelCalculationService/CalculateRegisteredModel")
	}
	browseDriversRequest := &driversv1.BrowseDriversRequest{
		Context: req.GetContext(),
		Filter: &driversv1.BrowseDriversRequest_Filter{
			Region:       calculateRegisteredModelReply.GetRegisteredModel().GetAddress().GetRegion(),
			CreatedAfter: calculateRegisteredModelReply.GetRegisteredModel().GetAudit().GetUpdatedAt(),
			OwnerId:      calculateRegisteredModelReply.GetRegisteredModel().GetOwner().GetUserId(),
		},
	}
	browseDriversReply := new(driversv1.BrowseDriversResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", driversDriverQueryServiceName, "BrowseDrivers"), browseDriversRequest, browseDriversReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.drivers.v1.DriverQueryService/BrowseDrivers")
	}
	return &pricingv1.ReinstateCurrencyRateResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: calculateRegisteredModelReply.GetResult().GetCorrelationId(),
			Status:        browseDriversReply.GetResult().GetStatus(),
		},
		Outcome: &pricingv1.ReinstateCurrencyRateResponse_Outcome{
			Message:     calculateRegisteredModelReply.GetResult().GetMessage(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		CurrencyRate: &pricingv1.CurrencyRate{CurrencyRateId: req.GetCurrencyRateId()},
	}, nil
}

// TerminateCurrencyRate terminates a currency rate.
func (s *CurrencyRateLifecycleServer) TerminateCurrencyRate(ctx context.Context, req *pricingv1.TerminateCurrencyRateRequest) (*pricingv1.TerminateCurrencyRateResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "currency_rate", req.GetCurrencyRateId(), "terminate_currency_rate"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "terminate currency rate", "tenant", req.GetContext().GetTenantId())
	currencyRateReporting, err := registry.Lookup[pricingv1.CurrencyRateReportingServiceClient](s.clients, "acme.pricing.v1.CurrencyRateReportingService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	listCurrencyRateReportsRequest := &pricingv1.ListCurrencyRateReportsRequest{
		Context: req.GetContext(),
		Filter: &pricingv1.ListCurrencyRateReportsRequest_Filter{
			CreatedAfter: req.GetDetails().GetEffectiveAt(),
			OwnerId:      req.GetDetails().GetNewOwner().GetUserId(),
			Labels:       req.GetAnnotations(),
		},
	}
	listCurrencyRateReportsReply, err := currencyRateReporting.ListCurrencyRateReports(ctx, listCurrencyRateReportsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.CurrencyRateReportingService/ListCurrencyRateReports")
	}
	return &pricingv1.TerminateCurrencyRateResponse{
		CurrencyRate: &pricingv1.CurrencyRate{
			CorrelationId:  listCurrencyRateReportsReply.GetResult().GetCorrelationId(),
			CurrencyRateId: req.GetCurrencyRateId(),
		},
		Result: &commonv1.OperationResult{
			Details: listCurrencyRateReportsReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Outcome: &pricingv1.TerminateCurrencyRateResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// RenewCurrencyRate renews a currency rate.
func (s *CurrencyRateLifecycleServer) RenewCurrencyRate(ctx context.Context, req *pricingv1.RenewCurrencyRateRequest) (*pricingv1.RenewCurrencyRateResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "currency_rate", req.GetCurrencyRateId(), "renew_currency_rate"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "renew currency rate", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "preferred_channel", req.GetDetails().GetNotifyContact().GetPreferredChannel())
	approveInspectionReply, err := s.inspectionApprovalClient.ApproveInspection(ctx, &returnsv1.ApproveInspectionRequest{
		Context: req.GetContext(),
		Inspection: &returnsv1.Inspection{
			Assignee:  &identityv1.UserRef{Email: req.GetDetails().GetNewOwner().GetEmail()},
			Requester: &commonv1.ContactInfo{PreferredChannel: tenancy.Value(ctx, "preferred_channel")},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.InspectionApprovalService/ApproveInspection")
	}
	advanceGiftCardBatchWorkflowRequest := &giftcardsv1.AdvanceGiftCardBatchWorkflowRequest{
		Context: req.GetContext(),
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			Audit: &commonv1.AuditInfo{
				CreatedBy: approveInspectionReply.GetInspection().GetAudit().GetCreatedBy(),
			},
		},
		Details: &giftcardsv1.AdvanceGiftCardBatchWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				Email: approveInspectionReply.GetInspection().GetRequester().GetEmail(),
			},
		},
	}
	advanceGiftCardBatchWorkflowReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*giftcardsv1.AdvanceGiftCardBatchWorkflowResponse, error) {
		return s.giftCardBatchWorkflowClient.AdvanceGiftCardBatchWorkflow(ctx, advanceGiftCardBatchWorkflowRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchWorkflowService/AdvanceGiftCardBatchWorkflow")
	}
	return &pricingv1.RenewCurrencyRateResponse{
		CurrencyRate: &pricingv1.CurrencyRate{
			CorrelationId:  approveInspectionReply.GetResult().GetCorrelationId(),
			CurrencyRateId: req.GetCurrencyRateId(),
		},
		Outcome: &pricingv1.RenewCurrencyRateResponse_Outcome{
			Status:      advanceGiftCardBatchWorkflowReply.GetResult().GetStatus(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{
			Status: advanceGiftCardBatchWorkflowReply.GetResult().GetStatus(),
		},
	}, nil
}

// ExpireCurrencyRate expires a currency rate.
func (s *CurrencyRateLifecycleServer) ExpireCurrencyRate(ctx context.Context, req *pricingv1.ExpireCurrencyRateRequest) (*pricingv1.ExpireCurrencyRateResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ExpireCurrencyRate", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "currency_rate", req.GetCurrencyRateId(), "expire_currency_rate"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "expire currency rate", "tenant", req.GetContext().GetTenantId())
	householdQuery, ok := s.clients.Get("acme.customers.v1.HouseholdQueryService").(customersv1.HouseholdQueryServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.customers.v1.HouseholdQueryService")
	}
	resolveHouseholdsRequest := &customersv1.ResolveHouseholdsRequest{Context: req.GetContext()}
	resolveHouseholdsReply, err := householdQuery.ResolveHouseholds(ctx, resolveHouseholdsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdQueryService/ResolveHouseholds")
	}
	countCurrencyRateTotalsRequest := &pricingv1.CountCurrencyRateTotalsRequest{
		Context: req.GetContext(),
		Filter:  &pricingv1.CountCurrencyRateTotalsRequest_Filter{OwnerId: req.GetDetails().GetNewOwner().GetUserId()},
	}
	countCurrencyRateTotalsReply, err := grpcx.Call(ctx, s.currencyRateReportingClient.CountCurrencyRateTotals, countCurrencyRateTotalsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.CurrencyRateReportingService/CountCurrencyRateTotals")
	}
	return &pricingv1.ExpireCurrencyRateResponse{
		Result: &commonv1.OperationResult{
			Details: resolveHouseholdsReply.GetResult().GetDetails(),
			Status:  countCurrencyRateTotalsReply.GetResult().GetStatus(),
		},
		CurrencyRate: &pricingv1.CurrencyRate{CurrencyRateId: req.GetCurrencyRateId()},
		Outcome:      &pricingv1.ExpireCurrencyRateResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// GetCurrencyRateLifecycle returns a currency rate.
func (s *CurrencyRateLifecycleServer) GetCurrencyRateLifecycle(ctx context.Context, req *pricingv1.GetCurrencyRateLifecycleRequest) (*pricingv1.GetCurrencyRateLifecycleResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetCurrencyRateLifecycle", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "currency_rate", req.GetCurrencyRateId(), "get_currency_rate_lifecycle"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get currency rate lifecycle", "tenant", req.GetContext().GetTenantId())
	return &pricingv1.GetCurrencyRateLifecycleResponse{
		CurrencyRate: &pricingv1.CurrencyRate{CurrencyRateId: req.GetCurrencyRateId()},
		Result:       &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ScheduleCurrencyRateExpiry schedules a currency rate.
func (s *CurrencyRateLifecycleServer) ScheduleCurrencyRateExpiry(ctx context.Context, req *pricingv1.ScheduleCurrencyRateExpiryRequest) (*pricingv1.ScheduleCurrencyRateExpiryResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveCurrencyRate(ctx, req.GetContext().GetTenantId(), req.GetCurrencyRate()); err != nil {
		return nil, grpcx.Wrap(err, "save currency rate")
	}
	s.log.InfoContext(ctx, "schedule currency rate expiry", "tenant", req.GetContext().GetTenantId())
	detailsCache := cache.New[string, *pricingv1.ScheduleCurrencyRateExpiryRequest_Details](1)
	detailsCache.Put(req.GetContext().GetTraceId(), req.GetDetails())
	cachedDetails, _ := detailsCache.Get(req.GetContext().GetTraceId())
	cloned := proto.Clone(req.GetCurrencyRate()).(*pricingv1.CurrencyRate)
	var envelope pricingv1.PriceListRef
	if err := req.GetCurrencyRate().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	compareApiCredentialVersionsRequest := &identityv1.CompareApiCredentialVersionsRequest{
		Context: req.GetContext(),
		ApiCredential: &identityv1.ApiCredential{
			Contact: &commonv1.ContactInfo{Name: cachedDetails.GetNotify().GetName()},
			Holder:  &identityv1.SessionRef{DisplayName: cloned.GetSubject().GetDisplayName()},
		},
		ApiCredentialId: envelope.GetDisplayName(),
	}
	compareApiCredentialVersionsReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*identityv1.CompareApiCredentialVersionsResponse, error) {
		return s.apiCredentialHistoryClient.CompareApiCredentialVersions(ctx, compareApiCredentialVersionsRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.ApiCredentialHistoryService/CompareApiCredentialVersions")
	}
	ctx = tenancy.With(ctx, "customer_id", req.GetCurrencyRate().GetSubject().GetCustomerId())
	var envelope2 pricingv1.PriceListRef
	if err := req.GetCurrencyRate().GetDetail().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	findHouseholdsRequest := &customersv1.FindHouseholdsRequest{
		Context: req.GetContext(),
		Filter: &customersv1.FindHouseholdsRequest_Filter{
			Labels:       compareApiCredentialVersionsReply.GetApiCredential().GetLabels(),
			CreatedAfter: compareApiCredentialVersionsReply.GetApiCredential().GetAudit().GetCreatedAt(),
			OwnerId:      tenancy.Value(ctx, "customer_id"),
		},
		Page: &commonv1.PageRequest{PageToken: envelope2.GetDisplayName()},
	}
	findHouseholdsReply, err := s.householdQueryPort.FindHouseholds(ctx, findHouseholdsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdQueryService/FindHouseholds")
	}
	var envelope3 pricingv1.PriceListRef
	if err := req.GetCurrencyRate().GetDetail().UnmarshalTo(&envelope3); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	step := &currencyRateLifecycleServerScheduleCurrencyRateExpiryNormalizePriceOverrideStep{request: &pricingv1.NormalizePriceOverrideRequest{
		Context: req.GetContext(),
		PriceOverride: &pricingv1.PriceOverride{
			Owner: &identityv1.UserRef{Email: req.GetDetails().GetNotify().GetEmail()},
		},
		Details: &pricingv1.NormalizePriceOverrideRequest_Details{
			Contact: &commonv1.ContactInfo{Name: req.GetDetails().GetNotify().GetName()},
		},
		PriceOverrideId: envelope3.GetDisplayName(),
	}}
	if err := s.runScheduleCurrencyRateExpiryNormalizePriceOverride(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceOverrideValidationService/NormalizePriceOverride")
	}
	normalizePriceOverrideReply := step.reply
	return &pricingv1.ScheduleCurrencyRateExpiryResponse{
		Outcome: &pricingv1.ScheduleCurrencyRateExpiryResponse_Outcome{
			Status: compareApiCredentialVersionsReply.GetOutcome().GetStatus(),
		},
		Result: &commonv1.OperationResult{
			Status:       compareApiCredentialVersionsReply.GetOutcome().GetStatus(),
			ReferenceIds: findHouseholdsReply.GetResult().GetReferenceIds(),
			Details:      normalizePriceOverrideReply.GetResult().GetDetails(),
		},
		CurrencyRate: &pricingv1.CurrencyRate{
			CurrencyRateId: req.GetCurrencyRateId(),
			Kind:           req.GetCurrencyRate().GetKind(),
		},
	}, nil
}

// priceOverrideCalculationLazy dials acme.pricing.v1.PriceOverrideCalculationService the first time a handler needs it.
func (s *CurrencyRateLifecycleServer) priceOverrideCalculationLazy() pricingv1.PriceOverrideCalculationServiceClient {
	s.priceOverrideCalculationOnce.Do(func() { s.priceOverrideCalculationCached = pricingv1.NewPriceOverrideCalculationServiceClient(s.conn) })
	return s.priceOverrideCalculationCached
}

// currencyRateLifecycleServerSuspendCurrencyRateRecalculatePriceOverridesStep carries the request of one step of
// SuspendCurrencyRate and, once it ran, its reply.
type currencyRateLifecycleServerSuspendCurrencyRateRecalculatePriceOverridesStep struct {
	request *pricingv1.RecalculatePriceOverridesRequest
	reply   *pricingv1.RecalculatePriceOverridesResponse
}

func (s *CurrencyRateLifecycleServer) runSuspendCurrencyRateRecalculatePriceOverrides(ctx context.Context, step *currencyRateLifecycleServerSuspendCurrencyRateRecalculatePriceOverridesStep) error {
	reply, err := s.priceOverrideCalculationClient.RecalculatePriceOverrides(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}

// currencyRateLifecycleServerScheduleCurrencyRateExpiryNormalizePriceOverrideStep carries the request of one step of
// ScheduleCurrencyRateExpiry and, once it ran, its reply.
type currencyRateLifecycleServerScheduleCurrencyRateExpiryNormalizePriceOverrideStep struct {
	request *pricingv1.NormalizePriceOverrideRequest
	reply   *pricingv1.NormalizePriceOverrideResponse
}

func (s *CurrencyRateLifecycleServer) runScheduleCurrencyRateExpiryNormalizePriceOverride(ctx context.Context, step *currencyRateLifecycleServerScheduleCurrencyRateExpiryNormalizePriceOverrideStep) error {
	reply, err := s.priceOverrideValidationClient.NormalizePriceOverride(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
