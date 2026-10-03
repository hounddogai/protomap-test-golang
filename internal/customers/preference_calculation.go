package customers

import (
	"context"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	deliveriesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/deliveries/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	labelingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/labeling/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	lendingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/lending/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	pricingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/pricing/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// PreferenceCalculationServer implements `acme.customers.v1.PreferenceCalculationService`.
// Computes the figures of a preference, such as totals, taxes, and estimates.
type PreferenceCalculationServer struct {
	customersv1.UnimplementedPreferenceCalculationServiceServer
	store                          *Store
	log                            *slog.Logger
	conn                           *grpc.ClientConn
	ledgerAccountValidationClient  ledgerv1.LedgerAccountValidationServiceClient
	meteredDelivery                meteredDelivery
	ledgerAccountValidationPort    ledgerAccountValidationPort
	labelingTaskCalculationClient  labelingv1.LabelingTaskCalculationServiceClient
	metricSeriesValidationClient   metricsv1.MetricSeriesValidationServiceClient
	recordApiCredentialVersionFn   func(context.Context, *identityv1.RecordApiCredentialVersionRequest, ...grpc.CallOption) (*identityv1.RecordApiCredentialVersionResponse, error)
	consentRecordCalculationClient customersv1.ConsentRecordCalculationServiceClient
	clients                        *registry.Registry
	priceQuoteValidationClient     pricingv1.PriceQuoteValidationServiceClient
	userQueryPort                  userQueryPort
	bus                            *eventbus.Bus
	customerCalculationClient      customersv1.CustomerCalculationServiceClient
	refundCalculationClient        paymentsv1.RefundCalculationServiceClient
}

// NewPreferenceCalculationServer wires a PreferenceCalculationServer.
func NewPreferenceCalculationServer(deps platform.Deps) *PreferenceCalculationServer {
	apiCredentialHistory := identityv1.NewApiCredentialHistoryServiceClient(deps.Conn)
	return &PreferenceCalculationServer{
		store:                          NewStore(deps.DB),
		log:                            deps.Logger.With("service", "acme.customers.v1.PreferenceCalculationService"),
		conn:                           deps.Conn,
		ledgerAccountValidationClient:  ledgerv1.NewLedgerAccountValidationServiceClient(deps.Conn),
		meteredDelivery:                meteredDelivery{DeliveryServiceClient: deliveriesv1.NewDeliveryServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		ledgerAccountValidationPort:    newLedgerAccountValidationPort(deps),
		labelingTaskCalculationClient:  labelingv1.NewLabelingTaskCalculationServiceClient(deps.Conn),
		metricSeriesValidationClient:   metricsv1.NewMetricSeriesValidationServiceClient(deps.Conn),
		recordApiCredentialVersionFn:   apiCredentialHistory.RecordApiCredentialVersion,
		consentRecordCalculationClient: customersv1.NewConsentRecordCalculationServiceClient(deps.Conn),
		clients:                        deps.Clients,
		priceQuoteValidationClient:     pricingv1.NewPriceQuoteValidationServiceClient(deps.Conn),
		userQueryPort:                  newUserQueryPort(deps),
		bus:                            deps.Bus,
		customerCalculationClient:      customersv1.NewCustomerCalculationServiceClient(deps.Conn),
		refundCalculationClient:        paymentsv1.NewRefundCalculationServiceClient(deps.Conn),
	}
}

// CalculatePreference calculates a preference.
func (s *PreferenceCalculationServer) CalculatePreference(ctx context.Context, req *customersv1.CalculatePreferenceRequest) (*customersv1.CalculatePreferenceResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SavePreference(ctx, req.GetContext().GetTenantId(), req.GetPreference()); err != nil {
		return nil, grpcx.Wrap(err, "save preference")
	}
	s.log.InfoContext(ctx, "calculate preference", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPreference().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetDetails()).(*customersv1.CalculatePreferenceRequest_Details)
	ctx = tenancy.With(ctx, "locality", req.GetDetails().GetDestination().GetLocality())
	cloneLoanApplicationRequest := &lendingv1.CloneLoanApplicationRequest{
		Context: req.GetContext(),
		LoanApplication: &lendingv1.LoanApplication{
			BillingAddress: &commonv1.PostalAddress{
				CountryCode: cloned.GetDestination().GetCountryCode(),
				Locality:    tenancy.Value(ctx, "locality"),
			},
		},
	}
	cloneLoanApplicationReply := new(lendingv1.CloneLoanApplicationResponse)
	if err := grpcx.Invoke(ctx, s.conn, lendingv1.LoanApplicationService_CloneLoanApplication_FullMethodName, cloneLoanApplicationRequest, cloneLoanApplicationReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.lending.v1.LoanApplicationService/CloneLoanApplication")
	}
	previewLedgerAccountRequest := &ledgerv1.PreviewLedgerAccountRequest{
		Context: req.GetContext(),
		LedgerAccount: &ledgerv1.LedgerAccount{
			Audit: &commonv1.AuditInfo{Etag: req.GetPreference().GetAudit().GetEtag()},
		},
	}
	type previewLedgerAccountResult struct {
		reply *ledgerv1.PreviewLedgerAccountResponse
		err   error
	}
	previewLedgerAccountResults := make(chan previewLedgerAccountResult, 1)
	go func() {
		reply, err := s.ledgerAccountValidationClient.PreviewLedgerAccount(ctx, previewLedgerAccountRequest)
		previewLedgerAccountResults <- previewLedgerAccountResult{reply, err}
	}()
	var previewLedgerAccountReply *ledgerv1.PreviewLedgerAccountResponse
	select {
	case result := <-previewLedgerAccountResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.ledger.v1.LedgerAccountValidationService/PreviewLedgerAccount")
		}
		previewLedgerAccountReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	packed, err := anypb.New(req.GetPreference())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	explainConsentRecordDecisionRequest := &customersv1.ExplainConsentRecordDecisionRequest{
		Context: req.GetContext(),
		Details: &customersv1.ExplainConsentRecordDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{
				Name:  previewLedgerAccountReply.GetLedgerAccount().GetContact().GetName(),
				Email: previewLedgerAccountReply.GetLedgerAccount().GetContact().GetEmail(),
				Phone: previewLedgerAccountReply.GetLedgerAccount().GetContact().GetPhone(),
			},
		},
		ConsentRecord: &customersv1.ConsentRecord{
			Subject: &customersv1.HouseholdRef{
				DisplayName: previewLedgerAccountReply.GetLedgerAccount().GetHolder().GetDisplayName(),
			},
			Detail: packed,
		},
	}
	explainConsentRecordDecisionReply := new(customersv1.ExplainConsentRecordDecisionResponse)
	if err := s.conn.Invoke(ctx, customersConsentRecordValidationServiceExplainConsentRecordDecisionMethod, explainConsentRecordDecisionRequest, explainConsentRecordDecisionReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordValidationService/ExplainConsentRecordDecision")
	}
	cloneDeliveryRequest := &deliveriesv1.CloneDeliveryRequest{
		Context: req.GetContext(),
		Delivery: &deliveriesv1.Delivery{
			Lines: []*deliveriesv1.DeliveryLine{&deliveriesv1.DeliveryLine{Amount: req.GetPreference().GetDiscount().GetAmount()}},
			Audit: &commonv1.AuditInfo{
				CreatedBy: req.GetPreference().GetAudit().GetCreatedBy(),
				UpdatedAt: previewLedgerAccountReply.GetLedgerAccount().GetAudit().GetUpdatedAt(),
			},
			BillingAddress: &commonv1.PostalAddress{
				Recipient: cloneLoanApplicationReply.GetLoanApplication().GetBillingAddress().GetRecipient(),
			},
		},
	}
	cloneDeliveryReply, err := s.meteredDelivery.CloneDelivery(ctx, cloneDeliveryRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.deliveries.v1.DeliveryService/CloneDelivery")
	}
	return &customersv1.CalculatePreferenceResponse{
		Preference: &customersv1.Preference{
			Audit: &commonv1.AuditInfo{
				Etag: cloneLoanApplicationReply.GetLoanApplication().GetAudit().GetEtag(),
			},
			Owner: &identityv1.UserRef{
				DisplayName: cloneDeliveryReply.GetDelivery().GetCounterparty().GetDisplayName(),
			},
			PreferenceId: req.GetPreferenceId(),
			Effective:    req.GetPreference().GetEffective(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: previewLedgerAccountReply.GetResult().GetCorrelationId(),
			Status:        explainConsentRecordDecisionReply.GetOutcome().GetStatus(),
			Message:       cloneDeliveryReply.GetResult().GetMessage(),
		},
	}, nil
}

// EstimatePreference estimates a preference.
func (s *PreferenceCalculationServer) EstimatePreference(ctx context.Context, req *customersv1.EstimatePreferenceRequest) (*customersv1.EstimatePreferenceResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePreference(ctx, req.GetContext().GetTenantId(), req.GetPreference()); err != nil {
		return nil, grpcx.Wrap(err, "save preference")
	}
	s.log.InfoContext(ctx, "estimate preference", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPreference().GetAudit().GetEtag())
	packed, err := anypb.New(req.GetPreference())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	explainConsentRecordDecisionRequest := &customersv1.ExplainConsentRecordDecisionRequest{
		Context: req.GetContext(),
		ConsentRecord: &customersv1.ConsentRecord{
			Subject: &customersv1.HouseholdRef{
				DisplayName: req.GetPreference().GetOwner().GetDisplayName(),
			},
			Detail: packed,
		},
	}
	explainConsentRecordDecisionReply := new(customersv1.ExplainConsentRecordDecisionResponse)
	if err := grpcx.Invoke(ctx, s.conn, customersv1.ConsentRecordValidationService_ExplainConsentRecordDecision_FullMethodName, explainConsentRecordDecisionRequest, explainConsentRecordDecisionReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordValidationService/ExplainConsentRecordDecision")
	}
	normalizeLedgerAccountRequest := &ledgerv1.NormalizeLedgerAccountRequest{
		Context: req.GetContext(),
		LedgerAccount: &ledgerv1.LedgerAccount{
			Audit: &commonv1.AuditInfo{CreatedAt: req.GetPreference().GetAudit().GetCreatedAt()},
		},
	}
	normalizeLedgerAccountReply, err := s.ledgerAccountValidationPort.NormalizeLedgerAccount(ctx, normalizeLedgerAccountRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountValidationService/NormalizeLedgerAccount")
	}
	compareLabelingTaskOptionsReply, err := s.labelingTaskCalculationClient.CompareLabelingTaskOptions(ctx, &labelingv1.CompareLabelingTaskOptionsRequest{
		Context: req.GetContext(),
		Details: &labelingv1.CompareLabelingTaskOptionsRequest_Details{
			Destination: &commonv1.PostalAddress{
				Locality:  req.GetDetails().GetDestination().GetLocality(),
				Recipient: req.GetDetails().GetDestination().GetRecipient(),
			},
		},
		LabelingTask: &labelingv1.LabelingTask{
			Audit: &commonv1.AuditInfo{UpdatedAt: req.GetPreference().GetAudit().GetUpdatedAt()},
			Comments: []*labelingv1.LabelingTaskComment{&labelingv1.LabelingTaskComment{
				CreatedAt: normalizeLedgerAccountReply.GetLedgerAccount().GetAudit().GetCreatedAt(),
			}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.labeling.v1.LabelingTaskCalculationService/CompareLabelingTaskOptions")
	}
	return &customersv1.EstimatePreferenceResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: explainConsentRecordDecisionReply.GetConsentRecord().GetCorrelationId(),
			ReferenceIds:  explainConsentRecordDecisionReply.GetResult().GetReferenceIds(),
			Details:       compareLabelingTaskOptionsReply.GetResult().GetDetails(),
			Status:        "ok",
		},
		Preference: &customersv1.Preference{
			Audit: &commonv1.AuditInfo{
				CreatedAt: normalizeLedgerAccountReply.GetLedgerAccount().GetAudit().GetCreatedAt(),
			},
			PreferenceId: req.GetPreferenceId(),
			Owner:        req.GetPreference().GetOwner(),
		},
	}, nil
}

// QuotePreference quotes a preference.
func (s *PreferenceCalculationServer) QuotePreference(ctx context.Context, req *customersv1.QuotePreferenceRequest) (*customersv1.QuotePreferenceResponse, error) {
	if err := s.store.SavePreference(ctx, req.GetContext().GetTenantId(), req.GetPreference()); err != nil {
		return nil, grpcx.Wrap(err, "save preference")
	}
	s.log.InfoContext(ctx, "quote preference", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPreference().GetAudit().GetEtag())
	encoded, err := protojson.Marshal(req.GetPreference())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded customersv1.Preference
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	validateMetricSeriesRequest := &metricsv1.ValidateMetricSeriesRequest{
		Context: req.GetContext(),
		MetricSeries: &metricsv1.MetricSeries{
			Locator: &metricsv1.MetricSeries_Address{Address: &commonv1.PostalAddress{
				PostalCode: req.GetDetails().GetDestination().GetPostalCode(),
			}},
			Owner: &identityv1.UserRef{
				UserId: req.GetPreference().GetOwner().GetUserId(),
				Email:  (&decoded).GetOwner().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{Etag: req.GetPreference().GetAudit().GetEtag()},
		},
	}
	send := s.validateMetricSeriesSender(validateMetricSeriesRequest)
	validateMetricSeriesReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.metrics.v1.MetricSeriesValidationService/ValidateMetricSeries")
	}
	return &customersv1.QuotePreferenceResponse{
		Preference: &customersv1.Preference{
			Audit: &commonv1.AuditInfo{
				CreatedBy: validateMetricSeriesReply.GetMetricSeries().GetAudit().GetCreatedBy(),
			},
			PreferenceId: req.GetPreferenceId(),
		},
		Breakdown: &customersv1.QuotePreferenceResponse_Breakdown{CurrencyCode: req.GetDetails().GetCurrencyCode()},
		Result:    &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ComparePreferenceOptions compares a preference.
func (s *PreferenceCalculationServer) ComparePreferenceOptions(ctx context.Context, req *customersv1.ComparePreferenceOptionsRequest) (*customersv1.ComparePreferenceOptionsResponse, error) {
	if err := s.store.SavePreference(ctx, req.GetContext().GetTenantId(), req.GetPreference()); err != nil {
		return nil, grpcx.Wrap(err, "save preference")
	}
	s.log.InfoContext(ctx, "compare preference options", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPreference().GetAudit().GetEtag())
	snapshot, err := protojson.Marshal(req.GetPreference())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	recordApiCredentialVersionRequest := &identityv1.RecordApiCredentialVersionRequest{
		Context: req.GetContext(),
		ApiCredential: &identityv1.ApiCredential{
			Audit: &commonv1.AuditInfo{
				CreatedAt: req.GetPreference().GetAudit().GetCreatedAt(),
				UpdatedAt: req.GetPreference().GetAudit().GetUpdatedAt(),
				CreatedBy: req.GetPreference().GetAudit().GetCreatedBy(),
			},
			Limits: []*identityv1.ApiCredentialLimit{&identityv1.ApiCredentialLimit{Amount: req.GetPreference().GetDiscount().GetAmount()}},
		},
		Details: &identityv1.RecordApiCredentialVersionRequest_Details{Snapshot: snapshot},
	}
	recordApiCredentialVersionReply, err := s.recordApiCredentialVersionFn(ctx, recordApiCredentialVersionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.ApiCredentialHistoryService/RecordApiCredentialVersion")
	}
	unsubscribeSearchQueryUpdatesRequest := &searchv1.UnsubscribeSearchQueryUpdatesRequest{
		Context: req.GetContext(),
		Details: &searchv1.UnsubscribeSearchQueryUpdatesRequest_Details{
			Recipient: &commonv1.ContactInfo{
				Name: recordApiCredentialVersionReply.GetApiCredential().GetContact().GetName(),
			},
		},
	}
	unsubscribeSearchQueryUpdatesReply := new(searchv1.UnsubscribeSearchQueryUpdatesResponse)
	if err := grpcx.LegacyInvoke(ctx, s.conn, "/acme.search.v1.SearchQueryNotificationService/UnsubscribeSearchQueryUpdates", unsubscribeSearchQueryUpdatesRequest, unsubscribeSearchQueryUpdatesReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryNotificationService/UnsubscribeSearchQueryUpdates")
	}
	encoded, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded customersv1.ComparePreferenceOptionsRequest_Details
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	packed, err := anypb.New(req.GetPreference())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	quoteConsentRecordRequest := newQuoteConsentRecordRequest(withQuoteConsentRecordContext(req.GetContext()), withQuoteConsentRecordDetails(&customersv1.QuoteConsentRecordRequest_Details{
		Destination: &commonv1.PostalAddress{
			CountryCode: req.GetDetails().GetDestination().GetCountryCode(),
			Region:      req.GetDetails().GetDestination().GetRegion(),
			Recipient:   (&decoded).GetDestination().GetRecipient(),
			PostalCode:  req.GetDetails().GetDestination().GetPostalCode(),
		},
	}), withQuoteConsentRecordConsentRecord(&customersv1.ConsentRecord{Detail: packed}))
	quoteConsentRecordReply, err := s.consentRecordCalculationClient.QuoteConsentRecord(ctx, quoteConsentRecordRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordCalculationService/QuoteConsentRecord")
	}
	customerCalculation, ok := s.clients.Get("acme.customers.v1.CustomerCalculationService").(customersv1.CustomerCalculationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.customers.v1.CustomerCalculationService")
	}
	recalculateCustomersRequest := &customersv1.RecalculateCustomersRequest{
		Context: req.GetContext(),
		Details: &customersv1.RecalculateCustomersRequest_Details{
			Destination: &commonv1.PostalAddress{Region: req.GetDetails().GetDestination().GetRegion()},
		},
		Customer: &customersv1.Customer{
			Contact: &commonv1.ContactInfo{
				Phone:            recordApiCredentialVersionReply.GetApiCredential().GetContact().GetPhone(),
				PreferredChannel: recordApiCredentialVersionReply.GetApiCredential().GetContact().GetPreferredChannel(),
			},
		},
	}
	recalculateCustomersReply, err := customerCalculation.RecalculateCustomers(ctx, recalculateCustomersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerCalculationService/RecalculateCustomers")
	}
	return &customersv1.ComparePreferenceOptionsResponse{
		Preference: &customersv1.Preference{
			Owner: &identityv1.UserRef{
				Email: recordApiCredentialVersionReply.GetApiCredential().GetContact().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{
				Etag: recalculateCustomersReply.GetCustomer().GetAudit().GetEtag(),
			},
			PreferenceId: req.GetPreferenceId(),
			Name:         req.GetPreference().GetName(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: unsubscribeSearchQueryUpdatesReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Breakdown: &customersv1.ComparePreferenceOptionsResponse_Breakdown{Total: quoteConsentRecordReply.GetBreakdown().GetTotal()},
	}, nil
}

// RecalculatePreferences recalculates a preference.
func (s *PreferenceCalculationServer) RecalculatePreferences(ctx context.Context, req *customersv1.RecalculatePreferencesRequest) (*customersv1.RecalculatePreferencesResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePreference(ctx, req.GetContext().GetTenantId(), req.GetPreference()); err != nil {
		return nil, grpcx.Wrap(err, "save preference")
	}
	s.log.InfoContext(ctx, "recalculate preferences", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPreference().GetAudit().GetEtag())
	explainPriceQuoteDecisionRequest := buildExplainPriceQuoteDecisionRequestForRecalculatePreferences(req)
	explainPriceQuoteDecisionReply, err := s.priceQuoteValidationClient.ExplainPriceQuoteDecision(ctx, explainPriceQuoteDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceQuoteValidationService/ExplainPriceQuoteDecision")
	}
	resolveUsersRequest := &identityv1.ResolveUsersRequest{Context: req.GetContext()}
	resolveUsersReply, err := s.userQueryPort.ResolveUsers(ctx, resolveUsersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserQueryService/ResolveUsers")
	}
	return &customersv1.RecalculatePreferencesResponse{
		Preference: &customersv1.Preference{
			Owner: &identityv1.UserRef{
				DisplayName: explainPriceQuoteDecisionReply.GetPriceQuote().GetCounterparty().GetDisplayName(),
			},
			PreferenceId: req.GetPreferenceId(),
			Version:      req.GetPreference().GetVersion(),
		},
		Breakdown: &customersv1.RecalculatePreferencesResponse_Breakdown{
			Total: explainPriceQuoteDecisionReply.GetPriceQuote().GetTotal(),
		},
		Result: &commonv1.OperationResult{
			Message: resolveUsersReply.GetResult().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// SimulatePreference simulates a preference.
func (s *PreferenceCalculationServer) SimulatePreference(ctx context.Context, req *customersv1.SimulatePreferenceRequest) (*customersv1.SimulatePreferenceResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SimulatePreference", "error", err)
		return nil, err
	}
	if err := s.store.SavePreference(ctx, req.GetContext().GetTenantId(), req.GetPreference()); err != nil {
		return nil, grpcx.Wrap(err, "save preference")
	}
	s.log.InfoContext(ctx, "simulate preference", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPreference().GetAudit().GetEtag())
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "customers.simulate_preference.quote_customer",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"customer.audit.created_by":                  req.GetPreference().GetAudit().GetCreatedBy(),
			"customer.contact.email":                     req.GetPreference().GetOwner().GetEmail(),
			"customer.relationship_manager.user_id":      req.GetPreference().GetOwner().GetUserId(),
			"customer.relationship_manager.display_name": req.GetPreference().GetOwner().GetDisplayName(),
			"context.tenant_id":                          req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	step := &preferenceCalculationServerSimulatePreferenceRecalculateCustomersStep{request: &customersv1.RecalculateCustomersRequest{Context: req.GetContext()}}
	if err := s.runSimulatePreferenceRecalculateCustomers(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerCalculationService/RecalculateCustomers")
	}
	recalculateCustomersReply := step.reply
	estimateRefundReply, err := s.refundCalculationClient.EstimateRefund(ctx, &paymentsv1.EstimateRefundRequest{Context: req.GetContext()})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.RefundCalculationService/EstimateRefund")
	}
	return &customersv1.SimulatePreferenceResponse{
		Preference: &customersv1.Preference{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: recalculateCustomersReply.GetCustomer().GetAudit().GetUpdatedAt(),
			},
			Owner: &identityv1.UserRef{
				Email: recalculateCustomersReply.GetCustomer().GetContact().GetEmail(),
			},
			PreferenceId: req.GetPreferenceId(),
			Name:         req.GetPreference().GetName(),
		},
		Result: &commonv1.OperationResult{
			Message: estimateRefundReply.GetResult().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// GetPreferenceRates returns a preference.
func (s *PreferenceCalculationServer) GetPreferenceRates(ctx context.Context, req *customersv1.GetPreferenceRatesRequest) (*customersv1.GetPreferenceRatesResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "preference", req.GetPreferenceId(), "get_preference_rates"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get preference rates", "tenant", req.GetContext().GetTenantId())
	return &customersv1.GetPreferenceRatesResponse{
		Preference: &customersv1.Preference{PreferenceId: req.GetPreferenceId()},
		Result:     &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// validateMetricSeriesSender returns a function that sends one request to
// acme.metrics.v1.MetricSeriesValidationService/ValidateMetricSeries.
func (s *PreferenceCalculationServer) validateMetricSeriesSender(request *metricsv1.ValidateMetricSeriesRequest) func(context.Context) (*metricsv1.ValidateMetricSeriesResponse, error) {
	return func(ctx context.Context) (*metricsv1.ValidateMetricSeriesResponse, error) {
		return s.metricSeriesValidationClient.ValidateMetricSeries(ctx, request)
	}
}

// preferenceCalculationServerSimulatePreferenceRecalculateCustomersStep carries the request of one step of
// SimulatePreference and, once it ran, its reply.
type preferenceCalculationServerSimulatePreferenceRecalculateCustomersStep struct {
	request *customersv1.RecalculateCustomersRequest
	reply   *customersv1.RecalculateCustomersResponse
}

func (s *PreferenceCalculationServer) runSimulatePreferenceRecalculateCustomers(ctx context.Context, step *preferenceCalculationServerSimulatePreferenceRecalculateCustomersStep) error {
	reply, err := s.customerCalculationClient.RecalculateCustomers(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
