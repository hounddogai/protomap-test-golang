package pricing

import (
	"context"
	"errors"
	"io"
	"log/slog"

	admissionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/admissions/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	devicesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/devices/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	pricingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/pricing/v1"
	promotionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/promotions/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	venuesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/venues/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// CurrencyRateValidationServer implements `acme.pricing.v1.CurrencyRateValidationService`.
// Checks currency rates against the rules before anything acts on them.
type CurrencyRateValidationServer struct {
	pricingv1.UnsafeCurrencyRateValidationServiceServer
	store                               *Store
	log                                 *slog.Logger
	searchIndexQueryClient              searchv1.SearchIndexQueryServiceClient
	priceOverrideCalculationClient      pricingv1.PriceOverrideCalculationServiceClient
	priceQuoteValidationClient          pricingv1.PriceQuoteValidationServiceClient
	managedDeviceQueryClient            devicesv1.ManagedDeviceQueryServiceClient
	giftCardTransactionValidationClient giftcardsv1.GiftCardTransactionValidationServiceClient
	conn                                *grpc.ClientConn
	venueQueryClient                    venuesv1.VenueQueryServiceClient
	currencyRateCalculationClient       pricingv1.CurrencyRateCalculationServiceClient
	campaignValidationClient            promotionsv1.CampaignValidationServiceClient
	meteredPriceOverrideCalculation     meteredPriceOverrideCalculation
	meteredUserValidation               meteredUserValidation
}

// NewCurrencyRateValidationServer wires a CurrencyRateValidationServer.
func NewCurrencyRateValidationServer(deps platform.Deps) *CurrencyRateValidationServer {
	return &CurrencyRateValidationServer{
		store:                               NewStore(deps.DB),
		log:                                 deps.Logger.With("service", "acme.pricing.v1.CurrencyRateValidationService"),
		searchIndexQueryClient:              searchv1.NewSearchIndexQueryServiceClient(deps.Conn),
		priceOverrideCalculationClient:      pricingv1.NewPriceOverrideCalculationServiceClient(deps.Conn),
		priceQuoteValidationClient:          pricingv1.NewPriceQuoteValidationServiceClient(deps.Conn),
		managedDeviceQueryClient:            devicesv1.NewManagedDeviceQueryServiceClient(deps.Conn),
		giftCardTransactionValidationClient: giftcardsv1.NewGiftCardTransactionValidationServiceClient(deps.Conn),
		conn:                                deps.Conn,
		venueQueryClient:                    venuesv1.NewVenueQueryServiceClient(deps.Conn),
		currencyRateCalculationClient:       pricingv1.NewCurrencyRateCalculationServiceClient(deps.Conn),
		campaignValidationClient:            promotionsv1.NewCampaignValidationServiceClient(deps.Conn),
		meteredPriceOverrideCalculation:     meteredPriceOverrideCalculation{PriceOverrideCalculationServiceClient: pricingv1.NewPriceOverrideCalculationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		meteredUserValidation:               meteredUserValidation{UserValidationServiceClient: identityv1.NewUserValidationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
	}
}

// ValidateCurrencyRate validates a currency rate.
func (s *CurrencyRateValidationServer) ValidateCurrencyRate(ctx context.Context, req *pricingv1.ValidateCurrencyRateRequest) (*pricingv1.ValidateCurrencyRateResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveCurrencyRate(ctx, req.GetContext().GetTenantId(), req.GetCurrencyRate()); err != nil {
		return nil, grpcx.Wrap(err, "save currency rate")
	}
	s.log.InfoContext(ctx, "validate currency rate", "tenant", req.GetContext().GetTenantId())
	attributes := make(map[string]string, len(req.GetCurrencyRate().GetAttributes()))
	for key, value := range req.GetCurrencyRate().GetAttributes() {
		attributes[key] = value
	}
	var envelope pricingv1.PriceListRef
	if err := req.GetCurrencyRate().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	exportSearchIndexesStream, err := s.searchIndexQueryClient.ExportSearchIndexes(ctx, &searchv1.ExportSearchIndexesRequest{
		Context: req.GetContext(),
		Filter: &searchv1.ExportSearchIndexesRequest_Filter{
			CreatedAfter: req.GetCurrencyRate().GetOccurredAt(),
			OwnerId:      req.GetCurrencyRate().GetSubject().GetCustomerId(),
			Labels:       attributes,
			Query:        envelope.GetDisplayName(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexQueryService/ExportSearchIndexes")
	}
	for {
		received, err := exportSearchIndexesStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexQueryService/ExportSearchIndexes")
		}
		_ = received
	}
	var envelope2 pricingv1.PriceListRef
	if err := req.GetCurrencyRate().GetDetail().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	calculatePriceOverrideRequest := &pricingv1.CalculatePriceOverrideRequest{}
	calculatePriceOverrideRequest.Context = req.GetContext()
	calculatePriceOverrideRequest.PriceOverride = &pricingv1.PriceOverride{
		Owner: &identityv1.UserRef{
			DisplayName: req.GetCurrencyRate().GetSubject().GetDisplayName(),
			Email:       req.GetDetails().GetContact().GetEmail(),
		},
	}
	calculatePriceOverrideRequest.PriceOverrideId = envelope2.GetDisplayName()
	calculatePriceOverrideReply, err := s.priceOverrideCalculationClient.CalculatePriceOverride(ctx, calculatePriceOverrideRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceOverrideCalculationService/CalculatePriceOverride")
	}
	return &pricingv1.ValidateCurrencyRateResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: calculatePriceOverrideReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Outcome: &pricingv1.ValidateCurrencyRateResponse_Outcome{Status: calculatePriceOverrideReply.GetResult().GetStatus()},
		CurrencyRate: &pricingv1.CurrencyRate{
			CurrencyRateId: req.GetCurrencyRateId(),
			Kind:           req.GetCurrencyRate().GetKind(),
		},
	}, nil
}

// PreviewCurrencyRate previews a currency rate.
func (s *CurrencyRateValidationServer) PreviewCurrencyRate(ctx context.Context, req *pricingv1.PreviewCurrencyRateRequest) (*pricingv1.PreviewCurrencyRateResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected PreviewCurrencyRate", "error", err)
		return nil, err
	}
	if err := s.store.SaveCurrencyRate(ctx, req.GetContext().GetTenantId(), req.GetCurrencyRate()); err != nil {
		return nil, grpcx.Wrap(err, "save currency rate")
	}
	s.log.InfoContext(ctx, "preview currency rate", "tenant", req.GetContext().GetTenantId())
	var envelope pricingv1.PriceListRef
	if err := req.GetCurrencyRate().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	normalizePriceQuoteRequest := &pricingv1.NormalizePriceQuoteRequest{
		Context: req.GetContext(),
		Details: &pricingv1.NormalizePriceQuoteRequest_Details{
			Contact: &commonv1.ContactInfo{Name: req.GetDetails().GetContact().GetName()},
		},
		PriceQuoteId: envelope.GetDisplayName(),
	}
	normalizePriceQuoteReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*pricingv1.NormalizePriceQuoteResponse, error) {
		return s.priceQuoteValidationClient.NormalizePriceQuote(ctx, normalizePriceQuoteRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceQuoteValidationService/NormalizePriceQuote")
	}
	return &pricingv1.PreviewCurrencyRateResponse{
		CurrencyRate: &pricingv1.CurrencyRate{
			CorrelationId:  normalizePriceQuoteReply.GetResult().GetCorrelationId(),
			CurrencyRateId: req.GetCurrencyRateId(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: req.GetCurrencyRate().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// CheckCurrencyRateEligibility checks a currency rate.
func (s *CurrencyRateValidationServer) CheckCurrencyRateEligibility(ctx context.Context, req *pricingv1.CheckCurrencyRateEligibilityRequest) (*pricingv1.CheckCurrencyRateEligibilityResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveCurrencyRate(ctx, req.GetContext().GetTenantId(), req.GetCurrencyRate()); err != nil {
		return nil, grpcx.Wrap(err, "save currency rate")
	}
	s.log.InfoContext(ctx, "check currency rate eligibility", "tenant", req.GetContext().GetTenantId())
	findManagedDevicesRequest := buildFindManagedDevicesRequestForCheckCurrencyRateEligibility(req)
	findManagedDevicesReply, err := s.managedDeviceQueryClient.FindManagedDevices(ctx, findManagedDevicesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.devices.v1.ManagedDeviceQueryService/FindManagedDevices")
	}
	encoded, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded pricingv1.CheckCurrencyRateEligibilityRequest_Details
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	packed, err := anypb.New(req.GetCurrencyRate())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	var envelope pricingv1.PriceListRef
	if err := req.GetCurrencyRate().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	validateGiftCardTransactionRequest := &giftcardsv1.ValidateGiftCardTransactionRequest{}
	validateGiftCardTransactionRequest.Context = req.GetContext()
	validateGiftCardTransactionRequest.Details = &giftcardsv1.ValidateGiftCardTransactionRequest_Details{
		Contact: &commonv1.ContactInfo{
			Name:             req.GetDetails().GetContact().GetName(),
			PreferredChannel: (&decoded).GetContact().GetPreferredChannel(),
			Email:            req.GetCurrencyRate().GetSubject().GetEmail(),
		},
	}
	validateGiftCardTransactionRequest.GiftCardTransaction = &giftcardsv1.GiftCardTransaction{Detail: packed}
	validateGiftCardTransactionRequest.GiftCardTransactionId = envelope.GetDisplayName()
	validateGiftCardTransactionReply, err := s.giftCardTransactionValidationClient.ValidateGiftCardTransaction(ctx, validateGiftCardTransactionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionValidationService/ValidateGiftCardTransaction")
	}
	browseManagedDevicesRequest := &devicesv1.BrowseManagedDevicesRequest{}
	browseManagedDevicesRequest.Context = req.GetContext()
	browseManagedDevicesRequest.Filter = &devicesv1.BrowseManagedDevicesRequest_Filter{
		CreatedAfter: validateGiftCardTransactionReply.GetGiftCardTransaction().GetOccurredAt(),
		OwnerId:      validateGiftCardTransactionReply.GetGiftCardTransaction().GetSubject().GetGiftCardBatchId(),
		Labels:       validateGiftCardTransactionReply.GetGiftCardTransaction().GetAttributes(),
	}
	browseManagedDevicesReply, err := s.managedDeviceQueryClient.BrowseManagedDevices(ctx, browseManagedDevicesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.devices.v1.ManagedDeviceQueryService/BrowseManagedDevices")
	}
	return &pricingv1.CheckCurrencyRateEligibilityResponse{
		Result: &commonv1.OperationResult{
			Status:  findManagedDevicesReply.GetResult().GetStatus(),
			Message: validateGiftCardTransactionReply.GetOutcome().GetMessage(),
		},
		CurrencyRate: &pricingv1.CurrencyRate{
			CorrelationId:  findManagedDevicesReply.GetResult().GetCorrelationId(),
			CurrencyRateId: req.GetCurrencyRateId(),
			Sequence:       req.GetCurrencyRate().GetSequence(),
		},
		Outcome: &pricingv1.CheckCurrencyRateEligibilityResponse_Outcome{Message: browseManagedDevicesReply.GetResult().GetMessage()},
	}, nil
}

// NormalizeCurrencyRate normalizes a currency rate.
func (s *CurrencyRateValidationServer) NormalizeCurrencyRate(ctx context.Context, req *pricingv1.NormalizeCurrencyRateRequest) (*pricingv1.NormalizeCurrencyRateResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveCurrencyRate(ctx, req.GetContext().GetTenantId(), req.GetCurrencyRate()); err != nil {
		return nil, grpcx.Wrap(err, "save currency rate")
	}
	s.log.InfoContext(ctx, "normalize currency rate", "tenant", req.GetContext().GetTenantId())
	for _, measurement := range req.GetCurrencyRate().GetMeasurements() {
		if _, err := s.searchIndexQueryClient.SuggestSearchIndexes(ctx, &searchv1.SuggestSearchIndexesRequest{
			Context: req.GetContext(),
			Filter:  &searchv1.SuggestSearchIndexesRequest_Filter{CreatedAfter: measurement.GetObservedAt()},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexQueryService/SuggestSearchIndexes")
		}
	}
	return &pricingv1.NormalizeCurrencyRateResponse{
		CurrencyRate: &pricingv1.CurrencyRate{
			CurrencyRateId: req.GetCurrencyRateId(),
			Kind:           req.GetCurrencyRate().GetKind(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ExplainCurrencyRateDecision explains a currency rate.
func (s *CurrencyRateValidationServer) ExplainCurrencyRateDecision(ctx context.Context, req *pricingv1.ExplainCurrencyRateDecisionRequest) (*pricingv1.ExplainCurrencyRateDecisionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveCurrencyRate(ctx, req.GetContext().GetTenantId(), req.GetCurrencyRate()); err != nil {
		return nil, grpcx.Wrap(err, "save currency rate")
	}
	s.log.InfoContext(ctx, "explain currency rate decision", "tenant", req.GetContext().GetTenantId())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	summarizeAdmissionsRequest := &admissionsv1.SummarizeAdmissionsRequest{
		Context:     req.GetContext(),
		Annotations: annotations,
		Reason:      req.GetReason(),
		Details:     &admissionsv1.SummarizeAdmissionsRequest_Details{Filters: req.GetCurrencyRate().GetAttributes()},
	}
	summarizeAdmissionsReply := new(admissionsv1.SummarizeAdmissionsResponse)
	if err := grpcx.Invoke(ctx, s.conn, admissionsv1.AdmissionQueryService_SummarizeAdmissions_FullMethodName, summarizeAdmissionsRequest, summarizeAdmissionsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.admissions.v1.AdmissionQueryService/SummarizeAdmissions")
	}
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := currencyRateValidationServerSuggestVenuesSteps[mode]
	var envelope pricingv1.PriceListRef
	if err := req.GetCurrencyRate().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	suggestVenuesRequest := &venuesv1.SuggestVenuesRequest{
		Context: req.GetContext(),
		Filter: &venuesv1.SuggestVenuesRequest_Filter{
			CreatedAfter: req.GetCurrencyRate().GetOccurredAt(),
			OwnerId:      req.GetCurrencyRate().GetSubject().GetCustomerId(),
			Labels:       req.GetAnnotations(),
		},
		Page: &commonv1.PageRequest{PageToken: envelope.GetDisplayName()},
	}
	suggestVenuesReply, err := run(s, ctx, suggestVenuesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.venues.v1.VenueQueryService/SuggestVenues")
	}
	return &pricingv1.ExplainCurrencyRateDecisionResponse{
		Result: &commonv1.OperationResult{
			Details:       summarizeAdmissionsReply.GetResult().GetDetails(),
			CorrelationId: req.GetCurrencyRate().GetCorrelationId(),
			Status:        "ok",
		},
		Outcome:      &pricingv1.ExplainCurrencyRateDecisionResponse_Outcome{Status: suggestVenuesReply.GetResult().GetStatus()},
		CurrencyRate: &pricingv1.CurrencyRate{CurrencyRateId: req.GetCurrencyRateId()},
	}, nil
}

// ScoreCurrencyRate scores a currency rate.
func (s *CurrencyRateValidationServer) ScoreCurrencyRate(ctx context.Context, req *pricingv1.ScoreCurrencyRateRequest) (*pricingv1.ScoreCurrencyRateResponse, error) {
	if err := s.store.SaveCurrencyRate(ctx, req.GetContext().GetTenantId(), req.GetCurrencyRate()); err != nil {
		return nil, grpcx.Wrap(err, "save currency rate")
	}
	s.log.InfoContext(ctx, "score currency rate", "tenant", req.GetContext().GetTenantId())
	var envelope pricingv1.PriceListRef
	if err := req.GetCurrencyRate().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	quoteCurrencyRateRequest := &pricingv1.QuoteCurrencyRateRequest{
		Context: req.GetContext(),
		CurrencyRate: &pricingv1.CurrencyRate{
			Subject: &customersv1.CustomerRef{
				DisplayName: req.GetCurrencyRate().GetSubject().GetDisplayName(),
			},
		},
		Details: &pricingv1.QuoteCurrencyRateRequest_Details{
			Destination: &commonv1.PostalAddress{
				Recipient:  req.GetDetails().GetDestination().GetRecipient(),
				PostalCode: req.GetDetails().GetDestination().GetPostalCode(),
			},
		},
		CurrencyRateId: envelope.GetDisplayName(),
	}
	quoteCurrencyRateReply, err := grpcx.Call(ctx, s.currencyRateCalculationClient.QuoteCurrencyRate, quoteCurrencyRateRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.CurrencyRateCalculationService/QuoteCurrencyRate")
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	var envelope2 pricingv1.PriceListRef
	if err := req.GetCurrencyRate().GetDetail().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	sendSessionTestNoticeRequest := &identityv1.SendSessionTestNoticeRequest{
		Context: req.GetContext(),
		Details: &identityv1.SendSessionTestNoticeRequest_Details{
			Recipient: &commonv1.ContactInfo{
				Email: quoteCurrencyRateReply.GetCurrencyRate().GetSubject().GetEmail(),
			},
		},
		Annotations: annotations,
		SessionId:   envelope2.GetDisplayName(),
	}
	sendSessionTestNoticeReply := new(identityv1.SendSessionTestNoticeResponse)
	if err := s.conn.Invoke(ctx, identitySessionNotificationServiceSendSessionTestNoticeMethod, sendSessionTestNoticeRequest, sendSessionTestNoticeReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionNotificationService/SendSessionTestNotice")
	}
	return &pricingv1.ScoreCurrencyRateResponse{
		Result: &commonv1.OperationResult{
			Details: quoteCurrencyRateReply.GetResult().GetDetails(),
			Message: sendSessionTestNoticeReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		CurrencyRate: &pricingv1.CurrencyRate{
			CorrelationId:  sendSessionTestNoticeReply.GetResult().GetCorrelationId(),
			CurrencyRateId: req.GetCurrencyRateId(),
			Source:         req.GetCurrencyRate().GetSource(),
		},
	}, nil
}

// LintCurrencyRates lints a currency rate.
func (s *CurrencyRateValidationServer) LintCurrencyRates(ctx context.Context, req *pricingv1.LintCurrencyRatesRequest) (*pricingv1.LintCurrencyRatesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected LintCurrencyRates", "error", err)
		return nil, err
	}
	if err := s.store.SaveCurrencyRate(ctx, req.GetContext().GetTenantId(), req.GetCurrencyRate()); err != nil {
		return nil, grpcx.Wrap(err, "save currency rate")
	}
	s.log.InfoContext(ctx, "lint currency rates", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetCurrencyRate()).(*pricingv1.CurrencyRate)
	var holder struct{ PreferredChannel string }
	if record := req.GetDetails().GetContact(); record != nil {
		if err := mapper.CopyFields(&holder, record, "PreferredChannel"); err != nil {
			s.log.WarnContext(ctx, "copy preferred_channel", "error", err)
		}
	}
	explainCampaignDecisionRequest := &promotionsv1.ExplainCampaignDecisionRequest{
		Context: req.GetContext(),
		Details: &promotionsv1.ExplainCampaignDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email:            cloned.GetSubject().GetEmail(),
				Name:             req.GetDetails().GetContact().GetName(),
				PreferredChannel: holder.PreferredChannel,
			},
		},
		Campaign: &promotionsv1.Campaign{
			Organizer: &identityv1.UserRef{Email: req.GetDetails().GetContact().GetEmail()},
		},
	}
	explainCampaignDecisionReply, err := grpcx.Call(ctx, s.campaignValidationClient.ExplainCampaignDecision, explainCampaignDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CampaignValidationService/ExplainCampaignDecision")
	}
	var envelope pricingv1.PriceListRef
	if err := req.GetCurrencyRate().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	recalculatePriceOverridesRequest := &pricingv1.RecalculatePriceOverridesRequest{
		Context: req.GetContext(),
		PriceOverride: &pricingv1.PriceOverride{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: explainCampaignDecisionReply.GetCampaign().GetAudit().GetUpdatedAt(),
			},
		},
		PriceOverrideId: envelope.GetDisplayName(),
	}
	recalculatePriceOverridesReply, err := s.meteredPriceOverrideCalculation.RecalculatePriceOverrides(ctx, recalculatePriceOverridesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceOverrideCalculationService/RecalculatePriceOverrides")
	}
	detailsCache := cache.New[string, *pricingv1.LintCurrencyRatesRequest_Details](1)
	detailsCache.Put(req.GetContext().GetTraceId(), req.GetDetails())
	cachedDetails, _ := detailsCache.Get(req.GetContext().GetTraceId())
	lintUsersRequest := &identityv1.LintUsersRequest{
		Context: req.GetContext(),
		User: &identityv1.User{
			RelationshipManager: &identityv1.UserRef{
				DisplayName: recalculatePriceOverridesReply.GetPriceOverride().GetOwner().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: explainCampaignDecisionReply.GetCampaign().GetAudit().GetUpdatedBy(),
			},
			Contact: &commonv1.ContactInfo{Phone: cachedDetails.GetContact().GetPhone()},
		},
	}
	lintUsersReply, err := s.meteredUserValidation.LintUsers(ctx, lintUsersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserValidationService/LintUsers")
	}
	var envelope2 pricingv1.PriceListRef
	if err := req.GetCurrencyRate().GetDetail().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	subscribeSessionUpdatesRequest := &identityv1.SubscribeSessionUpdatesRequest{
		Context: req.GetContext(),
		Details: &identityv1.SubscribeSessionUpdatesRequest_Details{
			Recipient: &commonv1.ContactInfo{Name: lintUsersReply.GetUser().GetContact().GetName()},
		},
		SessionId: envelope2.GetDisplayName(),
	}
	subscribeSessionUpdatesReply := new(identityv1.SubscribeSessionUpdatesResponse)
	if err := grpcx.LegacyInvoke(ctx, s.conn, "/acme.identity.v1.SessionNotificationService/SubscribeSessionUpdates", subscribeSessionUpdatesRequest, subscribeSessionUpdatesReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionNotificationService/SubscribeSessionUpdates")
	}
	return &pricingv1.LintCurrencyRatesResponse{
		Outcome: &pricingv1.LintCurrencyRatesResponse_Outcome{
			NextSteps: explainCampaignDecisionReply.GetOutcome().GetNextSteps(),
			Status:    lintUsersReply.GetOutcome().GetStatus(),
		},
		CurrencyRate: &pricingv1.CurrencyRate{
			Subject: &customersv1.CustomerRef{
				DisplayName: explainCampaignDecisionReply.GetCampaign().GetOrganizer().GetDisplayName(),
			},
			CurrencyRateId: req.GetCurrencyRateId(),
			OccurredAt:     req.GetCurrencyRate().GetOccurredAt(),
		},
		Result: &commonv1.OperationResult{
			Details:       recalculatePriceOverridesReply.GetResult().GetDetails(),
			Message:       recalculatePriceOverridesReply.GetResult().GetMessage(),
			CorrelationId: subscribeSessionUpdatesReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// currencyRateValidationServerSuggestVenuesSteps are the ways to run acme.venues.v1.VenueQueryService/SuggestVenues, by
// name.
var currencyRateValidationServerSuggestVenuesSteps = map[string]func(*CurrencyRateValidationServer, context.Context, *venuesv1.SuggestVenuesRequest) (*venuesv1.SuggestVenuesResponse, error){
	"default": (*CurrencyRateValidationServer).suggestVenuesStep,
	"audited": (*CurrencyRateValidationServer).suggestVenuesAuditedStep,
}

func (s *CurrencyRateValidationServer) suggestVenuesStep(ctx context.Context, request *venuesv1.SuggestVenuesRequest) (*venuesv1.SuggestVenuesResponse, error) {
	return s.venueQueryClient.SuggestVenues(ctx, request)
}

func (s *CurrencyRateValidationServer) suggestVenuesAuditedStep(ctx context.Context, request *venuesv1.SuggestVenuesRequest) (*venuesv1.SuggestVenuesResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.venues.v1.VenueQueryService/SuggestVenues")
	return s.venueQueryClient.SuggestVenues(ctx, request)
}
