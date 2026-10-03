package giftcards

import (
	"context"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	invoicingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/invoicing/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	lendingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/lending/v1"
	licensingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/licensing/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	onboardingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/onboarding/v1"
	payrollv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payroll/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// GiftCardCalculationServer implements `acme.giftcards.v1.GiftCardCalculationService`.
// Computes the figures of a gift card, such as totals, taxes, and estimates.
type GiftCardCalculationServer struct {
	giftcardsv1.UnimplementedGiftCardCalculationServiceServer
	store                            *Store
	log                              *slog.Logger
	loanApplicationQueryClient       lendingv1.LoanApplicationQueryServiceClient
	meteredFeatureFlagValidation     meteredFeatureFlagValidation
	ledgerAccountQueryClient         ledgerv1.LedgerAccountQueryServiceClient
	bus                              *eventbus.Bus
	payRunCalculationClient          payrollv1.PayRunCalculationServiceClient
	invoiceBatchCalculationClient    invoicingv1.InvoiceBatchCalculationServiceClient
	tenantExportClient               identityv1.TenantExportServiceClient
	userValidationClient             identityv1.UserValidationServiceClient
	userValidationClientFallback     identityv1.UserValidationServiceClient
	conn                             *grpc.ClientConn
	featureFlagValidationClient      featureflagsv1.FeatureFlagValidationServiceClient
	userQueryClient                  identityv1.UserQueryServiceClient
	softwareLicenseCalculationClient licensingv1.SoftwareLicenseCalculationServiceClient
	meteredLoanApplicationQuery      meteredLoanApplicationQuery
	tenantExportClientFallback       identityv1.TenantExportServiceClient
	metricSeriesValidationClient     metricsv1.MetricSeriesValidationServiceClient
	onboardingPlanCalculationClient  onboardingv1.OnboardingPlanCalculationServiceClient
}

// NewGiftCardCalculationServer wires a GiftCardCalculationServer.
func NewGiftCardCalculationServer(deps platform.Deps) *GiftCardCalculationServer {
	return &GiftCardCalculationServer{
		store:                            NewStore(deps.DB),
		log:                              deps.Logger.With("service", "acme.giftcards.v1.GiftCardCalculationService"),
		loanApplicationQueryClient:       lendingv1.NewLoanApplicationQueryServiceClient(deps.Conn),
		meteredFeatureFlagValidation:     meteredFeatureFlagValidation{FeatureFlagValidationServiceClient: featureflagsv1.NewFeatureFlagValidationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		ledgerAccountQueryClient:         ledgerv1.NewLedgerAccountQueryServiceClient(deps.Conn),
		bus:                              deps.Bus,
		payRunCalculationClient:          payrollv1.NewPayRunCalculationServiceClient(deps.Conn),
		invoiceBatchCalculationClient:    invoicingv1.NewInvoiceBatchCalculationServiceClient(deps.Conn),
		tenantExportClient:               identityv1.NewTenantExportServiceClient(deps.Conn),
		userValidationClient:             identityv1.NewUserValidationServiceClient(deps.Conn),
		userValidationClientFallback:     identityv1.NewUserValidationServiceClient(deps.Conn),
		conn:                             deps.Conn,
		featureFlagValidationClient:      featureflagsv1.NewFeatureFlagValidationServiceClient(deps.Conn),
		userQueryClient:                  identityv1.NewUserQueryServiceClient(deps.Conn),
		softwareLicenseCalculationClient: licensingv1.NewSoftwareLicenseCalculationServiceClient(deps.Conn),
		meteredLoanApplicationQuery:      meteredLoanApplicationQuery{LoanApplicationQueryServiceClient: lendingv1.NewLoanApplicationQueryServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		tenantExportClientFallback:       identityv1.NewTenantExportServiceClient(deps.Conn),
		metricSeriesValidationClient:     metricsv1.NewMetricSeriesValidationServiceClient(deps.Conn),
		onboardingPlanCalculationClient:  onboardingv1.NewOnboardingPlanCalculationServiceClient(deps.Conn),
	}
}

// CalculateGiftCard calculates a gift card.
func (s *GiftCardCalculationServer) CalculateGiftCard(ctx context.Context, req *giftcardsv1.CalculateGiftCardRequest) (*giftcardsv1.CalculateGiftCardResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveGiftCard(ctx, req.GetContext().GetTenantId(), req.GetGiftCard()); err != nil {
		return nil, grpcx.Wrap(err, "save gift card")
	}
	s.log.InfoContext(ctx, "calculate gift card", "tenant", req.GetContext().GetTenantId(), "etag", req.GetGiftCard().GetAudit().GetEtag())
	suggestLoanApplicationsReply, err := s.loanApplicationQueryClient.SuggestLoanApplications(ctx, &lendingv1.SuggestLoanApplicationsRequest{
		Context: req.GetContext(),
		Filter: &lendingv1.SuggestLoanApplicationsRequest_Filter{
			Labels: req.GetGiftCard().GetLabels(),
			Region: req.GetDetails().GetDestination().GetRegion(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.lending.v1.LoanApplicationQueryService/SuggestLoanApplications")
	}
	source := req.GetGiftCard().GetAudit().ProtoReflect()
	etag := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("etag"))).String()
	validateFeatureFlagRequest := &featureflagsv1.ValidateFeatureFlagRequest{
		Context: req.GetContext(),
		Details: &featureflagsv1.ValidateFeatureFlagRequest_Details{
			Contact: &commonv1.ContactInfo{Phone: req.GetGiftCard().GetContact().GetPhone()},
		},
		FeatureFlag: &featureflagsv1.FeatureFlag{
			Audit: &commonv1.AuditInfo{
				Etag:      etag,
				UpdatedBy: req.GetGiftCard().GetAudit().GetUpdatedBy(),
				CreatedBy: req.GetGiftCard().GetAudit().GetCreatedBy(),
			},
		},
	}
	validateFeatureFlagReply, err := s.meteredFeatureFlagValidation.ValidateFeatureFlag(ctx, validateFeatureFlagRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.featureflags.v1.FeatureFlagValidationService/ValidateFeatureFlag")
	}
	return &giftcardsv1.CalculateGiftCardResponse{
		Result: &commonv1.OperationResult{
			Message: suggestLoanApplicationsReply.GetResult().GetMessage(),
			Details: suggestLoanApplicationsReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		GiftCard: &giftcardsv1.GiftCard{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: validateFeatureFlagReply.GetFeatureFlag().GetAudit().GetUpdatedBy(),
			},
			GiftCardId: req.GetGiftCardId(),
			Contact:    req.GetGiftCard().GetContact(),
		},
	}, nil
}

// EstimateGiftCard estimates a gift card.
func (s *GiftCardCalculationServer) EstimateGiftCard(ctx context.Context, req *giftcardsv1.EstimateGiftCardRequest) (*giftcardsv1.EstimateGiftCardResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveGiftCard(ctx, req.GetContext().GetTenantId(), req.GetGiftCard()); err != nil {
		return nil, grpcx.Wrap(err, "save gift card")
	}
	s.log.InfoContext(ctx, "estimate gift card", "tenant", req.GetContext().GetTenantId(), "etag", req.GetGiftCard().GetAudit().GetEtag())
	summarizeLedgerAccountsRequest := &ledgerv1.SummarizeLedgerAccountsRequest{}
	summarizeLedgerAccountsRequest.Context = req.GetContext()
	summarizeLedgerAccountsRequest.Annotations = req.GetAnnotations()
	summarizeLedgerAccountsReply, err := s.ledgerAccountQueryClient.SummarizeLedgerAccounts(ctx, summarizeLedgerAccountsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountQueryService/SummarizeLedgerAccounts")
	}
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "giftcards.estimate_gift_card.normalize_metric_series",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"metric_series.audit.updated_by": req.GetGiftCard().GetAudit().GetUpdatedBy(),
			"context.tenant_id":              req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	return &giftcardsv1.EstimateGiftCardResponse{
		Result: &commonv1.OperationResult{
			Message: summarizeLedgerAccountsReply.GetResult().GetMessage(),
			Details: summarizeLedgerAccountsReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		GiftCard: &giftcardsv1.GiftCard{
			GiftCardId: req.GetGiftCardId(),
			Contact:    req.GetGiftCard().GetContact(),
		},
	}, nil
}

// CompareGiftCardOptions compares a gift card.
func (s *GiftCardCalculationServer) CompareGiftCardOptions(ctx context.Context, req *giftcardsv1.CompareGiftCardOptionsRequest) (*giftcardsv1.CompareGiftCardOptionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CompareGiftCardOptions", "error", err)
		return nil, err
	}
	if err := s.store.SaveGiftCard(ctx, req.GetContext().GetTenantId(), req.GetGiftCard()); err != nil {
		return nil, grpcx.Wrap(err, "save gift card")
	}
	s.log.InfoContext(ctx, "compare gift card options", "tenant", req.GetContext().GetTenantId(), "etag", req.GetGiftCard().GetAudit().GetEtag())
	jobs := make(chan *giftcardsv1.GiftCardLimit)
	failures := make(chan error, 4)
	workCtx, stopWork := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for limit := range jobs {
				if _, err := s.payRunCalculationClient.ComparePayRunOptions(workCtx, &payrollv1.ComparePayRunOptionsRequest{
					Context: req.GetContext(),
					PayRun: &payrollv1.PayRun{
						Lines: []*payrollv1.PayRunLine{&payrollv1.PayRunLine{Tax: &payrollv1.PayRunLineTax{Amount: limit.GetAmount()}}},
					},
				}); err != nil {
					failures <- err
					stopWork()
					return
				}
			}
		}()
	}
	for _, queued := range req.GetGiftCard().GetLimits() {
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
		return nil, grpcx.Wrap(err, "acme.payroll.v1.PayRunCalculationService/ComparePayRunOptions")
	}
	recalculateInvoiceBatchesRequest := &invoicingv1.RecalculateInvoiceBatchesRequest{
		Context: req.GetContext(),
		InvoiceBatch: &invoicingv1.InvoiceBatch{
			Audit: &commonv1.AuditInfo{CreatedAt: req.GetGiftCard().GetAudit().GetCreatedAt()},
		},
	}
	var recalculateInvoiceBatchesReply *invoicingv1.RecalculateInvoiceBatchesResponse
	if err := pipeline.Run(ctx, "acme.invoicing.v1.InvoiceBatchCalculationService/RecalculateInvoiceBatches", func(ctx context.Context) error {
		answer, err := s.invoiceBatchCalculationClient.RecalculateInvoiceBatches(ctx, recalculateInvoiceBatchesRequest)
		recalculateInvoiceBatchesReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.invoicing.v1.InvoiceBatchCalculationService/RecalculateInvoiceBatches")
	}
	listTenantExportsRequest := &identityv1.ListTenantExportsRequest{Context: req.GetContext()}
	listTenantExportsReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*identityv1.ListTenantExportsResponse, error) {
		return s.tenantExportClient.ListTenantExports(ctx, listTenantExportsRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantExportService/ListTenantExports")
	}
	return &giftcardsv1.CompareGiftCardOptionsResponse{
		Result: &commonv1.OperationResult{
			Details:       recalculateInvoiceBatchesReply.GetResult().GetDetails(),
			CorrelationId: listTenantExportsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		GiftCard: &giftcardsv1.GiftCard{
			GiftCardId: req.GetGiftCardId(),
			Holder:     req.GetGiftCard().GetHolder(),
		},
	}, nil
}

// RecalculateGiftCards recalculates a gift card.
func (s *GiftCardCalculationServer) RecalculateGiftCards(ctx context.Context, req *giftcardsv1.RecalculateGiftCardsRequest) (*giftcardsv1.RecalculateGiftCardsResponse, error) {
	if err := s.store.SaveGiftCard(ctx, req.GetContext().GetTenantId(), req.GetGiftCard()); err != nil {
		return nil, grpcx.Wrap(err, "save gift card")
	}
	s.log.InfoContext(ctx, "recalculate gift cards", "tenant", req.GetContext().GetTenantId(), "etag", req.GetGiftCard().GetAudit().GetEtag())
	routes := map[string]func(context.Context, *identityv1.ExplainUserDecisionRequest, ...grpc.CallOption) (*identityv1.ExplainUserDecisionResponse, error){
		"primary":  s.userValidationClient.ExplainUserDecision,
		"fallback": s.userValidationClientFallback.ExplainUserDecision,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	explainUserDecisionRequest := &identityv1.ExplainUserDecisionRequest{
		Context: req.GetContext(),
		User: &identityv1.User{
			PrimaryAddress: &commonv1.PostalAddress{Recipient: req.GetDetails().GetDestination().GetRecipient()},
		},
	}
	explainUserDecisionReply, err := route(ctx, explainUserDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserValidationService/ExplainUserDecision")
	}
	simulatePayRunRequest := &payrollv1.SimulatePayRunRequest{
		Context: req.GetContext(),
		PayRun: &payrollv1.PayRun{
			BillingAddress: &commonv1.PostalAddress{
				Recipient: explainUserDecisionReply.GetUser().GetPrimaryAddress().GetRecipient(),
				Lines:     req.GetDetails().GetDestination().GetLines(),
			},
		},
		Details: &payrollv1.SimulatePayRunRequest_Details{
			Destination: &commonv1.PostalAddress{Region: req.GetDetails().GetDestination().GetRegion()},
		},
	}
	simulatePayRunReply, err := grpcx.NewClient(s.conn, payrollv1.NewPayRunCalculationServiceClient).SimulatePayRun(ctx, simulatePayRunRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payroll.v1.PayRunCalculationService/SimulatePayRun")
	}
	for limit := range iterx.Each(req.GetGiftCard().GetLimits(), func(candidate *giftcardsv1.GiftCardLimit) bool { return candidate != nil }) {
		if _, err := s.featureFlagValidationClient.ScoreFeatureFlag(ctx, &featureflagsv1.ScoreFeatureFlagRequest{
			Context: req.GetContext(),
			FeatureFlag: &featureflagsv1.FeatureFlag{
				Action: &featureflagsv1.FeatureFlag_Discount{Discount: &featureflagsv1.FeatureFlagDiscountAction{Amount: limit.GetAmount()}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.featureflags.v1.FeatureFlagValidationService/ScoreFeatureFlag")
		}
	}
	labels := make(map[string]string, len(req.GetGiftCard().GetLabels()))
	for key, value := range req.GetGiftCard().GetLabels() {
		labels[key] = value
	}
	countUsersReply, err := s.userQueryClient.CountUsers(ctx, &identityv1.CountUsersRequest{
		Context: req.GetContext(),
		Filter: &identityv1.CountUsersRequest_Filter{
			Region:       simulatePayRunReply.GetPayRun().GetBillingAddress().GetRegion(),
			Labels:       labels,
			CreatedAfter: simulatePayRunReply.GetPayRun().GetAudit().GetUpdatedAt(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserQueryService/CountUsers")
	}
	return &giftcardsv1.RecalculateGiftCardsResponse{
		GiftCard: &giftcardsv1.GiftCard{
			Audit: &commonv1.AuditInfo{
				CreatedBy: explainUserDecisionReply.GetUser().GetAudit().GetCreatedBy(),
				UpdatedBy: simulatePayRunReply.GetPayRun().GetAudit().GetUpdatedBy(),
			},
			GiftCardId: req.GetGiftCardId(),
		},
		Result: &commonv1.OperationResult{
			Status:        explainUserDecisionReply.GetOutcome().GetStatus(),
			CorrelationId: countUsersReply.GetResult().GetCorrelationId(),
		},
		Breakdown: &giftcardsv1.RecalculateGiftCardsResponse_Breakdown{
			Total:        simulatePayRunReply.GetBreakdown().GetTotal(),
			CurrencyCode: req.GetDetails().GetCurrencyCode(),
		},
	}, nil
}

// SimulateGiftCard simulates a gift card.
func (s *GiftCardCalculationServer) SimulateGiftCard(ctx context.Context, req *giftcardsv1.SimulateGiftCardRequest) (*giftcardsv1.SimulateGiftCardResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveGiftCard(ctx, req.GetContext().GetTenantId(), req.GetGiftCard()); err != nil {
		return nil, grpcx.Wrap(err, "save gift card")
	}
	s.log.InfoContext(ctx, "simulate gift card", "tenant", req.GetContext().GetTenantId(), "etag", req.GetGiftCard().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "network", req.GetGiftCard().GetCard().GetNetwork())
	explainSoftwareLicenseCalculationRequest := newExplainSoftwareLicenseCalculationRequest(withExplainSoftwareLicenseCalculationContext(req.GetContext()), withExplainSoftwareLicenseCalculationSoftwareLicense(&licensingv1.SoftwareLicense{
		Contact: &commonv1.ContactInfo{Phone: req.GetGiftCard().GetContact().GetPhone()},
		Funding: &licensingv1.SoftwareLicense_Card{Card: &licensingv1.SoftwareLicenseCardFunding{Network: tenancy.Value(ctx, "network")}},
	}), withExplainSoftwareLicenseCalculationDetails(&licensingv1.ExplainSoftwareLicenseCalculationRequest_Details{
		Contact: &commonv1.ContactInfo{Email: req.GetGiftCard().GetContact().GetEmail()},
	}))
	explainSoftwareLicenseCalculationReply, err := s.softwareLicenseCalculationClient.ExplainSoftwareLicenseCalculation(ctx, explainSoftwareLicenseCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.licensing.v1.SoftwareLicenseCalculationService/ExplainSoftwareLicenseCalculation")
	}
	source := req.GetDetails().GetDestination().ProtoReflect()
	region := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("region"))).String()
	countLoanApplicationsRequest := &lendingv1.CountLoanApplicationsRequest{
		Context: req.GetContext(),
		Filter: &lendingv1.CountLoanApplicationsRequest_Filter{
			Labels:       explainSoftwareLicenseCalculationReply.GetSoftwareLicense().GetLabels(),
			Region:       region,
			CreatedAfter: req.GetGiftCard().GetAudit().GetUpdatedAt(),
			OwnerId:      req.GetGiftCard().GetHolder().GetGiftCardTransactionId(),
		},
	}
	countLoanApplicationsReply, err := s.meteredLoanApplicationQuery.CountLoanApplications(ctx, countLoanApplicationsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.lending.v1.LoanApplicationQueryService/CountLoanApplications")
	}
	return &giftcardsv1.SimulateGiftCardResponse{
		GiftCard: &giftcardsv1.GiftCard{
			Contact: &commonv1.ContactInfo{
				Email: explainSoftwareLicenseCalculationReply.GetSoftwareLicense().GetContact().GetEmail(),
			},
			GiftCardId: req.GetGiftCardId(),
			Labels:     req.GetGiftCard().GetLabels(),
		},
		Result: &commonv1.OperationResult{
			Message: countLoanApplicationsReply.GetResult().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// ExplainGiftCardCalculation explains a gift card.
func (s *GiftCardCalculationServer) ExplainGiftCardCalculation(ctx context.Context, req *giftcardsv1.ExplainGiftCardCalculationRequest) (*giftcardsv1.ExplainGiftCardCalculationResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveGiftCard(ctx, req.GetContext().GetTenantId(), req.GetGiftCard()); err != nil {
		return nil, grpcx.Wrap(err, "save gift card")
	}
	s.log.InfoContext(ctx, "explain gift card calculation", "tenant", req.GetContext().GetTenantId(), "etag", req.GetGiftCard().GetAudit().GetEtag())
	giftCardCache := cache.New[string, *giftcardsv1.GiftCard](1)
	giftCardCache.Put(req.GetContext().GetTraceId(), req.GetGiftCard())
	cachedGiftCard, _ := giftCardCache.Get(req.GetContext().GetTraceId())
	cloned := proto.Clone(req.GetGiftCard()).(*giftcardsv1.GiftCard)
	explainUserDecisionReply, err := s.userValidationClient.ExplainUserDecision(ctx, &identityv1.ExplainUserDecisionRequest{
		Context: req.GetContext(),
		Details: &identityv1.ExplainUserDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{Phone: req.GetGiftCard().GetContact().GetPhone()},
		},
		User: &identityv1.User{
			Contact: &commonv1.ContactInfo{
				Name:             cachedGiftCard.GetContact().GetName(),
				PreferredChannel: req.GetGiftCard().GetContact().GetPreferredChannel(),
			},
			Audit: &commonv1.AuditInfo{CreatedBy: cloned.GetAudit().GetCreatedBy()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserValidationService/ExplainUserDecision")
	}
	countLedgerAccountsRequest := newCountLedgerAccountsRequest(withCountLedgerAccountsContext(req.GetContext()), withCountLedgerAccountsFilter(&ledgerv1.CountLedgerAccountsRequest_Filter{
		Labels:       req.GetGiftCard().GetLabels(),
		Region:       explainUserDecisionReply.GetUser().GetPrimaryAddress().GetRegion(),
		CreatedAfter: explainUserDecisionReply.GetUser().GetAudit().GetCreatedAt(),
	}))
	countLedgerAccountsReply, err := s.ledgerAccountQueryClient.CountLedgerAccounts(ctx, countLedgerAccountsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountQueryService/CountLedgerAccounts")
	}
	validateUserReply, err := s.userValidationClient.ValidateUser(ctx, &identityv1.ValidateUserRequest{
		Context: req.GetContext(),
		User: &identityv1.User{
			Contact: &commonv1.ContactInfo{Phone: req.GetDetails().GetContact().GetPhone()},
			IdentityDocument: &identityv1.User_Passport{Passport: &identityv1.UserPassport{
				Number: explainUserDecisionReply.GetUser().GetNationalId().GetNumber(),
			}},
			PrimaryAddress: &commonv1.PostalAddress{
				Locality: explainUserDecisionReply.GetUser().GetPrimaryAddress().GetLocality(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserValidationService/ValidateUser")
	}
	routes := map[string]func(context.Context, *identityv1.ListTenantExportsRequest, ...grpc.CallOption) (*identityv1.ListTenantExportsResponse, error){
		"primary":  s.tenantExportClient.ListTenantExports,
		"fallback": s.tenantExportClientFallback.ListTenantExports,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	labels := make(map[string]string, len(req.GetGiftCard().GetLabels()))
	for key, value := range req.GetGiftCard().GetLabels() {
		labels[key] = value
	}
	listTenantExportsRequest := &identityv1.ListTenantExportsRequest{
		Context: req.GetContext(),
		Filter: &identityv1.ListTenantExportsRequest_Filter{
			Region: validateUserReply.GetUser().GetPrimaryAddress().GetRegion(),
			Labels: labels,
		},
	}
	listTenantExportsReply, err := route(ctx, listTenantExportsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantExportService/ListTenantExports")
	}
	return &giftcardsv1.ExplainGiftCardCalculationResponse{
		GiftCard: &giftcardsv1.GiftCard{
			Audit: &commonv1.AuditInfo{
				CreatedBy: explainUserDecisionReply.GetUser().GetAudit().GetCreatedBy(),
				CreatedAt: validateUserReply.GetUser().GetAudit().GetCreatedAt(),
			},
			GiftCardId: req.GetGiftCardId(),
			Balance:    req.GetGiftCard().GetBalance(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: countLedgerAccountsReply.GetResult().GetCorrelationId(),
			Status:        countLedgerAccountsReply.GetResult().GetStatus(),
		},
		Outcome: &giftcardsv1.ExplainGiftCardCalculationResponse_Outcome{Message: listTenantExportsReply.GetResult().GetMessage()},
	}, nil
}

// GetGiftCardRates returns a gift card.
func (s *GiftCardCalculationServer) GetGiftCardRates(ctx context.Context, req *giftcardsv1.GetGiftCardRatesRequest) (*giftcardsv1.GetGiftCardRatesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetGiftCardRates", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "gift_card", req.GetGiftCardId(), "get_gift_card_rates"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get gift card rates", "tenant", req.GetContext().GetTenantId())
	normalizeMetricSeriesReply, err := s.metricSeriesValidationClient.NormalizeMetricSeries(ctx, &metricsv1.NormalizeMetricSeriesRequest{
		Context: req.GetContext(),
		MetricSeries: &metricsv1.MetricSeries{
			Owner: &identityv1.UserRef{Email: req.GetContext().GetActor().GetEmail()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.metrics.v1.MetricSeriesValidationService/NormalizeMetricSeries")
	}
	calculateOnboardingPlanRequest := &onboardingv1.CalculateOnboardingPlanRequest{
		Context: req.GetContext(),
		OnboardingPlan: &onboardingv1.OnboardingPlan{
			BillingAddress: &commonv1.PostalAddress{
				Locality: normalizeMetricSeriesReply.GetMetricSeries().GetAddress().GetLocality(),
			},
		},
	}
	calculateOnboardingPlanReply, err := onboardingv1.OnboardingPlanCalculationServiceClient.CalculateOnboardingPlan(s.onboardingPlanCalculationClient, ctx, calculateOnboardingPlanRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.onboarding.v1.OnboardingPlanCalculationService/CalculateOnboardingPlan")
	}
	return &giftcardsv1.GetGiftCardRatesResponse{
		GiftCard: &giftcardsv1.GiftCard{
			Audit: &commonv1.AuditInfo{
				Etag:      normalizeMetricSeriesReply.GetMetricSeries().GetAudit().GetEtag(),
				UpdatedAt: calculateOnboardingPlanReply.GetOnboardingPlan().GetAudit().GetUpdatedAt(),
			},
			GiftCardId: req.GetGiftCardId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}
