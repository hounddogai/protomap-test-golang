package pricing

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"

	billingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/billing/v1"
	budgetingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/budgeting/v1"
	cartv1 "github.com/hounddogai/protomap-test-golang/gen/acme/cart/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	inventoryv1 "github.com/hounddogai/protomap-test-golang/gen/acme/inventory/v1"
	lendingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/lending/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	notificationsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/notifications/v1"
	onboardingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/onboarding/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	pricingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/pricing/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// PriceQuoteCalculationServer implements `acme.pricing.v1.PriceQuoteCalculationService`.
// Computes the figures of a price quote, such as totals, taxes, and estimates.
type PriceQuoteCalculationServer struct {
	pricingv1.UnimplementedPriceQuoteCalculationServiceServer
	store                             *Store
	log                               *slog.Logger
	clients                           *registry.Registry
	conn                              *grpc.ClientConn
	notificationValidationClient      notificationsv1.NotificationValidationServiceClient
	disputeValidationClient           paymentsv1.DisputeValidationServiceClient
	onboardingPlanClient              onboardingv1.OnboardingPlanServiceClient
	featureFlagQueryClient            featureflagsv1.FeatureFlagQueryServiceClient
	explainTierDecisionFn             func(context.Context, *loyaltyv1.ExplainTierDecisionRequest, ...grpc.CallOption) (*loyaltyv1.ExplainTierDecisionResponse, error)
	budgetClient                      budgetingv1.BudgetServiceClient
	loanApplicationCalculationClient  lendingv1.LoanApplicationCalculationServiceClient
	featureFlagValidationClient       featureflagsv1.FeatureFlagValidationServiceClient
	replenishmentPlanValidationClient inventoryv1.ReplenishmentPlanValidationServiceClient
	apiCredentialNotificationClient   identityv1.ApiCredentialNotificationServiceClient
}

// NewPriceQuoteCalculationServer wires a PriceQuoteCalculationServer.
func NewPriceQuoteCalculationServer(deps platform.Deps) *PriceQuoteCalculationServer {
	tierValidation := loyaltyv1.NewTierValidationServiceClient(deps.Conn)
	return &PriceQuoteCalculationServer{
		store:                             NewStore(deps.DB),
		log:                               deps.Logger.With("service", "acme.pricing.v1.PriceQuoteCalculationService"),
		clients:                           deps.Clients,
		conn:                              deps.Conn,
		notificationValidationClient:      notificationsv1.NewNotificationValidationServiceClient(deps.Conn),
		disputeValidationClient:           paymentsv1.NewDisputeValidationServiceClient(deps.Conn),
		onboardingPlanClient:              onboardingv1.NewOnboardingPlanServiceClient(deps.Conn),
		featureFlagQueryClient:            featureflagsv1.NewFeatureFlagQueryServiceClient(deps.Conn),
		explainTierDecisionFn:             tierValidation.ExplainTierDecision,
		budgetClient:                      budgetingv1.NewBudgetServiceClient(deps.Conn),
		loanApplicationCalculationClient:  lendingv1.NewLoanApplicationCalculationServiceClient(deps.Conn),
		featureFlagValidationClient:       featureflagsv1.NewFeatureFlagValidationServiceClient(deps.Conn),
		replenishmentPlanValidationClient: inventoryv1.NewReplenishmentPlanValidationServiceClient(deps.Conn),
		apiCredentialNotificationClient:   identityv1.NewApiCredentialNotificationServiceClient(deps.Conn),
	}
}

// CalculatePriceQuote calculates a price quote.
func (s *PriceQuoteCalculationServer) CalculatePriceQuote(ctx context.Context, req *pricingv1.CalculatePriceQuoteRequest) (*pricingv1.CalculatePriceQuoteResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePriceQuote(ctx, req.GetContext().GetTenantId(), req.GetPriceQuote()); err != nil {
		return nil, grpcx.Wrap(err, "save price quote")
	}
	s.log.InfoContext(ctx, "calculate price quote", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPriceQuote().GetAudit().GetEtag())
	billingAccountQuery, ok := s.clients.Get("acme.billing.v1.BillingAccountQueryService").(billingv1.BillingAccountQueryServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.billing.v1.BillingAccountQueryService")
	}
	suggestBillingAccountsRequest := &billingv1.SuggestBillingAccountsRequest{
		Context: req.GetContext(),
		Filter:  &billingv1.SuggestBillingAccountsRequest_Filter{Region: req.GetPriceQuote().GetBillingAddress().GetRegion()},
	}
	suggestBillingAccountsReply, err := billingAccountQuery.SuggestBillingAccounts(ctx, suggestBillingAccountsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.billing.v1.BillingAccountQueryService/SuggestBillingAccounts")
	}
	previewCartRequest := &cartv1.PreviewCartRequest{
		Context: req.GetContext(),
		Cart: &cartv1.Cart{
			Settlement: &cartv1.Cart_Bank{Bank: &cartv1.CartBankSettlement{
				AccountHolder: req.GetPriceQuote().GetBank().GetAccountHolder(),
			}},
			Counterparty: &cartv1.SavedListRef{
				DisplayName: req.GetPriceQuote().GetCounterparty().GetDisplayName(),
			},
		},
	}
	previewCartReply := new(cartv1.PreviewCartResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", cartCartValidationServiceName, "PreviewCart"), previewCartRequest, previewCartReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartValidationService/PreviewCart")
	}
	return &pricingv1.CalculatePriceQuoteResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: suggestBillingAccountsReply.GetResult().GetReferenceIds(),
			Status:       suggestBillingAccountsReply.GetResult().GetStatus(),
		},
		PriceQuote: &pricingv1.PriceQuote{
			Audit: &commonv1.AuditInfo{
				CreatedAt: previewCartReply.GetCart().GetAudit().GetCreatedAt(),
			},
			PriceQuoteId: req.GetPriceQuoteId(),
			Metadata:     req.GetPriceQuote().GetMetadata(),
		},
	}, nil
}

// EstimatePriceQuote estimates a price quote.
func (s *PriceQuoteCalculationServer) EstimatePriceQuote(ctx context.Context, req *pricingv1.EstimatePriceQuoteRequest) (*pricingv1.EstimatePriceQuoteResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePriceQuote(ctx, req.GetContext().GetTenantId(), req.GetPriceQuote()); err != nil {
		return nil, grpcx.Wrap(err, "save price quote")
	}
	s.log.InfoContext(ctx, "estimate price quote", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPriceQuote().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "created_by", req.GetPriceQuote().GetAudit().GetCreatedBy())
	cloned := proto.Clone(req.GetPriceQuote()).(*pricingv1.PriceQuote)
	validateNotificationReply, err := s.notificationValidationClient.ValidateNotification(ctx, &notificationsv1.ValidateNotificationRequest{
		Context: req.GetContext(),
		Notification: &notificationsv1.Notification{
			Comments: []*notificationsv1.NotificationComment{&notificationsv1.NotificationComment{CreatedAt: req.GetPriceQuote().GetAudit().GetCreatedAt()}},
			Audit: &commonv1.AuditInfo{
				CreatedBy: tenancy.Value(ctx, "created_by"),
				UpdatedBy: req.GetPriceQuote().GetAudit().GetUpdatedBy(),
			},
			Sla: &notificationsv1.NotificationSla{DueAt: cloned.GetDueAt()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.notifications.v1.NotificationValidationService/ValidateNotification")
	}
	return &pricingv1.EstimatePriceQuoteResponse{
		PriceQuote: &pricingv1.PriceQuote{
			Audit: &commonv1.AuditInfo{
				CreatedBy: validateNotificationReply.GetNotification().GetAudit().GetCreatedBy(),
			},
			PriceQuoteId: req.GetPriceQuoteId(),
		},
		Breakdown: &pricingv1.EstimatePriceQuoteResponse_Breakdown{Total: req.GetPriceQuote().GetTotal()},
		Result:    &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// QuotePriceQuote quotes a price quote.
func (s *PriceQuoteCalculationServer) QuotePriceQuote(ctx context.Context, req *pricingv1.QuotePriceQuoteRequest) (*pricingv1.QuotePriceQuoteResponse, error) {
	if err := s.store.SavePriceQuote(ctx, req.GetContext().GetTenantId(), req.GetPriceQuote()); err != nil {
		return nil, grpcx.Wrap(err, "save price quote")
	}
	s.log.InfoContext(ctx, "quote price quote", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPriceQuote().GetAudit().GetEtag())
	apiCredentialValidation, err := registry.Lookup[identityv1.ApiCredentialValidationServiceClient](s.clients, "acme.identity.v1.ApiCredentialValidationService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	validateApiCredentialRequest := &identityv1.ValidateApiCredentialRequest{
		Context: req.GetContext(),
		ApiCredential: &identityv1.ApiCredential{
			Funding: &identityv1.ApiCredential_Bank{Bank: &identityv1.ApiCredentialBankFunding{
				Iban: req.GetPriceQuote().GetBank().GetIban(),
				Bic:  req.GetPriceQuote().GetBank().GetBic(),
			}},
			Audit: &commonv1.AuditInfo{UpdatedAt: req.GetPriceQuote().GetAudit().GetUpdatedAt()},
		},
	}
	validateApiCredentialReply, err := apiCredentialValidation.ValidateApiCredential(ctx, validateApiCredentialRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.ApiCredentialValidationService/ValidateApiCredential")
	}
	cloned := proto.Clone(req.GetPriceQuote()).(*pricingv1.PriceQuote)
	encoded, err := protojson.Marshal(req.GetPriceQuote())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded pricingv1.PriceQuote
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	lintDisputesReply, err := s.disputeValidationClient.LintDisputes(ctx, &paymentsv1.LintDisputesRequest{
		Context: req.GetContext(),
		Dispute: &paymentsv1.Dispute{
			Audit:    &commonv1.AuditInfo{UpdatedAt: cloned.GetAudit().GetUpdatedAt()},
			Comments: []*paymentsv1.DisputeComment{&paymentsv1.DisputeComment{CreatedAt: (&decoded).GetAudit().GetCreatedAt()}},
			Requester: &commonv1.ContactInfo{
				Email: validateApiCredentialReply.GetApiCredential().GetContact().GetEmail(),
			},
			Assignee: &identityv1.UserRef{
				DisplayName: validateApiCredentialReply.GetApiCredential().GetHolder().GetDisplayName(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.DisputeValidationService/LintDisputes")
	}
	_ = lintDisputesReply
	return &pricingv1.QuotePriceQuoteResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: validateApiCredentialReply.GetResult().GetReferenceIds(),
			Status:       validateApiCredentialReply.GetOutcome().GetStatus(),
		},
		PriceQuote: &pricingv1.PriceQuote{
			PriceQuoteId: req.GetPriceQuoteId(),
			DueAt:        req.GetPriceQuote().GetDueAt(),
		},
	}, nil
}

// ComparePriceQuoteOptions compares a price quote.
func (s *PriceQuoteCalculationServer) ComparePriceQuoteOptions(ctx context.Context, req *pricingv1.ComparePriceQuoteOptionsRequest) (*pricingv1.ComparePriceQuoteOptionsResponse, error) {
	if err := s.store.SavePriceQuote(ctx, req.GetContext().GetTenantId(), req.GetPriceQuote()); err != nil {
		return nil, grpcx.Wrap(err, "save price quote")
	}
	s.log.InfoContext(ctx, "compare price quote options", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPriceQuote().GetAudit().GetEtag())
	encoded, err := protojson.Marshal(req.GetPriceQuote())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded pricingv1.PriceQuote
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	calculateGiftCardRequest := &giftcardsv1.CalculateGiftCardRequest{
		Context: req.GetContext(),
		GiftCard: &giftcardsv1.GiftCard{
			Funding: &giftcardsv1.GiftCard_Bank{Bank: &giftcardsv1.GiftCardBankFunding{
				AccountHolder: req.GetPriceQuote().GetBank().GetAccountHolder(),
			}},
			Audit: &commonv1.AuditInfo{Etag: req.GetPriceQuote().GetAudit().GetEtag()},
		},
		Details: &giftcardsv1.CalculateGiftCardRequest_Details{
			Destination: &commonv1.PostalAddress{PostalCode: (&decoded).GetBillingAddress().GetPostalCode()},
		},
	}
	calculateGiftCardReply := new(giftcardsv1.CalculateGiftCardResponse)
	if err := grpcx.Invoke(ctx, s.conn, giftcardsv1.GiftCardCalculationService_CalculateGiftCard_FullMethodName, calculateGiftCardRequest, calculateGiftCardReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardCalculationService/CalculateGiftCard")
	}
	return &pricingv1.ComparePriceQuoteOptionsResponse{
		Result: &commonv1.OperationResult{
			Details: calculateGiftCardReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		PriceQuote: &pricingv1.PriceQuote{
			PriceQuoteId: req.GetPriceQuoteId(),
			Status:       req.GetPriceQuote().GetStatus(),
		},
	}, nil
}

// RecalculatePriceQuotes recalculates a price quote.
func (s *PriceQuoteCalculationServer) RecalculatePriceQuotes(ctx context.Context, req *pricingv1.RecalculatePriceQuotesRequest) (*pricingv1.RecalculatePriceQuotesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SavePriceQuote(ctx, req.GetContext().GetTenantId(), req.GetPriceQuote()); err != nil {
		return nil, grpcx.Wrap(err, "save price quote")
	}
	s.log.InfoContext(ctx, "recalculate price quotes", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPriceQuote().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetPriceQuote()).(*pricingv1.PriceQuote)
	watchOnboardingPlansStream, err := s.onboardingPlanClient.WatchOnboardingPlans(ctx, &onboardingv1.WatchOnboardingPlansRequest{
		Context: req.GetContext(),
		Filter: &onboardingv1.WatchOnboardingPlansRequest_Filter{
			Region:       req.GetDetails().GetDestination().GetRegion(),
			CreatedAfter: cloned.GetAudit().GetCreatedAt(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.onboarding.v1.OnboardingPlanService/WatchOnboardingPlans")
	}
	for {
		received, err := watchOnboardingPlansStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.onboarding.v1.OnboardingPlanService/WatchOnboardingPlans")
		}
		_ = received
	}
	browseFeatureFlagsReply, err := s.featureFlagQueryClient.BrowseFeatureFlags(ctx, &featureflagsv1.BrowseFeatureFlagsRequest{
		Context: req.GetContext(),
		Filter: &featureflagsv1.BrowseFeatureFlagsRequest_Filter{
			Region:       req.GetPriceQuote().GetBillingAddress().GetRegion(),
			CreatedAfter: req.GetPriceQuote().GetAudit().GetUpdatedAt(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.featureflags.v1.FeatureFlagQueryService/BrowseFeatureFlags")
	}
	ctx = tenancy.With(ctx, "display_name", req.GetPriceQuote().GetCounterparty().GetDisplayName())
	var holder struct{ CreatedBy string }
	if record := req.GetPriceQuote().GetAudit(); record != nil {
		if err := mapper.CopyFields(&holder, record, "CreatedBy"); err != nil {
			s.log.WarnContext(ctx, "copy created_by", "error", err)
		}
	}
	normalizeDisputeRequest := newNormalizeDisputeRequest(withNormalizeDisputeContext(req.GetContext()), withNormalizeDisputeDispute(&paymentsv1.Dispute{
		Assignee: &identityv1.UserRef{DisplayName: tenancy.Value(ctx, "display_name")},
		Audit: &commonv1.AuditInfo{
			CreatedBy: holder.CreatedBy,
			UpdatedAt: req.GetPriceQuote().GetAudit().GetUpdatedAt(),
			UpdatedBy: req.GetPriceQuote().GetAudit().GetUpdatedBy(),
		},
	}))
	normalizeDisputeReply, err := s.disputeValidationClient.NormalizeDispute(ctx, normalizeDisputeRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.DisputeValidationService/NormalizeDispute")
	}
	explainTierDecisionRequest := &loyaltyv1.ExplainTierDecisionRequest{
		Context: req.GetContext(),
		Tier: &loyaltyv1.Tier{
			Owner: &identityv1.UserRef{
				DisplayName: normalizeDisputeReply.GetDispute().GetCustomer().GetDisplayName(),
			},
			Version: req.GetPriceQuote().GetCounterparty().GetVersion(),
		},
	}
	explainTierDecisionReply, err := s.explainTierDecisionFn(ctx, explainTierDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.TierValidationService/ExplainTierDecision")
	}
	return &pricingv1.RecalculatePriceQuotesResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: browseFeatureFlagsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		PriceQuote: &pricingv1.PriceQuote{
			DueAt: normalizeDisputeReply.GetDispute().GetSla().GetDueAt(),
			Counterparty: &pricingv1.PriceListRef{
				DisplayName: normalizeDisputeReply.GetDispute().GetAssignee().GetDisplayName(),
			},
			PriceQuoteId: req.GetPriceQuoteId(),
			Notes:        req.GetPriceQuote().GetNotes(),
		},
		Breakdown: &pricingv1.RecalculatePriceQuotesResponse_Breakdown{
			Lines: []*pricingv1.RecalculatePriceQuotesResponse_Breakdown_Line{&pricingv1.RecalculatePriceQuotesResponse_Breakdown_Line{
				Amount: explainTierDecisionReply.GetTier().GetDiscount().GetAmount(),
			}},
		},
	}, nil
}

// SimulatePriceQuote simulates a price quote.
func (s *PriceQuoteCalculationServer) SimulatePriceQuote(ctx context.Context, req *pricingv1.SimulatePriceQuoteRequest) (*pricingv1.SimulatePriceQuoteResponse, error) {
	if err := s.store.SavePriceQuote(ctx, req.GetContext().GetTenantId(), req.GetPriceQuote()); err != nil {
		return nil, grpcx.Wrap(err, "save price quote")
	}
	s.log.InfoContext(ctx, "simulate price quote", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPriceQuote().GetAudit().GetEtag())
	createBudgetRequest := buildCreateBudgetRequestForSimulatePriceQuote(req)
	createBudgetReply, err := s.budgetClient.CreateBudget(ctx, createBudgetRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.budgeting.v1.BudgetService/CreateBudget")
	}
	packed, err := anypb.New(req.GetPriceQuote())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	subscribeSynonymUpdatesRequest := &searchv1.SubscribeSynonymUpdatesRequest{
		Context: req.GetContext(),
		Details: &searchv1.SubscribeSynonymUpdatesRequest_Details{
			Recipient: &commonv1.ContactInfo{
				Name:  createBudgetReply.GetBudget().GetContact().GetName(),
				Phone: createBudgetReply.GetBudget().GetContact().GetPhone(),
				Email: createBudgetReply.GetBudget().GetContact().GetEmail(),
			},
			Payload: packed,
		},
	}
	subscribeSynonymUpdatesReply := new(searchv1.SubscribeSynonymUpdatesResponse)
	if err := grpc.Invoke(ctx, "/acme.search.v1.SynonymNotificationService/SubscribeSynonymUpdates", subscribeSynonymUpdatesRequest, subscribeSynonymUpdatesReply, s.conn); err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymNotificationService/SubscribeSynonymUpdates")
	}
	ctx = tenancy.With(ctx, "country_code", req.GetPriceQuote().GetBillingAddress().GetCountryCode())
	recalculateLoanApplicationsRequest := &lendingv1.RecalculateLoanApplicationsRequest{
		Context: req.GetContext(),
		LoanApplication: &lendingv1.LoanApplication{
			Audit: &commonv1.AuditInfo{
				CreatedAt: subscribeSynonymUpdatesReply.GetSynonym().GetAudit().GetCreatedAt(),
			},
			Settlement: &lendingv1.LoanApplication_Bank{Bank: &lendingv1.LoanApplicationBankSettlement{Iban: req.GetPriceQuote().GetBank().GetIban()}},
		},
		Details: &lendingv1.RecalculateLoanApplicationsRequest_Details{
			Destination: &commonv1.PostalAddress{
				Recipient:   req.GetPriceQuote().GetBillingAddress().GetRecipient(),
				CountryCode: tenancy.Value(ctx, "country_code"),
			},
		},
	}
	send := s.recalculateLoanApplicationsSender(recalculateLoanApplicationsRequest)
	recalculateLoanApplicationsReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.lending.v1.LoanApplicationCalculationService/RecalculateLoanApplications")
	}
	browseFeatureFlagsRequest := &featureflagsv1.BrowseFeatureFlagsRequest{
		Context: req.GetContext(),
		Filter: &featureflagsv1.BrowseFeatureFlagsRequest_Filter{
			Region: recalculateLoanApplicationsReply.GetLoanApplication().GetBillingAddress().GetRegion(),
			Labels: createBudgetReply.GetBudget().GetLabels(),
		},
	}
	browseFeatureFlagsReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*featureflagsv1.BrowseFeatureFlagsResponse, error) {
		return s.featureFlagQueryClient.BrowseFeatureFlags(ctx, browseFeatureFlagsRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.featureflags.v1.FeatureFlagQueryService/BrowseFeatureFlags")
	}
	return &pricingv1.SimulatePriceQuoteResponse{
		Result: &commonv1.OperationResult{
			Status:       createBudgetReply.GetResult().GetStatus(),
			ReferenceIds: recalculateLoanApplicationsReply.GetResult().GetReferenceIds(),
			Message:      browseFeatureFlagsReply.GetResult().GetMessage(),
		},
		PriceQuote: &pricingv1.PriceQuote{
			Counterparty: &pricingv1.PriceListRef{
				Version: subscribeSynonymUpdatesReply.GetSynonym().GetVersion(),
			},
			PriceQuoteId: req.GetPriceQuoteId(),
		},
		Breakdown: &pricingv1.SimulatePriceQuoteResponse_Breakdown{
			Total:        recalculateLoanApplicationsReply.GetLoanApplication().GetTotal(),
			CurrencyCode: req.GetDetails().GetCurrencyCode(),
		},
	}, nil
}

// ExplainPriceQuoteCalculation explains a price quote.
func (s *PriceQuoteCalculationServer) ExplainPriceQuoteCalculation(ctx context.Context, req *pricingv1.ExplainPriceQuoteCalculationRequest) (*pricingv1.ExplainPriceQuoteCalculationResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePriceQuote(ctx, req.GetContext().GetTenantId(), req.GetPriceQuote()); err != nil {
		return nil, grpcx.Wrap(err, "save price quote")
	}
	s.log.InfoContext(ctx, "explain price quote calculation", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPriceQuote().GetAudit().GetEtag())
	jobs := make(chan *pricingv1.PriceQuoteLine)
	failures := make(chan error, 4)
	workCtx, stopWork := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for line := range jobs {
				if _, err := s.featureFlagValidationClient.CheckFeatureFlagEligibility(workCtx, &featureflagsv1.CheckFeatureFlagEligibilityRequest{
					Context: req.GetContext(),
					FeatureFlag: &featureflagsv1.FeatureFlag{
						Description: line.GetDescription(),
						Action:      &featureflagsv1.FeatureFlag_Discount{Discount: &featureflagsv1.FeatureFlagDiscountAction{Amount: line.GetTax().GetAmount()}},
					},
				}); err != nil {
					failures <- err
					stopWork()
					return
				}
			}
		}()
	}
	for _, queued := range req.GetPriceQuote().GetLines() {
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
		return nil, grpcx.Wrap(err, "acme.featureflags.v1.FeatureFlagValidationService/CheckFeatureFlagEligibility")
	}
	explainReplenishmentPlanDecisionRequest := &inventoryv1.ExplainReplenishmentPlanDecisionRequest{}
	explainReplenishmentPlanDecisionRequest.Context = req.GetContext()
	explainReplenishmentPlanDecisionReply, err := s.replenishmentPlanValidationClient.ExplainReplenishmentPlanDecision(ctx, explainReplenishmentPlanDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.ReplenishmentPlanValidationService/ExplainReplenishmentPlanDecision")
	}
	streamApiCredentialNoticesStream, err := s.apiCredentialNotificationClient.StreamApiCredentialNotices(ctx, &identityv1.StreamApiCredentialNoticesRequest{
		Context: req.GetContext(),
		Filter: &identityv1.StreamApiCredentialNoticesRequest_Filter{
			Region: explainReplenishmentPlanDecisionReply.GetReplenishmentPlan().GetBillingAddress().GetRegion(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.ApiCredentialNotificationService/StreamApiCredentialNotices")
	}
	for {
		received, err := streamApiCredentialNoticesStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.identity.v1.ApiCredentialNotificationService/StreamApiCredentialNotices")
		}
		_ = received
	}
	return &pricingv1.ExplainPriceQuoteCalculationResponse{
		PriceQuote: &pricingv1.PriceQuote{
			Audit: &commonv1.AuditInfo{
				Etag: explainReplenishmentPlanDecisionReply.GetReplenishmentPlan().GetAudit().GetEtag(),
			},
			DueAt:          explainReplenishmentPlanDecisionReply.GetReplenishmentPlan().GetDueAt(),
			PriceQuoteId:   req.GetPriceQuoteId(),
			BillingAddress: req.GetPriceQuote().GetBillingAddress(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// recalculateLoanApplicationsSender returns a function that sends one request to
// acme.lending.v1.LoanApplicationCalculationService/RecalculateLoanApplications.
func (s *PriceQuoteCalculationServer) recalculateLoanApplicationsSender(request *lendingv1.RecalculateLoanApplicationsRequest) func(context.Context) (*lendingv1.RecalculateLoanApplicationsResponse, error) {
	return func(ctx context.Context) (*lendingv1.RecalculateLoanApplicationsResponse, error) {
		return s.loanApplicationCalculationClient.RecalculateLoanApplications(ctx, request)
	}
}
