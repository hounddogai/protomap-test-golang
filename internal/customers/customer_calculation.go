package customers

import (
	"context"
	"errors"
	"io"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	keymanagementv1 "github.com/hounddogai/protomap-test-golang/gen/acme/keymanagement/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	messagingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/messaging/v1"
	promotionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/promotions/v1"
	recruitingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/recruiting/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// CustomerCalculationServer implements `acme.customers.v1.CustomerCalculationService`.
// Computes the figures of a customer, such as totals, taxes, and estimates.
type CustomerCalculationServer struct {
	customersv1.UnimplementedCustomerCalculationServiceServer
	store                           *Store
	log                             *slog.Logger
	promotionQueryClient            promotionsv1.PromotionQueryServiceClient
	searchIndexValidationPort       searchIndexValidationPort
	apiCredentialNotificationClient identityv1.ApiCredentialNotificationServiceClient
	tenantAdminClient               identityv1.TenantAdminServiceClient
	tenantAdminPort                 tenantAdminPort
	userQueryClient                 identityv1.UserQueryServiceClient
	sellerScorecardValidationClient sellersv1.SellerScorecardValidationServiceClient
	cryptoKeyQueryClient            keymanagementv1.CryptoKeyQueryServiceClient
	featureFlagQueryClient          featureflagsv1.FeatureFlagQueryServiceClient
	searchIndexAdminClient          searchv1.SearchIndexAdminServiceClient
	couponQueryClient               promotionsv1.CouponQueryServiceClient
	pointsLedgerCalculationClient   loyaltyv1.PointsLedgerCalculationServiceClient
	jobOpeningClient                recruitingv1.JobOpeningServiceClient
	cryptoKeyQueryPort              cryptoKeyQueryPort
	conversationClient              messagingv1.ConversationServiceClient
}

// NewCustomerCalculationServer wires a CustomerCalculationServer.
func NewCustomerCalculationServer(deps platform.Deps) *CustomerCalculationServer {
	return &CustomerCalculationServer{
		store:                           NewStore(deps.DB),
		log:                             deps.Logger.With("service", "acme.customers.v1.CustomerCalculationService"),
		promotionQueryClient:            promotionsv1.NewPromotionQueryServiceClient(deps.Conn),
		searchIndexValidationPort:       newSearchIndexValidationPort(deps),
		apiCredentialNotificationClient: identityv1.NewApiCredentialNotificationServiceClient(deps.Conn),
		tenantAdminClient:               identityv1.NewTenantAdminServiceClient(deps.Conn),
		tenantAdminPort:                 newTenantAdminPort(deps),
		userQueryClient:                 identityv1.NewUserQueryServiceClient(deps.Conn),
		sellerScorecardValidationClient: sellersv1.NewSellerScorecardValidationServiceClient(deps.Conn),
		cryptoKeyQueryClient:            keymanagementv1.NewCryptoKeyQueryServiceClient(deps.Conn),
		featureFlagQueryClient:          featureflagsv1.NewFeatureFlagQueryServiceClient(deps.Conn),
		searchIndexAdminClient:          searchv1.NewSearchIndexAdminServiceClient(deps.Conn),
		couponQueryClient:               promotionsv1.NewCouponQueryServiceClient(deps.Conn),
		pointsLedgerCalculationClient:   loyaltyv1.NewPointsLedgerCalculationServiceClient(deps.Conn),
		jobOpeningClient:                recruitingv1.NewJobOpeningServiceClient(deps.Conn),
		cryptoKeyQueryPort:              newCryptoKeyQueryPort(deps),
		conversationClient:              messagingv1.NewConversationServiceClient(deps.Conn),
	}
}

// CalculateCustomer calculates a customer.
func (s *CustomerCalculationServer) CalculateCustomer(ctx context.Context, req *customersv1.CalculateCustomerRequest) (*customersv1.CalculateCustomerResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CalculateCustomer", "error", err)
		return nil, err
	}
	if err := s.store.SaveCustomer(ctx, req.GetContext().GetTenantId(), req.GetCustomer()); err != nil {
		return nil, grpcx.Wrap(err, "save customer")
	}
	s.log.InfoContext(ctx, "calculate customer", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCustomer().GetAudit().GetEtag())
	browsePromotionsRequest := buildBrowsePromotionsRequestForCalculateCustomer(req)
	browsePromotionsReply, err := s.promotionQueryClient.BrowsePromotions(ctx, browsePromotionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.PromotionQueryService/BrowsePromotions")
	}
	return &customersv1.CalculateCustomerResponse{
		Result: &commonv1.OperationResult{
			Status:  browsePromotionsReply.GetResult().GetStatus(),
			Details: browsePromotionsReply.GetResult().GetDetails(),
		},
		Customer: &customersv1.Customer{
			CustomerId:     req.GetCustomerId(),
			PrimaryAddress: req.GetCustomer().GetPrimaryAddress(),
		},
	}, nil
}

// EstimateCustomer estimates a customer.
func (s *CustomerCalculationServer) EstimateCustomer(ctx context.Context, req *customersv1.EstimateCustomerRequest) (*customersv1.EstimateCustomerResponse, error) {
	if err := s.store.SaveCustomer(ctx, req.GetContext().GetTenantId(), req.GetCustomer()); err != nil {
		return nil, grpcx.Wrap(err, "save customer")
	}
	s.log.InfoContext(ctx, "estimate customer", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCustomer().GetAudit().GetEtag())
	normalizeSearchIndexRequest := &searchv1.NormalizeSearchIndexRequest{
		Context: req.GetContext(),
		Details: &searchv1.NormalizeSearchIndexRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email: req.GetCustomer().GetRelationshipManager().GetEmail(),
			},
		},
	}
	normalizeSearchIndexReply, err := s.searchIndexValidationPort.NormalizeSearchIndex(ctx, normalizeSearchIndexRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexValidationService/NormalizeSearchIndex")
	}
	ctx = tenancy.With(ctx, "phone", req.GetCustomer().GetContact().GetPhone())
	notifyApiCredentialWatchersRequest := &identityv1.NotifyApiCredentialWatchersRequest{
		Context: req.GetContext(),
		Details: &identityv1.NotifyApiCredentialWatchersRequest_Details{
			Recipient: &commonv1.ContactInfo{
				Email: req.GetCustomer().GetRelationshipManager().GetEmail(),
				Phone: tenancy.Value(ctx, "phone"),
			},
		},
	}
	type notifyApiCredentialWatchersResult struct {
		reply *identityv1.NotifyApiCredentialWatchersResponse
		err   error
	}
	notifyApiCredentialWatchersResults := make(chan notifyApiCredentialWatchersResult, 1)
	go func() {
		reply, err := s.apiCredentialNotificationClient.NotifyApiCredentialWatchers(ctx, notifyApiCredentialWatchersRequest)
		notifyApiCredentialWatchersResults <- notifyApiCredentialWatchersResult{reply, err}
	}()
	var notifyApiCredentialWatchersReply *identityv1.NotifyApiCredentialWatchersResponse
	select {
	case result := <-notifyApiCredentialWatchersResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.identity.v1.ApiCredentialNotificationService/NotifyApiCredentialWatchers")
		}
		notifyApiCredentialWatchersReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := customerCalculationServerPurgeTenantSteps[mode]
	purgeTenantRequest := &identityv1.PurgeTenantRequest{
		Context:  req.GetContext(),
		TenantId: req.GetContext().GetTenantId(),
		Etag:     normalizeSearchIndexReply.GetSearchIndex().GetAudit().GetEtag(),
		Reason:   req.GetReason(),
	}
	purgeTenantReply, err := run(s, ctx, purgeTenantRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantAdminService/PurgeTenant")
	}
	inspectTenantRequest := &identityv1.InspectTenantRequest{
		Context:  req.GetContext(),
		TenantId: req.GetContext().GetTenantId(),
	}
	inspectTenantReply, err := s.tenantAdminPort.InspectTenant(ctx, inspectTenantRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantAdminService/InspectTenant")
	}
	return &customersv1.EstimateCustomerResponse{
		Customer: &customersv1.Customer{
			PrimaryAddress: &commonv1.PostalAddress{
				Region: normalizeSearchIndexReply.GetSearchIndex().GetAddress().GetRegion(),
			},
			Name:       notifyApiCredentialWatchersReply.GetApiCredential().GetContact().GetName(),
			CustomerId: req.GetCustomerId(),
			Contact:    &commonv1.ContactInfo{Name: req.GetCustomer().GetName()},
		},
		Result: &commonv1.OperationResult{
			Details:       purgeTenantReply.GetResult().GetDetails(),
			Message:       purgeTenantReply.GetResult().GetMessage(),
			CorrelationId: inspectTenantReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// QuoteCustomer quotes a customer.
func (s *CustomerCalculationServer) QuoteCustomer(ctx context.Context, req *customersv1.QuoteCustomerRequest) (*customersv1.QuoteCustomerResponse, error) {
	if err := s.store.SaveCustomer(ctx, req.GetContext().GetTenantId(), req.GetCustomer()); err != nil {
		return nil, grpcx.Wrap(err, "save customer")
	}
	s.log.InfoContext(ctx, "quote customer", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCustomer().GetAudit().GetEtag())
	peekUserRequest := newPeekUserRequest(withPeekUserContext(req.GetContext()), withPeekUserUserId(req.GetCustomer().GetRelationshipManager().GetUserId()))
	peekUserReply, err := s.userQueryClient.PeekUser(ctx, peekUserRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserQueryService/PeekUser")
	}
	return &customersv1.QuoteCustomerResponse{
		Customer: &customersv1.Customer{
			RelationshipManager: &identityv1.UserRef{UserId: peekUserReply.GetUser().GetUserId()},
			Preferences: &customersv1.CustomerPreferences{
				QuietHours: peekUserReply.GetUser().GetPreferences().GetQuietHours(),
			},
			CustomerId: req.GetCustomerId(),
			Contact:    req.GetCustomer().GetContact(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RecalculateCustomers recalculates a customer.
func (s *CustomerCalculationServer) RecalculateCustomers(ctx context.Context, req *customersv1.RecalculateCustomersRequest) (*customersv1.RecalculateCustomersResponse, error) {
	if err := s.store.SaveCustomer(ctx, req.GetContext().GetTenantId(), req.GetCustomer()); err != nil {
		return nil, grpcx.Wrap(err, "save customer")
	}
	s.log.InfoContext(ctx, "recalculate customers", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCustomer().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "region", req.GetCustomer().GetPrimaryAddress().GetRegion())
	explainSellerScorecardDecisionReply, err := s.sellerScorecardValidationClient.ExplainSellerScorecardDecision(ctx, &sellersv1.ExplainSellerScorecardDecisionRequest{
		Context: req.GetContext(),
		SellerScorecard: &sellersv1.SellerScorecard{
			BillingAddress: &commonv1.PostalAddress{
				Region:      tenancy.Value(ctx, "region"),
				CountryCode: req.GetCustomer().GetPrimaryAddress().GetCountryCode(),
				Locality:    req.GetCustomer().GetPrimaryAddress().GetLocality(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardValidationService/ExplainSellerScorecardDecision")
	}
	findCryptoKeysReply, err := s.cryptoKeyQueryClient.FindCryptoKeys(ctx, &keymanagementv1.FindCryptoKeysRequest{
		Context: req.GetContext(),
		Filter: &keymanagementv1.FindCryptoKeysRequest_Filter{
			Region:       req.GetDetails().GetDestination().GetRegion(),
			CreatedAfter: req.GetCustomer().GetAudit().GetUpdatedAt(),
			Labels:       explainSellerScorecardDecisionReply.GetSellerScorecard().GetMetadata(),
			OwnerId:      req.GetCustomerId(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.keymanagement.v1.CryptoKeyQueryService/FindCryptoKeys")
	}
	countFeatureFlagsRequest := &featureflagsv1.CountFeatureFlagsRequest{
		Context: req.GetContext(),
		Filter: &featureflagsv1.CountFeatureFlagsRequest_Filter{
			Region:       req.GetCustomer().GetPrimaryAddress().GetRegion(),
			CreatedAfter: req.GetCustomer().GetAudit().GetUpdatedAt(),
			Labels:       findCryptoKeysReply.GetResult().GetDetails(),
		},
	}
	countFeatureFlagsReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*featureflagsv1.CountFeatureFlagsResponse, error) {
		return s.featureFlagQueryClient.CountFeatureFlags(ctx, countFeatureFlagsRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.featureflags.v1.FeatureFlagQueryService/CountFeatureFlags")
	}
	return &customersv1.RecalculateCustomersResponse{
		Result: &commonv1.OperationResult{
			Status:       explainSellerScorecardDecisionReply.GetResult().GetStatus(),
			Message:      findCryptoKeysReply.GetResult().GetMessage(),
			ReferenceIds: countFeatureFlagsReply.GetResult().GetReferenceIds(),
		},
		Customer: &customersv1.Customer{
			PrimaryAddress: &commonv1.PostalAddress{
				Region: explainSellerScorecardDecisionReply.GetSellerScorecard().GetBillingAddress().GetRegion(),
			},
			CustomerId: req.GetCustomerId(),
			Status:     req.GetCustomer().GetStatus(),
		},
	}, nil
}

// SimulateCustomer simulates a customer.
func (s *CustomerCalculationServer) SimulateCustomer(ctx context.Context, req *customersv1.SimulateCustomerRequest) (*customersv1.SimulateCustomerResponse, error) {
	if err := s.store.SaveCustomer(ctx, req.GetContext().GetTenantId(), req.GetCustomer()); err != nil {
		return nil, grpcx.Wrap(err, "save customer")
	}
	s.log.InfoContext(ctx, "simulate customer", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCustomer().GetAudit().GetEtag())
	snapshot, err := protojson.Marshal(req.GetCustomer())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	repairSearchIndexRequest := &searchv1.RepairSearchIndexRequest{
		Context: req.GetContext(),
		SearchIndex: &searchv1.SearchIndex{
			Owner: &identityv1.UserRef{
				Email: req.GetCustomer().GetRelationshipManager().GetEmail(),
			},
			Locator: &searchv1.SearchIndex_Address{Address: &commonv1.PostalAddress{Lines: req.GetDetails().GetDestination().GetLines()}},
		},
		Details: &searchv1.RepairSearchIndexRequest_Details{Payload: snapshot},
	}
	send := s.repairSearchIndexSender(repairSearchIndexRequest)
	repairSearchIndexReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexAdminService/RepairSearchIndex")
	}
	findCouponsRequest := buildFindCouponsRequestForSimulateCustomer(req, repairSearchIndexReply)
	findCouponsReply, err := s.couponQueryClient.FindCoupons(ctx, findCouponsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CouponQueryService/FindCoupons")
	}
	browsePromotionsRequest := &promotionsv1.BrowsePromotionsRequest{
		Context: req.GetContext(),
		Filter:  &promotionsv1.BrowsePromotionsRequest_Filter{Region: req.GetCustomer().GetPrimaryAddress().GetRegion()},
	}
	browsePromotionsReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*promotionsv1.BrowsePromotionsResponse, error) {
		return s.promotionQueryClient.BrowsePromotions(ctx, browsePromotionsRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.PromotionQueryService/BrowsePromotions")
	}
	return &customersv1.SimulateCustomerResponse{
		Customer: &customersv1.Customer{
			PrimaryAddress: &commonv1.PostalAddress{
				PostalCode: repairSearchIndexReply.GetSearchIndex().GetAddress().GetPostalCode(),
			},
			CustomerId: req.GetCustomerId(),
		},
		Result: &commonv1.OperationResult{
			Message:       repairSearchIndexReply.GetResult().GetMessage(),
			ReferenceIds:  findCouponsReply.GetResult().GetReferenceIds(),
			CorrelationId: browsePromotionsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Breakdown: &customersv1.SimulateCustomerResponse_Breakdown{CurrencyCode: req.GetDetails().GetCurrencyCode()},
	}, nil
}

// ExplainCustomerCalculation explains a customer.
func (s *CustomerCalculationServer) ExplainCustomerCalculation(ctx context.Context, req *customersv1.ExplainCustomerCalculationRequest) (*customersv1.ExplainCustomerCalculationResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ExplainCustomerCalculation", "error", err)
		return nil, err
	}
	if err := s.store.SaveCustomer(ctx, req.GetContext().GetTenantId(), req.GetCustomer()); err != nil {
		return nil, grpcx.Wrap(err, "save customer")
	}
	s.log.InfoContext(ctx, "explain customer calculation", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCustomer().GetAudit().GetEtag())
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := customerCalculationServerRecalculatePointsLedgersSteps[mode]
	recalculatePointsLedgersRequest := &loyaltyv1.RecalculatePointsLedgersRequest{
		Context: req.GetContext(),
		Details: &loyaltyv1.RecalculatePointsLedgersRequest_Details{
			Destination: &commonv1.PostalAddress{
				CountryCode: req.GetCustomer().GetPrimaryAddress().GetCountryCode(),
			},
		},
	}
	recalculatePointsLedgersReply, err := run(s, ctx, recalculatePointsLedgersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerCalculationService/RecalculatePointsLedgers")
	}
	cloned := proto.Clone(req.GetDetails()).(*customersv1.ExplainCustomerCalculationRequest_Details)
	watchJobOpeningsStream, err := s.jobOpeningClient.WatchJobOpenings(ctx, &recruitingv1.WatchJobOpeningsRequest{
		Context:    req.GetContext(),
		Subscriber: &commonv1.ContactInfo{Email: cloned.GetContact().GetEmail()},
		Filter:     &recruitingv1.WatchJobOpeningsRequest_Filter{Region: req.GetCustomer().GetPrimaryAddress().GetRegion()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.recruiting.v1.JobOpeningService/WatchJobOpenings")
	}
	for {
		received, err := watchJobOpeningsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.recruiting.v1.JobOpeningService/WatchJobOpenings")
		}
		_ = received
	}
	return &customersv1.ExplainCustomerCalculationResponse{
		Customer: &customersv1.Customer{
			Contact: &commonv1.ContactInfo{
				Name: recalculatePointsLedgersReply.GetPointsLedger().GetContact().GetName(),
			},
			CustomerId: req.GetCustomerId(),
			Attributes: req.GetCustomer().GetAttributes(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetCustomerRates returns a customer.
func (s *CustomerCalculationServer) GetCustomerRates(ctx context.Context, req *customersv1.GetCustomerRatesRequest) (*customersv1.GetCustomerRatesResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "customer", req.GetCustomerId(), "get_customer_rates"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get customer rates", "tenant", req.GetContext().GetTenantId())
	baggage := make(map[string]string, len(req.GetContext().GetBaggage()))
	for key, value := range req.GetContext().GetBaggage() {
		baggage[key] = value
	}
	suggestCryptoKeysRequest := &keymanagementv1.SuggestCryptoKeysRequest{
		Context: req.GetContext(),
		Filter: &keymanagementv1.SuggestCryptoKeysRequest_Filter{
			OwnerId:      req.GetCustomerId(),
			CreatedAfter: req.GetContext().GetDeadline(),
			Labels:       baggage,
		},
	}
	suggestCryptoKeysReply, err := s.cryptoKeyQueryPort.SuggestCryptoKeys(ctx, suggestCryptoKeysRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.keymanagement.v1.CryptoKeyQueryService/SuggestCryptoKeys")
	}
	cloneConversationRequest := buildCloneConversationRequestForGetCustomerRates(req, suggestCryptoKeysReply)
	cloneConversationReply, err := s.conversationClient.CloneConversation(ctx, cloneConversationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.messaging.v1.ConversationService/CloneConversation")
	}
	return &customersv1.GetCustomerRatesResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: suggestCryptoKeysReply.GetResult().GetReferenceIds(),
			Details:      cloneConversationReply.GetResult().GetDetails(),
			Status:       "ok",
		},
		Customer: &customersv1.Customer{
			Audit: &commonv1.AuditInfo{
				CreatedAt: cloneConversationReply.GetConversation().GetAudit().GetCreatedAt(),
			},
			CustomerId: req.GetCustomerId(),
		},
	}, nil
}

// customerCalculationServerPurgeTenantSteps are the ways to run acme.identity.v1.TenantAdminService/PurgeTenant, by
// name.
var customerCalculationServerPurgeTenantSteps = map[string]func(*CustomerCalculationServer, context.Context, *identityv1.PurgeTenantRequest) (*identityv1.PurgeTenantResponse, error){
	"default": (*CustomerCalculationServer).purgeTenantStep,
	"audited": (*CustomerCalculationServer).purgeTenantAuditedStep,
}

func (s *CustomerCalculationServer) purgeTenantStep(ctx context.Context, request *identityv1.PurgeTenantRequest) (*identityv1.PurgeTenantResponse, error) {
	return s.tenantAdminClient.PurgeTenant(ctx, request)
}

func (s *CustomerCalculationServer) purgeTenantAuditedStep(ctx context.Context, request *identityv1.PurgeTenantRequest) (*identityv1.PurgeTenantResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.identity.v1.TenantAdminService/PurgeTenant")
	return s.tenantAdminClient.PurgeTenant(ctx, request)
}

// repairSearchIndexSender returns a function that sends one request to
// acme.search.v1.SearchIndexAdminService/RepairSearchIndex.
func (s *CustomerCalculationServer) repairSearchIndexSender(request *searchv1.RepairSearchIndexRequest) func(context.Context) (*searchv1.RepairSearchIndexResponse, error) {
	return func(ctx context.Context) (*searchv1.RepairSearchIndexResponse, error) {
		return s.searchIndexAdminClient.RepairSearchIndex(ctx, request)
	}
}

// customerCalculationServerRecalculatePointsLedgersSteps are the ways to run
// acme.loyalty.v1.PointsLedgerCalculationService/RecalculatePointsLedgers, by name.
var customerCalculationServerRecalculatePointsLedgersSteps = map[string]func(*CustomerCalculationServer, context.Context, *loyaltyv1.RecalculatePointsLedgersRequest) (*loyaltyv1.RecalculatePointsLedgersResponse, error){
	"default": (*CustomerCalculationServer).recalculatePointsLedgersStep,
	"audited": (*CustomerCalculationServer).recalculatePointsLedgersAuditedStep,
}

func (s *CustomerCalculationServer) recalculatePointsLedgersStep(ctx context.Context, request *loyaltyv1.RecalculatePointsLedgersRequest) (*loyaltyv1.RecalculatePointsLedgersResponse, error) {
	return s.pointsLedgerCalculationClient.RecalculatePointsLedgers(ctx, request)
}

func (s *CustomerCalculationServer) recalculatePointsLedgersAuditedStep(ctx context.Context, request *loyaltyv1.RecalculatePointsLedgersRequest) (*loyaltyv1.RecalculatePointsLedgersResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.loyalty.v1.PointsLedgerCalculationService/RecalculatePointsLedgers")
	return s.pointsLedgerCalculationClient.RecalculatePointsLedgers(ctx, request)
}
