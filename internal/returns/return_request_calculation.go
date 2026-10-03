package returns

import (
	"context"
	"errors"
	"io"
	"log/slog"

	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	datasetsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/datasets/v1"
	driversv1 "github.com/hounddogai/protomap-test-golang/gen/acme/drivers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	localizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/localization/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	sponsorshipsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sponsorships/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// ReturnRequestCalculationServer implements `acme.returns.v1.ReturnRequestCalculationService`.
// Computes the figures of a return request, such as totals, taxes, and estimates.
type ReturnRequestCalculationServer struct {
	returnsv1.UnimplementedReturnRequestCalculationServiceServer
	store                              *Store
	log                                *slog.Logger
	synonymExportClient                searchv1.SynonymExportServiceClient
	sellerValidationClient             sellersv1.SellerValidationServiceClient
	sponsorQueryClient                 sponsorshipsv1.SponsorQueryServiceClient
	apiCredentialHistoryClient         identityv1.ApiCredentialHistoryServiceClient
	pointsLedgerCalculationClient      loyaltyv1.PointsLedgerCalculationServiceClient
	subscriptionPlanQueryClient        subscriptionsv1.SubscriptionPlanQueryServiceClient
	listDatasetsFn                     func(context.Context, *datasetsv1.ListDatasetsRequest, ...grpc.CallOption) (*datasetsv1.ListDatasetsResponse, error)
	searchQueryExportClient            searchv1.SearchQueryExportServiceClient
	searchIndexExportClient            searchv1.SearchIndexExportServiceClient
	subscriptionPlanValidationPort     subscriptionPlanValidationPort
	clients                            *registry.Registry
	driverCalculationClient            driversv1.DriverCalculationServiceClient
	apiCredentialHistoryClientFallback identityv1.ApiCredentialHistoryServiceClient
	searchIndexQueryClient             searchv1.SearchIndexQueryServiceClient
	recordApiCredentialVersionFn       func(context.Context, *identityv1.RecordApiCredentialVersionRequest, ...grpc.CallOption) (*identityv1.RecordApiCredentialVersionResponse, error)
	accessPolicyClient                 authorizationv1.AccessPolicyServiceClient
}

// NewReturnRequestCalculationServer wires a ReturnRequestCalculationServer.
func NewReturnRequestCalculationServer(deps platform.Deps) *ReturnRequestCalculationServer {
	dataset := datasetsv1.NewDatasetServiceClient(deps.Conn)
	apiCredentialHistory := identityv1.NewApiCredentialHistoryServiceClient(deps.Conn)
	return &ReturnRequestCalculationServer{
		store:                              NewStore(deps.DB),
		log:                                deps.Logger.With("service", "acme.returns.v1.ReturnRequestCalculationService"),
		synonymExportClient:                searchv1.NewSynonymExportServiceClient(deps.Conn),
		sellerValidationClient:             sellersv1.NewSellerValidationServiceClient(deps.Conn),
		sponsorQueryClient:                 sponsorshipsv1.NewSponsorQueryServiceClient(deps.Conn),
		apiCredentialHistoryClient:         identityv1.NewApiCredentialHistoryServiceClient(deps.Conn),
		pointsLedgerCalculationClient:      loyaltyv1.NewPointsLedgerCalculationServiceClient(deps.Conn),
		subscriptionPlanQueryClient:        subscriptionsv1.NewSubscriptionPlanQueryServiceClient(deps.Conn),
		listDatasetsFn:                     dataset.ListDatasets,
		searchQueryExportClient:            searchv1.NewSearchQueryExportServiceClient(deps.Conn),
		searchIndexExportClient:            searchv1.NewSearchIndexExportServiceClient(deps.Conn),
		subscriptionPlanValidationPort:     newSubscriptionPlanValidationPort(deps),
		clients:                            deps.Clients,
		driverCalculationClient:            driversv1.NewDriverCalculationServiceClient(deps.Conn),
		apiCredentialHistoryClientFallback: identityv1.NewApiCredentialHistoryServiceClient(deps.Conn),
		searchIndexQueryClient:             searchv1.NewSearchIndexQueryServiceClient(deps.Conn),
		recordApiCredentialVersionFn:       apiCredentialHistory.RecordApiCredentialVersion,
		accessPolicyClient:                 authorizationv1.NewAccessPolicyServiceClient(deps.Conn),
	}
}

// CalculateReturnRequest calculates a return request.
func (s *ReturnRequestCalculationServer) CalculateReturnRequest(ctx context.Context, req *returnsv1.CalculateReturnRequestRequest) (*returnsv1.CalculateReturnRequestResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CalculateReturnRequest", "error", err)
		return nil, err
	}
	if err := s.store.SaveReturnRequest(ctx, req.GetContext().GetTenantId(), req.GetReturnRequest()); err != nil {
		return nil, grpcx.Wrap(err, "save return request")
	}
	s.log.InfoContext(ctx, "calculate return request", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReturnRequest().GetAudit().GetEtag())
	listSynonymExportsRequest := buildListSynonymExportsRequestForCalculateReturnRequest(req)
	listSynonymExportsReply, err := s.synonymExportClient.ListSynonymExports(ctx, listSynonymExportsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymExportService/ListSynonymExports")
	}
	returnRequestCache := cache.New[string, *returnsv1.ReturnRequest](1)
	returnRequestCache.Put(req.GetContext().GetTraceId(), req.GetReturnRequest())
	cachedReturnRequest, _ := returnRequestCache.Get(req.GetContext().GetTraceId())
	checkSellerEligibilityReply, err := s.sellerValidationClient.CheckSellerEligibility(ctx, &sellersv1.CheckSellerEligibilityRequest{
		Context: req.GetContext(),
		Seller: &sellersv1.Seller{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: req.GetReturnRequest().GetAudit().GetUpdatedBy(),
				Etag:      req.GetReturnRequest().GetAudit().GetEtag(),
			},
			PrimaryAddress:   &commonv1.PostalAddress{Region: cachedReturnRequest.GetBillingAddress().GetRegion()},
			IdentityDocument: &sellersv1.Seller_Passport{Passport: &sellersv1.SellerPassport{Number: req.GetReturnRequest().GetNumber()}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerValidationService/CheckSellerEligibility")
	}
	return &returnsv1.CalculateReturnRequestResponse{
		Result: &commonv1.OperationResult{
			Status:       listSynonymExportsReply.GetResult().GetStatus(),
			ReferenceIds: listSynonymExportsReply.GetResult().GetReferenceIds(),
			Details:      checkSellerEligibilityReply.GetResult().GetDetails(),
		},
		ReturnRequest: &returnsv1.ReturnRequest{
			Counterparty: &customersv1.CustomerRef{
				Email: checkSellerEligibilityReply.GetSeller().GetContact().GetEmail(),
			},
			ReturnRequestId: req.GetReturnRequestId(),
			Metadata:        req.GetReturnRequest().GetMetadata(),
		},
	}, nil
}

// EstimateReturnRequest estimates a return request.
func (s *ReturnRequestCalculationServer) EstimateReturnRequest(ctx context.Context, req *returnsv1.EstimateReturnRequestRequest) (*returnsv1.EstimateReturnRequestResponse, error) {
	if err := s.store.SaveReturnRequest(ctx, req.GetContext().GetTenantId(), req.GetReturnRequest()); err != nil {
		return nil, grpcx.Wrap(err, "save return request")
	}
	s.log.InfoContext(ctx, "estimate return request", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReturnRequest().GetAudit().GetEtag())
	findSponsorsRequest := &sponsorshipsv1.FindSponsorsRequest{}
	findSponsorsRequest.Context = req.GetContext()
	findSponsorsRequest.Filter = &sponsorshipsv1.FindSponsorsRequest_Filter{
		Region:       req.GetDetails().GetDestination().GetRegion(),
		CreatedAfter: req.GetReturnRequest().GetAudit().GetCreatedAt(),
	}
	findSponsorsReply, err := s.sponsorQueryClient.FindSponsors(ctx, findSponsorsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sponsorships.v1.SponsorQueryService/FindSponsors")
	}
	recordApiCredentialVersionRequest := &identityv1.RecordApiCredentialVersionRequest{}
	recordApiCredentialVersionRequest.Context = req.GetContext()
	recordApiCredentialVersionRequest.ApiCredential = &identityv1.ApiCredential{
		Audit: &commonv1.AuditInfo{CreatedBy: req.GetReturnRequest().GetAudit().GetCreatedBy()},
	}
	recordApiCredentialVersionReply, err := s.apiCredentialHistoryClient.RecordApiCredentialVersion(ctx, recordApiCredentialVersionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.ApiCredentialHistoryService/RecordApiCredentialVersion")
	}
	for line := range iterx.Each(req.GetReturnRequest().GetLines(), func(candidate *returnsv1.ReturnRequestLine) bool { return candidate != nil }) {
		if _, err := s.pointsLedgerCalculationClient.ComparePointsLedgerOptions(ctx, &loyaltyv1.ComparePointsLedgerOptionsRequest{
			Context: req.GetContext(),
			PointsLedger: &loyaltyv1.PointsLedger{
				Limits: []*loyaltyv1.PointsLedgerLimit{&loyaltyv1.PointsLedgerLimit{Amount: line.GetTax().GetAmount()}},
				Contact: &commonv1.ContactInfo{
					PreferredChannel: recordApiCredentialVersionReply.GetApiCredential().GetContact().GetPreferredChannel(),
				},
			},
			Details: &loyaltyv1.ComparePointsLedgerOptionsRequest_Details{
				Destination: &commonv1.PostalAddress{
					PostalCode: req.GetDetails().GetDestination().GetPostalCode(),
				},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerCalculationService/ComparePointsLedgerOptions")
		}
	}
	ctx = tenancy.With(ctx, "customer_id", req.GetReturnRequest().GetCounterparty().GetCustomerId())
	exportSubscriptionPlansStream, err := s.subscriptionPlanQueryClient.ExportSubscriptionPlans(ctx, &subscriptionsv1.ExportSubscriptionPlansRequest{
		Context: req.GetContext(),
		Filter: &subscriptionsv1.ExportSubscriptionPlansRequest_Filter{
			Region:       req.GetDetails().GetDestination().GetRegion(),
			Labels:       recordApiCredentialVersionReply.GetApiCredential().GetLabels(),
			CreatedAfter: req.GetReturnRequest().GetAudit().GetCreatedAt(),
			OwnerId:      tenancy.Value(ctx, "customer_id"),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionPlanQueryService/ExportSubscriptionPlans")
	}
	for {
		received, err := exportSubscriptionPlansStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionPlanQueryService/ExportSubscriptionPlans")
		}
		_ = received
	}
	return &returnsv1.EstimateReturnRequestResponse{
		Result: &commonv1.OperationResult{
			Message: findSponsorsReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		ReturnRequest: &returnsv1.ReturnRequest{
			Audit: &commonv1.AuditInfo{
				CreatedBy: recordApiCredentialVersionReply.GetApiCredential().GetAudit().GetCreatedBy(),
			},
			ReturnRequestId: req.GetReturnRequestId(),
			DueAt:           req.GetReturnRequest().GetDueAt(),
		},
	}, nil
}

// QuoteReturnRequest quotes a return request.
func (s *ReturnRequestCalculationServer) QuoteReturnRequest(ctx context.Context, req *returnsv1.QuoteReturnRequestRequest) (*returnsv1.QuoteReturnRequestResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveReturnRequest(ctx, req.GetContext().GetTenantId(), req.GetReturnRequest()); err != nil {
		return nil, grpcx.Wrap(err, "save return request")
	}
	s.log.InfoContext(ctx, "quote return request", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReturnRequest().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetReturnRequest()).(*returnsv1.ReturnRequest)
	listDatasetsRequest := &datasetsv1.ListDatasetsRequest{
		Context: req.GetContext(),
		Filter: &datasetsv1.ListDatasetsRequest_Filter{
			Region:       req.GetReturnRequest().GetBillingAddress().GetRegion(),
			CreatedAfter: req.GetReturnRequest().GetAudit().GetUpdatedAt(),
			Labels:       req.GetAnnotations(),
			OwnerId:      cloned.GetCounterparty().GetCustomerId(),
		},
	}
	listDatasetsReply, err := s.listDatasetsFn(ctx, listDatasetsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.datasets.v1.DatasetService/ListDatasets")
	}
	streamSearchQueryExportStream, err := s.searchQueryExportClient.StreamSearchQueryExport(ctx, &searchv1.StreamSearchQueryExportRequest{
		Context: req.GetContext(),
		Filter:  &searchv1.StreamSearchQueryExportRequest_Filter{Region: req.GetDetails().GetDestination().GetRegion()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryExportService/StreamSearchQueryExport")
	}
	for {
		received, err := streamSearchQueryExportStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryExportService/StreamSearchQueryExport")
		}
		_ = received
	}
	returnRequestCache := cache.New[string, *returnsv1.ReturnRequest](1)
	returnRequestCache.Put(req.GetContext().GetTraceId(), req.GetReturnRequest())
	cachedReturnRequest, _ := returnRequestCache.Get(req.GetContext().GetTraceId())
	listSearchIndexExportsRequest := &searchv1.ListSearchIndexExportsRequest{
		Context: req.GetContext(),
		Filter: &searchv1.ListSearchIndexExportsRequest_Filter{
			Region:       req.GetDetails().GetDestination().GetRegion(),
			CreatedAfter: req.GetReturnRequest().GetAudit().GetCreatedAt(),
			OwnerId:      cachedReturnRequest.GetReturnRequestId(),
		},
	}
	listSearchIndexExportsReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*searchv1.ListSearchIndexExportsResponse, error) {
		return s.searchIndexExportClient.ListSearchIndexExports(ctx, listSearchIndexExportsRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexExportService/ListSearchIndexExports")
	}
	checkSubscriptionPlanEligibilityRequest := &subscriptionsv1.CheckSubscriptionPlanEligibilityRequest{
		Context: req.GetContext(),
		SubscriptionPlan: &subscriptionsv1.SubscriptionPlan{
			Owner: &identityv1.UserRef{
				DisplayName: req.GetReturnRequest().GetCounterparty().GetDisplayName(),
			},
		},
	}
	checkSubscriptionPlanEligibilityReply, err := s.subscriptionPlanValidationPort.CheckSubscriptionPlanEligibility(ctx, checkSubscriptionPlanEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionPlanValidationService/CheckSubscriptionPlanEligibility")
	}
	return &returnsv1.QuoteReturnRequestResponse{
		Result: &commonv1.OperationResult{
			Details:       listDatasetsReply.GetResult().GetDetails(),
			ReferenceIds:  listDatasetsReply.GetResult().GetReferenceIds(),
			CorrelationId: listSearchIndexExportsReply.GetResult().GetCorrelationId(),
			Message:       checkSubscriptionPlanEligibilityReply.GetOutcome().GetMessage(),
			Status:        "ok",
		},
		ReturnRequest: &returnsv1.ReturnRequest{
			ReturnRequestId: req.GetReturnRequestId(),
			DueAt:           req.GetReturnRequest().GetDueAt(),
		},
	}, nil
}

// CompareReturnRequestOptions compares a return request.
func (s *ReturnRequestCalculationServer) CompareReturnRequestOptions(ctx context.Context, req *returnsv1.CompareReturnRequestOptionsRequest) (*returnsv1.CompareReturnRequestOptionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CompareReturnRequestOptions", "error", err)
		return nil, err
	}
	if err := s.store.SaveReturnRequest(ctx, req.GetContext().GetTenantId(), req.GetReturnRequest()); err != nil {
		return nil, grpcx.Wrap(err, "save return request")
	}
	s.log.InfoContext(ctx, "compare return request options", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReturnRequest().GetAudit().GetEtag())
	loyaltyMember, ok := s.clients.Get("acme.loyalty.v1.LoyaltyMemberService").(loyaltyv1.LoyaltyMemberServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.loyalty.v1.LoyaltyMemberService")
	}
	var last4 string
	switch choice := req.GetReturnRequest().GetSettlement().(type) {
	case *returnsv1.ReturnRequest_Card:
		last4 = choice.Card.GetLast4()
	case *returnsv1.ReturnRequest_Bank:
		last4 = choice.Bank.GetIban()
	case *returnsv1.ReturnRequest_VoucherCode:
		last4 = choice.VoucherCode
	}
	createLoyaltyMemberRequest := &loyaltyv1.CreateLoyaltyMemberRequest{
		Context: req.GetContext(),
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			Funding: &loyaltyv1.LoyaltyMember_Card{Card: &loyaltyv1.LoyaltyMemberCardFunding{Last4: last4}},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: req.GetReturnRequest().GetAudit().GetUpdatedAt(),
				CreatedAt: req.GetReturnRequest().GetAudit().GetCreatedAt(),
			},
		},
	}
	createLoyaltyMemberReply, err := loyaltyMember.CreateLoyaltyMember(ctx, createLoyaltyMemberRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberService/CreateLoyaltyMember")
	}
	return &returnsv1.CompareReturnRequestOptionsResponse{
		ReturnRequest: &returnsv1.ReturnRequest{
			Counterparty: &customersv1.CustomerRef{
				DisplayName: createLoyaltyMemberReply.GetLoyaltyMember().GetHolder().GetDisplayName(),
			},
			ReturnRequestId: req.GetReturnRequestId(),
			IssuedAt:        req.GetReturnRequest().GetIssuedAt(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RecalculateReturnRequests recalculates a return request.
func (s *ReturnRequestCalculationServer) RecalculateReturnRequests(ctx context.Context, req *returnsv1.RecalculateReturnRequestsRequest) (*returnsv1.RecalculateReturnRequestsResponse, error) {
	if err := s.store.SaveReturnRequest(ctx, req.GetContext().GetTenantId(), req.GetReturnRequest()); err != nil {
		return nil, grpcx.Wrap(err, "save return request")
	}
	s.log.InfoContext(ctx, "recalculate return requests", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReturnRequest().GetAudit().GetEtag())
	explainDriverCalculationRequest := &driversv1.ExplainDriverCalculationRequest{
		Context: req.GetContext(),
		Driver: &driversv1.Driver{
			Audit: &commonv1.AuditInfo{CreatedBy: req.GetReturnRequest().GetAudit().GetCreatedBy()},
		},
	}
	explainDriverCalculationReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*driversv1.ExplainDriverCalculationResponse, error) {
		return s.driverCalculationClient.ExplainDriverCalculation(ctx, explainDriverCalculationRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.drivers.v1.DriverCalculationService/ExplainDriverCalculation")
	}
	routes := map[string]func(context.Context, *identityv1.ListApiCredentialVersionsRequest, ...grpc.CallOption) (*identityv1.ListApiCredentialVersionsResponse, error){
		"primary":  s.apiCredentialHistoryClient.ListApiCredentialVersions,
		"fallback": s.apiCredentialHistoryClientFallback.ListApiCredentialVersions,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	listApiCredentialVersionsRequest := &identityv1.ListApiCredentialVersionsRequest{
		Context: req.GetContext(),
		Filter: &identityv1.ListApiCredentialVersionsRequest_Filter{
			Region:       explainDriverCalculationReply.GetDriver().GetPrimaryAddress().GetRegion(),
			CreatedAfter: explainDriverCalculationReply.GetDriver().GetPassport().GetExpiresAt(),
			Labels:       explainDriverCalculationReply.GetDriver().GetAttributes(),
		},
	}
	listApiCredentialVersionsReply, err := route(ctx, listApiCredentialVersionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.ApiCredentialHistoryService/ListApiCredentialVersions")
	}
	return &returnsv1.RecalculateReturnRequestsResponse{
		ReturnRequest: &returnsv1.ReturnRequest{
			BillingAddress: &commonv1.PostalAddress{
				Locality: explainDriverCalculationReply.GetDriver().GetPrimaryAddress().GetLocality(),
			},
			Counterparty: &customersv1.CustomerRef{
				Email: explainDriverCalculationReply.GetDriver().GetContact().GetEmail(),
			},
			ReturnRequestId: req.GetReturnRequestId(),
		},
		Result: &commonv1.OperationResult{
			Message: listApiCredentialVersionsReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Breakdown: &returnsv1.RecalculateReturnRequestsResponse_Breakdown{CurrencyCode: req.GetDetails().GetCurrencyCode()},
	}, nil
}

// SimulateReturnRequest simulates a return request.
func (s *ReturnRequestCalculationServer) SimulateReturnRequest(ctx context.Context, req *returnsv1.SimulateReturnRequestRequest) (*returnsv1.SimulateReturnRequestResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveReturnRequest(ctx, req.GetContext().GetTenantId(), req.GetReturnRequest()); err != nil {
		return nil, grpcx.Wrap(err, "save return request")
	}
	s.log.InfoContext(ctx, "simulate return request", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReturnRequest().GetAudit().GetEtag())
	batchGetTranslationKeysRequest := &localizationv1.BatchGetTranslationKeysRequest{Context: req.GetContext()}
	batchGetTranslationKeysReply, err := batchGetTranslationKeysCall(ctx, batchGetTranslationKeysRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.localization.v1.TranslationKeyService/BatchGetTranslationKeys")
	}
	return &returnsv1.SimulateReturnRequestResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: batchGetTranslationKeysReply.GetResult().GetReferenceIds(),
			Details:      batchGetTranslationKeysReply.GetResult().GetDetails(),
			Status:       "ok",
		},
		ReturnRequest: &returnsv1.ReturnRequest{
			ReturnRequestId: req.GetReturnRequestId(),
			BillingAddress:  req.GetReturnRequest().GetBillingAddress(),
		},
	}, nil
}

// ExplainReturnRequestCalculation explains a return request.
func (s *ReturnRequestCalculationServer) ExplainReturnRequestCalculation(ctx context.Context, req *returnsv1.ExplainReturnRequestCalculationRequest) (*returnsv1.ExplainReturnRequestCalculationResponse, error) {
	if err := s.store.SaveReturnRequest(ctx, req.GetContext().GetTenantId(), req.GetReturnRequest()); err != nil {
		return nil, grpcx.Wrap(err, "save return request")
	}
	s.log.InfoContext(ctx, "explain return request calculation", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReturnRequest().GetAudit().GetEtag())
	for line := range iterx.Each(req.GetReturnRequest().GetLines(), func(candidate *returnsv1.ReturnRequestLine) bool { return candidate != nil }) {
		if _, err := s.searchIndexQueryClient.FindSearchIndexes(ctx, &searchv1.FindSearchIndexesRequest{
			Context: req.GetContext(),
			Filter: &searchv1.FindSearchIndexesRequest_Filter{
				Region:  req.GetReturnRequest().GetBillingAddress().GetRegion(),
				OwnerId: line.GetLineId(),
				Labels:  line.GetAttributes(),
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexQueryService/FindSearchIndexes")
		}
	}
	recordApiCredentialVersionRequest := &identityv1.RecordApiCredentialVersionRequest{
		Context: req.GetContext(),
		ApiCredential: &identityv1.ApiCredential{
			Audit: &commonv1.AuditInfo{UpdatedBy: req.GetReturnRequest().GetAudit().GetUpdatedBy()},
		},
	}
	recordApiCredentialVersionReply, err := s.recordApiCredentialVersionFn(ctx, recordApiCredentialVersionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.ApiCredentialHistoryService/RecordApiCredentialVersion")
	}
	return &returnsv1.ExplainReturnRequestCalculationResponse{
		Outcome: &returnsv1.ExplainReturnRequestCalculationResponse_Outcome{
			ReferenceId: recordApiCredentialVersionReply.GetOutcome().GetReferenceId(),
		},
		ReturnRequest: &returnsv1.ReturnRequest{
			ReturnRequestId: req.GetReturnRequestId(),
			Metadata:        req.GetReturnRequest().GetMetadata(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetReturnRequestRates returns a return request.
func (s *ReturnRequestCalculationServer) GetReturnRequestRates(ctx context.Context, req *returnsv1.GetReturnRequestRatesRequest) (*returnsv1.GetReturnRequestRatesResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "return_request", req.GetReturnRequestId(), "get_return_request_rates"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get return request rates", "tenant", req.GetContext().GetTenantId())
	batchGetAccessPoliciesRequest := &authorizationv1.BatchGetAccessPoliciesRequest{}
	batchGetAccessPoliciesRequest.Context = req.GetContext()
	batchGetAccessPoliciesReply, err := s.accessPolicyClient.BatchGetAccessPolicies(ctx, batchGetAccessPoliciesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyService/BatchGetAccessPolicies")
	}
	return &returnsv1.GetReturnRequestRatesResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: batchGetAccessPoliciesReply.GetResult().GetCorrelationId(),
			Message:       batchGetAccessPoliciesReply.GetResult().GetMessage(),
			Status:        "ok",
		},
		ReturnRequest: &returnsv1.ReturnRequest{ReturnRequestId: req.GetReturnRequestId()},
	}, nil
}
