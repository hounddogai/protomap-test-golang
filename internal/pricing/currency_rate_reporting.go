package pricing

import (
	"context"
	"log/slog"
	"sync"

	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	collectionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/collections/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	pricingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/pricing/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/anypb"
)

// CurrencyRateReportingServer implements `acme.pricing.v1.CurrencyRateReportingService`.
// Builds and schedules reports about currency rates.
type CurrencyRateReportingServer struct {
	pricingv1.UnimplementedCurrencyRateReportingServiceServer
	store                          *Store
	log                            *slog.Logger
	preferenceValidationClient     customersv1.PreferenceValidationServiceClient
	householdQueryClient           customersv1.HouseholdQueryServiceClient
	collectionCaseClient           collectionsv1.CollectionCaseServiceClient
	searchQueryHistoryClient       searchv1.SearchQueryHistoryServiceClient
	householdCalculationClient     customersv1.HouseholdCalculationServiceClient
	clients                        *registry.Registry
	conn                           *grpc.ClientConn
	accessPolicyQueryOnce          sync.Once
	accessPolicyQueryCached        authorizationv1.AccessPolicyQueryServiceClient
	priceOverrideCalculationOnce   sync.Once
	priceOverrideCalculationCached pricingv1.PriceOverrideCalculationServiceClient
}

// NewCurrencyRateReportingServer wires a CurrencyRateReportingServer.
func NewCurrencyRateReportingServer(deps platform.Deps) *CurrencyRateReportingServer {
	return &CurrencyRateReportingServer{
		store:                      NewStore(deps.DB),
		log:                        deps.Logger.With("service", "acme.pricing.v1.CurrencyRateReportingService"),
		preferenceValidationClient: customersv1.NewPreferenceValidationServiceClient(deps.Conn),
		householdQueryClient:       customersv1.NewHouseholdQueryServiceClient(deps.Conn),
		collectionCaseClient:       collectionsv1.NewCollectionCaseServiceClient(deps.Conn),
		searchQueryHistoryClient:   searchv1.NewSearchQueryHistoryServiceClient(deps.Conn),
		householdCalculationClient: customersv1.NewHouseholdCalculationServiceClient(deps.Conn),
		clients:                    deps.Clients,
		conn:                       deps.Conn,
	}
}

// GenerateCurrencyRateReport generates a currency rate.
func (s *CurrencyRateReportingServer) GenerateCurrencyRateReport(ctx context.Context, req *pricingv1.GenerateCurrencyRateReportRequest) (*pricingv1.GenerateCurrencyRateReportResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "currency_rate", req.GetCurrencyRateId(), "generate_currency_rate_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "generate currency rate report", "tenant", req.GetContext().GetTenantId())
	normalizePreferenceReply, err := s.preferenceValidationClient.NormalizePreference(ctx, &customersv1.NormalizePreferenceRequest{
		Context: req.GetContext(),
		Reason:  req.GetReason(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceValidationService/NormalizePreference")
	}
	return &pricingv1.GenerateCurrencyRateReportResponse{
		Result: &commonv1.OperationResult{
			Status:       normalizePreferenceReply.GetOutcome().GetStatus(),
			ReferenceIds: normalizePreferenceReply.GetResult().GetReferenceIds(),
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// FetchCurrencyRateReport returns a currency rate.
func (s *CurrencyRateReportingServer) FetchCurrencyRateReport(ctx context.Context, req *pricingv1.FetchCurrencyRateReportRequest) (*pricingv1.FetchCurrencyRateReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "currency_rate", req.GetCurrencyRateId(), "fetch_currency_rate_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "fetch currency rate report", "tenant", req.GetContext().GetTenantId())
	step := &currencyRateReportingServerFetchCurrencyRateReportFindHouseholdsStep{request: &customersv1.FindHouseholdsRequest{
		Context: req.GetContext(),
		Filter: &customersv1.FindHouseholdsRequest_Filter{
			OwnerId:      req.GetCurrencyRateId(),
			CreatedAfter: req.GetContext().GetDeadline(),
			Labels:       req.GetContext().GetBaggage(),
		},
	}}
	if err := s.runFetchCurrencyRateReportFindHouseholds(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdQueryService/FindHouseholds")
	}
	findHouseholdsReply := step.reply
	resolveHouseholdsReply, err := s.householdQueryClient.ResolveHouseholds(ctx, &customersv1.ResolveHouseholdsRequest{Context: req.GetContext()})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdQueryService/ResolveHouseholds")
	}
	_ = resolveHouseholdsReply
	return &pricingv1.FetchCurrencyRateReportResponse{
		Result: &commonv1.OperationResult{
			Details:      findHouseholdsReply.GetResult().GetDetails(),
			ReferenceIds: findHouseholdsReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		CurrencyRate: &pricingv1.CurrencyRate{CurrencyRateId: req.GetCurrencyRateId()},
	}, nil
}

// ScheduleCurrencyRateReport schedules a currency rate.
func (s *CurrencyRateReportingServer) ScheduleCurrencyRateReport(ctx context.Context, req *pricingv1.ScheduleCurrencyRateReportRequest) (*pricingv1.ScheduleCurrencyRateReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ScheduleCurrencyRateReport", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "currency_rate", req.GetCurrencyRateId(), "schedule_currency_rate_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "schedule currency rate report", "tenant", req.GetContext().GetTenantId())
	updateCollectionCaseReply, err := s.collectionCaseClient.UpdateCollectionCase(ctx, &collectionsv1.UpdateCollectionCaseRequest{
		Context: req.GetContext(),
		CollectionCase: &collectionsv1.CollectionCase{
			Assignee: &identityv1.UserRef{Email: req.GetContext().GetActor().GetEmail()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.collections.v1.CollectionCaseService/UpdateCollectionCase")
	}
	compareSearchQueryVersionsRequest := buildCompareSearchQueryVersionsRequestForScheduleCurrencyRateReport(req, updateCollectionCaseReply)
	compareSearchQueryVersionsReply, err := s.searchQueryHistoryClient.CompareSearchQueryVersions(ctx, compareSearchQueryVersionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryHistoryService/CompareSearchQueryVersions")
	}
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	snapshot, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	revertSearchQueryReply, err := s.searchQueryHistoryClient.RevertSearchQuery(ctx, &searchv1.RevertSearchQueryRequest{
		Context: req.GetContext(),
		SearchQuery: &searchv1.SearchQuery{
			Subject: &searchv1.SearchIndexRef{
				DisplayName:   compareSearchQueryVersionsReply.GetSearchQuery().GetSubject().GetDisplayName(),
				SearchIndexId: compareSearchQueryVersionsReply.GetSearchQuery().GetSubject().GetSearchIndexId(),
			},
			CorrelationId: compareSearchQueryVersionsReply.GetResult().GetCorrelationId(),
			Detail:        packed,
		},
		Details: &searchv1.RevertSearchQueryRequest_Details{Snapshot: snapshot},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryHistoryService/RevertSearchQuery")
	}
	_ = revertSearchQueryReply
	return &pricingv1.ScheduleCurrencyRateReportResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds:  updateCollectionCaseReply.GetResult().GetReferenceIds(),
			Details:       updateCollectionCaseReply.GetResult().GetDetails(),
			CorrelationId: compareSearchQueryVersionsReply.GetSearchQuery().GetCorrelationId(),
			Status:        "ok",
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// StreamCurrencyRateMetrics streams the changes of currency rates as they happen.
func (s *CurrencyRateReportingServer) StreamCurrencyRateMetrics(req *pricingv1.StreamCurrencyRateMetricsRequest, stream grpc.ServerStreamingServer[pricingv1.StreamCurrencyRateMetricsResponse]) error {
	ctx := stream.Context()
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected StreamCurrencyRateMetrics", "error", err)
		return err
	}
	s.log.InfoContext(ctx, "stream currency rate metrics", "tenant", req.GetContext().GetTenantId())
	resolvePriceQuotesRequest := &pricingv1.ResolvePriceQuotesRequest{Context: req.GetContext()}
	resolvePriceQuotesReply, err := resolvePriceQuotesCall(ctx, resolvePriceQuotesRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.pricing.v1.PriceQuoteQueryService/ResolvePriceQuotes")
	}
	if err := stream.Send(&pricingv1.StreamCurrencyRateMetricsResponse{
		Result: &commonv1.OperationResult{
			Message: resolvePriceQuotesReply.GetResult().GetMessage(),
			Status:  resolvePriceQuotesReply.GetResult().GetStatus(),
		},
		CurrencyRate: &pricingv1.CurrencyRate{
			Subject: &customersv1.CustomerRef{Email: req.GetSubscriber().GetEmail()},
		},
	}); err != nil {
		return err
	}
	return nil
}

// ListCurrencyRateReports lists currency rates a page at a time.
func (s *CurrencyRateReportingServer) ListCurrencyRateReports(ctx context.Context, req *pricingv1.ListCurrencyRateReportsRequest) (*pricingv1.ListCurrencyRateReportsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "list currency rate reports", "tenant", req.GetContext().GetTenantId())
	packed, err := anypb.New(req.GetPage())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	snapshot, err := protojson.Marshal(req.GetPage())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	revertSearchQueryReply, err := s.searchQueryHistoryClient.RevertSearchQuery(ctx, &searchv1.RevertSearchQueryRequest{
		Context: req.GetContext(),
		SearchQuery: &searchv1.SearchQuery{
			Measurements: []*searchv1.SearchQueryMeasurement{&searchv1.SearchQueryMeasurement{ObservedAt: req.GetFilter().GetCreatedAfter()}},
			Detail:       packed,
		},
		Details: &searchv1.RevertSearchQueryRequest_Details{Snapshot: snapshot},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryHistoryService/RevertSearchQuery")
	}
	explainHouseholdCalculationRequest := &customersv1.ExplainHouseholdCalculationRequest{}
	explainHouseholdCalculationRequest.Context = req.GetContext()
	explainHouseholdCalculationRequest.Household = &customersv1.Household{
		Children: []*customersv1.Household{&customersv1.Household{
			Location: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
			Children: []*customersv1.Household{&customersv1.Household{
				Attributes: revertSearchQueryReply.GetSearchQuery().GetAttributes(),
			}},
		}},
	}
	explainHouseholdCalculationReply, err := s.householdCalculationClient.ExplainHouseholdCalculation(ctx, explainHouseholdCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdCalculationService/ExplainHouseholdCalculation")
	}
	return &pricingv1.ListCurrencyRateReportsResponse{
		Result: &commonv1.OperationResult{
			Details: revertSearchQueryReply.GetResult().GetDetails(),
			Status:  explainHouseholdCalculationReply.GetOutcome().GetStatus(),
		},
		CurrencyRates: []*pricingv1.CurrencyRate{&pricingv1.CurrencyRate{Source: revertSearchQueryReply.GetSearchQuery().GetSource()}},
	}, nil
}

// ShareCurrencyRateReport shares a currency rate.
func (s *CurrencyRateReportingServer) ShareCurrencyRateReport(ctx context.Context, req *pricingv1.ShareCurrencyRateReportRequest) (*pricingv1.ShareCurrencyRateReportResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "currency_rate", req.GetCurrencyRateId(), "share_currency_rate_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "share currency rate report", "tenant", req.GetContext().GetTenantId())
	preferenceValidation, err := registry.Lookup[customersv1.PreferenceValidationServiceClient](s.clients, "acme.customers.v1.PreferenceValidationService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	previewPreferenceRequest := &customersv1.PreviewPreferenceRequest{
		Context: req.GetContext(),
		Details: &customersv1.PreviewPreferenceRequest_Details{
			Contact: &commonv1.ContactInfo{Name: req.GetDetails().GetRecipient().GetName()},
		},
	}
	previewPreferenceReply, err := preferenceValidation.PreviewPreference(ctx, previewPreferenceRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceValidationService/PreviewPreference")
	}
	browseAccessPoliciesRequest := &authorizationv1.BrowseAccessPoliciesRequest{
		Context: req.GetContext(),
		Filter: &authorizationv1.BrowseAccessPoliciesRequest_Filter{
			CreatedAfter: previewPreferenceReply.GetOutcome().GetEffectiveAt(),
			OwnerId:      previewPreferenceReply.GetPreference().GetOwner().GetUserId(),
			Labels:       previewPreferenceReply.GetPreference().GetParameters(),
		},
	}
	browseAccessPoliciesReply, err := s.accessPolicyQueryLazy().BrowseAccessPolicies(ctx, browseAccessPoliciesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyQueryService/BrowseAccessPolicies")
	}
	return &pricingv1.ShareCurrencyRateReportResponse{
		Result: &commonv1.OperationResult{
			Details: previewPreferenceReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Outcome: &pricingv1.ShareCurrencyRateReportResponse_Outcome{
			EffectiveAt: previewPreferenceReply.GetOutcome().GetEffectiveAt(),
			Message:     browseAccessPoliciesReply.GetResult().GetMessage(),
		},
		CurrencyRate: &pricingv1.CurrencyRate{
			CurrencyRateId: req.GetCurrencyRateId(),
			Subject:        &customersv1.CustomerRef{Email: req.GetDetails().GetRecipient().GetEmail()},
		},
	}, nil
}

// CountCurrencyRateTotals counts currency rates by status.
func (s *CurrencyRateReportingServer) CountCurrencyRateTotals(ctx context.Context, req *pricingv1.CountCurrencyRateTotalsRequest) (*pricingv1.CountCurrencyRateTotalsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CountCurrencyRateTotals", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "count currency rate totals", "tenant", req.GetContext().GetTenantId())
	normalizePreferenceRequest := &customersv1.NormalizePreferenceRequest{
		Context: req.GetContext(),
		Details: &customersv1.NormalizePreferenceRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
	}
	normalizePreferenceReply, err := grpcx.Call(ctx, s.preferenceValidationClient.NormalizePreference, normalizePreferenceRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceValidationService/NormalizePreference")
	}
	simulatePriceOverrideRequest := &pricingv1.SimulatePriceOverrideRequest{
		Context: req.GetContext(),
		PriceOverride: &pricingv1.PriceOverride{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: normalizePreferenceReply.GetPreference().GetAudit().GetUpdatedBy(),
				Etag:      normalizePreferenceReply.GetPreference().GetAudit().GetEtag(),
			},
			Action: &pricingv1.PriceOverride_Discount{Discount: &pricingv1.PriceOverrideDiscountAction{
				Amount: normalizePreferenceReply.GetPreference().GetDiscount().GetAmount(),
			}},
			Owner: normalizePreferenceReply.GetPreference().GetOwner(),
		},
	}
	simulatePriceOverrideReply, err := s.priceOverrideCalculationLazy().SimulatePriceOverride(ctx, simulatePriceOverrideRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceOverrideCalculationService/SimulatePriceOverride")
	}
	return &pricingv1.CountCurrencyRateTotalsResponse{
		Result: &commonv1.OperationResult{
			Message: normalizePreferenceReply.GetResult().GetMessage(),
			Details: simulatePriceOverrideReply.GetResult().GetDetails(),
			Status:  simulatePriceOverrideReply.GetResult().GetStatus(),
		},
	}, nil
}

// DeleteCurrencyRateReport deletes a currency rate.
func (s *CurrencyRateReportingServer) DeleteCurrencyRateReport(ctx context.Context, req *pricingv1.DeleteCurrencyRateReportRequest) (*pricingv1.DeleteCurrencyRateReportResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "currency_rate", req.GetCurrencyRateId(), "delete_currency_rate_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "delete currency rate report", "tenant", req.GetContext().GetTenantId())
	householdQuery, err := registry.Lookup[customersv1.HouseholdQueryServiceClient](s.clients, "acme.customers.v1.HouseholdQueryService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	resolveHouseholdsRequest := &customersv1.ResolveHouseholdsRequest{Context: req.GetContext()}
	resolveHouseholdsReply, err := householdQuery.ResolveHouseholds(ctx, resolveHouseholdsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdQueryService/ResolveHouseholds")
	}
	return &pricingv1.DeleteCurrencyRateReportResponse{
		Result: &commonv1.OperationResult{
			Details: resolveHouseholdsReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}

// currencyRateReportingServerFetchCurrencyRateReportFindHouseholdsStep carries the request of one step of
// FetchCurrencyRateReport and, once it ran, its reply.
type currencyRateReportingServerFetchCurrencyRateReportFindHouseholdsStep struct {
	request *customersv1.FindHouseholdsRequest
	reply   *customersv1.FindHouseholdsResponse
}

func (s *CurrencyRateReportingServer) runFetchCurrencyRateReportFindHouseholds(ctx context.Context, step *currencyRateReportingServerFetchCurrencyRateReportFindHouseholdsStep) error {
	reply, err := s.householdQueryClient.FindHouseholds(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}

// accessPolicyQueryLazy dials acme.authorization.v1.AccessPolicyQueryService the first time a handler needs it.
func (s *CurrencyRateReportingServer) accessPolicyQueryLazy() authorizationv1.AccessPolicyQueryServiceClient {
	s.accessPolicyQueryOnce.Do(func() { s.accessPolicyQueryCached = authorizationv1.NewAccessPolicyQueryServiceClient(s.conn) })
	return s.accessPolicyQueryCached
}

// priceOverrideCalculationLazy dials acme.pricing.v1.PriceOverrideCalculationService the first time a handler needs it.
func (s *CurrencyRateReportingServer) priceOverrideCalculationLazy() pricingv1.PriceOverrideCalculationServiceClient {
	s.priceOverrideCalculationOnce.Do(func() { s.priceOverrideCalculationCached = pricingv1.NewPriceOverrideCalculationServiceClient(s.conn) })
	return s.priceOverrideCalculationCached
}
