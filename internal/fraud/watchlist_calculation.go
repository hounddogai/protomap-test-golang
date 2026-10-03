package fraud

import (
	"context"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	warehousingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/warehousing/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// WatchlistCalculationServer implements `acme.fraud.v1.WatchlistCalculationService`.
// Computes the figures of a watchlist, such as totals, taxes, and estimates.
type WatchlistCalculationServer struct {
	fraudv1.UnsafeWatchlistCalculationServiceServer
	store                               *Store
	log                                 *slog.Logger
	sessionQueryClient                  identityv1.SessionQueryServiceClient
	fraudSignalValidationClient         fraudv1.FraudSignalValidationServiceClient
	fraudSignalValidationClientFallback fraudv1.FraudSignalValidationServiceClient
	featureFlagQueryClient              featureflagsv1.FeatureFlagQueryServiceClient
	returnRequestQueryClient            returnsv1.ReturnRequestQueryServiceClient
	tenantAdminClient                   identityv1.TenantAdminServiceClient
	conn                                *grpc.ClientConn
	usageRecordCalculationClient        subscriptionsv1.UsageRecordCalculationServiceClient
	clients                             *registry.Registry
	storageBinQueryClient               warehousingv1.StorageBinQueryServiceClient
	riskAssessmentValidationClient      fraudv1.RiskAssessmentValidationServiceClient
}

// NewWatchlistCalculationServer wires a WatchlistCalculationServer.
func NewWatchlistCalculationServer(deps platform.Deps) *WatchlistCalculationServer {
	return &WatchlistCalculationServer{
		store:                               NewStore(deps.DB),
		log:                                 deps.Logger.With("service", "acme.fraud.v1.WatchlistCalculationService"),
		sessionQueryClient:                  identityv1.NewSessionQueryServiceClient(deps.Conn),
		fraudSignalValidationClient:         fraudv1.NewFraudSignalValidationServiceClient(deps.Conn),
		fraudSignalValidationClientFallback: fraudv1.NewFraudSignalValidationServiceClient(deps.Conn),
		featureFlagQueryClient:              featureflagsv1.NewFeatureFlagQueryServiceClient(deps.Conn),
		returnRequestQueryClient:            returnsv1.NewReturnRequestQueryServiceClient(deps.Conn),
		tenantAdminClient:                   identityv1.NewTenantAdminServiceClient(deps.Conn),
		conn:                                deps.Conn,
		usageRecordCalculationClient:        subscriptionsv1.NewUsageRecordCalculationServiceClient(deps.Conn),
		clients:                             deps.Clients,
		storageBinQueryClient:               warehousingv1.NewStorageBinQueryServiceClient(deps.Conn),
		riskAssessmentValidationClient:      fraudv1.NewRiskAssessmentValidationServiceClient(deps.Conn),
	}
}

// CalculateWatchlist calculates a watchlist.
func (s *WatchlistCalculationServer) CalculateWatchlist(ctx context.Context, req *fraudv1.CalculateWatchlistRequest) (*fraudv1.CalculateWatchlistResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CalculateWatchlist", "error", err)
		return nil, err
	}
	if err := s.store.SaveWatchlist(ctx, req.GetContext().GetTenantId(), req.GetWatchlist()); err != nil {
		return nil, grpcx.Wrap(err, "save watchlist")
	}
	s.log.InfoContext(ctx, "calculate watchlist", "tenant", req.GetContext().GetTenantId(), "etag", req.GetWatchlist().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "reason", req.GetReason())
	names := collectWatchlistNames(req.GetWatchlist(), nil)
	summarizeSessionsReply, err := s.sessionQueryClient.SummarizeSessions(ctx, &identityv1.SummarizeSessionsRequest{
		Context: req.GetContext(),
		Reason:  tenancy.Value(ctx, "reason"),
		Details: &identityv1.SummarizeSessionsRequest_Details{RecipientEmails: names},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionQueryService/SummarizeSessions")
	}
	routes := map[string]func(context.Context, *fraudv1.LintFraudSignalsRequest, ...grpc.CallOption) (*fraudv1.LintFraudSignalsResponse, error){
		"primary":  s.fraudSignalValidationClient.LintFraudSignals,
		"fallback": s.fraudSignalValidationClientFallback.LintFraudSignals,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	ctx = tenancy.With(ctx, "display_name", req.GetWatchlist().GetManager().GetDisplayName())
	packed, err := anypb.New(req.GetWatchlist())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	lintFraudSignalsRequest := &fraudv1.LintFraudSignalsRequest{
		Context: req.GetContext(),
		Details: &fraudv1.LintFraudSignalsRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetWatchlist().GetManager().GetEmail()},
		},
		FraudSignal: &fraudv1.FraudSignal{
			Subject:       &fraudv1.FraudRuleRef{DisplayName: tenancy.Value(ctx, "display_name")},
			Attributes:    req.GetWatchlist().GetAttributes(),
			CorrelationId: summarizeSessionsReply.GetResult().GetCorrelationId(),
			Detail:        packed,
		},
	}
	lintFraudSignalsReply, err := route(ctx, lintFraudSignalsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.FraudSignalValidationService/LintFraudSignals")
	}
	countFeatureFlagsRequest := buildCountFeatureFlagsRequestForCalculateWatchlist(req, lintFraudSignalsReply)
	countFeatureFlagsReply, err := s.featureFlagQueryClient.CountFeatureFlags(ctx, countFeatureFlagsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.featureflags.v1.FeatureFlagQueryService/CountFeatureFlags")
	}
	return &fraudv1.CalculateWatchlistResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: summarizeSessionsReply.GetResult().GetCorrelationId(),
			Message:       lintFraudSignalsReply.GetResult().GetMessage(),
			ReferenceIds:  countFeatureFlagsReply.GetResult().GetReferenceIds(),
			Status:        countFeatureFlagsReply.GetResult().GetStatus(),
		},
		Watchlist: &fraudv1.Watchlist{WatchlistId: req.GetWatchlistId()},
		Breakdown: &fraudv1.CalculateWatchlistResponse_Breakdown{CurrencyCode: req.GetDetails().GetCurrencyCode()},
	}, nil
}

// EstimateWatchlist estimates a watchlist.
func (s *WatchlistCalculationServer) EstimateWatchlist(ctx context.Context, req *fraudv1.EstimateWatchlistRequest) (*fraudv1.EstimateWatchlistResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveWatchlist(ctx, req.GetContext().GetTenantId(), req.GetWatchlist()); err != nil {
		return nil, grpcx.Wrap(err, "save watchlist")
	}
	s.log.InfoContext(ctx, "estimate watchlist", "tenant", req.GetContext().GetTenantId(), "etag", req.GetWatchlist().GetAudit().GetEtag())
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := watchlistCalculationServerNormalizeFraudSignalSteps[mode]
	normalizeFraudSignalRequest := &fraudv1.NormalizeFraudSignalRequest{
		Context: req.GetContext(),
		Details: &fraudv1.NormalizeFraudSignalRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetWatchlist().GetManager().GetEmail()},
		},
	}
	normalizeFraudSignalReply, err := run(s, ctx, normalizeFraudSignalRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.FraudSignalValidationService/NormalizeFraudSignal")
	}
	return &fraudv1.EstimateWatchlistResponse{
		Result: &commonv1.OperationResult{Status: normalizeFraudSignalReply.GetResult().GetStatus()},
		Watchlist: &fraudv1.Watchlist{
			WatchlistId: req.GetWatchlistId(),
			ParentId:    req.GetWatchlist().GetParentId(),
		},
	}, nil
}

// QuoteWatchlist quotes a watchlist.
func (s *WatchlistCalculationServer) QuoteWatchlist(ctx context.Context, req *fraudv1.QuoteWatchlistRequest) (*fraudv1.QuoteWatchlistResponse, error) {
	if err := s.store.SaveWatchlist(ctx, req.GetContext().GetTenantId(), req.GetWatchlist()); err != nil {
		return nil, grpcx.Wrap(err, "save watchlist")
	}
	s.log.InfoContext(ctx, "quote watchlist", "tenant", req.GetContext().GetTenantId(), "etag", req.GetWatchlist().GetAudit().GetEtag())
	source := req.GetWatchlist().GetManager().ProtoReflect()
	email := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("email"))).String()
	names := collectWatchlistNames(req.GetWatchlist(), nil)
	previewFraudSignalRequest := &fraudv1.PreviewFraudSignalRequest{}
	previewFraudSignalRequest.Context = req.GetContext()
	previewFraudSignalRequest.FraudSignal = &fraudv1.FraudSignal{
		Attributes: req.GetWatchlist().GetAttributes(),
		Subject: &fraudv1.FraudRuleRef{
			DisplayName: req.GetWatchlist().GetManager().GetDisplayName(),
		},
	}
	previewFraudSignalRequest.Details = &fraudv1.PreviewFraudSignalRequest_Details{
		Contact: &commonv1.ContactInfo{Email: email},
		Rules:   names,
	}
	previewFraudSignalRequest.Reason = req.GetReason()
	previewFraudSignalReply, err := s.fraudSignalValidationClient.PreviewFraudSignal(ctx, previewFraudSignalRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.FraudSignalValidationService/PreviewFraudSignal")
	}
	return &fraudv1.QuoteWatchlistResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: previewFraudSignalReply.GetFraudSignal().GetCorrelationId(),
			Status:        "ok",
		},
		Watchlist: &fraudv1.Watchlist{WatchlistId: req.GetWatchlistId()},
		Breakdown: &fraudv1.QuoteWatchlistResponse_Breakdown{CurrencyCode: req.GetDetails().GetCurrencyCode()},
	}, nil
}

// CompareWatchlistOptions compares a watchlist.
func (s *WatchlistCalculationServer) CompareWatchlistOptions(ctx context.Context, req *fraudv1.CompareWatchlistOptionsRequest) (*fraudv1.CompareWatchlistOptionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveWatchlist(ctx, req.GetContext().GetTenantId(), req.GetWatchlist()); err != nil {
		return nil, grpcx.Wrap(err, "save watchlist")
	}
	s.log.InfoContext(ctx, "compare watchlist options", "tenant", req.GetContext().GetTenantId(), "etag", req.GetWatchlist().GetAudit().GetEtag())
	suggestReturnRequestsRequest := &returnsv1.SuggestReturnRequestsRequest{
		Context: req.GetContext(),
		Filter:  &returnsv1.SuggestReturnRequestsRequest_Filter{Region: req.GetDetails().GetDestination().GetRegion()},
	}
	suggestReturnRequestsReply, err := grpcx.Call(ctx, s.returnRequestQueryClient.SuggestReturnRequests, suggestReturnRequestsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnRequestQueryService/SuggestReturnRequests")
	}
	return &fraudv1.CompareWatchlistOptionsResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: suggestReturnRequestsReply.GetResult().GetReferenceIds(),
			Status:       suggestReturnRequestsReply.GetResult().GetStatus(),
		},
		Watchlist: &fraudv1.Watchlist{
			WatchlistId: req.GetWatchlistId(),
			Manager:     req.GetWatchlist().GetManager(),
		},
	}, nil
}

// RecalculateWatchlists recalculates a watchlist.
func (s *WatchlistCalculationServer) RecalculateWatchlists(ctx context.Context, req *fraudv1.RecalculateWatchlistsRequest) (*fraudv1.RecalculateWatchlistsResponse, error) {
	if err := s.store.SaveWatchlist(ctx, req.GetContext().GetTenantId(), req.GetWatchlist()); err != nil {
		return nil, grpcx.Wrap(err, "save watchlist")
	}
	s.log.InfoContext(ctx, "recalculate watchlists", "tenant", req.GetContext().GetTenantId(), "etag", req.GetWatchlist().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "tenant_id", req.GetContext().GetTenantId())
	reassignTenantRequest := &identityv1.ReassignTenantRequest{}
	reassignTenantRequest.Context = req.GetContext()
	reassignTenantRequest.TenantId = tenancy.Value(ctx, "tenant_id")
	reassignTenantRequest.Details = &identityv1.ReassignTenantRequest_Details{
		Assignee: &identityv1.UserRef{
			UserId:      req.GetWatchlist().GetManager().GetUserId(),
			DisplayName: req.GetWatchlist().GetManager().GetDisplayName(),
			Email:       req.GetWatchlist().GetManager().GetEmail(),
		},
	}
	reassignTenantReply, err := s.tenantAdminClient.ReassignTenant(ctx, reassignTenantRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantAdminService/ReassignTenant")
	}
	names := collectWatchlistNames(req.GetWatchlist(), nil)
	resolveStorageBinsRequest := &warehousingv1.ResolveStorageBinsRequest{
		Context:       req.GetContext(),
		StorageBinIds: names,
	}
	resolveStorageBinsReply := new(warehousingv1.ResolveStorageBinsResponse)
	if err := grpc.Invoke(ctx, "/acme.warehousing.v1.StorageBinQueryService/ResolveStorageBins", resolveStorageBinsRequest, resolveStorageBinsReply, s.conn); err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.StorageBinQueryService/ResolveStorageBins")
	}
	return &fraudv1.RecalculateWatchlistsResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: reassignTenantReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Watchlist: &fraudv1.Watchlist{
			Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
				Location: &commonv1.PostalAddress{
					Recipient: reassignTenantReply.GetTenant().GetLocation().GetRecipient(),
				},
			}},
			WatchlistId: req.GetWatchlistId(),
			Manager:     req.GetWatchlist().GetManager(),
		},
	}, nil
}

// SimulateWatchlist simulates a watchlist.
func (s *WatchlistCalculationServer) SimulateWatchlist(ctx context.Context, req *fraudv1.SimulateWatchlistRequest) (*fraudv1.SimulateWatchlistResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveWatchlist(ctx, req.GetContext().GetTenantId(), req.GetWatchlist()); err != nil {
		return nil, grpcx.Wrap(err, "save watchlist")
	}
	s.log.InfoContext(ctx, "simulate watchlist", "tenant", req.GetContext().GetTenantId(), "etag", req.GetWatchlist().GetAudit().GetEtag())
	packed, err := anypb.New(req.GetWatchlist())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	names := collectWatchlistNames(req.GetWatchlist(), nil)
	normalizeFraudSignalRequest := &fraudv1.NormalizeFraudSignalRequest{
		Context: req.GetContext(),
		Details: &fraudv1.NormalizeFraudSignalRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetWatchlist().GetManager().GetEmail()},
			Rules:   names,
		},
		FraudSignal: &fraudv1.FraudSignal{Detail: packed},
	}
	normalizeFraudSignalReply, err := normalizeFraudSignalCall(ctx, normalizeFraudSignalRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.FraudSignalValidationService/NormalizeFraudSignal")
	}
	cloned := proto.Clone(req.GetWatchlist()).(*fraudv1.Watchlist)
	packed2, err := anypb.New(req.GetWatchlist())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	explainUsageRecordCalculationRequest := newExplainUsageRecordCalculationRequest(withExplainUsageRecordCalculationContext(req.GetContext()), withExplainUsageRecordCalculationUsageRecord(&subscriptionsv1.UsageRecord{
		Subject:    &customersv1.CustomerRef{DisplayName: cloned.GetManager().GetDisplayName()},
		Attributes: normalizeFraudSignalReply.GetFraudSignal().GetAttributes(),
		Detail:     packed2,
	}))
	explainUsageRecordCalculationReply, err := s.usageRecordCalculationClient.ExplainUsageRecordCalculation(ctx, explainUsageRecordCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.UsageRecordCalculationService/ExplainUsageRecordCalculation")
	}
	synonymSync, ok := s.clients.Get("acme.search.v1.SynonymSyncService").(searchv1.SynonymSyncServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.search.v1.SynonymSyncService")
	}
	listSynonymConflictsRequest := &searchv1.ListSynonymConflictsRequest{
		Context: req.GetContext(),
		Filter: &searchv1.ListSynonymConflictsRequest_Filter{
			Region:       req.GetDetails().GetDestination().GetRegion(),
			CreatedAfter: req.GetWatchlist().GetAudit().GetCreatedAt(),
		},
	}
	listSynonymConflictsReply, err := synonymSync.ListSynonymConflicts(ctx, listSynonymConflictsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymSyncService/ListSynonymConflicts")
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	names2 := collectWatchlistNames(req.GetWatchlist(), nil)
	summarizeStorageBinsReply, err := s.storageBinQueryClient.SummarizeStorageBins(ctx, &warehousingv1.SummarizeStorageBinsRequest{
		Context:     req.GetContext(),
		Annotations: annotations,
		Reason:      req.GetReason(),
		Details: &warehousingv1.SummarizeStorageBinsRequest_Details{
			Filters:         explainUsageRecordCalculationReply.GetResult().GetDetails(),
			RecipientEmails: names2,
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.StorageBinQueryService/SummarizeStorageBins")
	}
	return &fraudv1.SimulateWatchlistResponse{
		Result: &commonv1.OperationResult{
			Status:        normalizeFraudSignalReply.GetResult().GetStatus(),
			CorrelationId: explainUsageRecordCalculationReply.GetResult().GetCorrelationId(),
			Message:       explainUsageRecordCalculationReply.GetOutcome().GetMessage(),
			ReferenceIds:  listSynonymConflictsReply.GetResult().GetReferenceIds(),
			Details:       summarizeStorageBinsReply.GetResult().GetDetails(),
		},
		Watchlist: &fraudv1.Watchlist{
			WatchlistId: req.GetWatchlistId(),
			Location:    req.GetWatchlist().GetLocation(),
		},
	}, nil
}

// ExplainWatchlistCalculation explains a watchlist.
func (s *WatchlistCalculationServer) ExplainWatchlistCalculation(ctx context.Context, req *fraudv1.ExplainWatchlistCalculationRequest) (*fraudv1.ExplainWatchlistCalculationResponse, error) {
	if err := s.store.SaveWatchlist(ctx, req.GetContext().GetTenantId(), req.GetWatchlist()); err != nil {
		return nil, grpcx.Wrap(err, "save watchlist")
	}
	s.log.InfoContext(ctx, "explain watchlist calculation", "tenant", req.GetContext().GetTenantId(), "etag", req.GetWatchlist().GetAudit().GetEtag())
	names := collectWatchlistNames(req.GetWatchlist(), nil)
	checkRiskAssessmentEligibilityRequest := &fraudv1.CheckRiskAssessmentEligibilityRequest{
		Context: req.GetContext(),
		RiskAssessment: &fraudv1.RiskAssessment{
			Origin:   &fraudv1.RiskAssessment_Customer{Customer: &customersv1.CustomerRef{Email: req.GetDetails().GetContact().GetEmail()}},
			Assignee: &identityv1.UserRef{UserId: req.GetWatchlist().GetManager().GetUserId()},
			Tags:     names,
		},
		Details: &fraudv1.CheckRiskAssessmentEligibilityRequest_Details{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetContact().GetPreferredChannel(),
			},
		},
	}
	type checkRiskAssessmentEligibilityResult struct {
		reply *fraudv1.CheckRiskAssessmentEligibilityResponse
		err   error
	}
	checkRiskAssessmentEligibilityResults := make(chan checkRiskAssessmentEligibilityResult, 1)
	go func() {
		reply, err := s.riskAssessmentValidationClient.CheckRiskAssessmentEligibility(ctx, checkRiskAssessmentEligibilityRequest)
		checkRiskAssessmentEligibilityResults <- checkRiskAssessmentEligibilityResult{reply, err}
	}()
	var checkRiskAssessmentEligibilityReply *fraudv1.CheckRiskAssessmentEligibilityResponse
	select {
	case result := <-checkRiskAssessmentEligibilityResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.fraud.v1.RiskAssessmentValidationService/CheckRiskAssessmentEligibility")
		}
		checkRiskAssessmentEligibilityReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &fraudv1.ExplainWatchlistCalculationResponse{
		Outcome: &fraudv1.ExplainWatchlistCalculationResponse_Outcome{
			Message: checkRiskAssessmentEligibilityReply.GetResult().GetMessage(),
		},
		Watchlist: &fraudv1.Watchlist{
			WatchlistId: req.GetWatchlistId(),
			Status:      req.GetWatchlist().GetStatus(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetWatchlistRates returns a watchlist.
func (s *WatchlistCalculationServer) GetWatchlistRates(ctx context.Context, req *fraudv1.GetWatchlistRatesRequest) (*fraudv1.GetWatchlistRatesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetWatchlistRates", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "watchlist", req.GetWatchlistId(), "get_watchlist_rates"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get watchlist rates", "tenant", req.GetContext().GetTenantId())
	return &fraudv1.GetWatchlistRatesResponse{
		Watchlist: &fraudv1.Watchlist{WatchlistId: req.GetWatchlistId()},
		Result:    &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// watchlistCalculationServerNormalizeFraudSignalSteps are the ways to run
// acme.fraud.v1.FraudSignalValidationService/NormalizeFraudSignal, by name.
var watchlistCalculationServerNormalizeFraudSignalSteps = map[string]func(*WatchlistCalculationServer, context.Context, *fraudv1.NormalizeFraudSignalRequest) (*fraudv1.NormalizeFraudSignalResponse, error){
	"default": (*WatchlistCalculationServer).normalizeFraudSignalStep,
	"audited": (*WatchlistCalculationServer).normalizeFraudSignalAuditedStep,
}

func (s *WatchlistCalculationServer) normalizeFraudSignalStep(ctx context.Context, request *fraudv1.NormalizeFraudSignalRequest) (*fraudv1.NormalizeFraudSignalResponse, error) {
	return s.fraudSignalValidationClient.NormalizeFraudSignal(ctx, request)
}

func (s *WatchlistCalculationServer) normalizeFraudSignalAuditedStep(ctx context.Context, request *fraudv1.NormalizeFraudSignalRequest) (*fraudv1.NormalizeFraudSignalResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.fraud.v1.FraudSignalValidationService/NormalizeFraudSignal")
	return s.fraudSignalValidationClient.NormalizeFraudSignal(ctx, request)
}
