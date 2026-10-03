package customers

import (
	"context"
	"fmt"
	"log/slog"

	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	communityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/community/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	pricingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/pricing/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	venuesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/venues/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// ConsentRecordValidationServer implements `acme.customers.v1.ConsentRecordValidationService`.
// Checks consent records against the rules before anything acts on them.
type ConsentRecordValidationServer struct {
	customersv1.UnimplementedConsentRecordValidationServiceServer
	store                         *Store
	log                           *slog.Logger
	currencyRateCalculationClient pricingv1.CurrencyRateCalculationServiceClient
	venueQueryClient              venuesv1.VenueQueryServiceClient
	conn                          *grpc.ClientConn
	customerValidationClient      customersv1.CustomerValidationServiceClient
	forumQueryClient              communityv1.ForumQueryServiceClient
	synonymSyncClient             searchv1.SynonymSyncServiceClient
	sessionNotificationClient     identityv1.SessionNotificationServiceClient
	normalizePriceQuoteFn         func(context.Context, *pricingv1.NormalizePriceQuoteRequest, ...grpc.CallOption) (*pricingv1.NormalizePriceQuoteResponse, error)
}

// NewConsentRecordValidationServer wires a ConsentRecordValidationServer.
func NewConsentRecordValidationServer(deps platform.Deps) *ConsentRecordValidationServer {
	priceQuoteValidation := pricingv1.NewPriceQuoteValidationServiceClient(deps.Conn)
	return &ConsentRecordValidationServer{
		store:                         NewStore(deps.DB),
		log:                           deps.Logger.With("service", "acme.customers.v1.ConsentRecordValidationService"),
		currencyRateCalculationClient: pricingv1.NewCurrencyRateCalculationServiceClient(deps.Conn),
		venueQueryClient:              venuesv1.NewVenueQueryServiceClient(deps.Conn),
		conn:                          deps.Conn,
		customerValidationClient:      customersv1.NewCustomerValidationServiceClient(deps.Conn),
		forumQueryClient:              communityv1.NewForumQueryServiceClient(deps.Conn),
		synonymSyncClient:             searchv1.NewSynonymSyncServiceClient(deps.Conn),
		sessionNotificationClient:     identityv1.NewSessionNotificationServiceClient(deps.Conn),
		normalizePriceQuoteFn:         priceQuoteValidation.NormalizePriceQuote,
	}
}

// ValidateConsentRecord validates a consent record.
func (s *ConsentRecordValidationServer) ValidateConsentRecord(ctx context.Context, req *customersv1.ValidateConsentRecordRequest) (*customersv1.ValidateConsentRecordResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveConsentRecord(ctx, req.GetContext().GetTenantId(), req.GetConsentRecord()); err != nil {
		return nil, grpcx.Wrap(err, "save consent record")
	}
	s.log.InfoContext(ctx, "validate consent record", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "preferred_channel", req.GetDetails().GetContact().GetPreferredChannel())
	packed, err := anypb.New(req.GetConsentRecord())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	lintSearchQueriesRequest := &searchv1.LintSearchQueriesRequest{
		Context: req.GetContext(),
		Details: &searchv1.LintSearchQueriesRequest_Details{
			Contact: &commonv1.ContactInfo{PreferredChannel: tenancy.Value(ctx, "preferred_channel")},
		},
		SearchQuery: &searchv1.SearchQuery{Detail: packed},
	}
	lintSearchQueriesReply, err := lintSearchQueriesCall(ctx, lintSearchQueriesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryValidationService/LintSearchQueries")
	}
	compareCurrencyRateOptionsRequest := &pricingv1.CompareCurrencyRateOptionsRequest{
		Context: req.GetContext(),
		CurrencyRate: &pricingv1.CurrencyRate{
			Subject: &customersv1.CustomerRef{
				Email:       req.GetDetails().GetContact().GetEmail(),
				DisplayName: lintSearchQueriesReply.GetSearchQuery().GetSubject().GetDisplayName(),
			},
			Source:        lintSearchQueriesReply.GetSearchQuery().GetSource(),
			CorrelationId: lintSearchQueriesReply.GetSearchQuery().GetCorrelationId(),
		},
	}
	compareCurrencyRateOptionsReply, err := grpcx.Call(ctx, s.currencyRateCalculationClient.CompareCurrencyRateOptions, compareCurrencyRateOptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.CurrencyRateCalculationService/CompareCurrencyRateOptions")
	}
	return &customersv1.ValidateConsentRecordResponse{
		ConsentRecord: &customersv1.ConsentRecord{
			CorrelationId:   lintSearchQueriesReply.GetResult().GetCorrelationId(),
			Source:          lintSearchQueriesReply.GetSearchQuery().GetSource(),
			ConsentRecordId: req.GetConsentRecordId(),
			Kind:            req.GetConsentRecord().GetKind(),
		},
		Outcome: &customersv1.ValidateConsentRecordResponse_Outcome{
			Status:  compareCurrencyRateOptionsReply.GetResult().GetStatus(),
			Message: compareCurrencyRateOptionsReply.GetResult().GetMessage(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// PreviewConsentRecord previews a consent record.
func (s *ConsentRecordValidationServer) PreviewConsentRecord(ctx context.Context, req *customersv1.PreviewConsentRecordRequest) (*customersv1.PreviewConsentRecordResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected PreviewConsentRecord", "error", err)
		return nil, err
	}
	if err := s.store.SaveConsentRecord(ctx, req.GetContext().GetTenantId(), req.GetConsentRecord()); err != nil {
		return nil, grpcx.Wrap(err, "save consent record")
	}
	s.log.InfoContext(ctx, "preview consent record", "tenant", req.GetContext().GetTenantId())
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*venuesv1.CountVenuesResponse, len(req.GetConsentRecord().GetMeasurements()))
	for i, measurement := range req.GetConsentRecord().GetMeasurements() {
		group.Go(func() error {
			reply, err := s.venueQueryClient.CountVenues(groupCtx, &venuesv1.CountVenuesRequest{
				Context: req.GetContext(),
				Filter: &venuesv1.CountVenuesRequest_Filter{
					CreatedAfter: measurement.GetObservedAt(),
					Labels:       req.GetConsentRecord().GetAttributes(),
				},
			})
			if err != nil {
				return err
			}
			replies[i] = reply
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, grpcx.Wrap(err, "acme.venues.v1.VenueQueryService/CountVenues")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	source := req.GetConsentRecord().GetSubject().ProtoReflect()
	displayName := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("display_name"))).String()
	var envelope customersv1.CustomerRef
	if err := req.GetConsentRecord().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	lintAccessPoliciesRequest := &authorizationv1.LintAccessPoliciesRequest{
		Context: req.GetContext(),
		Details: &authorizationv1.LintAccessPoliciesRequest_Details{
			Contact: &commonv1.ContactInfo{Name: req.GetDetails().GetContact().GetName()},
		},
		AccessPolicy: &authorizationv1.AccessPolicy{
			Owner: &identityv1.UserRef{
				DisplayName: displayName,
				Email:       req.GetDetails().GetContact().GetEmail(),
			},
			Action: &authorizationv1.AccessPolicy_Block{Block: &authorizationv1.AccessPolicyBlockAction{Reason: req.GetReason()}},
		},
		AccessPolicyId: envelope.GetDisplayName(),
	}
	lintAccessPoliciesReply := new(authorizationv1.LintAccessPoliciesResponse)
	if err := s.conn.Invoke(ctx, authorizationAccessPolicyValidationServiceLintAccessPoliciesMethod, lintAccessPoliciesRequest, lintAccessPoliciesReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyValidationService/LintAccessPolicies")
	}
	return &customersv1.PreviewConsentRecordResponse{
		Outcome: &customersv1.PreviewConsentRecordResponse_Outcome{Status: lintAccessPoliciesReply.GetOutcome().GetStatus()},
		ConsentRecord: &customersv1.ConsentRecord{
			ConsentRecordId: req.GetConsentRecordId(),
			Kind:            req.GetConsentRecord().GetKind(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// NormalizeConsentRecord normalizes a consent record.
func (s *ConsentRecordValidationServer) NormalizeConsentRecord(ctx context.Context, req *customersv1.NormalizeConsentRecordRequest) (*customersv1.NormalizeConsentRecordResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected NormalizeConsentRecord", "error", err)
		return nil, err
	}
	if err := s.store.SaveConsentRecord(ctx, req.GetContext().GetTenantId(), req.GetConsentRecord()); err != nil {
		return nil, grpcx.Wrap(err, "save consent record")
	}
	s.log.InfoContext(ctx, "normalize consent record", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "phone", req.GetDetails().GetContact().GetPhone())
	var envelope customersv1.CustomerRef
	if err := req.GetConsentRecord().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	explainCustomerDecisionReply, err := s.customerValidationClient.ExplainCustomerDecision(ctx, &customersv1.ExplainCustomerDecisionRequest{
		Context: req.GetContext(),
		Details: &customersv1.ExplainCustomerDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email:            req.GetDetails().GetContact().GetEmail(),
				PreferredChannel: req.GetDetails().GetContact().GetPreferredChannel(),
				Phone:            tenancy.Value(ctx, "phone"),
			},
		},
		Customer: &customersv1.Customer{
			RelationshipManager: &identityv1.UserRef{
				DisplayName: req.GetConsentRecord().GetSubject().GetDisplayName(),
			},
		},
		CustomerId: envelope.GetDisplayName(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerValidationService/ExplainCustomerDecision")
	}
	return &customersv1.NormalizeConsentRecordResponse{
		Result: &commonv1.OperationResult{
			Message: explainCustomerDecisionReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Outcome: &customersv1.NormalizeConsentRecordResponse_Outcome{
			Status: explainCustomerDecisionReply.GetOutcome().GetStatus(),
		},
		ConsentRecord: &customersv1.ConsentRecord{
			ConsentRecordId: req.GetConsentRecordId(),
			CorrelationId:   req.GetConsentRecord().GetCorrelationId(),
		},
	}, nil
}

// ExplainConsentRecordDecision explains a consent record.
func (s *ConsentRecordValidationServer) ExplainConsentRecordDecision(ctx context.Context, req *customersv1.ExplainConsentRecordDecisionRequest) (*customersv1.ExplainConsentRecordDecisionResponse, error) {
	if err := s.store.SaveConsentRecord(ctx, req.GetContext().GetTenantId(), req.GetConsentRecord()); err != nil {
		return nil, grpcx.Wrap(err, "save consent record")
	}
	s.log.InfoContext(ctx, "explain consent record decision", "tenant", req.GetContext().GetTenantId())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	var envelope customersv1.CustomerRef
	if err := req.GetConsentRecord().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	summarizeForumsRequest := &communityv1.SummarizeForumsRequest{
		Context:     req.GetContext(),
		Annotations: annotations,
		Reason:      req.GetReason(),
		Details:     &communityv1.SummarizeForumsRequest_Details{Filters: req.GetConsentRecord().GetAttributes()},
		ForumId:     envelope.GetDisplayName(),
	}
	summarizeForumsReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*communityv1.SummarizeForumsResponse, error) {
		return s.forumQueryClient.SummarizeForums(ctx, summarizeForumsRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.community.v1.ForumQueryService/SummarizeForums")
	}
	simulateHouseholdRequest := &customersv1.SimulateHouseholdRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Children: []*customersv1.Household{&customersv1.Household{
						HouseholdId: req.GetConsentRecord().GetSubject().GetHouseholdId(),
					}},
				}},
			}},
		},
	}
	simulateHouseholdReply := new(customersv1.SimulateHouseholdResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", customersHouseholdCalculationServiceName, "SimulateHousehold"), simulateHouseholdRequest, simulateHouseholdReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdCalculationService/SimulateHousehold")
	}
	return &customersv1.ExplainConsentRecordDecisionResponse{
		Result: &commonv1.OperationResult{
			Message:       summarizeForumsReply.GetResult().GetMessage(),
			CorrelationId: summarizeForumsReply.GetResult().GetCorrelationId(),
			ReferenceIds:  simulateHouseholdReply.GetResult().GetReferenceIds(),
			Status:        "ok",
		},
		ConsentRecord: &customersv1.ConsentRecord{
			ConsentRecordId: req.GetConsentRecordId(),
			CorrelationId:   req.GetConsentRecord().GetCorrelationId(),
		},
	}, nil
}

// ScoreConsentRecord scores a consent record.
func (s *ConsentRecordValidationServer) ScoreConsentRecord(ctx context.Context, req *customersv1.ScoreConsentRecordRequest) (*customersv1.ScoreConsentRecordResponse, error) {
	if err := s.store.SaveConsentRecord(ctx, req.GetContext().GetTenantId(), req.GetConsentRecord()); err != nil {
		return nil, grpcx.Wrap(err, "save consent record")
	}
	s.log.InfoContext(ctx, "score consent record", "tenant", req.GetContext().GetTenantId())
	attributes := make(map[string]string, len(req.GetConsentRecord().GetAttributes()))
	for key, value := range req.GetConsentRecord().GetAttributes() {
		attributes[key] = value
	}
	listSynonymConflictsReply, err := s.synonymSyncClient.ListSynonymConflicts(ctx, &searchv1.ListSynonymConflictsRequest{
		Context: req.GetContext(),
		Filter: &searchv1.ListSynonymConflictsRequest_Filter{
			Region:       req.GetDetails().GetDestination().GetRegion(),
			CreatedAfter: req.GetConsentRecord().GetOccurredAt(),
			OwnerId:      req.GetConsentRecord().GetSubject().GetHouseholdId(),
			Labels:       attributes,
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymSyncService/ListSynonymConflicts")
	}
	return &customersv1.ScoreConsentRecordResponse{
		ConsentRecord: &customersv1.ConsentRecord{
			CorrelationId:   listSynonymConflictsReply.GetResult().GetCorrelationId(),
			ConsentRecordId: req.GetConsentRecordId(),
			Kind:            req.GetConsentRecord().GetKind(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetConsentRecordRules returns a consent record.
func (s *ConsentRecordValidationServer) GetConsentRecordRules(ctx context.Context, req *customersv1.GetConsentRecordRulesRequest) (*customersv1.GetConsentRecordRulesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "consent_record", req.GetConsentRecordId(), "get_consent_record_rules"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get consent record rules", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "consent_record_id", req.GetConsentRecordId())
	baggage := make(map[string]string, len(req.GetContext().GetBaggage()))
	for key, value := range req.GetContext().GetBaggage() {
		baggage[key] = value
	}
	packed, err := anypb.New(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	notifySessionWatchersRequest := &identityv1.NotifySessionWatchersRequest{}
	notifySessionWatchersRequest.Context = req.GetContext()
	notifySessionWatchersRequest.Details = &identityv1.NotifySessionWatchersRequest_Details{
		Recipient: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		Variables: baggage,
		Payload:   packed,
	}
	notifySessionWatchersRequest.SessionId = tenancy.Value(ctx, "consent_record_id")
	notifySessionWatchersReply, err := s.sessionNotificationClient.NotifySessionWatchers(ctx, notifySessionWatchersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionNotificationService/NotifySessionWatchers")
	}
	normalizePriceQuoteRequest := &pricingv1.NormalizePriceQuoteRequest{
		Context: req.GetContext(),
		PriceQuote: &pricingv1.PriceQuote{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: notifySessionWatchersReply.GetSession().GetAudit().GetUpdatedAt(),
			},
			Counterparty: &pricingv1.PriceListRef{
				DisplayName: notifySessionWatchersReply.GetSession().GetCustomer().GetDisplayName(),
			},
		},
		Details: &pricingv1.NormalizePriceQuoteRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email: notifySessionWatchersReply.GetSession().GetAssignee().GetEmail(),
			},
		},
	}
	normalizePriceQuoteReply, err := s.normalizePriceQuoteFn(ctx, normalizePriceQuoteRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceQuoteValidationService/NormalizePriceQuote")
	}
	return &customersv1.GetConsentRecordRulesResponse{
		Result: &commonv1.OperationResult{
			Message:      notifySessionWatchersReply.GetResult().GetMessage(),
			ReferenceIds: normalizePriceQuoteReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		ConsentRecord: &customersv1.ConsentRecord{ConsentRecordId: req.GetConsentRecordId()},
	}, nil
}
