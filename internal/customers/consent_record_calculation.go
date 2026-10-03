package customers

import (
	"context"
	"errors"
	"io"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	licensingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/licensing/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

// ConsentRecordCalculationServer implements `acme.customers.v1.ConsentRecordCalculationService`.
// Computes the figures of a consent record, such as totals, taxes, and estimates.
type ConsentRecordCalculationServer struct {
	customersv1.UnimplementedConsentRecordCalculationServiceServer
	store                          *Store
	log                            *slog.Logger
	metricSeriesClient             metricsv1.MetricSeriesServiceClient
	customerValidationClient       customersv1.CustomerValidationServiceClient
	userClient                     identityv1.UserServiceClient
	softwareLicenseClient          licensingv1.SoftwareLicenseServiceClient
	trackingEventCalculationClient shippingv1.TrackingEventCalculationServiceClient
	customerCalculationClient      customersv1.CustomerCalculationServiceClient
	meteredCustomerValidation      meteredCustomerValidation
	watchlistCalculationPort       watchlistCalculationPort
	searchIndexQueryClient         searchv1.SearchIndexQueryServiceClient
	searchIndexQueryClientFallback searchv1.SearchIndexQueryServiceClient
	sessionHistoryClient           identityv1.SessionHistoryServiceClient
}

// NewConsentRecordCalculationServer wires a ConsentRecordCalculationServer.
func NewConsentRecordCalculationServer(deps platform.Deps) *ConsentRecordCalculationServer {
	return &ConsentRecordCalculationServer{
		store:                          NewStore(deps.DB),
		log:                            deps.Logger.With("service", "acme.customers.v1.ConsentRecordCalculationService"),
		metricSeriesClient:             metricsv1.NewMetricSeriesServiceClient(deps.Conn),
		customerValidationClient:       customersv1.NewCustomerValidationServiceClient(deps.Conn),
		userClient:                     identityv1.NewUserServiceClient(deps.Conn),
		softwareLicenseClient:          licensingv1.NewSoftwareLicenseServiceClient(deps.Conn),
		trackingEventCalculationClient: shippingv1.NewTrackingEventCalculationServiceClient(deps.Conn),
		customerCalculationClient:      customersv1.NewCustomerCalculationServiceClient(deps.Conn),
		meteredCustomerValidation:      meteredCustomerValidation{CustomerValidationServiceClient: customersv1.NewCustomerValidationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		watchlistCalculationPort:       newWatchlistCalculationPort(deps),
		searchIndexQueryClient:         searchv1.NewSearchIndexQueryServiceClient(deps.Conn),
		searchIndexQueryClientFallback: searchv1.NewSearchIndexQueryServiceClient(deps.Conn),
		sessionHistoryClient:           identityv1.NewSessionHistoryServiceClient(deps.Conn),
	}
}

// CalculateConsentRecord calculates a consent record.
func (s *ConsentRecordCalculationServer) CalculateConsentRecord(ctx context.Context, req *customersv1.CalculateConsentRecordRequest) (*customersv1.CalculateConsentRecordResponse, error) {
	if err := s.store.SaveConsentRecord(ctx, req.GetContext().GetTenantId(), req.GetConsentRecord()); err != nil {
		return nil, grpcx.Wrap(err, "save consent record")
	}
	s.log.InfoContext(ctx, "calculate consent record", "tenant", req.GetContext().GetTenantId())
	watchMetricSeriesesStream, err := s.metricSeriesClient.WatchMetricSerieses(ctx, &metricsv1.WatchMetricSeriesesRequest{
		Context:    req.GetContext(),
		Filter:     &metricsv1.WatchMetricSeriesesRequest_Filter{Region: req.GetDetails().GetDestination().GetRegion()},
		Subscriber: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		Since:      req.GetConsentRecord().GetOccurredAt(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.metrics.v1.MetricSeriesService/WatchMetricSerieses")
	}
	for {
		received, err := watchMetricSeriesesStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.metrics.v1.MetricSeriesService/WatchMetricSerieses")
		}
		_ = received
	}
	for measurement := range iterx.Each(req.GetConsentRecord().GetMeasurements(), func(candidate *customersv1.ConsentRecordMeasurement) bool { return candidate != nil }) {
		if _, err := s.customerValidationClient.ExplainCustomerDecision(ctx, &customersv1.ExplainCustomerDecisionRequest{
			Context: req.GetContext(),
			Customer: &customersv1.Customer{
				Audit: &commonv1.AuditInfo{UpdatedAt: measurement.GetObservedAt()},
				RelationshipManager: &identityv1.UserRef{
					DisplayName: req.GetConsentRecord().GetSubject().GetDisplayName(),
				},
				PrimaryAddress: &commonv1.PostalAddress{
					CountryCode: req.GetDetails().GetDestination().GetCountryCode(),
				},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerValidationService/ExplainCustomerDecision")
		}
	}
	return &customersv1.CalculateConsentRecordResponse{
		ConsentRecord: &customersv1.ConsentRecord{
			ConsentRecordId: req.GetConsentRecordId(),
			CorrelationId:   req.GetConsentRecord().GetCorrelationId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// EstimateConsentRecord estimates a consent record.
func (s *ConsentRecordCalculationServer) EstimateConsentRecord(ctx context.Context, req *customersv1.EstimateConsentRecordRequest) (*customersv1.EstimateConsentRecordResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected EstimateConsentRecord", "error", err)
		return nil, err
	}
	if err := s.store.SaveConsentRecord(ctx, req.GetContext().GetTenantId(), req.GetConsentRecord()); err != nil {
		return nil, grpcx.Wrap(err, "save consent record")
	}
	s.log.InfoContext(ctx, "estimate consent record", "tenant", req.GetContext().GetTenantId())
	encoded, err := protojson.Marshal(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded commonv1.RequestContext
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	var envelope customersv1.CustomerRef
	if err := req.GetConsentRecord().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	watchUsersStream, err := s.userClient.WatchUsers(ctx, &identityv1.WatchUsersRequest{
		Context: req.GetContext(),
		Filter: &identityv1.WatchUsersRequest_Filter{
			Region:  req.GetDetails().GetDestination().GetRegion(),
			OwnerId: req.GetConsentRecord().GetSubject().GetHouseholdId(),
			Query:   envelope.GetDisplayName(),
		},
		Subscriber: &commonv1.ContactInfo{Email: (&decoded).GetActor().GetEmail()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserService/WatchUsers")
	}
	for {
		received, err := watchUsersStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.identity.v1.UserService/WatchUsers")
		}
		_ = received
	}
	cloneSoftwareLicenseRequest := buildCloneSoftwareLicenseRequestForEstimateConsentRecord(req)
	cloneSoftwareLicenseReply, err := s.softwareLicenseClient.CloneSoftwareLicense(ctx, cloneSoftwareLicenseRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.licensing.v1.SoftwareLicenseService/CloneSoftwareLicense")
	}
	return &customersv1.EstimateConsentRecordResponse{
		ConsentRecord: &customersv1.ConsentRecord{
			Subject: &customersv1.HouseholdRef{
				DisplayName: cloneSoftwareLicenseReply.GetSoftwareLicense().GetHolder().GetDisplayName(),
			},
			ConsentRecordId: req.GetConsentRecordId(),
			CorrelationId:   req.GetConsentRecord().GetCorrelationId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// QuoteConsentRecord quotes a consent record.
func (s *ConsentRecordCalculationServer) QuoteConsentRecord(ctx context.Context, req *customersv1.QuoteConsentRecordRequest) (*customersv1.QuoteConsentRecordResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveConsentRecord(ctx, req.GetContext().GetTenantId(), req.GetConsentRecord()); err != nil {
		return nil, grpcx.Wrap(err, "save consent record")
	}
	s.log.InfoContext(ctx, "quote consent record", "tenant", req.GetContext().GetTenantId())
	compareTrackingEventOptionsRequest := buildCompareTrackingEventOptionsRequestForQuoteConsentRecord(req)
	compareTrackingEventOptionsReply, err := s.trackingEventCalculationClient.CompareTrackingEventOptions(ctx, compareTrackingEventOptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.TrackingEventCalculationService/CompareTrackingEventOptions")
	}
	encoded, err := protojson.Marshal(req.GetConsentRecord())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded customersv1.ConsentRecord
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	explainCustomerDecisionRequest := &customersv1.ExplainCustomerDecisionRequest{}
	explainCustomerDecisionRequest.Context = req.GetContext()
	explainCustomerDecisionRequest.Customer = &customersv1.Customer{
		RelationshipManager: &identityv1.UserRef{DisplayName: (&decoded).GetSubject().GetDisplayName()},
		IdentityDocument: &customersv1.Customer_Passport{Passport: &customersv1.CustomerPassport{
			Number: compareTrackingEventOptionsReply.GetTrackingEvent().GetSubject().GetNumber(),
		}},
		PrimaryAddress: &commonv1.PostalAddress{
			Recipient: req.GetDetails().GetDestination().GetRecipient(),
			Region:    req.GetDetails().GetDestination().GetRegion(),
		},
	}
	explainCustomerDecisionReply, err := s.customerValidationClient.ExplainCustomerDecision(ctx, explainCustomerDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerValidationService/ExplainCustomerDecision")
	}
	ctx = tenancy.With(ctx, "postal_code", req.GetDetails().GetDestination().GetPostalCode())
	calculateCustomerReply, err := s.customerCalculationClient.CalculateCustomer(ctx, &customersv1.CalculateCustomerRequest{
		Context: req.GetContext(),
		Details: &customersv1.CalculateCustomerRequest_Details{
			Destination: &commonv1.PostalAddress{
				PostalCode: tenancy.Value(ctx, "postal_code"),
				Locality:   req.GetDetails().GetDestination().GetLocality(),
			},
		},
		Customer: &customersv1.Customer{
			IdentityDocument: &customersv1.Customer_NationalId{NationalId: &customersv1.CustomerNationalId{
				Number: compareTrackingEventOptionsReply.GetTrackingEvent().GetSubject().GetNumber(),
			}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerCalculationService/CalculateCustomer")
	}
	return &customersv1.QuoteConsentRecordResponse{
		Result: &commonv1.OperationResult{
			Details: compareTrackingEventOptionsReply.GetResult().GetDetails(),
			Message: calculateCustomerReply.GetResult().GetMessage(),
			Status:  calculateCustomerReply.GetResult().GetStatus(),
		},
		ConsentRecord: &customersv1.ConsentRecord{
			Attributes:      explainCustomerDecisionReply.GetCustomer().GetAttributes(),
			ConsentRecordId: req.GetConsentRecordId(),
			Source:          req.GetConsentRecord().GetSource(),
		},
	}, nil
}

// CompareConsentRecordOptions compares a consent record.
func (s *ConsentRecordCalculationServer) CompareConsentRecordOptions(ctx context.Context, req *customersv1.CompareConsentRecordOptionsRequest) (*customersv1.CompareConsentRecordOptionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CompareConsentRecordOptions", "error", err)
		return nil, err
	}
	if err := s.store.SaveConsentRecord(ctx, req.GetContext().GetTenantId(), req.GetConsentRecord()); err != nil {
		return nil, grpcx.Wrap(err, "save consent record")
	}
	s.log.InfoContext(ctx, "compare consent record options", "tenant", req.GetContext().GetTenantId())
	var envelope customersv1.CustomerRef
	if err := req.GetConsentRecord().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	checkCustomerEligibilityRequest := &customersv1.CheckCustomerEligibilityRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			PrimaryAddress: &commonv1.PostalAddress{
				Lines:       req.GetDetails().GetDestination().GetLines(),
				CountryCode: req.GetDetails().GetDestination().GetCountryCode(),
				PostalCode:  req.GetDetails().GetDestination().GetPostalCode(),
			},
		},
		CustomerId: envelope.GetDisplayName(),
	}
	checkCustomerEligibilityReply, err := s.meteredCustomerValidation.CheckCustomerEligibility(ctx, checkCustomerEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerValidationService/CheckCustomerEligibility")
	}
	var envelope2 customersv1.CustomerRef
	if err := req.GetConsentRecord().GetDetail().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	estimateWatchlistRequest := &fraudv1.EstimateWatchlistRequest{
		Context: req.GetContext(),
		Watchlist: &fraudv1.Watchlist{
			Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
				Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
					Manager: &identityv1.UserRef{
						Email: checkCustomerEligibilityReply.GetCustomer().GetRelationshipManager().GetEmail(),
					},
				}},
			}},
		},
		WatchlistId: envelope2.GetDisplayName(),
	}
	estimateWatchlistReply, err := s.watchlistCalculationPort.EstimateWatchlist(ctx, estimateWatchlistRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.WatchlistCalculationService/EstimateWatchlist")
	}
	var envelope3 customersv1.CustomerRef
	if err := req.GetConsentRecord().GetDetail().UnmarshalTo(&envelope3); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	explainCustomerDecisionRequest := &customersv1.ExplainCustomerDecisionRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: checkCustomerEligibilityReply.GetCustomer().GetAudit().GetUpdatedBy(),
				CreatedAt: checkCustomerEligibilityReply.GetCustomer().GetAudit().GetCreatedAt(),
			},
			PrimaryAddress: &commonv1.PostalAddress{
				Region: checkCustomerEligibilityReply.GetCustomer().GetPrimaryAddress().GetRegion(),
			},
			Contact: &commonv1.ContactInfo{
				Email: estimateWatchlistReply.GetWatchlist().GetManager().GetEmail(),
			},
		},
		CustomerId: envelope3.GetDisplayName(),
	}
	send := s.explainCustomerDecisionSender(explainCustomerDecisionRequest)
	explainCustomerDecisionReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerValidationService/ExplainCustomerDecision")
	}
	return &customersv1.CompareConsentRecordOptionsResponse{
		Result: &commonv1.OperationResult{
			Details: checkCustomerEligibilityReply.GetResult().GetDetails(),
			Status:  estimateWatchlistReply.GetResult().GetStatus(),
		},
		ConsentRecord: &customersv1.ConsentRecord{
			CorrelationId:   explainCustomerDecisionReply.GetResult().GetCorrelationId(),
			ConsentRecordId: req.GetConsentRecordId(),
			Sequence:        req.GetConsentRecord().GetSequence(),
		},
	}, nil
}

// RecalculateConsentRecords recalculates a consent record.
func (s *ConsentRecordCalculationServer) RecalculateConsentRecords(ctx context.Context, req *customersv1.RecalculateConsentRecordsRequest) (*customersv1.RecalculateConsentRecordsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveConsentRecord(ctx, req.GetContext().GetTenantId(), req.GetConsentRecord()); err != nil {
		return nil, grpcx.Wrap(err, "save consent record")
	}
	s.log.InfoContext(ctx, "recalculate consent records", "tenant", req.GetContext().GetTenantId())
	routes := map[string]func(context.Context, *searchv1.SummarizeSearchIndexesRequest, ...grpc.CallOption) (*searchv1.SummarizeSearchIndexesResponse, error){
		"primary":  s.searchIndexQueryClient.SummarizeSearchIndexes,
		"fallback": s.searchIndexQueryClientFallback.SummarizeSearchIndexes,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	var envelope customersv1.CustomerRef
	if err := req.GetConsentRecord().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	summarizeSearchIndexesRequest := &searchv1.SummarizeSearchIndexesRequest{
		Context:       req.GetContext(),
		Annotations:   req.GetAnnotations(),
		Reason:        req.GetReason(),
		SearchIndexId: envelope.GetDisplayName(),
	}
	summarizeSearchIndexesReply, err := route(ctx, summarizeSearchIndexesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexQueryService/SummarizeSearchIndexes")
	}
	ctx = tenancy.With(ctx, "currency_code", req.GetDetails().GetCurrencyCode())
	snapshot, err := protojson.Marshal(req.GetConsentRecord())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	revertSessionRequest := newRevertSessionRequest(withRevertSessionContext(req.GetContext()), withRevertSessionSession(&identityv1.Session{
		Origin: &identityv1.Session_Customer{Customer: &identityv1.ApiCredentialRef{CurrencyCode: tenancy.Value(ctx, "currency_code")}},
		Assignee: &identityv1.UserRef{
			DisplayName: req.GetConsentRecord().GetSubject().GetDisplayName(),
		},
	}), withRevertSessionReason(req.GetReason()), withRevertSessionDetails(&identityv1.RevertSessionRequest_Details{Snapshot: snapshot}))
	revertSessionReply, err := s.sessionHistoryClient.RevertSession(ctx, revertSessionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionHistoryService/RevertSession")
	}
	calculateCustomerReply, err := s.customerCalculationClient.CalculateCustomer(ctx, &customersv1.CalculateCustomerRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			Audit: &commonv1.AuditInfo{Etag: revertSessionReply.GetSession().GetAudit().GetEtag()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerCalculationService/CalculateCustomer")
	}
	return &customersv1.RecalculateConsentRecordsResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: summarizeSearchIndexesReply.GetResult().GetCorrelationId(),
			Message:       calculateCustomerReply.GetResult().GetMessage(),
			Status:        "ok",
		},
		ConsentRecord: &customersv1.ConsentRecord{
			CorrelationId: summarizeSearchIndexesReply.GetResult().GetCorrelationId(),
			Subject: &customersv1.HouseholdRef{
				DisplayName: revertSessionReply.GetSession().GetCustomer().GetDisplayName(),
			},
			ConsentRecordId: req.GetConsentRecordId(),
			Source:          req.GetConsentRecord().GetSource(),
		},
	}, nil
}

// GetConsentRecordRates returns a consent record.
func (s *ConsentRecordCalculationServer) GetConsentRecordRates(ctx context.Context, req *customersv1.GetConsentRecordRatesRequest) (*customersv1.GetConsentRecordRatesResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "consent_record", req.GetConsentRecordId(), "get_consent_record_rates"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get consent record rates", "tenant", req.GetContext().GetTenantId())
	recalculateCustomersRequest := &customersv1.RecalculateCustomersRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			RelationshipManager: &identityv1.UserRef{
				Email:  req.GetContext().GetActor().GetEmail(),
				UserId: req.GetContext().GetActor().GetUserId(),
			},
			Audit:    &commonv1.AuditInfo{CreatedAt: req.GetContext().GetDeadline()},
			Segments: req.GetContext().GetActor().GetRoles(),
		},
	}
	recalculateCustomersReply, err := customersv1.CustomerCalculationServiceClient.RecalculateCustomers(s.customerCalculationClient, ctx, recalculateCustomersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerCalculationService/RecalculateCustomers")
	}
	return &customersv1.GetConsentRecordRatesResponse{
		Result: &commonv1.OperationResult{
			Message:      recalculateCustomersReply.GetResult().GetMessage(),
			ReferenceIds: recalculateCustomersReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		ConsentRecord: &customersv1.ConsentRecord{ConsentRecordId: req.GetConsentRecordId()},
	}, nil
}

// explainCustomerDecisionSender returns a function that sends one request to
// acme.customers.v1.CustomerValidationService/ExplainCustomerDecision.
func (s *ConsentRecordCalculationServer) explainCustomerDecisionSender(request *customersv1.ExplainCustomerDecisionRequest) func(context.Context) (*customersv1.ExplainCustomerDecisionResponse, error) {
	return func(ctx context.Context) (*customersv1.ExplainCustomerDecisionResponse, error) {
		return s.customerValidationClient.ExplainCustomerDecision(ctx, request)
	}
}
