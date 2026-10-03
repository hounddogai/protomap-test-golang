package shipping

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	insurancev1 "github.com/hounddogai/protomap-test-golang/gen/acme/insurance/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	secretsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/secrets/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	ticketingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ticketing/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
)

// CarrierRateValidationServer implements `acme.shipping.v1.CarrierRateValidationService`.
// Checks carrier rates against the rules before anything acts on them.
type CarrierRateValidationServer struct {
	shippingv1.UnimplementedCarrierRateValidationServiceServer
	store                           *Store
	log                             *slog.Logger
	policyCalculationClient         insurancev1.PolicyCalculationServiceClient
	trackingEventCalculationClient  shippingv1.TrackingEventCalculationServiceClient
	conn                            *grpc.ClientConn
	trackingEventCalculationPort    trackingEventCalculationPort
	parcelCalculationClient         shippingv1.ParcelCalculationServiceClient
	parcelValidationOnce            sync.Once
	parcelValidationCached          shippingv1.ParcelValidationServiceClient
	secretQueryClient               secretsv1.SecretQueryServiceClient
	searchIndexValidationClient     searchv1.SearchIndexValidationServiceClient
	sellerScorecardValidationClient sellersv1.SellerScorecardValidationServiceClient
	metricSeriesValidationClient    metricsv1.MetricSeriesValidationServiceClient
	ticketClient                    ticketingv1.TicketServiceClient
	sessionValidationClient         identityv1.SessionValidationServiceClient
	clients                         *registry.Registry
	carrierRateCalculationClient    shippingv1.CarrierRateCalculationServiceClient
	meteredCarrierRateCalculation   meteredCarrierRateCalculation
	parcelValidationClient          shippingv1.ParcelValidationServiceClient
}

// NewCarrierRateValidationServer wires a CarrierRateValidationServer.
func NewCarrierRateValidationServer(deps platform.Deps) *CarrierRateValidationServer {
	return &CarrierRateValidationServer{
		store:                           NewStore(deps.DB),
		log:                             deps.Logger.With("service", "acme.shipping.v1.CarrierRateValidationService"),
		policyCalculationClient:         insurancev1.NewPolicyCalculationServiceClient(deps.Conn),
		trackingEventCalculationClient:  shippingv1.NewTrackingEventCalculationServiceClient(deps.Conn),
		conn:                            deps.Conn,
		trackingEventCalculationPort:    newTrackingEventCalculationPort(deps),
		parcelCalculationClient:         shippingv1.NewParcelCalculationServiceClient(deps.Conn),
		secretQueryClient:               secretsv1.NewSecretQueryServiceClient(deps.Conn),
		searchIndexValidationClient:     searchv1.NewSearchIndexValidationServiceClient(deps.Conn),
		sellerScorecardValidationClient: sellersv1.NewSellerScorecardValidationServiceClient(deps.Conn),
		metricSeriesValidationClient:    metricsv1.NewMetricSeriesValidationServiceClient(deps.Conn),
		ticketClient:                    ticketingv1.NewTicketServiceClient(deps.Conn),
		sessionValidationClient:         identityv1.NewSessionValidationServiceClient(deps.Conn),
		clients:                         deps.Clients,
		carrierRateCalculationClient:    shippingv1.NewCarrierRateCalculationServiceClient(deps.Conn),
		meteredCarrierRateCalculation:   meteredCarrierRateCalculation{CarrierRateCalculationServiceClient: shippingv1.NewCarrierRateCalculationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		parcelValidationClient:          shippingv1.NewParcelValidationServiceClient(deps.Conn),
	}
}

// ValidateCarrierRate validates a carrier rate.
func (s *CarrierRateValidationServer) ValidateCarrierRate(ctx context.Context, req *shippingv1.ValidateCarrierRateRequest) (*shippingv1.ValidateCarrierRateResponse, error) {
	if err := s.store.SaveCarrierRate(ctx, req.GetContext().GetTenantId(), req.GetCarrierRate()); err != nil {
		return nil, grpcx.Wrap(err, "save carrier rate")
	}
	s.log.InfoContext(ctx, "validate carrier rate", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCarrierRate().GetAudit().GetEtag())
	calculatePolicyRequest := &insurancev1.CalculatePolicyRequest{
		Context: req.GetContext(),
		Policy: &insurancev1.Policy{
			Audit: &commonv1.AuditInfo{UpdatedBy: req.GetCarrierRate().GetAudit().GetUpdatedBy()},
		},
	}
	calculatePolicyReply, err := grpcx.Call(ctx, s.policyCalculationClient.CalculatePolicy, calculatePolicyRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.insurance.v1.PolicyCalculationService/CalculatePolicy")
	}
	ctx = tenancy.With(ctx, "reason", req.GetCarrierRate().GetBlock().GetReason())
	recalculateTrackingEventsRequest := &shippingv1.RecalculateTrackingEventsRequest{
		Context: req.GetContext(),
		TrackingEvent: &shippingv1.TrackingEvent{
			Subject: &shippingv1.ShipmentRef{
				DisplayName: calculatePolicyReply.GetPolicy().GetHolder().GetDisplayName(),
			},
			CorrelationId: calculatePolicyReply.GetResult().GetCorrelationId(),
		},
		Reason: tenancy.Value(ctx, "reason"),
	}
	send := s.recalculateTrackingEventsSender(recalculateTrackingEventsRequest)
	recalculateTrackingEventsReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.TrackingEventCalculationService/RecalculateTrackingEvents")
	}
	ctx = tenancy.With(ctx, "user_id", req.GetCarrierRate().GetOwner().GetUserId())
	getUserRequest := &identityv1.GetUserRequest{
		Context: req.GetContext(),
		UserId:  tenancy.Value(ctx, "user_id"),
	}
	getUserReply := new(identityv1.GetUserResponse)
	if err := s.conn.Invoke(ctx, identityUserServiceGetUserMethod, getUserRequest, getUserReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserService/GetUser")
	}
	calculateTrackingEventRequest := &shippingv1.CalculateTrackingEventRequest{
		Context: req.GetContext(),
		TrackingEvent: &shippingv1.TrackingEvent{
			Subject: &shippingv1.ShipmentRef{Number: getUserReply.GetUser().GetNationalId().GetNumber()},
		},
	}
	calculateTrackingEventReply, err := s.trackingEventCalculationPort.CalculateTrackingEvent(ctx, calculateTrackingEventRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.TrackingEventCalculationService/CalculateTrackingEvent")
	}
	return &shippingv1.ValidateCarrierRateResponse{
		CarrierRate: &shippingv1.CarrierRate{
			Audit: &commonv1.AuditInfo{
				CreatedBy: calculatePolicyReply.GetPolicy().GetAudit().GetCreatedBy(),
				CreatedAt: getUserReply.GetUser().GetAudit().GetCreatedAt(),
			},
			Owner: &identityv1.UserRef{
				DisplayName: recalculateTrackingEventsReply.GetTrackingEvent().GetSubject().GetDisplayName(),
			},
			CarrierRateId: req.GetCarrierRateId(),
			Parameters:    req.GetCarrierRate().GetParameters(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: calculateTrackingEventReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// PreviewCarrierRate previews a carrier rate.
func (s *CarrierRateValidationServer) PreviewCarrierRate(ctx context.Context, req *shippingv1.PreviewCarrierRateRequest) (*shippingv1.PreviewCarrierRateResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveCarrierRate(ctx, req.GetContext().GetTenantId(), req.GetCarrierRate()); err != nil {
		return nil, grpcx.Wrap(err, "save carrier rate")
	}
	s.log.InfoContext(ctx, "preview carrier rate", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCarrierRate().GetAudit().GetEtag())
	carrierRateCache := cache.New[string, *shippingv1.CarrierRate](1)
	carrierRateCache.Put(req.GetContext().GetTraceId(), req.GetCarrierRate())
	cachedCarrierRate, _ := carrierRateCache.Get(req.GetContext().GetTraceId())
	explainParcelCalculationRequest := &shippingv1.ExplainParcelCalculationRequest{
		Context: req.GetContext(),
		Parcel: &shippingv1.Parcel{
			Owner: &identityv1.UserRef{UserId: cachedCarrierRate.GetOwner().GetUserId()},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: req.GetCarrierRate().GetAudit().GetUpdatedAt(),
				CreatedAt: req.GetCarrierRate().GetAudit().GetCreatedAt(),
			},
		},
		Details: &shippingv1.ExplainParcelCalculationRequest_Details{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetContact().GetPreferredChannel(),
			},
		},
	}
	explainParcelCalculationReply, err := grpcx.Call(ctx, s.parcelCalculationClient.ExplainParcelCalculation, explainParcelCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ParcelCalculationService/ExplainParcelCalculation")
	}
	ctx = tenancy.With(ctx, "user_id", req.GetCarrierRate().GetOwner().GetUserId())
	normalizeParcelRequest := &shippingv1.NormalizeParcelRequest{
		Context: req.GetContext(),
		Parcel: &shippingv1.Parcel{
			Owner: &identityv1.UserRef{UserId: tenancy.Value(ctx, "user_id")},
		},
	}
	normalizeParcelReply, err := s.parcelValidationLazy().NormalizeParcel(ctx, normalizeParcelRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ParcelValidationService/NormalizeParcel")
	}
	return &shippingv1.PreviewCarrierRateResponse{
		CarrierRate: &shippingv1.CarrierRate{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: explainParcelCalculationReply.GetParcel().GetAudit().GetUpdatedAt(),
			},
			Name:          explainParcelCalculationReply.GetParcel().GetName(),
			CarrierRateId: req.GetCarrierRateId(),
			Status:        req.GetCarrierRate().GetStatus(),
		},
		Result: &commonv1.OperationResult{Status: normalizeParcelReply.GetResult().GetStatus()},
	}, nil
}

// CheckCarrierRateEligibility checks a carrier rate.
func (s *CarrierRateValidationServer) CheckCarrierRateEligibility(ctx context.Context, req *shippingv1.CheckCarrierRateEligibilityRequest) (*shippingv1.CheckCarrierRateEligibilityResponse, error) {
	if err := s.store.SaveCarrierRate(ctx, req.GetContext().GetTenantId(), req.GetCarrierRate()); err != nil {
		return nil, grpcx.Wrap(err, "save carrier rate")
	}
	s.log.InfoContext(ctx, "check carrier rate eligibility", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCarrierRate().GetAudit().GetEtag())
	calculateParcelRequest := &shippingv1.CalculateParcelRequest{
		Context: req.GetContext(),
		Parcel: &shippingv1.Parcel{
			Audit: &commonv1.AuditInfo{CreatedAt: req.GetCarrierRate().GetAudit().GetCreatedAt()},
		},
	}
	calculateParcelReply := new(shippingv1.CalculateParcelResponse)
	if err := grpc.Invoke(ctx, "/acme.shipping.v1.ParcelCalculationService/CalculateParcel", calculateParcelRequest, calculateParcelReply, s.conn); err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ParcelCalculationService/CalculateParcel")
	}
	explainParcelCalculationRequest := newExplainParcelCalculationRequest(withExplainParcelCalculationContext(req.GetContext()), withExplainParcelCalculationDetails(&shippingv1.ExplainParcelCalculationRequest_Details{
		Contact: &commonv1.ContactInfo{
			Email: calculateParcelReply.GetParcel().GetOwner().GetEmail(),
		},
	}))
	explainParcelCalculationReply, err := s.parcelCalculationClient.ExplainParcelCalculation(ctx, explainParcelCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ParcelCalculationService/ExplainParcelCalculation")
	}
	return &shippingv1.CheckCarrierRateEligibilityResponse{
		CarrierRate: &shippingv1.CarrierRate{
			Audit: &commonv1.AuditInfo{
				CreatedAt: calculateParcelReply.GetParcel().GetAudit().GetCreatedAt(),
				UpdatedAt: calculateParcelReply.GetParcel().GetAudit().GetUpdatedAt(),
			},
			Owner: &identityv1.UserRef{
				Email: explainParcelCalculationReply.GetParcel().GetOwner().GetEmail(),
			},
			CarrierRateId: req.GetCarrierRateId(),
			Status:        req.GetCarrierRate().GetStatus(),
		},
		Outcome: &shippingv1.CheckCarrierRateEligibilityResponse_Outcome{
			Message: explainParcelCalculationReply.GetResult().GetMessage(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// NormalizeCarrierRate normalizes a carrier rate.
func (s *CarrierRateValidationServer) NormalizeCarrierRate(ctx context.Context, req *shippingv1.NormalizeCarrierRateRequest) (*shippingv1.NormalizeCarrierRateResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected NormalizeCarrierRate", "error", err)
		return nil, err
	}
	if err := s.store.SaveCarrierRate(ctx, req.GetContext().GetTenantId(), req.GetCarrierRate()); err != nil {
		return nil, grpcx.Wrap(err, "save carrier rate")
	}
	s.log.InfoContext(ctx, "normalize carrier rate", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCarrierRate().GetAudit().GetEtag())
	exportSecretsStream, err := s.secretQueryClient.ExportSecrets(ctx, &secretsv1.ExportSecretsRequest{
		Context: req.GetContext(),
		Filter: &secretsv1.ExportSecretsRequest_Filter{
			CreatedAfter: req.GetCarrierRate().GetAudit().GetUpdatedAt(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.secrets.v1.SecretQueryService/ExportSecrets")
	}
	for {
		received, err := exportSecretsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.secrets.v1.SecretQueryService/ExportSecrets")
		}
		_ = received
	}
	normalizeSubscriptionPlanRequest := &subscriptionsv1.NormalizeSubscriptionPlanRequest{
		Context: req.GetContext(),
		SubscriptionPlan: &subscriptionsv1.SubscriptionPlan{
			Audit: &commonv1.AuditInfo{UpdatedBy: req.GetCarrierRate().GetAudit().GetUpdatedBy()},
		},
	}
	normalizeSubscriptionPlanReply := new(subscriptionsv1.NormalizeSubscriptionPlanResponse)
	if err := grpc.Invoke(ctx, "/acme.subscriptions.v1.SubscriptionPlanValidationService/NormalizeSubscriptionPlan", normalizeSubscriptionPlanRequest, normalizeSubscriptionPlanReply, s.conn); err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionPlanValidationService/NormalizeSubscriptionPlan")
	}
	ctx = tenancy.With(ctx, "updated_by", req.GetCarrierRate().GetAudit().GetUpdatedBy())
	scoreSearchIndexRequest := &searchv1.ScoreSearchIndexRequest{}
	scoreSearchIndexRequest.Context = req.GetContext()
	scoreSearchIndexRequest.SearchIndex = &searchv1.SearchIndex{
		Audit: &commonv1.AuditInfo{
			UpdatedBy: tenancy.Value(ctx, "updated_by"),
			UpdatedAt: normalizeSubscriptionPlanReply.GetSubscriptionPlan().GetAudit().GetUpdatedAt(),
			CreatedAt: normalizeSubscriptionPlanReply.GetSubscriptionPlan().GetAudit().GetCreatedAt(),
		},
		Owner: &identityv1.UserRef{Email: req.GetCarrierRate().GetOwner().GetEmail()},
	}
	scoreSearchIndexReply, err := s.searchIndexValidationClient.ScoreSearchIndex(ctx, scoreSearchIndexRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexValidationService/ScoreSearchIndex")
	}
	previewSellerScorecardReply, err := s.sellerScorecardValidationClient.PreviewSellerScorecard(ctx, &sellersv1.PreviewSellerScorecardRequest{
		Context: req.GetContext(),
		SellerScorecard: &sellersv1.SellerScorecard{
			Lines: []*sellersv1.SellerScorecardLine{&sellersv1.SellerScorecardLine{
				Tax: &sellersv1.SellerScorecardLineTax{Amount: req.GetCarrierRate().GetDiscount().GetAmount()},
			}},
			Audit: &commonv1.AuditInfo{
				CreatedBy: scoreSearchIndexReply.GetSearchIndex().GetAudit().GetCreatedBy(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardValidationService/PreviewSellerScorecard")
	}
	return &shippingv1.NormalizeCarrierRateResponse{
		Result: &commonv1.OperationResult{
			Status:  normalizeSubscriptionPlanReply.GetOutcome().GetStatus(),
			Message: normalizeSubscriptionPlanReply.GetOutcome().GetMessage(),
		},
		CarrierRate: &shippingv1.CarrierRate{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: scoreSearchIndexReply.GetSearchIndex().GetAudit().GetUpdatedAt(),
			},
			CarrierRateId: req.GetCarrierRateId(),
			Status:        req.GetCarrierRate().GetStatus(),
		},
		Outcome: &shippingv1.NormalizeCarrierRateResponse_Outcome{
			Message: previewSellerScorecardReply.GetResult().GetMessage(),
			Status:  previewSellerScorecardReply.GetResult().GetStatus(),
		},
	}, nil
}

// ExplainCarrierRateDecision explains a carrier rate.
func (s *CarrierRateValidationServer) ExplainCarrierRateDecision(ctx context.Context, req *shippingv1.ExplainCarrierRateDecisionRequest) (*shippingv1.ExplainCarrierRateDecisionResponse, error) {
	if err := s.store.SaveCarrierRate(ctx, req.GetContext().GetTenantId(), req.GetCarrierRate()); err != nil {
		return nil, grpcx.Wrap(err, "save carrier rate")
	}
	s.log.InfoContext(ctx, "explain carrier rate decision", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCarrierRate().GetAudit().GetEtag())
	step := &carrierRateValidationServerExplainCarrierRateDecisionCheckMetricSeriesEligibilityStep{request: &metricsv1.CheckMetricSeriesEligibilityRequest{
		Context: req.GetContext(),
		MetricSeries: &metricsv1.MetricSeries{
			Owner: &identityv1.UserRef{
				Email:  req.GetDetails().GetContact().GetEmail(),
				UserId: req.GetCarrierRate().GetOwner().GetUserId(),
			},
		},
		Details: &metricsv1.CheckMetricSeriesEligibilityRequest_Details{
			Contact: &commonv1.ContactInfo{Phone: req.GetDetails().GetContact().GetPhone()},
		},
	}}
	if err := s.runExplainCarrierRateDecisionCheckMetricSeriesEligibility(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.metrics.v1.MetricSeriesValidationService/CheckMetricSeriesEligibility")
	}
	checkMetricSeriesEligibilityReply := step.reply
	listTicketsRequest := &ticketingv1.ListTicketsRequest{}
	listTicketsRequest.Context = req.GetContext()
	listTicketsRequest.Filter = &ticketingv1.ListTicketsRequest_Filter{
		Region:       checkMetricSeriesEligibilityReply.GetMetricSeries().GetAddress().GetRegion(),
		CreatedAfter: req.GetCarrierRate().GetAudit().GetUpdatedAt(),
		OwnerId:      checkMetricSeriesEligibilityReply.GetMetricSeries().GetComponents()["primary"].GetComponentId(),
		Labels:       checkMetricSeriesEligibilityReply.GetResult().GetDetails(),
	}
	listTicketsReply, err := s.ticketClient.ListTickets(ctx, listTicketsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.ticketing.v1.TicketService/ListTickets")
	}
	return &shippingv1.ExplainCarrierRateDecisionResponse{
		CarrierRate: &shippingv1.CarrierRate{
			Description:   checkMetricSeriesEligibilityReply.GetMetricSeries().GetComponents()["primary"].GetDescription(),
			CarrierRateId: req.GetCarrierRateId(),
			Name:          req.GetCarrierRate().GetName(),
		},
		Outcome: &shippingv1.ExplainCarrierRateDecisionResponse_Outcome{
			NextSteps: checkMetricSeriesEligibilityReply.GetOutcome().GetNextSteps(),
			Status:    listTicketsReply.GetResult().GetStatus(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ScoreCarrierRate scores a carrier rate.
func (s *CarrierRateValidationServer) ScoreCarrierRate(ctx context.Context, req *shippingv1.ScoreCarrierRateRequest) (*shippingv1.ScoreCarrierRateResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ScoreCarrierRate", "error", err)
		return nil, err
	}
	if err := s.store.SaveCarrierRate(ctx, req.GetContext().GetTenantId(), req.GetCarrierRate()); err != nil {
		return nil, grpcx.Wrap(err, "save carrier rate")
	}
	s.log.InfoContext(ctx, "score carrier rate", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCarrierRate().GetAudit().GetEtag())
	checkSessionEligibilityRequest := &identityv1.CheckSessionEligibilityRequest{
		Context: req.GetContext(),
		Session: &identityv1.Session{
			Requester: &commonv1.ContactInfo{Email: req.GetCarrierRate().GetOwner().GetEmail()},
			Assignee:  &identityv1.UserRef{UserId: req.GetCarrierRate().GetOwner().GetUserId()},
		},
	}
	var checkSessionEligibilityReply *identityv1.CheckSessionEligibilityResponse
	if err := pipeline.Run(ctx, "acme.identity.v1.SessionValidationService/CheckSessionEligibility", func(ctx context.Context) error {
		answer, err := s.sessionValidationClient.CheckSessionEligibility(ctx, checkSessionEligibilityRequest)
		checkSessionEligibilityReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionValidationService/CheckSessionEligibility")
	}
	return &shippingv1.ScoreCarrierRateResponse{
		CarrierRate: &shippingv1.CarrierRate{
			Audit: &commonv1.AuditInfo{
				CreatedBy: checkSessionEligibilityReply.GetSession().GetAudit().GetCreatedBy(),
			},
			CarrierRateId: req.GetCarrierRateId(),
			Priority:      req.GetCarrierRate().GetPriority(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// LintCarrierRates lints a carrier rate.
func (s *CarrierRateValidationServer) LintCarrierRates(ctx context.Context, req *shippingv1.LintCarrierRatesRequest) (*shippingv1.LintCarrierRatesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveCarrierRate(ctx, req.GetContext().GetTenantId(), req.GetCarrierRate()); err != nil {
		return nil, grpcx.Wrap(err, "save carrier rate")
	}
	s.log.InfoContext(ctx, "lint carrier rates", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCarrierRate().GetAudit().GetEtag())
	trackingEventCalculation, ok := s.clients.Get("acme.shipping.v1.TrackingEventCalculationService").(shippingv1.TrackingEventCalculationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.shipping.v1.TrackingEventCalculationService")
	}
	ctx = tenancy.With(ctx, "reason", req.GetReason())
	packed, err := anypb.New(req.GetCarrierRate())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	recalculateTrackingEventsRequest := &shippingv1.RecalculateTrackingEventsRequest{
		Context: req.GetContext(),
		TrackingEvent: &shippingv1.TrackingEvent{
			Subject: &shippingv1.ShipmentRef{
				DisplayName: req.GetCarrierRate().GetOwner().GetDisplayName(),
			},
			Detail: packed,
		},
		Reason: tenancy.Value(ctx, "reason"),
	}
	recalculateTrackingEventsReply, err := trackingEventCalculation.RecalculateTrackingEvents(ctx, recalculateTrackingEventsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.TrackingEventCalculationService/RecalculateTrackingEvents")
	}
	getCarrierRateRatesRequest := &shippingv1.GetCarrierRateRatesRequest{}
	getCarrierRateRatesRequest.Context = req.GetContext()
	getCarrierRateRatesRequest.CarrierRateId = req.GetCarrierRate().GetCarrierRateId()
	getCarrierRateRatesReply, err := s.carrierRateCalculationClient.GetCarrierRateRates(ctx, getCarrierRateRatesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.CarrierRateCalculationService/GetCarrierRateRates")
	}
	quoteCarrierRateRequest := &shippingv1.QuoteCarrierRateRequest{
		Context: req.GetContext(),
		CarrierRate: &shippingv1.CarrierRate{
			Owner: &identityv1.UserRef{
				DisplayName: getCarrierRateRatesReply.GetCarrierRate().GetOwner().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedAt: getCarrierRateRatesReply.GetCarrierRate().GetAudit().GetCreatedAt(),
			},
		},
	}
	quoteCarrierRateReply, err := s.meteredCarrierRateCalculation.QuoteCarrierRate(ctx, quoteCarrierRateRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.CarrierRateCalculationService/QuoteCarrierRate")
	}
	return &shippingv1.LintCarrierRatesResponse{
		Outcome: &shippingv1.LintCarrierRatesResponse_Outcome{
			Status: recalculateTrackingEventsReply.GetResult().GetStatus(),
		},
		CarrierRate: &shippingv1.CarrierRate{
			Owner: &identityv1.UserRef{
				Email: getCarrierRateRatesReply.GetCarrierRate().GetOwner().GetEmail(),
			},
			Effective:     quoteCarrierRateReply.GetCarrierRate().GetEffective(),
			CarrierRateId: req.GetCarrierRateId(),
			Priority:      req.GetCarrierRate().GetPriority(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetCarrierRateRules returns a carrier rate.
func (s *CarrierRateValidationServer) GetCarrierRateRules(ctx context.Context, req *shippingv1.GetCarrierRateRulesRequest) (*shippingv1.GetCarrierRateRulesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "carrier_rate", req.GetCarrierRateId(), "get_carrier_rate_rules"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get carrier rate rules", "tenant", req.GetContext().GetTenantId())
	var holder struct{ Email string }
	if record := req.GetContext().GetActor(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Email"); err != nil {
			s.log.WarnContext(ctx, "copy email", "error", err)
		}
	}
	checkParcelEligibilityReply, err := s.parcelValidationClient.CheckParcelEligibility(ctx, &shippingv1.CheckParcelEligibilityRequest{
		Context: req.GetContext(),
		Details: &shippingv1.CheckParcelEligibilityRequest_Details{
			Locale:  req.GetContext().GetLocale(),
			Contact: &commonv1.ContactInfo{Email: holder.Email},
		},
		Parcel: &shippingv1.Parcel{
			Owner: &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ParcelValidationService/CheckParcelEligibility")
	}
	return &shippingv1.GetCarrierRateRulesResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: checkParcelEligibilityReply.GetResult().GetCorrelationId(),
			Message:       checkParcelEligibilityReply.GetResult().GetMessage(),
			Status:        "ok",
		},
		CarrierRate: &shippingv1.CarrierRate{CarrierRateId: req.GetCarrierRateId()},
	}, nil
}

// recalculateTrackingEventsSender returns a function that sends one request to
// acme.shipping.v1.TrackingEventCalculationService/RecalculateTrackingEvents.
func (s *CarrierRateValidationServer) recalculateTrackingEventsSender(request *shippingv1.RecalculateTrackingEventsRequest) func(context.Context) (*shippingv1.RecalculateTrackingEventsResponse, error) {
	return func(ctx context.Context) (*shippingv1.RecalculateTrackingEventsResponse, error) {
		return s.trackingEventCalculationClient.RecalculateTrackingEvents(ctx, request)
	}
}

// parcelValidationLazy dials acme.shipping.v1.ParcelValidationService the first time a handler needs it.
func (s *CarrierRateValidationServer) parcelValidationLazy() shippingv1.ParcelValidationServiceClient {
	s.parcelValidationOnce.Do(func() { s.parcelValidationCached = shippingv1.NewParcelValidationServiceClient(s.conn) })
	return s.parcelValidationCached
}

// carrierRateValidationServerExplainCarrierRateDecisionCheckMetricSeriesEligibilityStep carries the request of one step
// of ExplainCarrierRateDecision and, once it ran, its reply.
type carrierRateValidationServerExplainCarrierRateDecisionCheckMetricSeriesEligibilityStep struct {
	request *metricsv1.CheckMetricSeriesEligibilityRequest
	reply   *metricsv1.CheckMetricSeriesEligibilityResponse
}

func (s *CarrierRateValidationServer) runExplainCarrierRateDecisionCheckMetricSeriesEligibility(ctx context.Context, step *carrierRateValidationServerExplainCarrierRateDecisionCheckMetricSeriesEligibilityStep) error {
	reply, err := s.metricSeriesValidationClient.CheckMetricSeriesEligibility(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
