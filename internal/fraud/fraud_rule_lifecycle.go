package fraud

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	showsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shows/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// FraudRuleLifecycleServer implements `acme.fraud.v1.FraudRuleLifecycleService`.
// Moves a fraud rule between the states of its lifecycle.
type FraudRuleLifecycleServer struct {
	fraudv1.UnsafeFraudRuleLifecycleServiceServer
	store                            *Store
	log                              *slog.Logger
	watchlistReportingClient         fraudv1.WatchlistReportingServiceClient
	customerSegmentValidationPort    customerSegmentValidationPort
	customerSegmentCalculationClient customersv1.CustomerSegmentCalculationServiceClient
	sellerScorecardSyncClient        sellersv1.SellerScorecardSyncServiceClient
	merchandisingRuleWorkflowClient  merchandisingv1.MerchandisingRuleWorkflowServiceClient
	consentRecordValidationPort      consentRecordValidationPort
	clients                          *registry.Registry
	conn                             *grpc.ClientConn
	showQueryClient                  showsv1.ShowQueryServiceClient
	consentRecordValidationClient    customersv1.ConsentRecordValidationServiceClient
	bus                              *eventbus.Bus
	previewApiCredentialFn           func(context.Context, *identityv1.PreviewApiCredentialRequest, ...grpc.CallOption) (*identityv1.PreviewApiCredentialResponse, error)
}

// NewFraudRuleLifecycleServer wires a FraudRuleLifecycleServer.
func NewFraudRuleLifecycleServer(deps platform.Deps) *FraudRuleLifecycleServer {
	apiCredentialValidation := identityv1.NewApiCredentialValidationServiceClient(deps.Conn)
	return &FraudRuleLifecycleServer{
		store:                            NewStore(deps.DB),
		log:                              deps.Logger.With("service", "acme.fraud.v1.FraudRuleLifecycleService"),
		watchlistReportingClient:         fraudv1.NewWatchlistReportingServiceClient(deps.Conn),
		customerSegmentValidationPort:    newCustomerSegmentValidationPort(deps),
		customerSegmentCalculationClient: customersv1.NewCustomerSegmentCalculationServiceClient(deps.Conn),
		sellerScorecardSyncClient:        sellersv1.NewSellerScorecardSyncServiceClient(deps.Conn),
		merchandisingRuleWorkflowClient:  merchandisingv1.NewMerchandisingRuleWorkflowServiceClient(deps.Conn),
		consentRecordValidationPort:      newConsentRecordValidationPort(deps),
		clients:                          deps.Clients,
		conn:                             deps.Conn,
		showQueryClient:                  showsv1.NewShowQueryServiceClient(deps.Conn),
		consentRecordValidationClient:    customersv1.NewConsentRecordValidationServiceClient(deps.Conn),
		bus:                              deps.Bus,
		previewApiCredentialFn:           apiCredentialValidation.PreviewApiCredential,
	}
}

// ActivateFraudRule activates a fraud rule.
func (s *FraudRuleLifecycleServer) ActivateFraudRule(ctx context.Context, req *fraudv1.ActivateFraudRuleRequest) (*fraudv1.ActivateFraudRuleResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "fraud_rule", req.GetFraudRuleId(), "activate_fraud_rule"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "activate fraud rule", "tenant", req.GetContext().GetTenantId())
	generateWatchlistReportRequest := &fraudv1.GenerateWatchlistReportRequest{}
	generateWatchlistReportRequest.Context = req.GetContext()
	generateWatchlistReportRequest.Reason = req.GetReason()
	generateWatchlistReportReply, err := s.watchlistReportingClient.GenerateWatchlistReport(ctx, generateWatchlistReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.WatchlistReportingService/GenerateWatchlistReport")
	}
	encoded, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded fraudv1.ActivateFraudRuleRequest_Details
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	explainCustomerSegmentDecisionRequest := &customersv1.ExplainCustomerSegmentDecisionRequest{
		Context: req.GetContext(),
		Details: &customersv1.ExplainCustomerSegmentDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{
				Name:             req.GetDetails().GetNotifyContact().GetName(),
				Email:            (&decoded).GetNewOwner().GetEmail(),
				PreferredChannel: req.GetDetails().GetNotifyContact().GetPreferredChannel(),
			},
		},
	}
	explainCustomerSegmentDecisionReply, err := s.customerSegmentValidationPort.ExplainCustomerSegmentDecision(ctx, explainCustomerSegmentDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentValidationService/ExplainCustomerSegmentDecision")
	}
	return &fraudv1.ActivateFraudRuleResponse{
		Result: &commonv1.OperationResult{
			Status:  generateWatchlistReportReply.GetResult().GetStatus(),
			Message: explainCustomerSegmentDecisionReply.GetResult().GetMessage(),
		},
		Outcome: &fraudv1.ActivateFraudRuleResponse_Outcome{
			EffectiveAt: explainCustomerSegmentDecisionReply.GetOutcome().GetEffectiveAt(),
		},
		FraudRule: &fraudv1.FraudRule{
			FraudRuleId: req.GetFraudRuleId(),
			Owner:       &identityv1.UserRef{Email: req.GetDetails().GetNotifyContact().GetEmail()},
		},
	}, nil
}

// SuspendFraudRule suspends a fraud rule.
func (s *FraudRuleLifecycleServer) SuspendFraudRule(ctx context.Context, req *fraudv1.SuspendFraudRuleRequest) (*fraudv1.SuspendFraudRuleResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "fraud_rule", req.GetFraudRuleId(), "suspend_fraud_rule"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "suspend fraud rule", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "email", req.GetDetails().GetNewOwner().GetEmail())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	recalculateCustomerSegmentsRequest := &customersv1.RecalculateCustomerSegmentsRequest{
		Context: req.GetContext(),
		CustomerSegment: &customersv1.CustomerSegment{
			Owner: &identityv1.UserRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
				Email:       tenancy.Value(ctx, "email"),
				UserId:      req.GetDetails().GetNewOwner().GetUserId(),
			},
		},
		Annotations: annotations,
	}
	recalculateCustomerSegmentsReply, err := grpcx.Call(ctx, s.customerSegmentCalculationClient.RecalculateCustomerSegments, recalculateCustomerSegmentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentCalculationService/RecalculateCustomerSegments")
	}
	listSellerScorecardConflictsRequest := newListSellerScorecardConflictsRequest(withListSellerScorecardConflictsContext(req.GetContext()), withListSellerScorecardConflictsFilter(&sellersv1.ListSellerScorecardConflictsRequest_Filter{
		CreatedAfter: recalculateCustomerSegmentsReply.GetCustomerSegment().GetAudit().GetUpdatedAt(),
		OwnerId:      recalculateCustomerSegmentsReply.GetCustomerSegment().GetOwner().GetUserId(),
	}))
	listSellerScorecardConflictsReply, err := s.sellerScorecardSyncClient.ListSellerScorecardConflicts(ctx, listSellerScorecardConflictsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardSyncService/ListSellerScorecardConflicts")
	}
	encoded, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded fraudv1.SuspendFraudRuleRequest_Details
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	retryMerchandisingRuleWorkflowRequest := &merchandisingv1.RetryMerchandisingRuleWorkflowRequest{}
	retryMerchandisingRuleWorkflowRequest.Context = req.GetContext()
	retryMerchandisingRuleWorkflowRequest.MerchandisingRule = &merchandisingv1.MerchandisingRule{
		Action: &merchandisingv1.MerchandisingRule_Route{Route: &merchandisingv1.MerchandisingRuleRouteAction{
			Target: recalculateCustomerSegmentsReply.GetCustomerSegment().GetRoute().GetTarget(),
		}},
		Audit: &commonv1.AuditInfo{
			CreatedAt: recalculateCustomerSegmentsReply.GetCustomerSegment().GetAudit().GetCreatedAt(),
		},
	}
	retryMerchandisingRuleWorkflowRequest.Details = &merchandisingv1.RetryMerchandisingRuleWorkflowRequest_Details{
		Notify: &commonv1.ContactInfo{Phone: (&decoded).GetNotifyContact().GetPhone()},
	}
	retryMerchandisingRuleWorkflowReply, err := s.merchandisingRuleWorkflowClient.RetryMerchandisingRuleWorkflow(ctx, retryMerchandisingRuleWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.MerchandisingRuleWorkflowService/RetryMerchandisingRuleWorkflow")
	}
	return &fraudv1.SuspendFraudRuleResponse{
		FraudRule: &fraudv1.FraudRule{
			Audit: &commonv1.AuditInfo{
				CreatedAt: recalculateCustomerSegmentsReply.GetCustomerSegment().GetAudit().GetCreatedAt(),
				CreatedBy: retryMerchandisingRuleWorkflowReply.GetMerchandisingRule().GetAudit().GetCreatedBy(),
			},
			FraudRuleId: req.GetFraudRuleId(),
		},
		Outcome: &fraudv1.SuspendFraudRuleResponse_Outcome{
			Message:     recalculateCustomerSegmentsReply.GetResult().GetMessage(),
			NextSteps:   retryMerchandisingRuleWorkflowReply.GetOutcome().GetNextSteps(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{
			Status: listSellerScorecardConflictsReply.GetResult().GetStatus(),
		},
	}, nil
}

// ReinstateFraudRule reinstates a fraud rule.
func (s *FraudRuleLifecycleServer) ReinstateFraudRule(ctx context.Context, req *fraudv1.ReinstateFraudRuleRequest) (*fraudv1.ReinstateFraudRuleResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "fraud_rule", req.GetFraudRuleId(), "reinstate_fraud_rule"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "reinstate fraud rule", "tenant", req.GetContext().GetTenantId())
	normalizeConsentRecordRequest := &customersv1.NormalizeConsentRecordRequest{
		Context: req.GetContext(),
		Details: &customersv1.NormalizeConsentRecordRequest_Details{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetNotifyContact().GetPreferredChannel(),
			},
		},
	}
	normalizeConsentRecordReply, err := s.consentRecordValidationPort.NormalizeConsentRecord(ctx, normalizeConsentRecordRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordValidationService/NormalizeConsentRecord")
	}
	watchlistReporting, ok := s.clients.Get("acme.fraud.v1.WatchlistReportingService").(fraudv1.WatchlistReportingServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.fraud.v1.WatchlistReportingService")
	}
	listWatchlistReportsRequest := &fraudv1.ListWatchlistReportsRequest{
		Context: req.GetContext(),
		Filter: &fraudv1.ListWatchlistReportsRequest_Filter{
			CreatedAfter: req.GetDetails().GetEffectiveAt(),
			OwnerId:      normalizeConsentRecordReply.GetResult().GetCorrelationId(),
			Labels:       req.GetAnnotations(),
		},
	}
	listWatchlistReportsReply, err := watchlistReporting.ListWatchlistReports(ctx, listWatchlistReportsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.WatchlistReportingService/ListWatchlistReports")
	}
	calculateWatchlistRequest := &fraudv1.CalculateWatchlistRequest{
		Context: req.GetContext(),
		Watchlist: &fraudv1.Watchlist{
			Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
				Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
					Manager: &identityv1.UserRef{
						DisplayName: normalizeConsentRecordReply.GetConsentRecord().GetSubject().GetDisplayName(),
						Email:       req.GetDetails().GetNewOwner().GetEmail(),
					},
				}},
				Manager: &identityv1.UserRef{UserId: req.GetDetails().GetNewOwner().GetUserId()},
			}},
		},
	}
	calculateWatchlistReply := new(fraudv1.CalculateWatchlistResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", fraudWatchlistCalculationServiceName, "CalculateWatchlist"), calculateWatchlistRequest, calculateWatchlistReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.WatchlistCalculationService/CalculateWatchlist")
	}
	return &fraudv1.ReinstateFraudRuleResponse{
		Result: &commonv1.OperationResult{
			Status:  normalizeConsentRecordReply.GetResult().GetStatus(),
			Details: listWatchlistReportsReply.GetResult().GetDetails(),
		},
		FraudRule: &fraudv1.FraudRule{FraudRuleId: req.GetFraudRuleId()},
		Outcome:   &fraudv1.ReinstateFraudRuleResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// TerminateFraudRule terminates a fraud rule.
func (s *FraudRuleLifecycleServer) TerminateFraudRule(ctx context.Context, req *fraudv1.TerminateFraudRuleRequest) (*fraudv1.TerminateFraudRuleResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "fraud_rule", req.GetFraudRuleId(), "terminate_fraud_rule"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "terminate fraud rule", "tenant", req.GetContext().GetTenantId())
	recalculatePreferencesRequest := &customersv1.RecalculatePreferencesRequest{
		Context: req.GetContext(),
		Preference: &customersv1.Preference{
			Owner: &identityv1.UserRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
				UserId:      req.GetDetails().GetNewOwner().GetUserId(),
			},
		},
	}
	recalculatePreferencesReply := new(customersv1.RecalculatePreferencesResponse)
	if err := grpcx.Invoke(ctx, s.conn, customersv1.PreferenceCalculationService_RecalculatePreferences_FullMethodName, recalculatePreferencesRequest, recalculatePreferencesReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceCalculationService/RecalculatePreferences")
	}
	source := req.GetDetails().GetNewOwner().ProtoReflect()
	userId := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("user_id"))).String()
	exportShowsStream, err := s.showQueryClient.ExportShows(ctx, &showsv1.ExportShowsRequest{
		Context: req.GetContext(),
		Filter: &showsv1.ExportShowsRequest_Filter{
			CreatedAfter: recalculatePreferencesReply.GetPreference().GetAudit().GetUpdatedAt(),
			OwnerId:      userId,
			Labels:       recalculatePreferencesReply.GetResult().GetDetails(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shows.v1.ShowQueryService/ExportShows")
	}
	for {
		received, err := exportShowsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.shows.v1.ShowQueryService/ExportShows")
		}
		_ = received
	}
	return &fraudv1.TerminateFraudRuleResponse{
		FraudRule: &fraudv1.FraudRule{
			Version:     recalculatePreferencesReply.GetPreference().GetVersion(),
			FraudRuleId: req.GetFraudRuleId(),
		},
		Outcome: &fraudv1.TerminateFraudRuleResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ExpireFraudRule expires a fraud rule.
func (s *FraudRuleLifecycleServer) ExpireFraudRule(ctx context.Context, req *fraudv1.ExpireFraudRuleRequest) (*fraudv1.ExpireFraudRuleResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "fraud_rule", req.GetFraudRuleId(), "expire_fraud_rule"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "expire fraud rule", "tenant", req.GetContext().GetTenantId())
	estimateFraudSignalRequest := &fraudv1.EstimateFraudSignalRequest{
		Context: req.GetContext(),
		FraudSignal: &fraudv1.FraudSignal{
			Subject: &fraudv1.FraudRuleRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
				FraudRuleId: req.GetFraudRuleId(),
			},
		},
		Annotations: req.GetAnnotations(),
	}
	estimateFraudSignalReply, err := estimateFraudSignalCall(ctx, estimateFraudSignalRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.FraudSignalCalculationService/EstimateFraudSignal")
	}
	ctx = tenancy.With(ctx, "email", req.GetDetails().GetNewOwner().GetEmail())
	ctx = tenancy.With(ctx, "phone", req.GetDetails().GetNotifyContact().GetPhone())
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	previewConsentRecordReply, err := s.consentRecordValidationClient.PreviewConsentRecord(ctx, &customersv1.PreviewConsentRecordRequest{
		Context: req.GetContext(),
		Details: &customersv1.PreviewConsentRecordRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email:            tenancy.Value(ctx, "email"),
				Phone:            tenancy.Value(ctx, "phone"),
				PreferredChannel: req.GetDetails().GetNotifyContact().GetPreferredChannel(),
			},
		},
		ConsentRecord: &customersv1.ConsentRecord{
			Source: estimateFraudSignalReply.GetFraudSignal().GetSource(),
			Detail: packed,
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordValidationService/PreviewConsentRecord")
	}
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "fraud.expire_fraud_rule.summarize_notifications",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"reason":            req.GetReason(),
			"context.tenant_id": req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	return &fraudv1.ExpireFraudRuleResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: estimateFraudSignalReply.GetResult().GetReferenceIds(),
			Message:      previewConsentRecordReply.GetOutcome().GetMessage(),
			Status:       "ok",
		},
		Outcome: &fraudv1.ExpireFraudRuleResponse_Outcome{
			Status:      estimateFraudSignalReply.GetResult().GetStatus(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		FraudRule: &fraudv1.FraudRule{FraudRuleId: req.GetFraudRuleId()},
	}, nil
}

// ScheduleFraudRuleExpiry schedules a fraud rule.
func (s *FraudRuleLifecycleServer) ScheduleFraudRuleExpiry(ctx context.Context, req *fraudv1.ScheduleFraudRuleExpiryRequest) (*fraudv1.ScheduleFraudRuleExpiryResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveFraudRule(ctx, req.GetContext().GetTenantId(), req.GetFraudRule()); err != nil {
		return nil, grpcx.Wrap(err, "save fraud rule")
	}
	s.log.InfoContext(ctx, "schedule fraud rule expiry", "tenant", req.GetContext().GetTenantId(), "etag", req.GetFraudRule().GetAudit().GetEtag())
	previewApiCredentialRequest := &identityv1.PreviewApiCredentialRequest{
		Context: req.GetContext(),
		ApiCredential: &identityv1.ApiCredential{
			Audit: &commonv1.AuditInfo{
				CreatedBy: req.GetFraudRule().GetAudit().GetCreatedBy(),
				UpdatedAt: req.GetFraudRule().GetAudit().GetUpdatedAt(),
			},
			Limits: []*identityv1.ApiCredentialLimit{&identityv1.ApiCredentialLimit{Amount: req.GetFraudRule().GetDiscount().GetAmount()}},
		},
	}
	previewApiCredentialReply, err := s.previewApiCredentialFn(ctx, previewApiCredentialRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.ApiCredentialValidationService/PreviewApiCredential")
	}
	return &fraudv1.ScheduleFraudRuleExpiryResponse{
		Outcome: &fraudv1.ScheduleFraudRuleExpiryResponse_Outcome{Status: previewApiCredentialReply.GetResult().GetStatus()},
		Result: &commonv1.OperationResult{
			Message: previewApiCredentialReply.GetOutcome().GetMessage(),
			Status:  "ok",
		},
		FraudRule: &fraudv1.FraudRule{
			FraudRuleId: req.GetFraudRuleId(),
			Status:      req.GetFraudRule().GetStatus(),
		},
	}, nil
}
