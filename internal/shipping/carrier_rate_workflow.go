package shipping

import (
	"context"
	"errors"
	"io"
	"log/slog"

	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	fuelv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fuel/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	surveysv1 "github.com/hounddogai/protomap-test-golang/gen/acme/surveys/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// CarrierRateWorkflowServer implements `acme.shipping.v1.CarrierRateWorkflowService`.
// Runs the long-lived workflow that takes a carrier rate from start to finish.
type CarrierRateWorkflowServer struct {
	shippingv1.UnimplementedCarrierRateWorkflowServiceServer
	store                            *Store
	log                              *slog.Logger
	trackingEventLifecycleClient     shippingv1.TrackingEventLifecycleServiceClient
	restockDecisionApprovalClient    returnsv1.RestockDecisionApprovalServiceClient
	carrierRateLifecycleClient       shippingv1.CarrierRateLifecycleServiceClient
	customerSegmentCalculationClient customersv1.CustomerSegmentCalculationServiceClient
	accessPolicyClient               authorizationv1.AccessPolicyServiceClient
	sessionValidationClient          identityv1.SessionValidationServiceClient
	restockDecisionReportingClient   returnsv1.RestockDecisionReportingServiceClient
	fuelCardClient                   fuelv1.FuelCardServiceClient
	clients                          *registry.Registry
	surveyClient                     surveysv1.SurveyServiceClient
	ledgerAccountValidationClient    ledgerv1.LedgerAccountValidationServiceClient
}

// NewCarrierRateWorkflowServer wires a CarrierRateWorkflowServer.
func NewCarrierRateWorkflowServer(deps platform.Deps) *CarrierRateWorkflowServer {
	return &CarrierRateWorkflowServer{
		store:                            NewStore(deps.DB),
		log:                              deps.Logger.With("service", "acme.shipping.v1.CarrierRateWorkflowService"),
		trackingEventLifecycleClient:     shippingv1.NewTrackingEventLifecycleServiceClient(deps.Conn),
		restockDecisionApprovalClient:    returnsv1.NewRestockDecisionApprovalServiceClient(deps.Conn),
		carrierRateLifecycleClient:       shippingv1.NewCarrierRateLifecycleServiceClient(deps.Conn),
		customerSegmentCalculationClient: customersv1.NewCustomerSegmentCalculationServiceClient(deps.Conn),
		accessPolicyClient:               authorizationv1.NewAccessPolicyServiceClient(deps.Conn),
		sessionValidationClient:          identityv1.NewSessionValidationServiceClient(deps.Conn),
		restockDecisionReportingClient:   returnsv1.NewRestockDecisionReportingServiceClient(deps.Conn),
		fuelCardClient:                   fuelv1.NewFuelCardServiceClient(deps.Conn),
		clients:                          deps.Clients,
		surveyClient:                     surveysv1.NewSurveyServiceClient(deps.Conn),
		ledgerAccountValidationClient:    ledgerv1.NewLedgerAccountValidationServiceClient(deps.Conn),
	}
}

// StartCarrierRateWorkflow starts a carrier rate.
func (s *CarrierRateWorkflowServer) StartCarrierRateWorkflow(ctx context.Context, req *shippingv1.StartCarrierRateWorkflowRequest) (*shippingv1.StartCarrierRateWorkflowResponse, error) {
	if err := s.store.SaveCarrierRate(ctx, req.GetContext().GetTenantId(), req.GetCarrierRate()); err != nil {
		return nil, grpcx.Wrap(err, "save carrier rate")
	}
	s.log.InfoContext(ctx, "start carrier rate workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCarrierRate().GetAudit().GetEtag())
	scheduleTrackingEventExpiryRequest := buildScheduleTrackingEventExpiryRequestForStartCarrierRateWorkflow(req)
	scheduleTrackingEventExpiryReply, err := s.trackingEventLifecycleClient.ScheduleTrackingEventExpiry(ctx, scheduleTrackingEventExpiryRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.TrackingEventLifecycleService/ScheduleTrackingEventExpiry")
	}
	return &shippingv1.StartCarrierRateWorkflowResponse{
		Result: &commonv1.OperationResult{
			Message: scheduleTrackingEventExpiryReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		CarrierRate: &shippingv1.CarrierRate{
			CarrierRateId: req.GetCarrierRateId(),
			Owner:         req.GetCarrierRate().GetOwner(),
		},
	}, nil
}

// AdvanceCarrierRateWorkflow advances a carrier rate.
func (s *CarrierRateWorkflowServer) AdvanceCarrierRateWorkflow(ctx context.Context, req *shippingv1.AdvanceCarrierRateWorkflowRequest) (*shippingv1.AdvanceCarrierRateWorkflowResponse, error) {
	if err := s.store.SaveCarrierRate(ctx, req.GetContext().GetTenantId(), req.GetCarrierRate()); err != nil {
		return nil, grpcx.Wrap(err, "save carrier rate")
	}
	s.log.InfoContext(ctx, "advance carrier rate workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCarrierRate().GetAudit().GetEtag())
	encoded, err := protojson.Marshal(req.GetCarrierRate())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded shippingv1.CarrierRate
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	encoded2, err := protojson.Marshal(req.GetCarrierRate())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded2 shippingv1.CarrierRate
	if err := protojson.Unmarshal(encoded2, &decoded2); err != nil {
		return nil, err
	}
	delegateRestockDecisionApprovalRequest := newDelegateRestockDecisionApprovalRequest(withDelegateRestockDecisionApprovalContext(req.GetContext()), withDelegateRestockDecisionApprovalRestockDecision(&returnsv1.RestockDecision{
		Audit: &commonv1.AuditInfo{
			Etag:      req.GetCarrierRate().GetAudit().GetEtag(),
			CreatedBy: (&decoded2).GetAudit().GetCreatedBy(),
		},
	}), withDelegateRestockDecisionApprovalDetails(&returnsv1.DelegateRestockDecisionApprovalRequest_Details{
		Approver: &identityv1.UserRef{UserId: (&decoded).GetOwner().GetUserId()},
	}))
	delegateRestockDecisionApprovalReply, err := s.restockDecisionApprovalClient.DelegateRestockDecisionApproval(ctx, delegateRestockDecisionApprovalRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.RestockDecisionApprovalService/DelegateRestockDecisionApproval")
	}
	carrierRateCache := cache.New[string, *shippingv1.CarrierRate](1)
	carrierRateCache.Put(req.GetContext().GetTraceId(), req.GetCarrierRate())
	cachedCarrierRate, _ := carrierRateCache.Get(req.GetContext().GetTraceId())
	expireCarrierRateRequest := &shippingv1.ExpireCarrierRateRequest{
		Context:       req.GetContext(),
		CarrierRateId: cachedCarrierRate.GetCarrierRateId(),
		Details: &shippingv1.ExpireCarrierRateRequest_Details{
			NewOwner: &identityv1.UserRef{
				DisplayName: delegateRestockDecisionApprovalReply.GetRestockDecision().GetOwner().GetDisplayName(),
				UserId:      req.GetCarrierRate().GetOwner().GetUserId(),
			},
			NotifyContact: &commonv1.ContactInfo{
				Email: delegateRestockDecisionApprovalReply.GetRestockDecision().GetOwner().GetEmail(),
			},
		},
	}
	expireCarrierRateReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*shippingv1.ExpireCarrierRateResponse, error) {
		return s.carrierRateLifecycleClient.ExpireCarrierRate(ctx, expireCarrierRateRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.CarrierRateLifecycleService/ExpireCarrierRate")
	}
	return &shippingv1.AdvanceCarrierRateWorkflowResponse{
		CarrierRate: &shippingv1.CarrierRate{
			Priority:      delegateRestockDecisionApprovalReply.GetRestockDecision().GetPriority(),
			Status:        expireCarrierRateReply.GetCarrierRate().GetStatus(),
			Owner:         expireCarrierRateReply.GetCarrierRate().GetOwner(),
			CarrierRateId: req.GetCarrierRateId(),
			Parameters:    req.GetCarrierRate().GetParameters(),
		},
		Outcome: &shippingv1.AdvanceCarrierRateWorkflowResponse_Outcome{
			EffectiveAt: delegateRestockDecisionApprovalReply.GetOutcome().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RetryCarrierRateWorkflow retrys a carrier rate.
func (s *CarrierRateWorkflowServer) RetryCarrierRateWorkflow(ctx context.Context, req *shippingv1.RetryCarrierRateWorkflowRequest) (*shippingv1.RetryCarrierRateWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RetryCarrierRateWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SaveCarrierRate(ctx, req.GetContext().GetTenantId(), req.GetCarrierRate()); err != nil {
		return nil, grpcx.Wrap(err, "save carrier rate")
	}
	s.log.InfoContext(ctx, "retry carrier rate workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCarrierRate().GetAudit().GetEtag())
	for _, condition := range req.GetCarrierRate().GetConditions() {
		if _, err := s.customerSegmentCalculationClient.CalculateCustomerSegment(ctx, &customersv1.CalculateCustomerSegmentRequest{
			Context: req.GetContext(),
			CustomerSegment: &customersv1.CustomerSegment{
				Conditions: []*customersv1.CustomerSegmentCondition{&customersv1.CustomerSegmentCondition{
					Operator:  condition.GetOperator(),
					Attribute: condition.GetAttribute(),
					Negate:    condition.GetNegate(),
					Value:     condition.GetValue(),
				}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentCalculationService/CalculateCustomerSegment")
		}
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*authorizationv1.UpdateAccessPolicyResponse, len(req.GetCarrierRate().GetConditions()))
	for i, condition2 := range req.GetCarrierRate().GetConditions() {
		group.Go(func() error {
			reply, err := s.accessPolicyClient.UpdateAccessPolicy(groupCtx, &authorizationv1.UpdateAccessPolicyRequest{
				Context: req.GetContext(),
				AccessPolicy: &authorizationv1.AccessPolicy{
					Conditions: []*authorizationv1.AccessPolicyCondition{&authorizationv1.AccessPolicyCondition{
						Value:     condition2.GetValue(),
						Operator:  condition2.GetOperator(),
						Negate:    condition2.GetNegate(),
						Attribute: condition2.GetAttribute(),
					}},
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
		return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyService/UpdateAccessPolicy")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	return &shippingv1.RetryCarrierRateWorkflowResponse{
		CarrierRate: &shippingv1.CarrierRate{
			CarrierRateId: req.GetCarrierRateId(),
			Version:       req.GetCarrierRate().GetVersion(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ResumeCarrierRateWorkflow resumes a carrier rate.
func (s *CarrierRateWorkflowServer) ResumeCarrierRateWorkflow(ctx context.Context, req *shippingv1.ResumeCarrierRateWorkflowRequest) (*shippingv1.ResumeCarrierRateWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ResumeCarrierRateWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SaveCarrierRate(ctx, req.GetContext().GetTenantId(), req.GetCarrierRate()); err != nil {
		return nil, grpcx.Wrap(err, "save carrier rate")
	}
	s.log.InfoContext(ctx, "resume carrier rate workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCarrierRate().GetAudit().GetEtag())
	suspendCarrierRateRequest := buildSuspendCarrierRateRequestForResumeCarrierRateWorkflow(req)
	suspendCarrierRateReply, err := s.carrierRateLifecycleClient.SuspendCarrierRate(ctx, suspendCarrierRateRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.CarrierRateLifecycleService/SuspendCarrierRate")
	}
	previewSessionRequest := &identityv1.PreviewSessionRequest{
		Context: req.GetContext(),
		Session: &identityv1.Session{
			Requester: &commonv1.ContactInfo{Phone: req.GetDetails().GetNotify().GetPhone()},
		},
	}
	previewSessionReply, err := grpcx.Call(ctx, s.sessionValidationClient.PreviewSession, previewSessionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionValidationService/PreviewSession")
	}
	streamRestockDecisionMetricsStream, err := s.restockDecisionReportingClient.StreamRestockDecisionMetrics(ctx, &returnsv1.StreamRestockDecisionMetricsRequest{
		Context: req.GetContext(),
		Subscriber: &commonv1.ContactInfo{
			Phone: previewSessionReply.GetSession().GetRequester().GetPhone(),
			Email: req.GetDetails().GetNotify().GetEmail(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.RestockDecisionReportingService/StreamRestockDecisionMetrics")
	}
	for {
		received, err := streamRestockDecisionMetricsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.returns.v1.RestockDecisionReportingService/StreamRestockDecisionMetrics")
		}
		_ = received
	}
	return &shippingv1.ResumeCarrierRateWorkflowResponse{
		CarrierRate: &shippingv1.CarrierRate{
			Priority: suspendCarrierRateReply.GetCarrierRate().GetPriority(),
			Audit: &commonv1.AuditInfo{
				CreatedBy: suspendCarrierRateReply.GetCarrierRate().GetAudit().GetCreatedBy(),
				UpdatedBy: previewSessionReply.GetSession().GetAudit().GetUpdatedBy(),
			},
			CarrierRateId: req.GetCarrierRateId(),
			Parameters:    req.GetDetails().GetParameters(),
		},
		Outcome: &shippingv1.ResumeCarrierRateWorkflowResponse_Outcome{
			EffectiveAt: previewSessionReply.GetOutcome().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// InspectCarrierRateWorkflow returns a carrier rate.
func (s *CarrierRateWorkflowServer) InspectCarrierRateWorkflow(ctx context.Context, req *shippingv1.InspectCarrierRateWorkflowRequest) (*shippingv1.InspectCarrierRateWorkflowResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "carrier_rate", req.GetCarrierRateId(), "inspect_carrier_rate_workflow"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "inspect carrier rate workflow", "tenant", req.GetContext().GetTenantId())
	watchFuelCardsStream, err := s.fuelCardClient.WatchFuelCards(ctx, &fuelv1.WatchFuelCardsRequest{
		Context:    req.GetContext(),
		Subscriber: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		Filter: &fuelv1.WatchFuelCardsRequest_Filter{
			CreatedAfter: req.GetContext().GetDeadline(),
			OwnerId:      req.GetCarrierRateId(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fuel.v1.FuelCardService/WatchFuelCards")
	}
	for {
		received, err := watchFuelCardsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.fuel.v1.FuelCardService/WatchFuelCards")
		}
		_ = received
	}
	return &shippingv1.InspectCarrierRateWorkflowResponse{
		CarrierRate: &shippingv1.CarrierRate{CarrierRateId: req.GetCarrierRateId()},
		Result:      &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ListCarrierRateWorkflows lists carrier rates a page at a time.
func (s *CarrierRateWorkflowServer) ListCarrierRateWorkflows(ctx context.Context, req *shippingv1.ListCarrierRateWorkflowsRequest) (*shippingv1.ListCarrierRateWorkflowsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "list carrier rate workflows", "tenant", req.GetContext().GetTenantId())
	ledgerAccountQuery, ok := s.clients.Get("acme.ledger.v1.LedgerAccountQueryService").(ledgerv1.LedgerAccountQueryServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.ledger.v1.LedgerAccountQueryService")
	}
	ctx = tenancy.With(ctx, "query", req.GetFilter().GetQuery())
	cloned := proto.Clone(req.GetFilter()).(*shippingv1.ListCarrierRateWorkflowsRequest_Filter)
	countLedgerAccountsRequest := &ledgerv1.CountLedgerAccountsRequest{
		Context: req.GetContext(),
		Page: &commonv1.PageRequest{
			PageToken: req.GetPage().GetPageToken(),
			PageSize:  req.GetPage().GetPageSize(),
		},
		Filter: &ledgerv1.CountLedgerAccountsRequest_Filter{
			Query:  tenancy.Value(ctx, "query"),
			Region: cloned.GetRegion(),
		},
	}
	countLedgerAccountsReply, err := ledgerAccountQuery.CountLedgerAccounts(ctx, countLedgerAccountsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountQueryService/CountLedgerAccounts")
	}
	return &shippingv1.ListCarrierRateWorkflowsResponse{
		Result: &commonv1.OperationResult{
			Details: countLedgerAccountsReply.GetResult().GetDetails(),
			Message: countLedgerAccountsReply.GetResult().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// SignalCarrierRateWorkflow signals a carrier rate.
func (s *CarrierRateWorkflowServer) SignalCarrierRateWorkflow(ctx context.Context, req *shippingv1.SignalCarrierRateWorkflowRequest) (*shippingv1.SignalCarrierRateWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveCarrierRate(ctx, req.GetContext().GetTenantId(), req.GetCarrierRate()); err != nil {
		return nil, grpcx.Wrap(err, "save carrier rate")
	}
	s.log.InfoContext(ctx, "signal carrier rate workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCarrierRate().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "preferred_channel", req.GetDetails().GetNotify().GetPreferredChannel())
	watchSurveysStream, err := s.surveyClient.WatchSurveys(ctx, &surveysv1.WatchSurveysRequest{
		Context: req.GetContext(),
		Subscriber: &commonv1.ContactInfo{
			PreferredChannel: tenancy.Value(ctx, "preferred_channel"),
			Phone:            req.GetDetails().GetNotify().GetPhone(),
			Name:             req.GetDetails().GetNotify().GetName(),
			Email:            req.GetCarrierRate().GetBlock().GetNotifyEmail(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.surveys.v1.SurveyService/WatchSurveys")
	}
	for {
		received, err := watchSurveysStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.surveys.v1.SurveyService/WatchSurveys")
		}
		_ = received
	}
	ctx = tenancy.With(ctx, "email", req.GetDetails().GetNotify().GetEmail())
	scoreLedgerAccountRequest := &ledgerv1.ScoreLedgerAccountRequest{
		Context: req.GetContext(),
		LedgerAccount: &ledgerv1.LedgerAccount{
			Limits: []*ledgerv1.LedgerAccountLimit{&ledgerv1.LedgerAccountLimit{Amount: req.GetCarrierRate().GetDiscount().GetAmount()}},
			Contact: &commonv1.ContactInfo{
				Name:  req.GetDetails().GetNotify().GetName(),
				Email: tenancy.Value(ctx, "email"),
			},
			Audit: &commonv1.AuditInfo{UpdatedAt: req.GetCarrierRate().GetAudit().GetUpdatedAt()},
		},
	}
	var scoreLedgerAccountReply *ledgerv1.ScoreLedgerAccountResponse
	if err := pipeline.Run(ctx, "acme.ledger.v1.LedgerAccountValidationService/ScoreLedgerAccount", func(ctx context.Context) error {
		answer, err := s.ledgerAccountValidationClient.ScoreLedgerAccount(ctx, scoreLedgerAccountRequest)
		scoreLedgerAccountReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountValidationService/ScoreLedgerAccount")
	}
	return &shippingv1.SignalCarrierRateWorkflowResponse{
		CarrierRate: &shippingv1.CarrierRate{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: scoreLedgerAccountReply.GetLedgerAccount().GetAudit().GetUpdatedBy(),
			},
			CarrierRateId: req.GetCarrierRateId(),
			Priority:      req.GetCarrierRate().GetPriority(),
		},
		Outcome: &shippingv1.SignalCarrierRateWorkflowResponse_Outcome{Message: scoreLedgerAccountReply.GetResult().GetMessage()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}
