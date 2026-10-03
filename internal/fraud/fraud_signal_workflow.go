package fraud

import (
	"context"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	localizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/localization/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	telematicsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/telematics/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// FraudSignalWorkflowServer implements `acme.fraud.v1.FraudSignalWorkflowService`.
// Runs the long-lived workflow that takes a fraud signal from start to finish.
type FraudSignalWorkflowServer struct {
	fraudv1.UnimplementedFraudSignalWorkflowServiceServer
	store                         *Store
	log                           *slog.Logger
	telemetryReadingQueryClient   telematicsv1.TelemetryReadingQueryServiceClient
	fraudSignalQueryClient        fraudv1.FraudSignalQueryServiceClient
	fraudSignalLifecycleClient    fraudv1.FraudSignalLifecycleServiceClient
	fraudRuleLifecycleClient      fraudv1.FraudRuleLifecycleServiceClient
	userValidationClient          identityv1.UserValidationServiceClient
	translationKeyQueryClient     localizationv1.TranslationKeyQueryServiceClient
	conn                          *grpc.ClientConn
	watchlistValidationClient     fraudv1.WatchlistValidationServiceClient
	meteredRiskAssessmentWorkflow meteredRiskAssessmentWorkflow
}

// NewFraudSignalWorkflowServer wires a FraudSignalWorkflowServer.
func NewFraudSignalWorkflowServer(deps platform.Deps) *FraudSignalWorkflowServer {
	return &FraudSignalWorkflowServer{
		store:                         NewStore(deps.DB),
		log:                           deps.Logger.With("service", "acme.fraud.v1.FraudSignalWorkflowService"),
		telemetryReadingQueryClient:   telematicsv1.NewTelemetryReadingQueryServiceClient(deps.Conn),
		fraudSignalQueryClient:        fraudv1.NewFraudSignalQueryServiceClient(deps.Conn),
		fraudSignalLifecycleClient:    fraudv1.NewFraudSignalLifecycleServiceClient(deps.Conn),
		fraudRuleLifecycleClient:      fraudv1.NewFraudRuleLifecycleServiceClient(deps.Conn),
		userValidationClient:          identityv1.NewUserValidationServiceClient(deps.Conn),
		translationKeyQueryClient:     localizationv1.NewTranslationKeyQueryServiceClient(deps.Conn),
		conn:                          deps.Conn,
		watchlistValidationClient:     fraudv1.NewWatchlistValidationServiceClient(deps.Conn),
		meteredRiskAssessmentWorkflow: meteredRiskAssessmentWorkflow{RiskAssessmentWorkflowServiceClient: fraudv1.NewRiskAssessmentWorkflowServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
	}
}

// StartFraudSignalWorkflow starts a fraud signal.
func (s *FraudSignalWorkflowServer) StartFraudSignalWorkflow(ctx context.Context, req *fraudv1.StartFraudSignalWorkflowRequest) (*fraudv1.StartFraudSignalWorkflowResponse, error) {
	if err := s.store.SaveFraudSignal(ctx, req.GetContext().GetTenantId(), req.GetFraudSignal()); err != nil {
		return nil, grpcx.Wrap(err, "save fraud signal")
	}
	s.log.InfoContext(ctx, "start fraud signal workflow", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "fraud_rule_id", req.GetFraudSignal().GetSubject().GetFraudRuleId())
	var envelope fraudv1.RiskAssessmentRef
	if err := req.GetFraudSignal().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	countTelemetryReadingsRequest := &telematicsv1.CountTelemetryReadingsRequest{}
	countTelemetryReadingsRequest.Context = req.GetContext()
	countTelemetryReadingsRequest.Filter = &telematicsv1.CountTelemetryReadingsRequest_Filter{
		OwnerId:      tenancy.Value(ctx, "fraud_rule_id"),
		CreatedAfter: req.GetFraudSignal().GetOccurredAt(),
		Labels:       req.GetFraudSignal().GetAttributes(),
	}
	countTelemetryReadingsRequest.Page = &commonv1.PageRequest{PageToken: envelope.GetDisplayName()}
	countTelemetryReadingsReply, err := s.telemetryReadingQueryClient.CountTelemetryReadings(ctx, countTelemetryReadingsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.telematics.v1.TelemetryReadingQueryService/CountTelemetryReadings")
	}
	resolveFraudSignalsRequest := &fraudv1.ResolveFraudSignalsRequest{Context: req.GetContext()}
	var resolveFraudSignalsReply *fraudv1.ResolveFraudSignalsResponse
	if err := pipeline.Run(ctx, "acme.fraud.v1.FraudSignalQueryService/ResolveFraudSignals", func(ctx context.Context) error {
		answer, err := s.fraudSignalQueryClient.ResolveFraudSignals(ctx, resolveFraudSignalsRequest)
		resolveFraudSignalsReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.FraudSignalQueryService/ResolveFraudSignals")
	}
	return &fraudv1.StartFraudSignalWorkflowResponse{
		Outcome: &fraudv1.StartFraudSignalWorkflowResponse_Outcome{Status: countTelemetryReadingsReply.GetResult().GetStatus()},
		Result: &commonv1.OperationResult{
			Message: countTelemetryReadingsReply.GetResult().GetMessage(),
			Details: resolveFraudSignalsReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		FraudSignal: &fraudv1.FraudSignal{
			FraudSignalId: req.GetFraudSignalId(),
			Source:        req.GetFraudSignal().GetSource(),
		},
	}, nil
}

// AdvanceFraudSignalWorkflow advances a fraud signal.
func (s *FraudSignalWorkflowServer) AdvanceFraudSignalWorkflow(ctx context.Context, req *fraudv1.AdvanceFraudSignalWorkflowRequest) (*fraudv1.AdvanceFraudSignalWorkflowResponse, error) {
	if err := s.store.SaveFraudSignal(ctx, req.GetContext().GetTenantId(), req.GetFraudSignal()); err != nil {
		return nil, grpcx.Wrap(err, "save fraud signal")
	}
	s.log.InfoContext(ctx, "advance fraud signal workflow", "tenant", req.GetContext().GetTenantId())
	step := &fraudSignalWorkflowServerAdvanceFraudSignalWorkflowActivateFraudSignalStep{request: &fraudv1.ActivateFraudSignalRequest{
		Context:       req.GetContext(),
		FraudSignalId: req.GetFraudSignalId(),
		Details: &fraudv1.ActivateFraudSignalRequest_Details{
			NotifyContact: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetNotify().GetPreferredChannel(),
			},
		},
	}}
	if err := s.runAdvanceFraudSignalWorkflowActivateFraudSignal(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.FraudSignalLifecycleService/ActivateFraudSignal")
	}
	activateFraudSignalReply := step.reply
	return &fraudv1.AdvanceFraudSignalWorkflowResponse{
		FraudSignal: &fraudv1.FraudSignal{
			Sequence:      activateFraudSignalReply.GetFraudSignal().GetSequence(),
			FraudSignalId: req.GetFraudSignalId(),
			Attributes:    req.GetFraudSignal().GetAttributes(),
		},
		Result: &commonv1.OperationResult{Status: activateFraudSignalReply.GetResult().GetStatus()},
	}, nil
}

// CancelFraudSignalWorkflow cancels a fraud signal.
func (s *FraudSignalWorkflowServer) CancelFraudSignalWorkflow(ctx context.Context, req *fraudv1.CancelFraudSignalWorkflowRequest) (*fraudv1.CancelFraudSignalWorkflowResponse, error) {
	if err := s.store.SaveFraudSignal(ctx, req.GetContext().GetTenantId(), req.GetFraudSignal()); err != nil {
		return nil, grpcx.Wrap(err, "save fraud signal")
	}
	s.log.InfoContext(ctx, "cancel fraud signal workflow", "tenant", req.GetContext().GetTenantId())
	var holder struct{ FraudRuleId string }
	if record := req.GetFraudSignal().GetSubject(); record != nil {
		if err := mapper.CopyFields(&holder, record, "FraudRuleId"); err != nil {
			s.log.WarnContext(ctx, "copy fraud_rule_id", "error", err)
		}
	}
	cloned := proto.Clone(req.GetDetails()).(*fraudv1.CancelFraudSignalWorkflowRequest_Details)
	cloned2 := proto.Clone(req.GetDetails()).(*fraudv1.CancelFraudSignalWorkflowRequest_Details)
	step := &fraudSignalWorkflowServerCancelFraudSignalWorkflowReinstateFraudRuleStep{request: &fraudv1.ReinstateFraudRuleRequest{
		Context:     req.GetContext(),
		FraudRuleId: holder.FraudRuleId,
		Details: &fraudv1.ReinstateFraudRuleRequest_Details{
			NewOwner: &identityv1.UserRef{Email: req.GetDetails().GetNotify().GetEmail()},
			NotifyContact: &commonv1.ContactInfo{
				Phone:            cloned.GetNotify().GetPhone(),
				PreferredChannel: req.GetDetails().GetNotify().GetPreferredChannel(),
				Name:             cloned2.GetNotify().GetName(),
			},
		},
	}}
	if err := s.runCancelFraudSignalWorkflowReinstateFraudRule(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.FraudRuleLifecycleService/ReinstateFraudRule")
	}
	reinstateFraudRuleReply := step.reply
	var envelope fraudv1.RiskAssessmentRef
	if err := req.GetFraudSignal().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	lintUsersRequest := &identityv1.LintUsersRequest{}
	lintUsersRequest.Context = req.GetContext()
	lintUsersRequest.User = &identityv1.User{
		Audit: &commonv1.AuditInfo{
			UpdatedBy: reinstateFraudRuleReply.GetFraudRule().GetAudit().GetUpdatedBy(),
		},
		Contact: &commonv1.ContactInfo{Phone: req.GetDetails().GetNotify().GetPhone()},
	}
	lintUsersRequest.UserId = envelope.GetDisplayName()
	lintUsersReply, err := s.userValidationClient.LintUsers(ctx, lintUsersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserValidationService/LintUsers")
	}
	return &fraudv1.CancelFraudSignalWorkflowResponse{
		FraudSignal: &fraudv1.FraudSignal{
			Subject: &fraudv1.FraudRuleRef{
				DisplayName: reinstateFraudRuleReply.GetFraudRule().GetOwner().GetDisplayName(),
			},
			FraudSignalId: req.GetFraudSignalId(),
			Attributes:    req.GetFraudSignal().GetAttributes(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: reinstateFraudRuleReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Outcome: &fraudv1.CancelFraudSignalWorkflowResponse_Outcome{
			ReferenceId: lintUsersReply.GetOutcome().GetReferenceId(),
			Message:     lintUsersReply.GetResult().GetMessage(),
		},
	}, nil
}

// PauseFraudSignalWorkflow pauses a fraud signal.
func (s *FraudSignalWorkflowServer) PauseFraudSignalWorkflow(ctx context.Context, req *fraudv1.PauseFraudSignalWorkflowRequest) (*fraudv1.PauseFraudSignalWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveFraudSignal(ctx, req.GetContext().GetTenantId(), req.GetFraudSignal()); err != nil {
		return nil, grpcx.Wrap(err, "save fraud signal")
	}
	s.log.InfoContext(ctx, "pause fraud signal workflow", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetFraudSignal()).(*fraudv1.FraudSignal)
	var envelope fraudv1.RiskAssessmentRef
	if err := req.GetFraudSignal().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	countTranslationKeysReply, err := s.translationKeyQueryClient.CountTranslationKeys(ctx, &localizationv1.CountTranslationKeysRequest{
		Context: req.GetContext(),
		Filter:  &localizationv1.CountTranslationKeysRequest_Filter{CreatedAfter: cloned.GetOccurredAt()},
		Page:    &commonv1.PageRequest{PageToken: envelope.GetDisplayName()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.localization.v1.TranslationKeyQueryService/CountTranslationKeys")
	}
	var envelope2 fraudv1.RiskAssessmentRef
	if err := req.GetFraudSignal().GetDetail().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	commentSellerListingApprovalRequest := &sellersv1.CommentSellerListingApprovalRequest{
		Context: req.GetContext(),
		SellerListing: &sellersv1.SellerListing{
			Owner: &identityv1.UserRef{
				DisplayName: req.GetFraudSignal().GetSubject().GetDisplayName(),
			},
		},
		SellerListingId: envelope2.GetDisplayName(),
	}
	commentSellerListingApprovalReply, err := commentSellerListingApprovalCall(ctx, commentSellerListingApprovalRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingApprovalService/CommentSellerListingApproval")
	}
	return &fraudv1.PauseFraudSignalWorkflowResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: countTranslationKeysReply.GetResult().GetReferenceIds(),
			Message:      countTranslationKeysReply.GetResult().GetMessage(),
			Status:       "ok",
		},
		Outcome: &fraudv1.PauseFraudSignalWorkflowResponse_Outcome{
			Status: commentSellerListingApprovalReply.GetOutcome().GetStatus(),
		},
		FraudSignal: &fraudv1.FraudSignal{
			FraudSignalId: req.GetFraudSignalId(),
			OccurredAt:    req.GetFraudSignal().GetOccurredAt(),
		},
	}, nil
}

// ResumeFraudSignalWorkflow resumes a fraud signal.
func (s *FraudSignalWorkflowServer) ResumeFraudSignalWorkflow(ctx context.Context, req *fraudv1.ResumeFraudSignalWorkflowRequest) (*fraudv1.ResumeFraudSignalWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveFraudSignal(ctx, req.GetContext().GetTenantId(), req.GetFraudSignal()); err != nil {
		return nil, grpcx.Wrap(err, "save fraud signal")
	}
	s.log.InfoContext(ctx, "resume fraud signal workflow", "tenant", req.GetContext().GetTenantId())
	lookupFraudSignalRequest := &fraudv1.LookupFraudSignalRequest{
		Context:       req.GetContext(),
		FraudSignalId: req.GetFraudSignal().GetFraudSignalId(),
	}
	lookupFraudSignalReply := new(fraudv1.LookupFraudSignalResponse)
	if err := grpcx.Invoke(ctx, s.conn, fraudv1.FraudSignalQueryService_LookupFraudSignal_FullMethodName, lookupFraudSignalRequest, lookupFraudSignalReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.FraudSignalQueryService/LookupFraudSignal")
	}
	var envelope fraudv1.RiskAssessmentRef
	if err := req.GetFraudSignal().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	estimateFraudRuleRequest := &fraudv1.EstimateFraudRuleRequest{
		Context: req.GetContext(),
		FraudRule: &fraudv1.FraudRule{
			Owner: &identityv1.UserRef{
				DisplayName: req.GetFraudSignal().GetSubject().GetDisplayName(),
			},
		},
		FraudRuleId: envelope.GetDisplayName(),
	}
	estimateFraudRuleReply, err := estimateFraudRuleCall(ctx, estimateFraudRuleRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.FraudRuleCalculationService/EstimateFraudRule")
	}
	var envelope2 fraudv1.RiskAssessmentRef
	if err := req.GetFraudSignal().GetDetail().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	previewWatchlistReply, err := s.watchlistValidationClient.PreviewWatchlist(ctx, &fraudv1.PreviewWatchlistRequest{
		Context: req.GetContext(),
		Watchlist: &fraudv1.Watchlist{
			Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
				Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
					Audit: &commonv1.AuditInfo{
						CreatedAt: estimateFraudRuleReply.GetFraudRule().GetAudit().GetCreatedAt(),
					},
				}},
			}},
		},
		WatchlistId: envelope2.GetDisplayName(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.WatchlistValidationService/PreviewWatchlist")
	}
	return &fraudv1.ResumeFraudSignalWorkflowResponse{
		FraudSignal: &fraudv1.FraudSignal{
			CorrelationId: lookupFraudSignalReply.GetFraudSignal().GetCorrelationId(),
			Source:        lookupFraudSignalReply.GetFraudSignal().GetSource(),
			FraudSignalId: req.GetFraudSignalId(),
			Sequence:      req.GetFraudSignal().GetSequence(),
		},
		Outcome: &fraudv1.ResumeFraudSignalWorkflowResponse_Outcome{
			Message:   estimateFraudRuleReply.GetResult().GetMessage(),
			NextSteps: previewWatchlistReply.GetOutcome().GetNextSteps(),
		},
		Result: &commonv1.OperationResult{
			Message: previewWatchlistReply.GetResult().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// InspectFraudSignalWorkflow returns a fraud signal.
func (s *FraudSignalWorkflowServer) InspectFraudSignalWorkflow(ctx context.Context, req *fraudv1.InspectFraudSignalWorkflowRequest) (*fraudv1.InspectFraudSignalWorkflowResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "fraud_signal", req.GetFraudSignalId(), "inspect_fraud_signal_workflow"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "inspect fraud signal workflow", "tenant", req.GetContext().GetTenantId())
	pauseRiskAssessmentWorkflowRequest := &fraudv1.PauseRiskAssessmentWorkflowRequest{
		Context: req.GetContext(),
		RiskAssessment: &fraudv1.RiskAssessment{
			Requester: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Assignee:  &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
			Origin:    &fraudv1.RiskAssessment_ExternalReference{ExternalReference: req.GetFraudSignalId()},
		},
	}
	pauseRiskAssessmentWorkflowReply, err := s.meteredRiskAssessmentWorkflow.PauseRiskAssessmentWorkflow(ctx, pauseRiskAssessmentWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.RiskAssessmentWorkflowService/PauseRiskAssessmentWorkflow")
	}
	return &fraudv1.InspectFraudSignalWorkflowResponse{
		Result: &commonv1.OperationResult{
			Status: pauseRiskAssessmentWorkflowReply.GetResult().GetStatus(),
		},
		FraudSignal: &fraudv1.FraudSignal{
			Subject: &fraudv1.FraudRuleRef{
				DisplayName: pauseRiskAssessmentWorkflowReply.GetRiskAssessment().GetCustomer().GetDisplayName(),
			},
			FraudSignalId: req.GetFraudSignalId(),
		},
	}, nil
}

// ListFraudSignalWorkflows lists fraud signals a page at a time.
func (s *FraudSignalWorkflowServer) ListFraudSignalWorkflows(ctx context.Context, req *fraudv1.ListFraudSignalWorkflowsRequest) (*fraudv1.ListFraudSignalWorkflowsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "list fraud signal workflows", "tenant", req.GetContext().GetTenantId())
	scheduleWatchlistExpiryRequest := &fraudv1.ScheduleWatchlistExpiryRequest{
		Context: req.GetContext(),
		Watchlist: &fraudv1.Watchlist{
			Location: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
	}
	scheduleWatchlistExpiryReply, err := grpcx.NewClient(s.conn, fraudv1.NewWatchlistLifecycleServiceClient).ScheduleWatchlistExpiry(ctx, scheduleWatchlistExpiryRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.WatchlistLifecycleService/ScheduleWatchlistExpiry")
	}
	return &fraudv1.ListFraudSignalWorkflowsResponse{
		Result: &commonv1.OperationResult{
			Status: scheduleWatchlistExpiryReply.GetResult().GetStatus(),
		},
	}, nil
}

// SignalFraudSignalWorkflow signals a fraud signal.
func (s *FraudSignalWorkflowServer) SignalFraudSignalWorkflow(ctx context.Context, req *fraudv1.SignalFraudSignalWorkflowRequest) (*fraudv1.SignalFraudSignalWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveFraudSignal(ctx, req.GetContext().GetTenantId(), req.GetFraudSignal()); err != nil {
		return nil, grpcx.Wrap(err, "save fraud signal")
	}
	s.log.InfoContext(ctx, "signal fraud signal workflow", "tenant", req.GetContext().GetTenantId())
	expireFraudRuleRequest := &fraudv1.ExpireFraudRuleRequest{}
	expireFraudRuleRequest.Context = req.GetContext()
	expireFraudRuleRequest.FraudRuleId = req.GetFraudSignal().GetSubject().GetFraudRuleId()
	expireFraudRuleRequest.Details = &fraudv1.ExpireFraudRuleRequest_Details{
		NotifyContact: &commonv1.ContactInfo{Name: req.GetDetails().GetNotify().GetName()},
	}
	expireFraudRuleReply, err := s.fraudRuleLifecycleClient.ExpireFraudRule(ctx, expireFraudRuleRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.FraudRuleLifecycleService/ExpireFraudRule")
	}
	var envelope fraudv1.RiskAssessmentRef
	if err := req.GetFraudSignal().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	listPendingFraudSignalsRequest := &fraudv1.ListPendingFraudSignalsRequest{
		Context: req.GetContext(),
		Filter: &fraudv1.ListPendingFraudSignalsRequest_Filter{
			CreatedAfter: expireFraudRuleReply.GetFraudRule().GetAudit().GetCreatedAt(),
		},
		Page: &commonv1.PageRequest{PageToken: envelope.GetDisplayName()},
	}
	listPendingFraudSignalsReply := new(fraudv1.ListPendingFraudSignalsResponse)
	if err := grpcx.Invoke(ctx, s.conn, fraudv1.FraudSignalApprovalService_ListPendingFraudSignals_FullMethodName, listPendingFraudSignalsRequest, listPendingFraudSignalsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.FraudSignalApprovalService/ListPendingFraudSignals")
	}
	return &fraudv1.SignalFraudSignalWorkflowResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds:  expireFraudRuleReply.GetResult().GetReferenceIds(),
			CorrelationId: listPendingFraudSignalsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		FraudSignal: &fraudv1.FraudSignal{
			CorrelationId: listPendingFraudSignalsReply.GetResult().GetCorrelationId(),
			FraudSignalId: req.GetFraudSignalId(),
			Sequence:      req.GetFraudSignal().GetSequence(),
		},
	}, nil
}

// fraudSignalWorkflowServerAdvanceFraudSignalWorkflowActivateFraudSignalStep carries the request of one step of
// AdvanceFraudSignalWorkflow and, once it ran, its reply.
type fraudSignalWorkflowServerAdvanceFraudSignalWorkflowActivateFraudSignalStep struct {
	request *fraudv1.ActivateFraudSignalRequest
	reply   *fraudv1.ActivateFraudSignalResponse
}

func (s *FraudSignalWorkflowServer) runAdvanceFraudSignalWorkflowActivateFraudSignal(ctx context.Context, step *fraudSignalWorkflowServerAdvanceFraudSignalWorkflowActivateFraudSignalStep) error {
	reply, err := s.fraudSignalLifecycleClient.ActivateFraudSignal(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}

// fraudSignalWorkflowServerCancelFraudSignalWorkflowReinstateFraudRuleStep carries the request of one step of
// CancelFraudSignalWorkflow and, once it ran, its reply.
type fraudSignalWorkflowServerCancelFraudSignalWorkflowReinstateFraudRuleStep struct {
	request *fraudv1.ReinstateFraudRuleRequest
	reply   *fraudv1.ReinstateFraudRuleResponse
}

func (s *FraudSignalWorkflowServer) runCancelFraudSignalWorkflowReinstateFraudRule(ctx context.Context, step *fraudSignalWorkflowServerCancelFraudSignalWorkflowReinstateFraudRuleStep) error {
	reply, err := s.fraudRuleLifecycleClient.ReinstateFraudRule(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
