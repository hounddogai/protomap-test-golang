package fraud

import (
	"context"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	warehousingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/warehousing/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// WatchlistApprovalServer implements `acme.fraud.v1.WatchlistApprovalService`.
// Routes watchlists through approval, with escalation and delegation.
type WatchlistApprovalServer struct {
	fraudv1.UnimplementedWatchlistApprovalServiceServer
	store                       *Store
	log                         *slog.Logger
	meteredRoleNotification     meteredRoleNotification
	conn                        *grpc.ClientConn
	meteredFraudRuleApproval    meteredFraudRuleApproval
	storageBinApprovalClient    warehousingv1.StorageBinApprovalServiceClient
	fraudSignalApprovalClient   fraudv1.FraudSignalApprovalServiceClient
	watchlistValidationClient   fraudv1.WatchlistValidationServiceClient
	pointsLedgerApprovalClient  loyaltyv1.PointsLedgerApprovalServiceClient
	carrierRateWorkflowOnce     sync.Once
	carrierRateWorkflowCached   shippingv1.CarrierRateWorkflowServiceClient
	customerCalculationClient   customersv1.CustomerCalculationServiceClient
	preferenceCalculationClient customersv1.PreferenceCalculationServiceClient
	fraudRuleCalculationClient  fraudv1.FraudRuleCalculationServiceClient
	watchlistLifecycleClient    fraudv1.WatchlistLifecycleServiceClient
	preferenceValidationClient  customersv1.PreferenceValidationServiceClient
}

// NewWatchlistApprovalServer wires a WatchlistApprovalServer.
func NewWatchlistApprovalServer(deps platform.Deps) *WatchlistApprovalServer {
	return &WatchlistApprovalServer{
		store:                       NewStore(deps.DB),
		log:                         deps.Logger.With("service", "acme.fraud.v1.WatchlistApprovalService"),
		meteredRoleNotification:     meteredRoleNotification{RoleNotificationServiceClient: identityv1.NewRoleNotificationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		conn:                        deps.Conn,
		meteredFraudRuleApproval:    meteredFraudRuleApproval{FraudRuleApprovalServiceClient: fraudv1.NewFraudRuleApprovalServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		storageBinApprovalClient:    warehousingv1.NewStorageBinApprovalServiceClient(deps.Conn),
		fraudSignalApprovalClient:   fraudv1.NewFraudSignalApprovalServiceClient(deps.Conn),
		watchlistValidationClient:   fraudv1.NewWatchlistValidationServiceClient(deps.Conn),
		pointsLedgerApprovalClient:  loyaltyv1.NewPointsLedgerApprovalServiceClient(deps.Conn),
		customerCalculationClient:   customersv1.NewCustomerCalculationServiceClient(deps.Conn),
		preferenceCalculationClient: customersv1.NewPreferenceCalculationServiceClient(deps.Conn),
		fraudRuleCalculationClient:  fraudv1.NewFraudRuleCalculationServiceClient(deps.Conn),
		watchlistLifecycleClient:    fraudv1.NewWatchlistLifecycleServiceClient(deps.Conn),
		preferenceValidationClient:  customersv1.NewPreferenceValidationServiceClient(deps.Conn),
	}
}

// SubmitWatchlist submits a watchlist.
func (s *WatchlistApprovalServer) SubmitWatchlist(ctx context.Context, req *fraudv1.SubmitWatchlistRequest) (*fraudv1.SubmitWatchlistResponse, error) {
	if err := s.store.SaveWatchlist(ctx, req.GetContext().GetTenantId(), req.GetWatchlist()); err != nil {
		return nil, grpcx.Wrap(err, "save watchlist")
	}
	s.log.InfoContext(ctx, "submit watchlist", "tenant", req.GetContext().GetTenantId(), "etag", req.GetWatchlist().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "reason", req.GetReason())
	packed, err := anypb.New(req.GetWatchlist())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	unsubscribeRoleUpdatesRequest := &identityv1.UnsubscribeRoleUpdatesRequest{
		Context: req.GetContext(),
		Details: &identityv1.UnsubscribeRoleUpdatesRequest_Details{
			Recipient: &commonv1.ContactInfo{Email: req.GetWatchlist().GetManager().GetEmail()},
			Payload:   packed,
		},
		Reason: tenancy.Value(ctx, "reason"),
	}
	unsubscribeRoleUpdatesReply, err := s.meteredRoleNotification.UnsubscribeRoleUpdates(ctx, unsubscribeRoleUpdatesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.RoleNotificationService/UnsubscribeRoleUpdates")
	}
	recalculatePreferencesRequest := &customersv1.RecalculatePreferencesRequest{
		Context: req.GetContext(),
		Details: &customersv1.RecalculatePreferencesRequest_Details{
			Destination: &commonv1.PostalAddress{Recipient: req.GetWatchlist().GetLocation().GetRecipient()},
		},
		Preference: &customersv1.Preference{
			Audit: &commonv1.AuditInfo{UpdatedBy: req.GetWatchlist().GetAudit().GetUpdatedBy()},
			Owner: &identityv1.UserRef{Email: req.GetWatchlist().GetManager().GetEmail()},
		},
	}
	recalculatePreferencesReply := new(customersv1.RecalculatePreferencesResponse)
	if err := grpc.Invoke(ctx, "/acme.customers.v1.PreferenceCalculationService/RecalculatePreferences", recalculatePreferencesRequest, recalculatePreferencesReply, s.conn); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceCalculationService/RecalculatePreferences")
	}
	return &fraudv1.SubmitWatchlistResponse{
		Result: &commonv1.OperationResult{
			Message: unsubscribeRoleUpdatesReply.GetOutcome().GetMessage(),
			Status:  "ok",
		},
		Watchlist: &fraudv1.Watchlist{
			Manager: &identityv1.UserRef{
				UserId: recalculatePreferencesReply.GetPreference().GetOwner().GetUserId(),
			},
			Children:    []*fraudv1.Watchlist{&fraudv1.Watchlist{Name: recalculatePreferencesReply.GetPreference().GetName()}},
			WatchlistId: req.GetWatchlistId(),
		},
		Outcome: &fraudv1.SubmitWatchlistResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// ApproveWatchlist approves a watchlist.
func (s *WatchlistApprovalServer) ApproveWatchlist(ctx context.Context, req *fraudv1.ApproveWatchlistRequest) (*fraudv1.ApproveWatchlistResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveWatchlist(ctx, req.GetContext().GetTenantId(), req.GetWatchlist()); err != nil {
		return nil, grpcx.Wrap(err, "save watchlist")
	}
	s.log.InfoContext(ctx, "approve watchlist", "tenant", req.GetContext().GetTenantId(), "etag", req.GetWatchlist().GetAudit().GetEtag())
	listPendingFraudRulesRequest := &fraudv1.ListPendingFraudRulesRequest{
		Context: req.GetContext(),
		Filter: &fraudv1.ListPendingFraudRulesRequest_Filter{
			Region:       req.GetWatchlist().GetLocation().GetRegion(),
			CreatedAfter: req.GetWatchlist().GetAudit().GetUpdatedAt(),
		},
	}
	listPendingFraudRulesReply, err := s.meteredFraudRuleApproval.ListPendingFraudRules(ctx, listPendingFraudRulesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.FraudRuleApprovalService/ListPendingFraudRules")
	}
	return &fraudv1.ApproveWatchlistResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: listPendingFraudRulesReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Watchlist: &fraudv1.Watchlist{WatchlistId: req.GetWatchlistId()},
		Outcome:   &fraudv1.ApproveWatchlistResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// RejectWatchlist rejects a watchlist.
func (s *WatchlistApprovalServer) RejectWatchlist(ctx context.Context, req *fraudv1.RejectWatchlistRequest) (*fraudv1.RejectWatchlistResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RejectWatchlist", "error", err)
		return nil, err
	}
	if err := s.store.SaveWatchlist(ctx, req.GetContext().GetTenantId(), req.GetWatchlist()); err != nil {
		return nil, grpcx.Wrap(err, "save watchlist")
	}
	s.log.InfoContext(ctx, "reject watchlist", "tenant", req.GetContext().GetTenantId(), "etag", req.GetWatchlist().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "user_id", req.GetWatchlist().GetManager().GetUserId())
	listPendingStorageBinsRequest := &warehousingv1.ListPendingStorageBinsRequest{}
	listPendingStorageBinsRequest.Context = req.GetContext()
	listPendingStorageBinsRequest.Filter = &warehousingv1.ListPendingStorageBinsRequest_Filter{
		Region:       req.GetWatchlist().GetLocation().GetRegion(),
		CreatedAfter: req.GetWatchlist().GetAudit().GetCreatedAt(),
		OwnerId:      tenancy.Value(ctx, "user_id"),
		Labels:       req.GetWatchlist().GetAttributes(),
	}
	listPendingStorageBinsReply, err := s.storageBinApprovalClient.ListPendingStorageBins(ctx, listPendingStorageBinsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.StorageBinApprovalService/ListPendingStorageBins")
	}
	listPendingFraudSignalsReply, err := s.fraudSignalApprovalClient.ListPendingFraudSignals(ctx, &fraudv1.ListPendingFraudSignalsRequest{
		Context: req.GetContext(),
		Filter: &fraudv1.ListPendingFraudSignalsRequest_Filter{
			Region:       req.GetWatchlist().GetLocation().GetRegion(),
			CreatedAfter: req.GetWatchlist().GetAudit().GetUpdatedAt(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.FraudSignalApprovalService/ListPendingFraudSignals")
	}
	previewWatchlistRequest := &fraudv1.PreviewWatchlistRequest{
		Context: req.GetContext(),
		Watchlist: &fraudv1.Watchlist{
			Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
				Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
					Location: &commonv1.PostalAddress{Lines: req.GetWatchlist().GetLocation().GetLines()},
				}},
			}},
		},
	}
	previewWatchlistReply, err := fraudv1.WatchlistValidationServiceClient.PreviewWatchlist(s.watchlistValidationClient, ctx, previewWatchlistRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.WatchlistValidationService/PreviewWatchlist")
	}
	commentPointsLedgerApprovalReply, err := s.pointsLedgerApprovalClient.CommentPointsLedgerApproval(ctx, &loyaltyv1.CommentPointsLedgerApprovalRequest{
		Context: req.GetContext(),
		PointsLedger: &loyaltyv1.PointsLedger{
			Audit: &commonv1.AuditInfo{
				CreatedBy: previewWatchlistReply.GetWatchlist().GetAudit().GetCreatedBy(),
			},
		},
		Details: &loyaltyv1.CommentPointsLedgerApprovalRequest_Details{
			Approver: &identityv1.UserRef{
				Email:       req.GetDetails().GetApprover().GetEmail(),
				DisplayName: previewWatchlistReply.GetWatchlist().GetManager().GetDisplayName(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerApprovalService/CommentPointsLedgerApproval")
	}
	return &fraudv1.RejectWatchlistResponse{
		Outcome: &fraudv1.RejectWatchlistResponse_Outcome{
			Status:      listPendingStorageBinsReply.GetResult().GetStatus(),
			Message:     listPendingFraudSignalsReply.GetResult().GetMessage(),
			EffectiveAt: commentPointsLedgerApprovalReply.GetOutcome().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{
			Message: listPendingStorageBinsReply.GetResult().GetMessage(),
			Details: listPendingFraudSignalsReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Watchlist: &fraudv1.Watchlist{
			Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
				Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
					Audit: &commonv1.AuditInfo{
						CreatedBy: previewWatchlistReply.GetWatchlist().GetAudit().GetCreatedBy(),
					},
				}},
			}},
			WatchlistId: req.GetWatchlistId(),
			Name:        req.GetWatchlist().GetName(),
		},
	}, nil
}

// ListPendingWatchlists lists watchlists a page at a time.
func (s *WatchlistApprovalServer) ListPendingWatchlists(ctx context.Context, req *fraudv1.ListPendingWatchlistsRequest) (*fraudv1.ListPendingWatchlistsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ListPendingWatchlists", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "list pending watchlists", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetContext()).(*commonv1.RequestContext)
	signalCarrierRateWorkflowRequest := &shippingv1.SignalCarrierRateWorkflowRequest{
		Context: req.GetContext(),
		Details: &shippingv1.SignalCarrierRateWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
		CarrierRate: &shippingv1.CarrierRate{
			Owner: &identityv1.UserRef{UserId: cloned.GetActor().GetUserId()},
		},
	}
	signalCarrierRateWorkflowReply, err := s.carrierRateWorkflowLazy().SignalCarrierRateWorkflow(ctx, signalCarrierRateWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.CarrierRateWorkflowService/SignalCarrierRateWorkflow")
	}
	return &fraudv1.ListPendingWatchlistsResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: signalCarrierRateWorkflowReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Watchlists: []*fraudv1.Watchlist{&fraudv1.Watchlist{
			Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
				Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
					Audit: &commonv1.AuditInfo{
						UpdatedAt: signalCarrierRateWorkflowReply.GetCarrierRate().GetAudit().GetUpdatedAt(),
					},
				}},
			}},
		}},
	}, nil
}

// DelegateWatchlistApproval delegates a watchlist.
func (s *WatchlistApprovalServer) DelegateWatchlistApproval(ctx context.Context, req *fraudv1.DelegateWatchlistApprovalRequest) (*fraudv1.DelegateWatchlistApprovalResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveWatchlist(ctx, req.GetContext().GetTenantId(), req.GetWatchlist()); err != nil {
		return nil, grpcx.Wrap(err, "save watchlist")
	}
	s.log.InfoContext(ctx, "delegate watchlist approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetWatchlist().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "email", req.GetDetails().GetApprover().GetEmail())
	source := req.GetDetails().GetApprover().ProtoReflect()
	displayName := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("display_name"))).String()
	names := collectWatchlistNames(req.GetWatchlist(), nil)
	step := &watchlistApprovalServerDelegateWatchlistApprovalExplainCustomerCalculationStep{request: &customersv1.ExplainCustomerCalculationRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			Contact: &commonv1.ContactInfo{Email: tenancy.Value(ctx, "email")},
			RelationshipManager: &identityv1.UserRef{
				DisplayName: displayName,
				UserId:      req.GetWatchlist().GetManager().GetUserId(),
			},
			PrimaryAddress: &commonv1.PostalAddress{Lines: names},
		},
		Details: &customersv1.ExplainCustomerCalculationRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetWatchlist().GetManager().GetEmail()},
		},
	}}
	if err := s.runDelegateWatchlistApprovalExplainCustomerCalculation(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerCalculationService/ExplainCustomerCalculation")
	}
	explainCustomerCalculationReply := step.reply
	return &fraudv1.DelegateWatchlistApprovalResponse{
		Watchlist: &fraudv1.Watchlist{
			Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
				Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
					Audit: &commonv1.AuditInfo{
						UpdatedBy: explainCustomerCalculationReply.GetCustomer().GetAudit().GetUpdatedBy(),
					},
				}},
			}},
			WatchlistId: req.GetWatchlistId(),
		},
		Outcome: &fraudv1.DelegateWatchlistApprovalResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RecallWatchlist recalls a watchlist.
func (s *WatchlistApprovalServer) RecallWatchlist(ctx context.Context, req *fraudv1.RecallWatchlistRequest) (*fraudv1.RecallWatchlistResponse, error) {
	if err := s.store.SaveWatchlist(ctx, req.GetContext().GetTenantId(), req.GetWatchlist()); err != nil {
		return nil, grpcx.Wrap(err, "save watchlist")
	}
	s.log.InfoContext(ctx, "recall watchlist", "tenant", req.GetContext().GetTenantId(), "etag", req.GetWatchlist().GetAudit().GetEtag())
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := watchlistApprovalServerSimulatePreferenceSteps[mode]
	names := collectWatchlistNames(req.GetWatchlist(), nil)
	simulatePreferenceRequest := &customersv1.SimulatePreferenceRequest{
		Context: req.GetContext(),
		Preference: &customersv1.Preference{
			Audit: &commonv1.AuditInfo{
				CreatedBy: req.GetWatchlist().GetAudit().GetCreatedBy(),
				CreatedAt: req.GetWatchlist().GetAudit().GetCreatedAt(),
			},
			Scopes: names,
		},
		Details: &customersv1.SimulatePreferenceRequest_Details{
			Destination: &commonv1.PostalAddress{
				PostalCode: req.GetWatchlist().GetLocation().GetPostalCode(),
				Region:     req.GetWatchlist().GetLocation().GetRegion(),
			},
		},
	}
	simulatePreferenceReply, err := run(s, ctx, simulatePreferenceRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceCalculationService/SimulatePreference")
	}
	calculateFraudRuleRequest := &fraudv1.CalculateFraudRuleRequest{
		Context: req.GetContext(),
		FraudRule: &fraudv1.FraudRule{
			Owner: &identityv1.UserRef{
				Email: simulatePreferenceReply.GetPreference().GetOwner().GetEmail(),
			},
		},
	}
	type calculateFraudRuleResult struct {
		reply *fraudv1.CalculateFraudRuleResponse
		err   error
	}
	calculateFraudRuleResults := make(chan calculateFraudRuleResult, 1)
	go func() {
		reply, err := s.fraudRuleCalculationClient.CalculateFraudRule(ctx, calculateFraudRuleRequest)
		calculateFraudRuleResults <- calculateFraudRuleResult{reply, err}
	}()
	var calculateFraudRuleReply *fraudv1.CalculateFraudRuleResponse
	select {
	case result := <-calculateFraudRuleResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.fraud.v1.FraudRuleCalculationService/CalculateFraudRule")
		}
		calculateFraudRuleReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &fraudv1.RecallWatchlistResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: simulatePreferenceReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Watchlist: &fraudv1.Watchlist{
			Name: simulatePreferenceReply.GetPreference().GetName(),
			Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
				Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
					Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{Name: calculateFraudRuleReply.GetFraudRule().GetName()}},
				}},
			}},
			WatchlistId: req.GetWatchlistId(),
			ParentId:    req.GetWatchlist().GetParentId(),
		},
	}, nil
}

// GetWatchlistApprovalStatus returns a watchlist.
func (s *WatchlistApprovalServer) GetWatchlistApprovalStatus(ctx context.Context, req *fraudv1.GetWatchlistApprovalStatusRequest) (*fraudv1.GetWatchlistApprovalStatusResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "watchlist", req.GetWatchlistId(), "get_watchlist_approval_status"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get watchlist approval status", "tenant", req.GetContext().GetTenantId())
	renewWatchlistRequest := &fraudv1.RenewWatchlistRequest{
		Context:     req.GetContext(),
		WatchlistId: req.GetWatchlistId(),
		Details: &fraudv1.RenewWatchlistRequest_Details{
			NewOwner: &identityv1.UserRef{
				Email:  req.GetContext().GetActor().GetEmail(),
				UserId: req.GetContext().GetActor().GetUserId(),
			},
			EffectiveAt: req.GetContext().GetDeadline(),
		},
	}
	renewWatchlistReply, err := fraudv1.WatchlistLifecycleServiceClient.RenewWatchlist(s.watchlistLifecycleClient, ctx, renewWatchlistRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.WatchlistLifecycleService/RenewWatchlist")
	}
	return &fraudv1.GetWatchlistApprovalStatusResponse{
		Watchlist: &fraudv1.Watchlist{
			Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
				Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
					Audit: &commonv1.AuditInfo{
						Etag:      renewWatchlistReply.GetWatchlist().GetAudit().GetEtag(),
						CreatedBy: renewWatchlistReply.GetWatchlist().GetAudit().GetCreatedBy(),
					},
				}},
			}},
			WatchlistId: req.GetWatchlistId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CommentWatchlistApproval comments a watchlist.
func (s *WatchlistApprovalServer) CommentWatchlistApproval(ctx context.Context, req *fraudv1.CommentWatchlistApprovalRequest) (*fraudv1.CommentWatchlistApprovalResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveWatchlist(ctx, req.GetContext().GetTenantId(), req.GetWatchlist()); err != nil {
		return nil, grpcx.Wrap(err, "save watchlist")
	}
	s.log.InfoContext(ctx, "comment watchlist approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetWatchlist().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "display_name", req.GetWatchlist().GetManager().GetDisplayName())
	previewPreferenceReply, err := s.preferenceValidationClient.PreviewPreference(ctx, &customersv1.PreviewPreferenceRequest{
		Context: req.GetContext(),
		Preference: &customersv1.Preference{
			Audit: &commonv1.AuditInfo{Etag: req.GetWatchlist().GetAudit().GetEtag()},
			Owner: &identityv1.UserRef{
				UserId:      req.GetDetails().GetApprover().GetUserId(),
				DisplayName: tenancy.Value(ctx, "display_name"),
			},
		},
		Details: &customersv1.PreviewPreferenceRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetWatchlist().GetManager().GetEmail()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceValidationService/PreviewPreference")
	}
	suspendWatchlistRequest := &fraudv1.SuspendWatchlistRequest{
		Context:     req.GetContext(),
		WatchlistId: req.GetWatchlist().GetWatchlistId(),
		Details: &fraudv1.SuspendWatchlistRequest_Details{
			NewOwner: &identityv1.UserRef{Email: req.GetDetails().GetApprover().GetEmail()},
		},
	}
	suspendWatchlistReply, err := grpcx.Call(ctx, s.watchlistLifecycleClient.SuspendWatchlist, suspendWatchlistRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.WatchlistLifecycleService/SuspendWatchlist")
	}
	return &fraudv1.CommentWatchlistApprovalResponse{
		Watchlist: &fraudv1.Watchlist{
			Audit: &commonv1.AuditInfo{
				CreatedBy: previewPreferenceReply.GetPreference().GetAudit().GetCreatedBy(),
			},
			WatchlistId: req.GetWatchlistId(),
			Manager:     req.GetWatchlist().GetManager(),
		},
		Result: &commonv1.OperationResult{Status: suspendWatchlistReply.GetResult().GetStatus()},
	}, nil
}

// carrierRateWorkflowLazy dials acme.shipping.v1.CarrierRateWorkflowService the first time a handler needs it.
func (s *WatchlistApprovalServer) carrierRateWorkflowLazy() shippingv1.CarrierRateWorkflowServiceClient {
	s.carrierRateWorkflowOnce.Do(func() { s.carrierRateWorkflowCached = shippingv1.NewCarrierRateWorkflowServiceClient(s.conn) })
	return s.carrierRateWorkflowCached
}

// watchlistApprovalServerDelegateWatchlistApprovalExplainCustomerCalculationStep carries the request of one step of
// DelegateWatchlistApproval and, once it ran, its reply.
type watchlistApprovalServerDelegateWatchlistApprovalExplainCustomerCalculationStep struct {
	request *customersv1.ExplainCustomerCalculationRequest
	reply   *customersv1.ExplainCustomerCalculationResponse
}

func (s *WatchlistApprovalServer) runDelegateWatchlistApprovalExplainCustomerCalculation(ctx context.Context, step *watchlistApprovalServerDelegateWatchlistApprovalExplainCustomerCalculationStep) error {
	reply, err := s.customerCalculationClient.ExplainCustomerCalculation(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}

// watchlistApprovalServerSimulatePreferenceSteps are the ways to run
// acme.customers.v1.PreferenceCalculationService/SimulatePreference, by name.
var watchlistApprovalServerSimulatePreferenceSteps = map[string]func(*WatchlistApprovalServer, context.Context, *customersv1.SimulatePreferenceRequest) (*customersv1.SimulatePreferenceResponse, error){
	"default": (*WatchlistApprovalServer).simulatePreferenceStep,
	"audited": (*WatchlistApprovalServer).simulatePreferenceAuditedStep,
}

func (s *WatchlistApprovalServer) simulatePreferenceStep(ctx context.Context, request *customersv1.SimulatePreferenceRequest) (*customersv1.SimulatePreferenceResponse, error) {
	return s.preferenceCalculationClient.SimulatePreference(ctx, request)
}

func (s *WatchlistApprovalServer) simulatePreferenceAuditedStep(ctx context.Context, request *customersv1.SimulatePreferenceRequest) (*customersv1.SimulatePreferenceResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.customers.v1.PreferenceCalculationService/SimulatePreference")
	return s.preferenceCalculationClient.SimulatePreference(ctx, request)
}
