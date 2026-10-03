package fraud

import (
	"context"
	"errors"
	"io"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	concessionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/concessions/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// WatchlistLifecycleServer implements `acme.fraud.v1.WatchlistLifecycleService`.
// Moves a watchlist between the states of its lifecycle.
type WatchlistLifecycleServer struct {
	fraudv1.UnimplementedWatchlistLifecycleServiceServer
	store                           *Store
	log                             *slog.Logger
	subscriptionApprovalClient      subscriptionsv1.SubscriptionApprovalServiceClient
	searchIndexNotificationClient   searchv1.SearchIndexNotificationServiceClient
	customerQueryClient             customersv1.CustomerQueryServiceClient
	synonymValidationClient         searchv1.SynonymValidationServiceClient
	conn                            *grpc.ClientConn
	customerClient                  customersv1.CustomerServiceClient
	returnLabelApprovalClient       returnsv1.ReturnLabelApprovalServiceClient
	fraudRuleCalculationPort        fraudRuleCalculationPort
	fraudSignalCalculationClient    fraudv1.FraudSignalCalculationServiceClient
	customerSegmentValidationClient customersv1.CustomerSegmentValidationServiceClient
	fraudSignalQueryPort            fraudSignalQueryPort
	meteredCustomer                 meteredCustomer
	rewardWorkflowClient            loyaltyv1.RewardWorkflowServiceClient
	userHistoryClient               identityv1.UserHistoryServiceClient
}

// NewWatchlistLifecycleServer wires a WatchlistLifecycleServer.
func NewWatchlistLifecycleServer(deps platform.Deps) *WatchlistLifecycleServer {
	return &WatchlistLifecycleServer{
		store:                           NewStore(deps.DB),
		log:                             deps.Logger.With("service", "acme.fraud.v1.WatchlistLifecycleService"),
		subscriptionApprovalClient:      subscriptionsv1.NewSubscriptionApprovalServiceClient(deps.Conn),
		searchIndexNotificationClient:   searchv1.NewSearchIndexNotificationServiceClient(deps.Conn),
		customerQueryClient:             customersv1.NewCustomerQueryServiceClient(deps.Conn),
		synonymValidationClient:         searchv1.NewSynonymValidationServiceClient(deps.Conn),
		conn:                            deps.Conn,
		customerClient:                  customersv1.NewCustomerServiceClient(deps.Conn),
		returnLabelApprovalClient:       returnsv1.NewReturnLabelApprovalServiceClient(deps.Conn),
		fraudRuleCalculationPort:        newFraudRuleCalculationPort(deps),
		fraudSignalCalculationClient:    fraudv1.NewFraudSignalCalculationServiceClient(deps.Conn),
		customerSegmentValidationClient: customersv1.NewCustomerSegmentValidationServiceClient(deps.Conn),
		fraudSignalQueryPort:            newFraudSignalQueryPort(deps),
		meteredCustomer:                 meteredCustomer{CustomerServiceClient: customersv1.NewCustomerServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		rewardWorkflowClient:            loyaltyv1.NewRewardWorkflowServiceClient(deps.Conn),
		userHistoryClient:               identityv1.NewUserHistoryServiceClient(deps.Conn),
	}
}

// ActivateWatchlist activates a watchlist.
func (s *WatchlistLifecycleServer) ActivateWatchlist(ctx context.Context, req *fraudv1.ActivateWatchlistRequest) (*fraudv1.ActivateWatchlistResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "watchlist", req.GetWatchlistId(), "activate_watchlist"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "activate watchlist", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "user_id", req.GetDetails().GetNewOwner().GetUserId())
	submitSubscriptionReply, err := s.subscriptionApprovalClient.SubmitSubscription(ctx, &subscriptionsv1.SubmitSubscriptionRequest{
		Context: req.GetContext(),
		Details: &subscriptionsv1.SubmitSubscriptionRequest_Details{
			Approver: &identityv1.UserRef{
				UserId: tenancy.Value(ctx, "user_id"),
				Email:  req.GetDetails().GetNewOwner().GetEmail(),
			},
		},
		Subscription: &subscriptionsv1.Subscription{
			Holder:  &customersv1.CustomerRef{Email: req.GetDetails().GetNotifyContact().GetEmail()},
			Contact: &commonv1.ContactInfo{Name: req.GetDetails().GetNotifyContact().GetName()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionApprovalService/SubmitSubscription")
	}
	listSearchIndexSubscriptionsRequest := buildListSearchIndexSubscriptionsRequestForActivateWatchlist(req, submitSubscriptionReply)
	listSearchIndexSubscriptionsReply, err := s.searchIndexNotificationClient.ListSearchIndexSubscriptions(ctx, listSearchIndexSubscriptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexNotificationService/ListSearchIndexSubscriptions")
	}
	return &fraudv1.ActivateWatchlistResponse{
		Outcome: &fraudv1.ActivateWatchlistResponse_Outcome{
			EffectiveAt: submitSubscriptionReply.GetOutcome().GetEffectiveAt(),
		},
		Watchlist: &fraudv1.Watchlist{
			Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
				Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
					Audit: &commonv1.AuditInfo{
						Etag: submitSubscriptionReply.GetSubscription().GetAudit().GetEtag(),
					},
				}},
			}},
			WatchlistId: req.GetWatchlistId(),
			Manager:     &identityv1.UserRef{UserId: req.GetDetails().GetNewOwner().GetUserId()},
		},
		Result: &commonv1.OperationResult{
			CorrelationId: listSearchIndexSubscriptionsReply.GetResult().GetCorrelationId(),
			Details:       listSearchIndexSubscriptionsReply.GetResult().GetDetails(),
			Status:        "ok",
		},
	}, nil
}

// SuspendWatchlist suspends a watchlist.
func (s *WatchlistLifecycleServer) SuspendWatchlist(ctx context.Context, req *fraudv1.SuspendWatchlistRequest) (*fraudv1.SuspendWatchlistResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "watchlist", req.GetWatchlistId(), "suspend_watchlist"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "suspend watchlist", "tenant", req.GetContext().GetTenantId())
	browseCustomersRequest := newBrowseCustomersRequest(withBrowseCustomersContext(req.GetContext()), withBrowseCustomersFilter(&customersv1.BrowseCustomersRequest_Filter{CreatedAfter: req.GetDetails().GetEffectiveAt()}))
	browseCustomersReply, err := s.customerQueryClient.BrowseCustomers(ctx, browseCustomersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerQueryService/BrowseCustomers")
	}
	ctx = tenancy.With(ctx, "email", req.GetDetails().GetNewOwner().GetEmail())
	checkSynonymEligibilityRequest := &searchv1.CheckSynonymEligibilityRequest{}
	checkSynonymEligibilityRequest.Context = req.GetContext()
	checkSynonymEligibilityRequest.Synonym = &searchv1.Synonym{
		Owner: &identityv1.UserRef{
			DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			Email:       tenancy.Value(ctx, "email"),
		},
	}
	checkSynonymEligibilityRequest.Details = &searchv1.CheckSynonymEligibilityRequest_Details{
		Contact: &commonv1.ContactInfo{
			PreferredChannel: req.GetDetails().GetNotifyContact().GetPreferredChannel(),
			Email:            req.GetDetails().GetNotifyContact().GetEmail(),
		},
	}
	checkSynonymEligibilityReply, err := s.synonymValidationClient.CheckSynonymEligibility(ctx, checkSynonymEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymValidationService/CheckSynonymEligibility")
	}
	return &fraudv1.SuspendWatchlistResponse{
		Result: &commonv1.OperationResult{
			Details: browseCustomersReply.GetResult().GetDetails(),
			Status:  browseCustomersReply.GetResult().GetStatus(),
		},
		Watchlist: &fraudv1.Watchlist{
			Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
				Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
					Manager: &identityv1.UserRef{
						DisplayName: checkSynonymEligibilityReply.GetSynonym().GetOwner().GetDisplayName(),
					},
				}},
			}},
			WatchlistId: req.GetWatchlistId(),
		},
		Outcome: &fraudv1.SuspendWatchlistResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// ReinstateWatchlist reinstates a watchlist.
func (s *WatchlistLifecycleServer) ReinstateWatchlist(ctx context.Context, req *fraudv1.ReinstateWatchlistRequest) (*fraudv1.ReinstateWatchlistResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "watchlist", req.GetWatchlistId(), "reinstate_watchlist"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "reinstate watchlist", "tenant", req.GetContext().GetTenantId())
	resolveConcessionStandsRequest := &concessionsv1.ResolveConcessionStandsRequest{Context: req.GetContext()}
	resolveConcessionStandsReply := new(concessionsv1.ResolveConcessionStandsResponse)
	if err := grpcx.Invoke(ctx, s.conn, concessionsv1.ConcessionStandQueryService_ResolveConcessionStands_FullMethodName, resolveConcessionStandsRequest, resolveConcessionStandsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.concessions.v1.ConcessionStandQueryService/ResolveConcessionStands")
	}
	cloned := proto.Clone(req.GetDetails()).(*fraudv1.ReinstateWatchlistRequest_Details)
	watchCustomersStream, err := s.customerClient.WatchCustomers(ctx, &customersv1.WatchCustomersRequest{
		Context: req.GetContext(),
		Subscriber: &commonv1.ContactInfo{
			Name:             req.GetDetails().GetNotifyContact().GetName(),
			PreferredChannel: req.GetDetails().GetNotifyContact().GetPreferredChannel(),
			Email:            cloned.GetNewOwner().GetEmail(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerService/WatchCustomers")
	}
	for {
		received, err := watchCustomersStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerService/WatchCustomers")
		}
		_ = received
	}
	commentReturnLabelApprovalRequest := buildCommentReturnLabelApprovalRequestForReinstateWatchlist(req)
	commentReturnLabelApprovalReply, err := s.returnLabelApprovalClient.CommentReturnLabelApproval(ctx, commentReturnLabelApprovalRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnLabelApprovalService/CommentReturnLabelApproval")
	}
	approveReturnLabelRequest := &returnsv1.ApproveReturnLabelRequest{
		Context: req.GetContext(),
		Details: &returnsv1.ApproveReturnLabelRequest_Details{
			Approver: &identityv1.UserRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
		},
		ReturnLabel: &returnsv1.ReturnLabel{
			BillingAddress: &commonv1.PostalAddress{
				PostalCode:  commentReturnLabelApprovalReply.GetReturnLabel().GetBillingAddress().GetPostalCode(),
				CountryCode: commentReturnLabelApprovalReply.GetReturnLabel().GetBillingAddress().GetCountryCode(),
			},
		},
	}
	approveReturnLabelReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*returnsv1.ApproveReturnLabelResponse, error) {
		return s.returnLabelApprovalClient.ApproveReturnLabel(ctx, approveReturnLabelRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnLabelApprovalService/ApproveReturnLabel")
	}
	return &fraudv1.ReinstateWatchlistResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: resolveConcessionStandsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Watchlist: &fraudv1.Watchlist{
			Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
				Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
					Location: &commonv1.PostalAddress{
						Region: commentReturnLabelApprovalReply.GetReturnLabel().GetBillingAddress().GetRegion(),
					},
				}},
				Location: &commonv1.PostalAddress{
					Recipient: approveReturnLabelReply.GetReturnLabel().GetBillingAddress().GetRecipient(),
				},
			}},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: commentReturnLabelApprovalReply.GetReturnLabel().GetAudit().GetUpdatedAt(),
			},
			Location: &commonv1.PostalAddress{
				Lines: approveReturnLabelReply.GetReturnLabel().GetBillingAddress().GetLines(),
			},
			WatchlistId: req.GetWatchlistId(),
		},
		Outcome: &fraudv1.ReinstateWatchlistResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// TerminateWatchlist terminates a watchlist.
func (s *WatchlistLifecycleServer) TerminateWatchlist(ctx context.Context, req *fraudv1.TerminateWatchlistRequest) (*fraudv1.TerminateWatchlistResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "watchlist", req.GetWatchlistId(), "terminate_watchlist"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "terminate watchlist", "tenant", req.GetContext().GetTenantId())
	simulateFraudRuleRequest := &fraudv1.SimulateFraudRuleRequest{
		Context: req.GetContext(),
		FraudRule: &fraudv1.FraudRule{
			Owner: &identityv1.UserRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
		},
	}
	simulateFraudRuleReply, err := s.fraudRuleCalculationPort.SimulateFraudRule(ctx, simulateFraudRuleRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.FraudRuleCalculationService/SimulateFraudRule")
	}
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	recalculateFraudSignalsRequest := &fraudv1.RecalculateFraudSignalsRequest{}
	recalculateFraudSignalsRequest.Context = req.GetContext()
	recalculateFraudSignalsRequest.FraudSignal = &fraudv1.FraudSignal{
		Subject: &fraudv1.FraudRuleRef{
			DisplayName: simulateFraudRuleReply.GetFraudRule().GetOwner().GetDisplayName(),
			FraudRuleId: simulateFraudRuleReply.GetFraudRule().GetFraudRuleId(),
		},
		Detail: packed,
	}
	recalculateFraudSignalsRequest.Details = &fraudv1.RecalculateFraudSignalsRequest_Details{
		CurrencyCode: simulateFraudRuleReply.GetBreakdown().GetCurrencyCode(),
	}
	recalculateFraudSignalsRequest.Reason = simulateFraudRuleReply.GetFraudRule().GetBlock().GetReason()
	recalculateFraudSignalsReply, err := s.fraudSignalCalculationClient.RecalculateFraudSignals(ctx, recalculateFraudSignalsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.FraudSignalCalculationService/RecalculateFraudSignals")
	}
	return &fraudv1.TerminateWatchlistResponse{
		Outcome: &fraudv1.TerminateWatchlistResponse_Outcome{
			Message:     simulateFraudRuleReply.GetResult().GetMessage(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Watchlist: &fraudv1.Watchlist{
			Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
				Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
					Audit: &commonv1.AuditInfo{
						UpdatedAt: simulateFraudRuleReply.GetFraudRule().GetAudit().GetUpdatedAt(),
					},
				}},
			}},
			WatchlistId: req.GetWatchlistId(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: recalculateFraudSignalsReply.GetResult().GetReferenceIds(),
			Details:      recalculateFraudSignalsReply.GetResult().GetDetails(),
			Status:       "ok",
		},
	}, nil
}

// RenewWatchlist renews a watchlist.
func (s *WatchlistLifecycleServer) RenewWatchlist(ctx context.Context, req *fraudv1.RenewWatchlistRequest) (*fraudv1.RenewWatchlistResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "watchlist", req.GetWatchlistId(), "renew_watchlist"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "renew watchlist", "tenant", req.GetContext().GetTenantId())
	lintCustomerSegmentsRequest := &customersv1.LintCustomerSegmentsRequest{
		Context: req.GetContext(),
		CustomerSegment: &customersv1.CustomerSegment{
			Owner: &identityv1.UserRef{
				Email:  req.GetDetails().GetNotifyContact().GetEmail(),
				UserId: req.GetDetails().GetNewOwner().GetUserId(),
			},
		},
		Details: &customersv1.LintCustomerSegmentsRequest_Details{
			Contact: &commonv1.ContactInfo{
				Phone: req.GetDetails().GetNotifyContact().GetPhone(),
				Name:  req.GetDetails().GetNotifyContact().GetName(),
			},
		},
	}
	lintCustomerSegmentsReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*customersv1.LintCustomerSegmentsResponse, error) {
		return s.customerSegmentValidationClient.LintCustomerSegments(ctx, lintCustomerSegmentsRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentValidationService/LintCustomerSegments")
	}
	return &fraudv1.RenewWatchlistResponse{
		Watchlist: &fraudv1.Watchlist{
			Audit: &commonv1.AuditInfo{
				CreatedBy: lintCustomerSegmentsReply.GetCustomerSegment().GetAudit().GetCreatedBy(),
			},
			WatchlistId: req.GetWatchlistId(),
		},
		Outcome: &fraudv1.RenewWatchlistResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ExpireWatchlist expires a watchlist.
func (s *WatchlistLifecycleServer) ExpireWatchlist(ctx context.Context, req *fraudv1.ExpireWatchlistRequest) (*fraudv1.ExpireWatchlistResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "watchlist", req.GetWatchlistId(), "expire_watchlist"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "expire watchlist", "tenant", req.GetContext().GetTenantId())
	resolveFraudSignalsRequest := &fraudv1.ResolveFraudSignalsRequest{Context: req.GetContext()}
	resolveFraudSignalsReply, err := s.fraudSignalQueryPort.ResolveFraudSignals(ctx, resolveFraudSignalsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.FraudSignalQueryService/ResolveFraudSignals")
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	listCustomersRequest := &customersv1.ListCustomersRequest{
		Context: req.GetContext(),
		Filter: &customersv1.ListCustomersRequest_Filter{
			CreatedAfter: req.GetDetails().GetEffectiveAt(),
			OwnerId:      resolveFraudSignalsReply.GetResult().GetCorrelationId(),
			Labels:       annotations,
		},
	}
	listCustomersReply, err := s.meteredCustomer.ListCustomers(ctx, listCustomersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerService/ListCustomers")
	}
	resolveRiskAssessmentsRequest := &fraudv1.ResolveRiskAssessmentsRequest{Context: req.GetContext()}
	resolveRiskAssessmentsReply, err := grpcx.NewClient(s.conn, fraudv1.NewRiskAssessmentQueryServiceClient).ResolveRiskAssessments(ctx, resolveRiskAssessmentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.RiskAssessmentQueryService/ResolveRiskAssessments")
	}
	return &fraudv1.ExpireWatchlistResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: resolveFraudSignalsReply.GetResult().GetCorrelationId(),
			Details:       listCustomersReply.GetResult().GetDetails(),
			Status:        "ok",
		},
		Outcome: &fraudv1.ExpireWatchlistResponse_Outcome{
			Message:     resolveRiskAssessmentsReply.GetResult().GetMessage(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Watchlist: &fraudv1.Watchlist{WatchlistId: req.GetWatchlistId()},
	}, nil
}

// GetWatchlistLifecycle returns a watchlist.
func (s *WatchlistLifecycleServer) GetWatchlistLifecycle(ctx context.Context, req *fraudv1.GetWatchlistLifecycleRequest) (*fraudv1.GetWatchlistLifecycleResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetWatchlistLifecycle", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "watchlist", req.GetWatchlistId(), "get_watchlist_lifecycle"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get watchlist lifecycle", "tenant", req.GetContext().GetTenantId())
	return &fraudv1.GetWatchlistLifecycleResponse{
		Watchlist: &fraudv1.Watchlist{WatchlistId: req.GetWatchlistId()},
		Result:    &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ScheduleWatchlistExpiry schedules a watchlist.
func (s *WatchlistLifecycleServer) ScheduleWatchlistExpiry(ctx context.Context, req *fraudv1.ScheduleWatchlistExpiryRequest) (*fraudv1.ScheduleWatchlistExpiryResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveWatchlist(ctx, req.GetContext().GetTenantId(), req.GetWatchlist()); err != nil {
		return nil, grpcx.Wrap(err, "save watchlist")
	}
	s.log.InfoContext(ctx, "schedule watchlist expiry", "tenant", req.GetContext().GetTenantId(), "etag", req.GetWatchlist().GetAudit().GetEtag())
	advanceRewardWorkflowRequest := &loyaltyv1.AdvanceRewardWorkflowRequest{}
	advanceRewardWorkflowRequest.Context = req.GetContext()
	advanceRewardWorkflowRequest.Reward = &loyaltyv1.Reward{
		Audit: &commonv1.AuditInfo{
			CreatedAt: req.GetWatchlist().GetAudit().GetCreatedAt(),
			UpdatedAt: req.GetWatchlist().GetAudit().GetUpdatedAt(),
		},
		Owner: &identityv1.UserRef{Email: req.GetWatchlist().GetManager().GetEmail()},
	}
	advanceRewardWorkflowReply, err := s.rewardWorkflowClient.AdvanceRewardWorkflow(ctx, advanceRewardWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.RewardWorkflowService/AdvanceRewardWorkflow")
	}
	snapshot, err := protojson.Marshal(req.GetWatchlist())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	revertUserReply, err := s.userHistoryClient.RevertUser(ctx, &identityv1.RevertUserRequest{
		Context: req.GetContext(),
		User: &identityv1.User{
			PrimaryAddress: &commonv1.PostalAddress{
				Locality: req.GetWatchlist().GetLocation().GetLocality(),
				Lines:    req.GetWatchlist().GetLocation().GetLines(),
				Region:   req.GetWatchlist().GetLocation().GetRegion(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedBy: advanceRewardWorkflowReply.GetReward().GetAudit().GetCreatedBy(),
			},
		},
		Details: &identityv1.RevertUserRequest_Details{Snapshot: snapshot},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserHistoryService/RevertUser")
	}
	return &fraudv1.ScheduleWatchlistExpiryResponse{
		Result: &commonv1.OperationResult{
			Message: advanceRewardWorkflowReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Watchlist: &fraudv1.Watchlist{
			Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
				Manager: &identityv1.UserRef{
					DisplayName: advanceRewardWorkflowReply.GetReward().GetOwner().GetDisplayName(),
				},
				Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
					Manager: &identityv1.UserRef{
						DisplayName: revertUserReply.GetUser().GetRelationshipManager().GetDisplayName(),
					},
				}},
			}},
			WatchlistId: req.GetWatchlistId(),
			ParentId:    req.GetWatchlist().GetParentId(),
		},
	}, nil
}
