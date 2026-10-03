package returns

import (
	"context"
	"log/slog"

	budgetingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/budgeting/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	notificationsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/notifications/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// RestockDecisionValidationServer implements `acme.returns.v1.RestockDecisionValidationService`.
// Checks restock decisions against the rules before anything acts on them.
type RestockDecisionValidationServer struct {
	returnsv1.UnimplementedRestockDecisionValidationServiceServer
	store                                 *Store
	log                                   *slog.Logger
	sellerPayoutScheduleCalculationClient sellersv1.SellerPayoutScheduleCalculationServiceClient
	conn                                  *grpc.ClientConn
	notificationValidationClient          notificationsv1.NotificationValidationServiceClient
	sellerListingValidationClient         sellersv1.SellerListingValidationServiceClient
	inspectionCalculationClient           returnsv1.InspectionCalculationServiceClient
	searchQueryHistoryClient              searchv1.SearchQueryHistoryServiceClient
	findPointsLedgersFn                   func(context.Context, *loyaltyv1.FindPointsLedgersRequest, ...grpc.CallOption) (*loyaltyv1.FindPointsLedgersResponse, error)
	clients                               *registry.Registry
	returnRequestCalculationPort          returnRequestCalculationPort
	budgetCalculationClient               budgetingv1.BudgetCalculationServiceClient
	budgetCalculationClientFallback       budgetingv1.BudgetCalculationServiceClient
	inspectionValidationClient            returnsv1.InspectionValidationServiceClient
	bus                                   *eventbus.Bus
}

// NewRestockDecisionValidationServer wires a RestockDecisionValidationServer.
func NewRestockDecisionValidationServer(deps platform.Deps) RestockDecisionValidationServer {
	pointsLedgerQuery := loyaltyv1.NewPointsLedgerQueryServiceClient(deps.Conn)
	return RestockDecisionValidationServer{
		store:                                 NewStore(deps.DB),
		log:                                   deps.Logger.With("service", "acme.returns.v1.RestockDecisionValidationService"),
		sellerPayoutScheduleCalculationClient: sellersv1.NewSellerPayoutScheduleCalculationServiceClient(deps.Conn),
		conn:                                  deps.Conn,
		notificationValidationClient:          notificationsv1.NewNotificationValidationServiceClient(deps.Conn),
		sellerListingValidationClient:         sellersv1.NewSellerListingValidationServiceClient(deps.Conn),
		inspectionCalculationClient:           returnsv1.NewInspectionCalculationServiceClient(deps.Conn),
		searchQueryHistoryClient:              searchv1.NewSearchQueryHistoryServiceClient(deps.Conn),
		findPointsLedgersFn:                   pointsLedgerQuery.FindPointsLedgers,
		clients:                               deps.Clients,
		returnRequestCalculationPort:          newReturnRequestCalculationPort(deps),
		budgetCalculationClient:               budgetingv1.NewBudgetCalculationServiceClient(deps.Conn),
		budgetCalculationClientFallback:       budgetingv1.NewBudgetCalculationServiceClient(deps.Conn),
		inspectionValidationClient:            returnsv1.NewInspectionValidationServiceClient(deps.Conn),
		bus:                                   deps.Bus,
	}
}

// ValidateRestockDecision validates a restock decision.
func (s RestockDecisionValidationServer) ValidateRestockDecision(ctx context.Context, req *returnsv1.ValidateRestockDecisionRequest) (*returnsv1.ValidateRestockDecisionResponse, error) {
	if err := s.store.SaveRestockDecision(ctx, req.GetContext().GetTenantId(), req.GetRestockDecision()); err != nil {
		return nil, grpcx.Wrap(err, "save restock decision")
	}
	s.log.InfoContext(ctx, "validate restock decision", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRestockDecision().GetAudit().GetEtag())
	quoteSellerPayoutScheduleRequest := &sellersv1.QuoteSellerPayoutScheduleRequest{
		Context: req.GetContext(),
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
			Participants: []*sellersv1.SellerPayoutScheduleParticipant{&sellersv1.SellerPayoutScheduleParticipant{
				Contact: &commonv1.ContactInfo{
					Phone: req.GetDetails().GetContact().GetPhone(),
					Name:  req.GetDetails().GetContact().GetName(),
				},
			}},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: req.GetRestockDecision().GetAudit().GetUpdatedAt(),
			},
		},
	}
	quoteSellerPayoutScheduleReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*sellersv1.QuoteSellerPayoutScheduleResponse, error) {
		return s.sellerPayoutScheduleCalculationClient.QuoteSellerPayoutSchedule(ctx, quoteSellerPayoutScheduleRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerPayoutScheduleCalculationService/QuoteSellerPayoutSchedule")
	}
	simulateRestockDecisionRequest := &returnsv1.SimulateRestockDecisionRequest{
		Context: req.GetContext(),
		RestockDecision: &returnsv1.RestockDecision{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: quoteSellerPayoutScheduleReply.GetSellerPayoutSchedule().GetAudit().GetUpdatedAt(),
				CreatedBy: quoteSellerPayoutScheduleReply.GetSellerPayoutSchedule().GetAudit().GetCreatedBy(),
			},
			Action: &returnsv1.RestockDecision_Block{Block: &returnsv1.RestockDecisionBlockAction{
				NotifyEmail: req.GetRestockDecision().GetBlock().GetNotifyEmail(),
			}},
		},
		Details: &returnsv1.SimulateRestockDecisionRequest_Details{
			Destination: &commonv1.PostalAddress{
				CountryCode: quoteSellerPayoutScheduleReply.GetSellerPayoutSchedule().GetLocation().GetCountryCode(),
			},
		},
	}
	simulateRestockDecisionReply := new(returnsv1.SimulateRestockDecisionResponse)
	if err := grpcx.Invoke(ctx, s.conn, returnsv1.RestockDecisionCalculationService_SimulateRestockDecision_FullMethodName, simulateRestockDecisionRequest, simulateRestockDecisionReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.RestockDecisionCalculationService/SimulateRestockDecision")
	}
	return &returnsv1.ValidateRestockDecisionResponse{
		RestockDecision: &returnsv1.RestockDecision{
			Audit: &commonv1.AuditInfo{
				CreatedBy: quoteSellerPayoutScheduleReply.GetSellerPayoutSchedule().GetAudit().GetCreatedBy(),
			},
			Owner: &identityv1.UserRef{
				UserId: simulateRestockDecisionReply.GetRestockDecision().GetOwner().GetUserId(),
			},
			RestockDecisionId: req.GetRestockDecisionId(),
			Name:              req.GetRestockDecision().GetName(),
		},
		Result: &commonv1.OperationResult{
			Message: quoteSellerPayoutScheduleReply.GetResult().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// PreviewRestockDecision previews a restock decision.
func (s RestockDecisionValidationServer) PreviewRestockDecision(ctx context.Context, req *returnsv1.PreviewRestockDecisionRequest) (*returnsv1.PreviewRestockDecisionResponse, error) {
	if err := s.store.SaveRestockDecision(ctx, req.GetContext().GetTenantId(), req.GetRestockDecision()); err != nil {
		return nil, grpcx.Wrap(err, "save restock decision")
	}
	s.log.InfoContext(ctx, "preview restock decision", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRestockDecision().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetDetails()).(*returnsv1.PreviewRestockDecisionRequest_Details)
	lintNotificationsRequest := &notificationsv1.LintNotificationsRequest{}
	lintNotificationsRequest.Context = req.GetContext()
	lintNotificationsRequest.Notification = &notificationsv1.Notification{
		Assignee: &identityv1.UserRef{Email: req.GetRestockDecision().GetOwner().GetEmail()},
		Origin: &notificationsv1.Notification_Customer{Customer: &notificationsv1.NotificationTemplateRef{
			DisplayName: req.GetRestockDecision().GetOwner().GetDisplayName(),
		}},
		Audit: &commonv1.AuditInfo{
			CreatedBy: req.GetRestockDecision().GetAudit().GetCreatedBy(),
		},
	}
	lintNotificationsRequest.Details = &notificationsv1.LintNotificationsRequest_Details{
		Contact: &commonv1.ContactInfo{Email: cloned.GetContact().GetEmail()},
	}
	lintNotificationsReply, err := s.notificationValidationClient.LintNotifications(ctx, lintNotificationsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.notifications.v1.NotificationValidationService/LintNotifications")
	}
	ctx = tenancy.With(ctx, "user_id", req.GetRestockDecision().GetOwner().GetUserId())
	cloned2 := proto.Clone(req.GetRestockDecision()).(*returnsv1.RestockDecision)
	checkSellerListingEligibilityReply, err := s.sellerListingValidationClient.CheckSellerListingEligibility(ctx, &sellersv1.CheckSellerListingEligibilityRequest{
		Context: req.GetContext(),
		SellerListing: &sellersv1.SellerListing{
			Owner: &identityv1.UserRef{
				Email:  req.GetDetails().GetContact().GetEmail(),
				UserId: tenancy.Value(ctx, "user_id"),
			},
		},
		Details: &sellersv1.CheckSellerListingEligibilityRequest_Details{
			Contact: &commonv1.ContactInfo{Email: cloned2.GetOwner().GetEmail()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingValidationService/CheckSellerListingEligibility")
	}
	return &returnsv1.PreviewRestockDecisionResponse{
		RestockDecision: &returnsv1.RestockDecision{
			Owner: &identityv1.UserRef{
				Email: lintNotificationsReply.GetNotification().GetRequester().GetEmail(),
			},
			RestockDecisionId: req.GetRestockDecisionId(),
			Description:       req.GetRestockDecision().GetDescription(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: checkSellerListingEligibilityReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// CheckRestockDecisionEligibility checks a restock decision.
func (s RestockDecisionValidationServer) CheckRestockDecisionEligibility(ctx context.Context, req *returnsv1.CheckRestockDecisionEligibilityRequest) (*returnsv1.CheckRestockDecisionEligibilityResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CheckRestockDecisionEligibility", "error", err)
		return nil, err
	}
	if err := s.store.SaveRestockDecision(ctx, req.GetContext().GetTenantId(), req.GetRestockDecision()); err != nil {
		return nil, grpcx.Wrap(err, "save restock decision")
	}
	s.log.InfoContext(ctx, "check restock decision eligibility", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRestockDecision().GetAudit().GetEtag())
	compareInspectionOptionsRequest := buildCompareInspectionOptionsRequestForCheckRestockDecisionEligibility(req)
	compareInspectionOptionsReply, err := s.inspectionCalculationClient.CompareInspectionOptions(ctx, compareInspectionOptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.InspectionCalculationService/CompareInspectionOptions")
	}
	return &returnsv1.CheckRestockDecisionEligibilityResponse{
		RestockDecision: &returnsv1.RestockDecision{
			Owner: &identityv1.UserRef{
				DisplayName: compareInspectionOptionsReply.GetInspection().GetAssignee().GetDisplayName(),
			},
			RestockDecisionId: req.GetRestockDecisionId(),
			Effective:         req.GetRestockDecision().GetEffective(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// NormalizeRestockDecision normalizes a restock decision.
func (s RestockDecisionValidationServer) NormalizeRestockDecision(ctx context.Context, req *returnsv1.NormalizeRestockDecisionRequest) (*returnsv1.NormalizeRestockDecisionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveRestockDecision(ctx, req.GetContext().GetTenantId(), req.GetRestockDecision()); err != nil {
		return nil, grpcx.Wrap(err, "save restock decision")
	}
	s.log.InfoContext(ctx, "normalize restock decision", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRestockDecision().GetAudit().GetEtag())
	packed, err := anypb.New(req.GetRestockDecision())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	snapshot, err := protojson.Marshal(req.GetRestockDecision())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	step := &restockDecisionValidationServerNormalizeRestockDecisionRecordSearchQueryVersionStep{request: &searchv1.RecordSearchQueryVersionRequest{
		Context: req.GetContext(),
		Details: &searchv1.RecordSearchQueryVersionRequest_Details{
			Version:  req.GetRestockDecision().GetVersion(),
			Snapshot: snapshot,
		},
		SearchQuery: &searchv1.SearchQuery{
			Subject: &searchv1.SearchIndexRef{
				DisplayName: req.GetRestockDecision().GetOwner().GetDisplayName(),
			},
			Detail: packed,
		},
		Annotations: req.GetAnnotations(),
	}}
	if err := s.runNormalizeRestockDecisionRecordSearchQueryVersion(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryHistoryService/RecordSearchQueryVersion")
	}
	recordSearchQueryVersionReply := step.reply
	compareSellerPayoutScheduleOptionsRequest := newCompareSellerPayoutScheduleOptionsRequest(withCompareSellerPayoutScheduleOptionsContext(req.GetContext()), withCompareSellerPayoutScheduleOptionsSellerPayoutSchedule(&sellersv1.SellerPayoutSchedule{
		Participants: []*sellersv1.SellerPayoutScheduleParticipant{&sellersv1.SellerPayoutScheduleParticipant{
			Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetContact().GetEmail()},
		}},
	}))
	compareSellerPayoutScheduleOptionsReply, err := s.sellerPayoutScheduleCalculationClient.CompareSellerPayoutScheduleOptions(ctx, compareSellerPayoutScheduleOptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerPayoutScheduleCalculationService/CompareSellerPayoutScheduleOptions")
	}
	return &returnsv1.NormalizeRestockDecisionResponse{
		Outcome: &returnsv1.NormalizeRestockDecisionResponse_Outcome{
			Status: recordSearchQueryVersionReply.GetResult().GetStatus(),
		},
		RestockDecision: &returnsv1.RestockDecision{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: compareSellerPayoutScheduleOptionsReply.GetSellerPayoutSchedule().GetAudit().GetUpdatedBy(),
			},
			RestockDecisionId: req.GetRestockDecisionId(),
			Status:            req.GetRestockDecision().GetStatus(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: compareSellerPayoutScheduleOptionsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// ExplainRestockDecisionDecision explains a restock decision.
func (s RestockDecisionValidationServer) ExplainRestockDecisionDecision(ctx context.Context, req *returnsv1.ExplainRestockDecisionDecisionRequest) (*returnsv1.ExplainRestockDecisionDecisionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveRestockDecision(ctx, req.GetContext().GetTenantId(), req.GetRestockDecision()); err != nil {
		return nil, grpcx.Wrap(err, "save restock decision")
	}
	s.log.InfoContext(ctx, "explain restock decision decision", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRestockDecision().GetAudit().GetEtag())
	checkLoyaltyMemberEligibilityRequest := &loyaltyv1.CheckLoyaltyMemberEligibilityRequest{
		Context: req.GetContext(),
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			Holder: &loyaltyv1.RewardRef{
				DisplayName: req.GetRestockDecision().GetOwner().GetDisplayName(),
			},
			Contact: &commonv1.ContactInfo{
				Name:             req.GetDetails().GetContact().GetName(),
				PreferredChannel: req.GetDetails().GetContact().GetPreferredChannel(),
			},
		},
		Details: &loyaltyv1.CheckLoyaltyMemberEligibilityRequest_Details{
			Contact: &commonv1.ContactInfo{Phone: req.GetDetails().GetContact().GetPhone()},
		},
	}
	checkLoyaltyMemberEligibilityReply := new(loyaltyv1.CheckLoyaltyMemberEligibilityResponse)
	if err := grpcx.Invoke(ctx, s.conn, loyaltyv1.LoyaltyMemberValidationService_CheckLoyaltyMemberEligibility_FullMethodName, checkLoyaltyMemberEligibilityRequest, checkLoyaltyMemberEligibilityReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberValidationService/CheckLoyaltyMemberEligibility")
	}
	explainInspectionCalculationRequest := &returnsv1.ExplainInspectionCalculationRequest{
		Context: req.GetContext(),
		Inspection: &returnsv1.Inspection{
			Origin: &returnsv1.Inspection_Customer{Customer: &customersv1.CustomerRef{
				DisplayName: checkLoyaltyMemberEligibilityReply.GetLoyaltyMember().GetHolder().GetDisplayName(),
			}},
		},
	}
	explainInspectionCalculationReply, err := grpcx.Call(ctx, s.inspectionCalculationClient.ExplainInspectionCalculation, explainInspectionCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.InspectionCalculationService/ExplainInspectionCalculation")
	}
	findPointsLedgersRequest := &loyaltyv1.FindPointsLedgersRequest{
		Context: req.GetContext(),
		Filter: &loyaltyv1.FindPointsLedgersRequest_Filter{
			Labels:       checkLoyaltyMemberEligibilityReply.GetLoyaltyMember().GetLabels(),
			CreatedAfter: explainInspectionCalculationReply.GetInspection().GetAudit().GetCreatedAt(),
		},
	}
	findPointsLedgersReply, err := s.findPointsLedgersFn(ctx, findPointsLedgersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerQueryService/FindPointsLedgers")
	}
	synonymSync, err := registry.Lookup[searchv1.SynonymSyncServiceClient](s.clients, "acme.search.v1.SynonymSyncService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	snapshot, err := protojson.Marshal(req.GetRestockDecision())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	syncSynonymsRequest := &searchv1.SyncSynonymsRequest{
		Context: req.GetContext(),
		Synonym: &searchv1.Synonym{
			Owner: &identityv1.UserRef{Email: req.GetDetails().GetContact().GetEmail()},
			Audit: &commonv1.AuditInfo{
				Etag:      checkLoyaltyMemberEligibilityReply.GetLoyaltyMember().GetAudit().GetEtag(),
				CreatedBy: checkLoyaltyMemberEligibilityReply.GetLoyaltyMember().GetAudit().GetCreatedBy(),
			},
			Action: &searchv1.Synonym_Route{Route: &searchv1.SynonymRouteAction{Target: req.GetRestockDecision().GetRoute().GetTarget()}},
		},
		Details: &searchv1.SyncSynonymsRequest_Details{Payload: snapshot},
	}
	syncSynonymsReply, err := synonymSync.SyncSynonyms(ctx, syncSynonymsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymSyncService/SyncSynonyms")
	}
	return &returnsv1.ExplainRestockDecisionDecisionResponse{
		Outcome: &returnsv1.ExplainRestockDecisionDecisionResponse_Outcome{
			Message:   checkLoyaltyMemberEligibilityReply.GetOutcome().GetMessage(),
			NextSteps: explainInspectionCalculationReply.GetOutcome().GetNextSteps(),
			Status:    findPointsLedgersReply.GetResult().GetStatus(),
		},
		Result: &commonv1.OperationResult{
			Details: findPointsLedgersReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		RestockDecision: &returnsv1.RestockDecision{
			Scopes:            syncSynonymsReply.GetSynonym().GetScopes(),
			RestockDecisionId: req.GetRestockDecisionId(),
			Status:            req.GetRestockDecision().GetStatus(),
		},
	}, nil
}

// LintRestockDecisions lints a restock decision.
func (s RestockDecisionValidationServer) LintRestockDecisions(ctx context.Context, req *returnsv1.LintRestockDecisionsRequest) (*returnsv1.LintRestockDecisionsResponse, error) {
	if err := s.store.SaveRestockDecision(ctx, req.GetContext().GetTenantId(), req.GetRestockDecision()); err != nil {
		return nil, grpcx.Wrap(err, "save restock decision")
	}
	s.log.InfoContext(ctx, "lint restock decisions", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRestockDecision().GetAudit().GetEtag())
	compareReturnRequestOptionsRequest := &returnsv1.CompareReturnRequestOptionsRequest{
		Context: req.GetContext(),
		ReturnRequest: &returnsv1.ReturnRequest{
			Lines: []*returnsv1.ReturnRequestLine{&returnsv1.ReturnRequestLine{
				Tax: &returnsv1.ReturnRequestLineTax{Amount: req.GetRestockDecision().GetDiscount().GetAmount()},
			}},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: req.GetRestockDecision().GetAudit().GetUpdatedBy(),
				CreatedAt: req.GetRestockDecision().GetAudit().GetCreatedAt(),
			},
		},
	}
	compareReturnRequestOptionsReply, err := s.returnRequestCalculationPort.CompareReturnRequestOptions(ctx, compareReturnRequestOptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnRequestCalculationService/CompareReturnRequestOptions")
	}
	routes := map[string]func(context.Context, *budgetingv1.QuoteBudgetRequest, ...grpc.CallOption) (*budgetingv1.QuoteBudgetResponse, error){
		"primary":  s.budgetCalculationClient.QuoteBudget,
		"fallback": s.budgetCalculationClientFallback.QuoteBudget,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	cloned := proto.Clone(req.GetDetails()).(*returnsv1.LintRestockDecisionsRequest_Details)
	quoteBudgetRequest := &budgetingv1.QuoteBudgetRequest{
		Context: req.GetContext(),
		Budget: &budgetingv1.Budget{
			Funding: &budgetingv1.Budget_Card{Card: &budgetingv1.BudgetCardFunding{
				Token: compareReturnRequestOptionsReply.GetReturnRequest().GetCard().GetToken(),
			}},
			Holder: &budgetingv1.ForecastRef{
				DisplayName: compareReturnRequestOptionsReply.GetReturnRequest().GetCounterparty().GetDisplayName(),
			},
			Contact: &commonv1.ContactInfo{PreferredChannel: cloned.GetContact().GetPreferredChannel()},
		},
	}
	quoteBudgetReply, err := route(ctx, quoteBudgetRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.budgeting.v1.BudgetCalculationService/QuoteBudget")
	}
	lintInspectionsRequest := &returnsv1.LintInspectionsRequest{
		Context: req.GetContext(),
		Inspection: &returnsv1.Inspection{
			Requester: &commonv1.ContactInfo{
				Email: compareReturnRequestOptionsReply.GetReturnRequest().GetCounterparty().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedBy: compareReturnRequestOptionsReply.GetReturnRequest().GetAudit().GetCreatedBy(),
			},
		},
	}
	lintInspectionsReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*returnsv1.LintInspectionsResponse, error) {
		return s.inspectionValidationClient.LintInspections(ctx, lintInspectionsRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.InspectionValidationService/LintInspections")
	}
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "returns.lint_restock_decisions.suggest_search_queries",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"filter.region":     compareReturnRequestOptionsReply.GetReturnRequest().GetBillingAddress().GetRegion(),
			"context.tenant_id": req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	_ = lintInspectionsReply
	return &returnsv1.LintRestockDecisionsResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: compareReturnRequestOptionsReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		RestockDecision: &returnsv1.RestockDecision{
			Owner: &identityv1.UserRef{
				Email: compareReturnRequestOptionsReply.GetReturnRequest().GetCounterparty().GetEmail(),
			},
			RestockDecisionId: req.GetRestockDecisionId(),
			Priority:          req.GetRestockDecision().GetPriority(),
		},
		Outcome: &returnsv1.LintRestockDecisionsResponse_Outcome{Status: quoteBudgetReply.GetResult().GetStatus()},
	}, nil
}

// GetRestockDecisionRules returns a restock decision.
func (s RestockDecisionValidationServer) GetRestockDecisionRules(ctx context.Context, req *returnsv1.GetRestockDecisionRulesRequest) (*returnsv1.GetRestockDecisionRulesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "restock_decision", req.GetRestockDecisionId(), "get_restock_decision_rules"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get restock decision rules", "tenant", req.GetContext().GetTenantId())
	return &returnsv1.GetRestockDecisionRulesResponse{
		RestockDecision: &returnsv1.RestockDecision{RestockDecisionId: req.GetRestockDecisionId()},
		Result:          &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// restockDecisionValidationServerNormalizeRestockDecisionRecordSearchQueryVersionStep carries the request of one step
// of NormalizeRestockDecision and, once it ran, its reply.
type restockDecisionValidationServerNormalizeRestockDecisionRecordSearchQueryVersionStep struct {
	request *searchv1.RecordSearchQueryVersionRequest
	reply   *searchv1.RecordSearchQueryVersionResponse
}

func (s RestockDecisionValidationServer) runNormalizeRestockDecisionRecordSearchQueryVersion(ctx context.Context, step *restockDecisionValidationServerNormalizeRestockDecisionRecordSearchQueryVersionStep) error {
	reply, err := s.searchQueryHistoryClient.RecordSearchQueryVersion(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
