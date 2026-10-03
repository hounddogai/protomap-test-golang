package cart

import (
	"context"
	"fmt"
	"log/slog"

	cartv1 "github.com/hounddogai/protomap-test-golang/gen/acme/cart/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	venuesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/venues/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// CartRuleLifecycleServer implements `acme.cart.v1.CartRuleLifecycleService`.
// Moves a cart rule between the states of its lifecycle.
type CartRuleLifecycleServer struct {
	cartv1.UnimplementedCartRuleLifecycleServiceServer
	store                        *Store
	log                          *slog.Logger
	savedListCalculationPort     savedListCalculationPort
	fraudRuleWorkflowPort        fraudRuleWorkflowPort
	savedListValidationClient    cartv1.SavedListValidationServiceClient
	fraudRuleWorkflowClient      fraudv1.FraudRuleWorkflowServiceClient
	searchQueryQueryClient       searchv1.SearchQueryQueryServiceClient
	payoutApprovalClient         paymentsv1.PayoutApprovalServiceClient
	cartCalculationClient        cartv1.CartCalculationServiceClient
	venueClient                  venuesv1.VenueServiceClient
	venueClientFallback          venuesv1.VenueServiceClient
	cartValidationClient         cartv1.CartValidationServiceClient
	trackingEventReportingClient shippingv1.TrackingEventReportingServiceClient
	synonymValidationClient      searchv1.SynonymValidationServiceClient
	clients                      *registry.Registry
	conn                         *grpc.ClientConn
}

// NewCartRuleLifecycleServer wires a CartRuleLifecycleServer.
func NewCartRuleLifecycleServer(deps platform.Deps) *CartRuleLifecycleServer {
	return &CartRuleLifecycleServer{
		store:                        NewStore(deps.DB),
		log:                          deps.Logger.With("service", "acme.cart.v1.CartRuleLifecycleService"),
		savedListCalculationPort:     newSavedListCalculationPort(deps),
		fraudRuleWorkflowPort:        newFraudRuleWorkflowPort(deps),
		savedListValidationClient:    cartv1.NewSavedListValidationServiceClient(deps.Conn),
		fraudRuleWorkflowClient:      fraudv1.NewFraudRuleWorkflowServiceClient(deps.Conn),
		searchQueryQueryClient:       searchv1.NewSearchQueryQueryServiceClient(deps.Conn),
		payoutApprovalClient:         paymentsv1.NewPayoutApprovalServiceClient(deps.Conn),
		cartCalculationClient:        cartv1.NewCartCalculationServiceClient(deps.Conn),
		venueClient:                  venuesv1.NewVenueServiceClient(deps.Conn),
		venueClientFallback:          venuesv1.NewVenueServiceClient(deps.Conn),
		cartValidationClient:         cartv1.NewCartValidationServiceClient(deps.Conn),
		trackingEventReportingClient: shippingv1.NewTrackingEventReportingServiceClient(deps.Conn),
		synonymValidationClient:      searchv1.NewSynonymValidationServiceClient(deps.Conn),
		clients:                      deps.Clients,
		conn:                         deps.Conn,
	}
}

// ActivateCartRule activates a cart rule.
func (s *CartRuleLifecycleServer) ActivateCartRule(ctx context.Context, req *cartv1.ActivateCartRuleRequest) (*cartv1.ActivateCartRuleResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ActivateCartRule", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "cart_rule", req.GetCartRuleId(), "activate_cart_rule"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "activate cart rule", "tenant", req.GetContext().GetTenantId())
	recalculateSavedListsRequest := &cartv1.RecalculateSavedListsRequest{
		Context: req.GetContext(),
		SavedList: &cartv1.SavedList{
			Counterparty: &cartv1.CartRuleRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
				CartRuleId:  req.GetCartRuleId(),
			},
		},
		Annotations: req.GetAnnotations(),
		Reason:      req.GetReason(),
	}
	recalculateSavedListsReply, err := s.savedListCalculationPort.RecalculateSavedLists(ctx, recalculateSavedListsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.SavedListCalculationService/RecalculateSavedLists")
	}
	advanceFraudRuleWorkflowRequest := &fraudv1.AdvanceFraudRuleWorkflowRequest{
		Context: req.GetContext(),
		Details: &fraudv1.AdvanceFraudRuleWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Name: req.GetDetails().GetNotifyContact().GetName()},
		},
	}
	advanceFraudRuleWorkflowReply, err := s.fraudRuleWorkflowPort.AdvanceFraudRuleWorkflow(ctx, advanceFraudRuleWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.FraudRuleWorkflowService/AdvanceFraudRuleWorkflow")
	}
	normalizeSavedListReply, err := s.savedListValidationClient.NormalizeSavedList(ctx, &cartv1.NormalizeSavedListRequest{
		Context: req.GetContext(),
		SavedList: &cartv1.SavedList{
			Settlement: &cartv1.SavedList_Bank{Bank: &cartv1.SavedListBankSettlement{
				Bic: recalculateSavedListsReply.GetSavedList().GetBank().GetBic(),
			}},
			Counterparty: &cartv1.CartRuleRef{
				DisplayName: advanceFraudRuleWorkflowReply.GetFraudRule().GetOwner().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedBy: recalculateSavedListsReply.GetSavedList().GetAudit().GetCreatedBy(),
				CreatedAt: advanceFraudRuleWorkflowReply.GetFraudRule().GetAudit().GetCreatedAt(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.SavedListValidationService/NormalizeSavedList")
	}
	return &cartv1.ActivateCartRuleResponse{
		CartRule: &cartv1.CartRule{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: recalculateSavedListsReply.GetSavedList().GetAudit().GetUpdatedAt(),
				CreatedAt: advanceFraudRuleWorkflowReply.GetFraudRule().GetAudit().GetCreatedAt(),
			},
			CartRuleId: req.GetCartRuleId(),
		},
		Result: &commonv1.OperationResult{
			Message: recalculateSavedListsReply.GetResult().GetMessage(),
			Status:  normalizeSavedListReply.GetResult().GetStatus(),
		},
		Outcome: &cartv1.ActivateCartRuleResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// SuspendCartRule suspends a cart rule.
func (s *CartRuleLifecycleServer) SuspendCartRule(ctx context.Context, req *cartv1.SuspendCartRuleRequest) (*cartv1.SuspendCartRuleResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SuspendCartRule", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "cart_rule", req.GetCartRuleId(), "suspend_cart_rule"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "suspend cart rule", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "phone", req.GetDetails().GetNotifyContact().GetPhone())
	resumeFraudRuleWorkflowRequest := &fraudv1.ResumeFraudRuleWorkflowRequest{
		Context: req.GetContext(),
		Details: &fraudv1.ResumeFraudRuleWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				Phone:            tenancy.Value(ctx, "phone"),
				Email:            req.GetDetails().GetNotifyContact().GetEmail(),
				PreferredChannel: req.GetDetails().GetNotifyContact().GetPreferredChannel(),
			},
		},
	}
	resumeFraudRuleWorkflowReply, err := grpcx.Call(ctx, s.fraudRuleWorkflowClient.ResumeFraudRuleWorkflow, resumeFraudRuleWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.FraudRuleWorkflowService/ResumeFraudRuleWorkflow")
	}
	browseSearchQueriesReply, err := s.searchQueryQueryClient.BrowseSearchQueries(ctx, &searchv1.BrowseSearchQueriesRequest{
		Context: req.GetContext(),
		Filter: &searchv1.BrowseSearchQueriesRequest_Filter{
			CreatedAfter: resumeFraudRuleWorkflowReply.GetFraudRule().GetAudit().GetCreatedAt(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryQueryService/BrowseSearchQueries")
	}
	return &cartv1.SuspendCartRuleResponse{
		CartRule: &cartv1.CartRule{
			Version:    resumeFraudRuleWorkflowReply.GetFraudRule().GetVersion(),
			Scopes:     resumeFraudRuleWorkflowReply.GetFraudRule().GetScopes(),
			CartRuleId: req.GetCartRuleId(),
		},
		Outcome: &cartv1.SuspendCartRuleResponse_Outcome{
			Message:     browseSearchQueriesReply.GetResult().GetMessage(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: browseSearchQueriesReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// TerminateCartRule terminates a cart rule.
func (s *CartRuleLifecycleServer) TerminateCartRule(ctx context.Context, req *cartv1.TerminateCartRuleRequest) (*cartv1.TerminateCartRuleResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected TerminateCartRule", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "cart_rule", req.GetCartRuleId(), "terminate_cart_rule"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "terminate cart rule", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetDetails()).(*cartv1.TerminateCartRuleRequest_Details)
	recallPayoutRequest := &paymentsv1.RecallPayoutRequest{}
	recallPayoutRequest.Context = req.GetContext()
	recallPayoutRequest.Details = &paymentsv1.RecallPayoutRequest_Details{
		Approver: &identityv1.UserRef{Email: cloned.GetNewOwner().GetEmail()},
	}
	recallPayoutReply, err := s.payoutApprovalClient.RecallPayout(ctx, recallPayoutRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PayoutApprovalService/RecallPayout")
	}
	estimateCartReply, err := s.cartCalculationClient.EstimateCart(ctx, &cartv1.EstimateCartRequest{
		Context: req.GetContext(),
		Cart: &cartv1.Cart{
			Audit: &commonv1.AuditInfo{Etag: recallPayoutReply.GetPayout().GetAudit().GetEtag()},
			BillingAddress: &commonv1.PostalAddress{
				Lines: recallPayoutReply.GetPayout().GetBillingAddress().GetLines(),
			},
		},
		Details: &cartv1.EstimateCartRequest_Details{
			Destination: &commonv1.PostalAddress{
				Region: recallPayoutReply.GetPayout().GetBillingAddress().GetRegion(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartCalculationService/EstimateCart")
	}
	step := &cartRuleLifecycleServerTerminateCartRuleFindSearchQueriesStep{request: &searchv1.FindSearchQueriesRequest{
		Context: req.GetContext(),
		Filter: &searchv1.FindSearchQueriesRequest_Filter{
			Region:       estimateCartReply.GetCart().GetBillingAddress().GetRegion(),
			CreatedAfter: recallPayoutReply.GetPayout().GetAudit().GetCreatedAt(),
			OwnerId:      req.GetDetails().GetNewOwner().GetUserId(),
		},
	}}
	if err := s.runTerminateCartRuleFindSearchQueries(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryQueryService/FindSearchQueries")
	}
	findSearchQueriesReply := step.reply
	return &cartv1.TerminateCartRuleResponse{
		Outcome: &cartv1.TerminateCartRuleResponse_Outcome{
			Message:     recallPayoutReply.GetOutcome().GetMessage(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		CartRule: &cartv1.CartRule{
			Owner: &identityv1.UserRef{
				DisplayName: recallPayoutReply.GetPayout().GetCounterparty().GetDisplayName(),
			},
			CartRuleId: req.GetCartRuleId(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: estimateCartReply.GetResult().GetCorrelationId(),
			Details:       findSearchQueriesReply.GetResult().GetDetails(),
			Status:        "ok",
		},
	}, nil
}

// RenewCartRule renews a cart rule.
func (s *CartRuleLifecycleServer) RenewCartRule(ctx context.Context, req *cartv1.RenewCartRuleRequest) (*cartv1.RenewCartRuleResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "cart_rule", req.GetCartRuleId(), "renew_cart_rule"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "renew cart rule", "tenant", req.GetContext().GetTenantId())
	routes := map[string]func(context.Context, *venuesv1.CreateVenueRequest, ...grpc.CallOption) (*venuesv1.CreateVenueResponse, error){
		"primary":  s.venueClient.CreateVenue,
		"fallback": s.venueClientFallback.CreateVenue,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	createVenueRequest := &venuesv1.CreateVenueRequest{
		Context: req.GetContext(),
		Venue: &venuesv1.Venue{
			Owner: &identityv1.UserRef{
				UserId:      req.GetDetails().GetNewOwner().GetUserId(),
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
		},
	}
	createVenueReply, err := route(ctx, createVenueRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.venues.v1.VenueService/CreateVenue")
	}
	return &cartv1.RenewCartRuleResponse{
		CartRule: &cartv1.CartRule{
			Owner: &identityv1.UserRef{
				DisplayName: createVenueReply.GetVenue().GetOwner().GetDisplayName(),
			},
			CartRuleId: req.GetCartRuleId(),
		},
		Outcome: &cartv1.RenewCartRuleResponse_Outcome{
			Message:     createVenueReply.GetResult().GetMessage(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ExpireCartRule expires a cart rule.
func (s *CartRuleLifecycleServer) ExpireCartRule(ctx context.Context, req *cartv1.ExpireCartRuleRequest) (*cartv1.ExpireCartRuleResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "cart_rule", req.GetCartRuleId(), "expire_cart_rule"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "expire cart rule", "tenant", req.GetContext().GetTenantId())
	checkCartEligibilityRequest := &cartv1.CheckCartEligibilityRequest{
		Context: req.GetContext(),
		Details: &cartv1.CheckCartEligibilityRequest_Details{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetNotifyContact().GetPreferredChannel(),
				Email:            req.GetDetails().GetNewOwner().GetEmail(),
			},
		},
	}
	var checkCartEligibilityReply *cartv1.CheckCartEligibilityResponse
	if err := pipeline.Run(ctx, "acme.cart.v1.CartValidationService/CheckCartEligibility", func(ctx context.Context) error {
		answer, err := s.cartValidationClient.CheckCartEligibility(ctx, checkCartEligibilityRequest)
		checkCartEligibilityReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartValidationService/CheckCartEligibility")
	}
	scheduleTrackingEventReportRequest := &shippingv1.ScheduleTrackingEventReportRequest{
		Context: req.GetContext(),
		Reason:  req.GetReason(),
	}
	type scheduleTrackingEventReportResult struct {
		reply *shippingv1.ScheduleTrackingEventReportResponse
		err   error
	}
	scheduleTrackingEventReportResults := make(chan scheduleTrackingEventReportResult, 1)
	go func() {
		reply, err := s.trackingEventReportingClient.ScheduleTrackingEventReport(ctx, scheduleTrackingEventReportRequest)
		scheduleTrackingEventReportResults <- scheduleTrackingEventReportResult{reply, err}
	}()
	var scheduleTrackingEventReportReply *shippingv1.ScheduleTrackingEventReportResponse
	select {
	case result := <-scheduleTrackingEventReportResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.shipping.v1.TrackingEventReportingService/ScheduleTrackingEventReport")
		}
		scheduleTrackingEventReportReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := cartRuleLifecycleServerCheckSynonymEligibilitySteps[mode]
	checkSynonymEligibilityRequest := &searchv1.CheckSynonymEligibilityRequest{
		Context: req.GetContext(),
		Synonym: &searchv1.Synonym{
			Audit: &commonv1.AuditInfo{
				CreatedBy: checkCartEligibilityReply.GetCart().GetAudit().GetCreatedBy(),
				Etag:      checkCartEligibilityReply.GetCart().GetAudit().GetEtag(),
			},
			Owner: &identityv1.UserRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
		},
		Details: &searchv1.CheckSynonymEligibilityRequest_Details{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetNotifyContact().GetPreferredChannel(),
			},
		},
	}
	checkSynonymEligibilityReply, err := run(s, ctx, checkSynonymEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymValidationService/CheckSynonymEligibility")
	}
	previewCartRequest := &cartv1.PreviewCartRequest{
		Context: req.GetContext(),
		Details: &cartv1.PreviewCartRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetNotifyContact().GetEmail()},
		},
		Cart: &cartv1.Cart{
			Counterparty: &cartv1.SavedListRef{
				Number: checkCartEligibilityReply.GetCart().GetCounterparty().GetNumber(),
			},
		},
	}
	previewCartReply, err := cartv1.CartValidationServiceClient.PreviewCart(s.cartValidationClient, ctx, previewCartRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartValidationService/PreviewCart")
	}
	return &cartv1.ExpireCartRuleResponse{
		Outcome: &cartv1.ExpireCartRuleResponse_Outcome{
			Message:     checkCartEligibilityReply.GetOutcome().GetMessage(),
			ReferenceId: previewCartReply.GetOutcome().GetReferenceId(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: scheduleTrackingEventReportReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		CartRule: &cartv1.CartRule{
			Owner: &identityv1.UserRef{
				UserId: checkSynonymEligibilityReply.GetSynonym().GetOwner().GetUserId(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedBy: previewCartReply.GetCart().GetAudit().GetCreatedBy(),
			},
			CartRuleId: req.GetCartRuleId(),
		},
	}, nil
}

// GetCartRuleLifecycle returns a cart rule.
func (s *CartRuleLifecycleServer) GetCartRuleLifecycle(ctx context.Context, req *cartv1.GetCartRuleLifecycleRequest) (*cartv1.GetCartRuleLifecycleResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "cart_rule", req.GetCartRuleId(), "get_cart_rule_lifecycle"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get cart rule lifecycle", "tenant", req.GetContext().GetTenantId())
	payoutApproval, ok := s.clients.Get("acme.payments.v1.PayoutApprovalService").(paymentsv1.PayoutApprovalServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.payments.v1.PayoutApprovalService")
	}
	rejectPayoutRequest := &paymentsv1.RejectPayoutRequest{
		Context: req.GetContext(),
		Details: &paymentsv1.RejectPayoutRequest_Details{
			Approver: &identityv1.UserRef{
				Email:  req.GetContext().GetActor().GetEmail(),
				UserId: req.GetContext().GetActor().GetUserId(),
			},
		},
		Payout: &paymentsv1.Payout{
			Audit: &commonv1.AuditInfo{CreatedAt: req.GetContext().GetDeadline()},
			Lines: []*paymentsv1.PayoutLine{&paymentsv1.PayoutLine{Attributes: req.GetContext().GetBaggage()}},
		},
	}
	rejectPayoutReply, err := payoutApproval.RejectPayout(ctx, rejectPayoutRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PayoutApprovalService/RejectPayout")
	}
	lintSavedListsRequest := &cartv1.LintSavedListsRequest{
		Context: req.GetContext(),
		SavedList: &cartv1.SavedList{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: rejectPayoutReply.GetPayout().GetAudit().GetUpdatedBy(),
			},
			Settlement: &cartv1.SavedList_Bank{Bank: &cartv1.SavedListBankSettlement{Bic: rejectPayoutReply.GetPayout().GetBank().GetBic()}},
		},
	}
	lintSavedListsReply := new(cartv1.LintSavedListsResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", cartSavedListValidationServiceName, "LintSavedLists"), lintSavedListsRequest, lintSavedListsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.SavedListValidationService/LintSavedLists")
	}
	return &cartv1.GetCartRuleLifecycleResponse{
		CartRule: &cartv1.CartRule{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: rejectPayoutReply.GetPayout().GetAudit().GetUpdatedAt(),
				Etag:      lintSavedListsReply.GetSavedList().GetAudit().GetEtag(),
			},
			CartRuleId: req.GetCartRuleId(),
		},
		Result: &commonv1.OperationResult{Status: lintSavedListsReply.GetResult().GetStatus()},
	}, nil
}

// cartRuleLifecycleServerTerminateCartRuleFindSearchQueriesStep carries the request of one step of TerminateCartRule
// and, once it ran, its reply.
type cartRuleLifecycleServerTerminateCartRuleFindSearchQueriesStep struct {
	request *searchv1.FindSearchQueriesRequest
	reply   *searchv1.FindSearchQueriesResponse
}

func (s *CartRuleLifecycleServer) runTerminateCartRuleFindSearchQueries(ctx context.Context, step *cartRuleLifecycleServerTerminateCartRuleFindSearchQueriesStep) error {
	reply, err := s.searchQueryQueryClient.FindSearchQueries(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}

// cartRuleLifecycleServerCheckSynonymEligibilitySteps are the ways to run
// acme.search.v1.SynonymValidationService/CheckSynonymEligibility, by name.
var cartRuleLifecycleServerCheckSynonymEligibilitySteps = map[string]func(*CartRuleLifecycleServer, context.Context, *searchv1.CheckSynonymEligibilityRequest) (*searchv1.CheckSynonymEligibilityResponse, error){
	"default": (*CartRuleLifecycleServer).checkSynonymEligibilityStep,
	"audited": (*CartRuleLifecycleServer).checkSynonymEligibilityAuditedStep,
}

func (s *CartRuleLifecycleServer) checkSynonymEligibilityStep(ctx context.Context, request *searchv1.CheckSynonymEligibilityRequest) (*searchv1.CheckSynonymEligibilityResponse, error) {
	return s.synonymValidationClient.CheckSynonymEligibility(ctx, request)
}

func (s *CartRuleLifecycleServer) checkSynonymEligibilityAuditedStep(ctx context.Context, request *searchv1.CheckSynonymEligibilityRequest) (*searchv1.CheckSynonymEligibilityResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.search.v1.SynonymValidationService/CheckSynonymEligibility")
	return s.synonymValidationClient.CheckSynonymEligibility(ctx, request)
}
