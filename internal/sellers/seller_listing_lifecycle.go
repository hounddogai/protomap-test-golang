package sellers

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// SellerListingLifecycleServer implements `acme.sellers.v1.SellerListingLifecycleService`.
// Moves a seller listing between the states of its lifecycle.
type SellerListingLifecycleServer struct {
	sellersv1.UnimplementedSellerListingLifecycleServiceServer
	store                           *Store
	log                             *slog.Logger
	clients                         *registry.Registry
	giftCardWorkflowPort            giftCardWorkflowPort
	conn                            *grpc.ClientConn
	sellerListingReportingOnce      sync.Once
	sellerListingReportingCached    sellersv1.SellerListingReportingServiceClient
	preferenceCalculationClient     customersv1.PreferenceCalculationServiceClient
	preferenceValidationClient      customersv1.PreferenceValidationServiceClient
	sellerScorecardValidationClient sellersv1.SellerScorecardValidationServiceClient
	sellerClient                    sellersv1.SellerServiceClient
	watchlistCalculationClient      fraudv1.WatchlistCalculationServiceClient
	sellerListingQueryClient        sellersv1.SellerListingQueryServiceClient
	giftCardApprovalClient          giftcardsv1.GiftCardApprovalServiceClient
	sellerListingReportingClient    sellersv1.SellerListingReportingServiceClient
	userValidationClient            identityv1.UserValidationServiceClient
	sellerPort                      sellerPort
}

// NewSellerListingLifecycleServer wires a SellerListingLifecycleServer.
func NewSellerListingLifecycleServer(deps platform.Deps) *SellerListingLifecycleServer {
	return &SellerListingLifecycleServer{
		store:                           NewStore(deps.DB),
		log:                             deps.Logger.With("service", "acme.sellers.v1.SellerListingLifecycleService"),
		clients:                         deps.Clients,
		giftCardWorkflowPort:            newGiftCardWorkflowPort(deps),
		conn:                            deps.Conn,
		preferenceCalculationClient:     customersv1.NewPreferenceCalculationServiceClient(deps.Conn),
		preferenceValidationClient:      customersv1.NewPreferenceValidationServiceClient(deps.Conn),
		sellerScorecardValidationClient: sellersv1.NewSellerScorecardValidationServiceClient(deps.Conn),
		sellerClient:                    sellersv1.NewSellerServiceClient(deps.Conn),
		watchlistCalculationClient:      fraudv1.NewWatchlistCalculationServiceClient(deps.Conn),
		sellerListingQueryClient:        sellersv1.NewSellerListingQueryServiceClient(deps.Conn),
		giftCardApprovalClient:          giftcardsv1.NewGiftCardApprovalServiceClient(deps.Conn),
		sellerListingReportingClient:    sellersv1.NewSellerListingReportingServiceClient(deps.Conn),
		userValidationClient:            identityv1.NewUserValidationServiceClient(deps.Conn),
		sellerPort:                      newSellerPort(deps),
	}
}

// ActivateSellerListing activates a seller listing.
func (s *SellerListingLifecycleServer) ActivateSellerListing(ctx context.Context, req *sellersv1.ActivateSellerListingRequest) (*sellersv1.ActivateSellerListingResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "seller_listing", req.GetSellerListingId(), "activate_seller_listing"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "activate seller listing", "tenant", req.GetContext().GetTenantId())
	sessionValidation, ok := s.clients.Get("acme.identity.v1.SessionValidationService").(identityv1.SessionValidationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.identity.v1.SessionValidationService")
	}
	cloned := proto.Clone(req.GetDetails()).(*sellersv1.ActivateSellerListingRequest_Details)
	lintSessionsRequest := &identityv1.LintSessionsRequest{
		Context: req.GetContext(),
		Session: &identityv1.Session{
			Requester: &commonv1.ContactInfo{
				Phone: cloned.GetNotifyContact().GetPhone(),
				Name:  req.GetDetails().GetNotifyContact().GetName(),
			},
			Origin: &identityv1.Session_Customer{Customer: &identityv1.ApiCredentialRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			}},
		},
	}
	lintSessionsReply, err := sessionValidation.LintSessions(ctx, lintSessionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionValidationService/LintSessions")
	}
	retryGiftCardWorkflowRequest := &giftcardsv1.RetryGiftCardWorkflowRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.RetryGiftCardWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Name: req.GetDetails().GetNotifyContact().GetName()},
		},
	}
	retryGiftCardWorkflowReply, err := s.giftCardWorkflowPort.RetryGiftCardWorkflow(ctx, retryGiftCardWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardWorkflowService/RetryGiftCardWorkflow")
	}
	_ = retryGiftCardWorkflowReply
	return &sellersv1.ActivateSellerListingResponse{
		Outcome: &sellersv1.ActivateSellerListingResponse_Outcome{
			EffectiveAt: lintSessionsReply.GetOutcome().GetEffectiveAt(),
		},
		SellerListing: &sellersv1.SellerListing{
			SellerListingId: req.GetSellerListingId(),
			Owner:           &identityv1.UserRef{Email: req.GetDetails().GetNotifyContact().GetEmail()},
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// SuspendSellerListing suspends a seller listing.
func (s *SellerListingLifecycleServer) SuspendSellerListing(ctx context.Context, req *sellersv1.SuspendSellerListingRequest) (*sellersv1.SuspendSellerListingResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "seller_listing", req.GetSellerListingId(), "suspend_seller_listing"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "suspend seller listing", "tenant", req.GetContext().GetTenantId())
	scheduleSellerListingReportRequest := &sellersv1.ScheduleSellerListingReportRequest{
		Context:         req.GetContext(),
		SellerListingId: req.GetSellerListingId(),
	}
	scheduleSellerListingReportReply, err := s.sellerListingReportingLazy().ScheduleSellerListingReport(ctx, scheduleSellerListingReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingReportingService/ScheduleSellerListingReport")
	}
	comparePreferenceOptionsRequest := &customersv1.ComparePreferenceOptionsRequest{
		Context: req.GetContext(),
		Preference: &customersv1.Preference{
			Owner: &identityv1.UserRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
		},
	}
	type comparePreferenceOptionsResult struct {
		reply *customersv1.ComparePreferenceOptionsResponse
		err   error
	}
	comparePreferenceOptionsResults := make(chan comparePreferenceOptionsResult, 1)
	go func() {
		reply, err := s.preferenceCalculationClient.ComparePreferenceOptions(ctx, comparePreferenceOptionsRequest)
		comparePreferenceOptionsResults <- comparePreferenceOptionsResult{reply, err}
	}()
	var comparePreferenceOptionsReply *customersv1.ComparePreferenceOptionsResponse
	select {
	case result := <-comparePreferenceOptionsResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.customers.v1.PreferenceCalculationService/ComparePreferenceOptions")
		}
		comparePreferenceOptionsReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &sellersv1.SuspendSellerListingResponse{
		Result: &commonv1.OperationResult{
			Status:        scheduleSellerListingReportReply.GetResult().GetStatus(),
			CorrelationId: comparePreferenceOptionsReply.GetResult().GetCorrelationId(),
		},
		SellerListing: &sellersv1.SellerListing{SellerListingId: req.GetSellerListingId()},
		Outcome:       &sellersv1.SuspendSellerListingResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// ReinstateSellerListing reinstates a seller listing.
func (s *SellerListingLifecycleServer) ReinstateSellerListing(ctx context.Context, req *sellersv1.ReinstateSellerListingRequest) (*sellersv1.ReinstateSellerListingResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "seller_listing", req.GetSellerListingId(), "reinstate_seller_listing"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "reinstate seller listing", "tenant", req.GetContext().GetTenantId())
	normalizePreferenceRequest := buildNormalizePreferenceRequestForReinstateSellerListing(req)
	normalizePreferenceReply, err := s.preferenceValidationClient.NormalizePreference(ctx, normalizePreferenceRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceValidationService/NormalizePreference")
	}
	source := req.GetDetails().GetNewOwner().ProtoReflect()
	displayName := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("display_name"))).String()
	normalizeSellerScorecardRequest := &sellersv1.NormalizeSellerScorecardRequest{
		Context: req.GetContext(),
		SellerScorecard: &sellersv1.SellerScorecard{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: normalizePreferenceReply.GetPreference().GetAudit().GetUpdatedBy(),
			},
			Counterparty: &customersv1.CustomerRef{DisplayName: displayName},
		},
		Details: &sellersv1.NormalizeSellerScorecardRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetNewOwner().GetEmail()},
		},
	}
	normalizeSellerScorecardReply, err := grpcx.Call(ctx, s.sellerScorecardValidationClient.NormalizeSellerScorecard, normalizeSellerScorecardRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardValidationService/NormalizeSellerScorecard")
	}
	return &sellersv1.ReinstateSellerListingResponse{
		SellerListing: &sellersv1.SellerListing{
			Owner:           normalizePreferenceReply.GetPreference().GetOwner(),
			SellerListingId: req.GetSellerListingId(),
		},
		Outcome: &sellersv1.ReinstateSellerListingResponse_Outcome{
			Status:      normalizeSellerScorecardReply.GetOutcome().GetStatus(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// TerminateSellerListing terminates a seller listing.
func (s *SellerListingLifecycleServer) TerminateSellerListing(ctx context.Context, req *sellersv1.TerminateSellerListingRequest) (*sellersv1.TerminateSellerListingResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "seller_listing", req.GetSellerListingId(), "terminate_seller_listing"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "terminate seller listing", "tenant", req.GetContext().GetTenantId())
	createSellerRequest := &sellersv1.CreateSellerRequest{
		Context: req.GetContext(),
		Seller: &sellersv1.Seller{
			Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetNewOwner().GetEmail()},
		},
	}
	send := s.createSellerSender(createSellerRequest)
	createSellerReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerService/CreateSeller")
	}
	simulateWatchlistRequest := &fraudv1.SimulateWatchlistRequest{
		Context: req.GetContext(),
		Watchlist: &fraudv1.Watchlist{
			Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
				Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
					Manager: &identityv1.UserRef{Email: req.GetDetails().GetNewOwner().GetEmail()},
					Location: &commonv1.PostalAddress{
						Lines: createSellerReply.GetSeller().GetPrimaryAddress().GetLines(),
					},
				}},
			}},
		},
	}
	send2 := s.simulateWatchlistSender(simulateWatchlistRequest)
	simulateWatchlistReply, err := send2(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.WatchlistCalculationService/SimulateWatchlist")
	}
	sellerPayoutScheduleValidation, err := registry.Lookup[sellersv1.SellerPayoutScheduleValidationServiceClient](s.clients, "acme.sellers.v1.SellerPayoutScheduleValidationService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	previewSellerPayoutScheduleRequest := &sellersv1.PreviewSellerPayoutScheduleRequest{
		Context: req.GetContext(),
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
			Participants: []*sellersv1.SellerPayoutScheduleParticipant{&sellersv1.SellerPayoutScheduleParticipant{
				Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetNotifyContact().GetEmail()},
			}},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: simulateWatchlistReply.GetWatchlist().GetAudit().GetUpdatedAt(),
			},
		},
		Details: &sellersv1.PreviewSellerPayoutScheduleRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetNewOwner().GetEmail()},
		},
	}
	previewSellerPayoutScheduleReply, err := sellerPayoutScheduleValidation.PreviewSellerPayoutSchedule(ctx, previewSellerPayoutScheduleRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerPayoutScheduleValidationService/PreviewSellerPayoutSchedule")
	}
	lookupSellerListingReply, err := s.sellerListingQueryClient.LookupSellerListing(ctx, &sellersv1.LookupSellerListingRequest{
		Context:         req.GetContext(),
		SellerListingId: req.GetSellerListingId(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingQueryService/LookupSellerListing")
	}
	return &sellersv1.TerminateSellerListingResponse{
		Result: &commonv1.OperationResult{
			Message:      createSellerReply.GetResult().GetMessage(),
			ReferenceIds: simulateWatchlistReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		SellerListing: &sellersv1.SellerListing{
			Owner: &identityv1.UserRef{
				UserId: simulateWatchlistReply.GetWatchlist().GetManager().GetUserId(),
			},
			Audit: &commonv1.AuditInfo{
				Etag: previewSellerPayoutScheduleReply.GetSellerPayoutSchedule().GetAudit().GetEtag(),
			},
			Status:          lookupSellerListingReply.GetSellerListing().GetStatus(),
			SellerListingId: req.GetSellerListingId(),
		},
		Outcome: &sellersv1.TerminateSellerListingResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// RenewSellerListing renews a seller listing.
func (s *SellerListingLifecycleServer) RenewSellerListing(ctx context.Context, req *sellersv1.RenewSellerListingRequest) (*sellersv1.RenewSellerListingResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "seller_listing", req.GetSellerListingId(), "renew_seller_listing"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "renew seller listing", "tenant", req.GetContext().GetTenantId())
	step := &sellerListingLifecycleServerRenewSellerListingListPendingGiftCardsStep{request: &giftcardsv1.ListPendingGiftCardsRequest{
		Context: req.GetContext(),
		Filter: &giftcardsv1.ListPendingGiftCardsRequest_Filter{
			CreatedAfter: req.GetDetails().GetEffectiveAt(),
			OwnerId:      req.GetDetails().GetNewOwner().GetUserId(),
		},
	}}
	if err := s.runRenewSellerListingListPendingGiftCards(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardApprovalService/ListPendingGiftCards")
	}
	listPendingGiftCardsReply := step.reply
	return &sellersv1.RenewSellerListingResponse{
		Result: &commonv1.OperationResult{
			Details: listPendingGiftCardsReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		SellerListing: &sellersv1.SellerListing{SellerListingId: req.GetSellerListingId()},
		Outcome:       &sellersv1.RenewSellerListingResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// ExpireSellerListing expires a seller listing.
func (s *SellerListingLifecycleServer) ExpireSellerListing(ctx context.Context, req *sellersv1.ExpireSellerListingRequest) (*sellersv1.ExpireSellerListingResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "seller_listing", req.GetSellerListingId(), "expire_seller_listing"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "expire seller listing", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetDetails()).(*sellersv1.ExpireSellerListingRequest_Details)
	detailsCache := cache.New[string, *sellersv1.ExpireSellerListingRequest_Details](1)
	detailsCache.Put(req.GetContext().GetTraceId(), req.GetDetails())
	cachedDetails, _ := detailsCache.Get(req.GetContext().GetTraceId())
	streamSellerListingMetricsStream, err := s.sellerListingReportingClient.StreamSellerListingMetrics(ctx, &sellersv1.StreamSellerListingMetricsRequest{
		Context: req.GetContext(),
		Subscriber: &commonv1.ContactInfo{
			Name:             cloned.GetNotifyContact().GetName(),
			PreferredChannel: req.GetDetails().GetNotifyContact().GetPreferredChannel(),
			Email:            req.GetDetails().GetNewOwner().GetEmail(),
			Phone:            cachedDetails.GetNotifyContact().GetPhone(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingReportingService/StreamSellerListingMetrics")
	}
	for {
		received, err := streamSellerListingMetricsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingReportingService/StreamSellerListingMetrics")
		}
		_ = received
	}
	return &sellersv1.ExpireSellerListingResponse{
		SellerListing: &sellersv1.SellerListing{SellerListingId: req.GetSellerListingId()},
		Outcome:       &sellersv1.ExpireSellerListingResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
		Result:        &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetSellerListingLifecycle returns a seller listing.
func (s *SellerListingLifecycleServer) GetSellerListingLifecycle(ctx context.Context, req *sellersv1.GetSellerListingLifecycleRequest) (*sellersv1.GetSellerListingLifecycleResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetSellerListingLifecycle", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "seller_listing", req.GetSellerListingId(), "get_seller_listing_lifecycle"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get seller listing lifecycle", "tenant", req.GetContext().GetTenantId())
	return &sellersv1.GetSellerListingLifecycleResponse{
		SellerListing: &sellersv1.SellerListing{SellerListingId: req.GetSellerListingId()},
		Result:        &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ScheduleSellerListingExpiry schedules a seller listing.
func (s *SellerListingLifecycleServer) ScheduleSellerListingExpiry(ctx context.Context, req *sellersv1.ScheduleSellerListingExpiryRequest) (*sellersv1.ScheduleSellerListingExpiryResponse, error) {
	if err := s.store.SaveSellerListing(ctx, req.GetContext().GetTenantId(), req.GetSellerListing()); err != nil {
		return nil, grpcx.Wrap(err, "save seller listing")
	}
	s.log.InfoContext(ctx, "schedule seller listing expiry", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSellerListing().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetSellerListing()).(*sellersv1.SellerListing)
	cloned2 := proto.Clone(req.GetDetails()).(*sellersv1.ScheduleSellerListingExpiryRequest_Details)
	packed, err := anypb.New(req.GetSellerListing())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	shareSellerListingReportRequest := &sellersv1.ShareSellerListingReportRequest{}
	shareSellerListingReportRequest.Context = req.GetContext()
	shareSellerListingReportRequest.Details = &sellersv1.ShareSellerListingReportRequest_Details{
		Recipient: &commonv1.ContactInfo{
			Email:            cloned.GetOwner().GetEmail(),
			Phone:            req.GetDetails().GetNotify().GetPhone(),
			PreferredChannel: cloned2.GetNotify().GetPreferredChannel(),
			Name:             req.GetDetails().GetNotify().GetName(),
		},
		Payload: packed,
	}
	shareSellerListingReportReply, err := s.sellerListingReportingClient.ShareSellerListingReport(ctx, shareSellerListingReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingReportingService/ShareSellerListingReport")
	}
	var region string
	switch choice := req.GetSellerListing().GetLocator().(type) {
	case *sellersv1.SellerListing_Address:
		region = choice.Address.GetRegion()
	case *sellersv1.SellerListing_FacilityCode:
		region = choice.FacilityCode
	}
	lintUsersReply, err := s.userValidationClient.LintUsers(ctx, &identityv1.LintUsersRequest{
		Context: req.GetContext(),
		User:    &identityv1.User{PrimaryAddress: &commonv1.PostalAddress{Region: region}},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserValidationService/LintUsers")
	}
	batchGetSellersRequest := &sellersv1.BatchGetSellersRequest{Context: req.GetContext()}
	batchGetSellersReply, err := s.sellerPort.BatchGetSellers(ctx, batchGetSellersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerService/BatchGetSellers")
	}
	return &sellersv1.ScheduleSellerListingExpiryResponse{
		Outcome: &sellersv1.ScheduleSellerListingExpiryResponse_Outcome{
			NextSteps: shareSellerListingReportReply.GetOutcome().GetNextSteps(),
			Message:   lintUsersReply.GetOutcome().GetMessage(),
			Status:    batchGetSellersReply.GetResult().GetStatus(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: shareSellerListingReportReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		SellerListing: &sellersv1.SellerListing{
			SellerListingId: req.GetSellerListingId(),
			Specs:           req.GetSellerListing().GetSpecs(),
		},
	}, nil
}

// sellerListingReportingLazy dials acme.sellers.v1.SellerListingReportingService the first time a handler needs it.
func (s *SellerListingLifecycleServer) sellerListingReportingLazy() sellersv1.SellerListingReportingServiceClient {
	s.sellerListingReportingOnce.Do(func() { s.sellerListingReportingCached = sellersv1.NewSellerListingReportingServiceClient(s.conn) })
	return s.sellerListingReportingCached
}

// createSellerSender returns a function that sends one request to acme.sellers.v1.SellerService/CreateSeller.
func (s *SellerListingLifecycleServer) createSellerSender(request *sellersv1.CreateSellerRequest) func(context.Context) (*sellersv1.CreateSellerResponse, error) {
	return func(ctx context.Context) (*sellersv1.CreateSellerResponse, error) {
		return s.sellerClient.CreateSeller(ctx, request)
	}
}

// simulateWatchlistSender returns a function that sends one request to
// acme.fraud.v1.WatchlistCalculationService/SimulateWatchlist.
func (s *SellerListingLifecycleServer) simulateWatchlistSender(request *fraudv1.SimulateWatchlistRequest) func(context.Context) (*fraudv1.SimulateWatchlistResponse, error) {
	return func(ctx context.Context) (*fraudv1.SimulateWatchlistResponse, error) {
		return s.watchlistCalculationClient.SimulateWatchlist(ctx, request)
	}
}

// sellerListingLifecycleServerRenewSellerListingListPendingGiftCardsStep carries the request of one step of
// RenewSellerListing and, once it ran, its reply.
type sellerListingLifecycleServerRenewSellerListingListPendingGiftCardsStep struct {
	request *giftcardsv1.ListPendingGiftCardsRequest
	reply   *giftcardsv1.ListPendingGiftCardsResponse
}

func (s *SellerListingLifecycleServer) runRenewSellerListingListPendingGiftCards(ctx context.Context, step *sellerListingLifecycleServerRenewSellerListingListPendingGiftCardsStep) error {
	reply, err := s.giftCardApprovalClient.ListPendingGiftCards(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
