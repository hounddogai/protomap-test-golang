package sellers

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	documentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/documents/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	venuesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/venues/v1"
	workplacesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/workplaces/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// SellerPayoutScheduleWorkflowServer implements `acme.sellers.v1.SellerPayoutScheduleWorkflowService`.
// Runs the long-lived workflow that takes a seller payout schedule from start to finish.
type SellerPayoutScheduleWorkflowServer struct {
	*sellerPayoutScheduleWorkflowHandlers
}

// sellerPayoutScheduleWorkflowHandlers holds the handlers of SellerPayoutScheduleWorkflowServer, with what they use.
type sellerPayoutScheduleWorkflowHandlers struct {
	sellersv1.UnimplementedSellerPayoutScheduleWorkflowServiceServer
	store                                *Store
	log                                  *slog.Logger
	preferenceCalculationClient          customersv1.PreferenceCalculationServiceClient
	venueValidationClient                venuesv1.VenueValidationServiceClient
	customerSegmentValidationClient      customersv1.CustomerSegmentValidationServiceClient
	sellerCalculationClient              sellersv1.SellerCalculationServiceClient
	clients                              *registry.Registry
	giftCardTransactionCalculationClient giftcardsv1.GiftCardTransactionCalculationServiceClient
	sellerPayoutScheduleReportingClient  sellersv1.SellerPayoutScheduleReportingServiceClient
	sellerScorecardApprovalClient        sellersv1.SellerScorecardApprovalServiceClient
	ledgerAccountClient                  ledgerv1.LedgerAccountServiceClient
	giftCardBatchApprovalClient          giftcardsv1.GiftCardBatchApprovalServiceClient
	buildingQueryClient                  workplacesv1.BuildingQueryServiceClient
	sellerApprovalClient                 sellersv1.SellerApprovalServiceClient
	sellerListingApprovalClient          sellersv1.SellerListingApprovalServiceClient
	tenantAdminClient                    identityv1.TenantAdminServiceClient
	conn                                 *grpc.ClientConn
	bus                                  *eventbus.Bus
	meteredSellerScorecardApproval       meteredSellerScorecardApproval
	searchQueryQueryClient               searchv1.SearchQueryQueryServiceClient
	sellerApprovalPort                   sellerApprovalPort
}

// NewSellerPayoutScheduleWorkflowServer wires a SellerPayoutScheduleWorkflowServer.
func NewSellerPayoutScheduleWorkflowServer(deps platform.Deps) *SellerPayoutScheduleWorkflowServer {
	return &SellerPayoutScheduleWorkflowServer{sellerPayoutScheduleWorkflowHandlers: &sellerPayoutScheduleWorkflowHandlers{
		store:                                NewStore(deps.DB),
		log:                                  deps.Logger.With("service", "acme.sellers.v1.SellerPayoutScheduleWorkflowService"),
		preferenceCalculationClient:          customersv1.NewPreferenceCalculationServiceClient(deps.Conn),
		venueValidationClient:                venuesv1.NewVenueValidationServiceClient(deps.Conn),
		customerSegmentValidationClient:      customersv1.NewCustomerSegmentValidationServiceClient(deps.Conn),
		sellerCalculationClient:              sellersv1.NewSellerCalculationServiceClient(deps.Conn),
		clients:                              deps.Clients,
		giftCardTransactionCalculationClient: giftcardsv1.NewGiftCardTransactionCalculationServiceClient(deps.Conn),
		sellerPayoutScheduleReportingClient:  sellersv1.NewSellerPayoutScheduleReportingServiceClient(deps.Conn),
		sellerScorecardApprovalClient:        sellersv1.NewSellerScorecardApprovalServiceClient(deps.Conn),
		ledgerAccountClient:                  ledgerv1.NewLedgerAccountServiceClient(deps.Conn),
		giftCardBatchApprovalClient:          giftcardsv1.NewGiftCardBatchApprovalServiceClient(deps.Conn),
		buildingQueryClient:                  workplacesv1.NewBuildingQueryServiceClient(deps.Conn),
		sellerApprovalClient:                 sellersv1.NewSellerApprovalServiceClient(deps.Conn),
		sellerListingApprovalClient:          sellersv1.NewSellerListingApprovalServiceClient(deps.Conn),
		tenantAdminClient:                    identityv1.NewTenantAdminServiceClient(deps.Conn),
		conn:                                 deps.Conn,
		bus:                                  deps.Bus,
		meteredSellerScorecardApproval:       meteredSellerScorecardApproval{SellerScorecardApprovalServiceClient: sellersv1.NewSellerScorecardApprovalServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		searchQueryQueryClient:               searchv1.NewSearchQueryQueryServiceClient(deps.Conn),
		sellerApprovalPort:                   newSellerApprovalPort(deps),
	}}
}

// StartSellerPayoutScheduleWorkflow starts a seller payout schedule.
func (s *sellerPayoutScheduleWorkflowHandlers) StartSellerPayoutScheduleWorkflow(ctx context.Context, req *sellersv1.StartSellerPayoutScheduleWorkflowRequest) (*sellersv1.StartSellerPayoutScheduleWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveSellerPayoutSchedule(ctx, req.GetContext().GetTenantId(), req.GetSellerPayoutSchedule()); err != nil {
		return nil, grpcx.Wrap(err, "save seller payout schedule")
	}
	s.log.InfoContext(ctx, "start seller payout schedule workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSellerPayoutSchedule().GetAudit().GetEtag())
	jobs := make(chan *sellersv1.SellerPayoutScheduleParticipant)
	failures := make(chan error, 4)
	workCtx, stopWork := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for participant := range jobs {
				if _, err := s.preferenceCalculationClient.CalculatePreference(workCtx, &customersv1.CalculatePreferenceRequest{
					Context: req.GetContext(),
					Preference: &customersv1.Preference{
						Owner: &identityv1.UserRef{
							Email:       participant.GetContact().GetEmail(),
							DisplayName: req.GetSellerPayoutSchedule().GetOrganizer().GetDisplayName(),
						},
						Audit: &commonv1.AuditInfo{Etag: req.GetSellerPayoutSchedule().GetAudit().GetEtag()},
					},
				}); err != nil {
					failures <- err
					stopWork()
					return
				}
			}
		}()
	}
	for _, queued := range req.GetSellerPayoutSchedule().GetParticipants() {
		if workCtx.Err() != nil {
			break
		}
		select {
		case jobs <- queued:
		case <-workCtx.Done():
		}
	}
	close(jobs)
	wg.Wait()
	stopWork()
	close(failures)
	if err := <-failures; err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceCalculationService/CalculatePreference")
	}
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := sellerPayoutScheduleWorkflowHandlersNormalizeVenueSteps[mode]
	normalizeVenueRequest := &venuesv1.NormalizeVenueRequest{
		Context: req.GetContext(),
		Venue: &venuesv1.Venue{
			Audit: &commonv1.AuditInfo{Etag: req.GetSellerPayoutSchedule().GetAudit().GetEtag()},
		},
		Details: &venuesv1.NormalizeVenueRequest_Details{
			Contact: &commonv1.ContactInfo{Phone: req.GetDetails().GetNotify().GetPhone()},
		},
	}
	normalizeVenueReply, err := run(s, ctx, normalizeVenueRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.venues.v1.VenueValidationService/NormalizeVenue")
	}
	checkCustomerSegmentEligibilityRequest := &customersv1.CheckCustomerSegmentEligibilityRequest{
		Context: req.GetContext(),
		CustomerSegment: &customersv1.CustomerSegment{
			Owner: &identityv1.UserRef{
				DisplayName: normalizeVenueReply.GetVenue().GetOwner().GetDisplayName(),
			},
		},
	}
	checkCustomerSegmentEligibilityReply, err := customersv1.CustomerSegmentValidationServiceClient.CheckCustomerSegmentEligibility(s.customerSegmentValidationClient, ctx, checkCustomerSegmentEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentValidationService/CheckCustomerSegmentEligibility")
	}
	for participant2 := range iterx.Each(req.GetSellerPayoutSchedule().GetParticipants(), func(candidate *sellersv1.SellerPayoutScheduleParticipant) bool { return candidate != nil }) {
		if _, err := s.sellerCalculationClient.ExplainSellerCalculation(ctx, &sellersv1.ExplainSellerCalculationRequest{
			Context: req.GetContext(),
			Details: &sellersv1.ExplainSellerCalculationRequest_Details{
				Contact: &commonv1.ContactInfo{
					Phone:            participant2.GetContact().GetPhone(),
					PreferredChannel: participant2.GetContact().GetPreferredChannel(),
				},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerCalculationService/ExplainSellerCalculation")
		}
	}
	return &sellersv1.StartSellerPayoutScheduleWorkflowResponse{
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
			Participants: []*sellersv1.SellerPayoutScheduleParticipant{&sellersv1.SellerPayoutScheduleParticipant{
				Contact: &commonv1.ContactInfo{Email: normalizeVenueReply.GetVenue().GetOwner().GetEmail()},
			}},
			SellerPayoutScheduleId: req.GetSellerPayoutScheduleId(),
			Title:                  req.GetSellerPayoutSchedule().GetTitle(),
		},
		Result: &commonv1.OperationResult{
			Message: normalizeVenueReply.GetOutcome().GetMessage(),
			Status:  "ok",
		},
		Outcome: &sellersv1.StartSellerPayoutScheduleWorkflowResponse_Outcome{
			Status: checkCustomerSegmentEligibilityReply.GetOutcome().GetStatus(),
		},
	}, nil
}

// AdvanceSellerPayoutScheduleWorkflow advances a seller payout schedule.
func (s *sellerPayoutScheduleWorkflowHandlers) AdvanceSellerPayoutScheduleWorkflow(ctx context.Context, req *sellersv1.AdvanceSellerPayoutScheduleWorkflowRequest) (*sellersv1.AdvanceSellerPayoutScheduleWorkflowResponse, error) {
	if err := s.store.SaveSellerPayoutSchedule(ctx, req.GetContext().GetTenantId(), req.GetSellerPayoutSchedule()); err != nil {
		return nil, grpcx.Wrap(err, "save seller payout schedule")
	}
	s.log.InfoContext(ctx, "advance seller payout schedule workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSellerPayoutSchedule().GetAudit().GetEtag())
	preferenceCalculation, ok := s.clients.Get("acme.customers.v1.PreferenceCalculationService").(customersv1.PreferenceCalculationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.customers.v1.PreferenceCalculationService")
	}
	calculatePreferenceRequest := &customersv1.CalculatePreferenceRequest{
		Context: req.GetContext(),
		Preference: &customersv1.Preference{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: req.GetSellerPayoutSchedule().GetAudit().GetUpdatedBy(),
				CreatedAt: req.GetSellerPayoutSchedule().GetAudit().GetCreatedAt(),
			},
			Owner: &identityv1.UserRef{
				DisplayName: req.GetSellerPayoutSchedule().GetOrganizer().GetDisplayName(),
			},
		},
	}
	calculatePreferenceReply, err := preferenceCalculation.CalculatePreference(ctx, calculatePreferenceRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceCalculationService/CalculatePreference")
	}
	quoteGiftCardTransactionRequest := &giftcardsv1.QuoteGiftCardTransactionRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.QuoteGiftCardTransactionRequest_Details{
			Destination: &commonv1.PostalAddress{
				Recipient: req.GetSellerPayoutSchedule().GetLocation().GetRecipient(),
				Locality:  req.GetSellerPayoutSchedule().GetLocation().GetLocality(),
			},
		},
		Reason: calculatePreferenceReply.GetPreference().GetBlock().GetReason(),
	}
	send := s.quoteGiftCardTransactionSender(quoteGiftCardTransactionRequest)
	quoteGiftCardTransactionReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionCalculationService/QuoteGiftCardTransaction")
	}
	streamSellerPayoutScheduleMetricsStream, err := s.sellerPayoutScheduleReportingClient.StreamSellerPayoutScheduleMetrics(ctx, &sellersv1.StreamSellerPayoutScheduleMetricsRequest{Context: req.GetContext()})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerPayoutScheduleReportingService/StreamSellerPayoutScheduleMetrics")
	}
	for {
		received, err := streamSellerPayoutScheduleMetricsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerPayoutScheduleReportingService/StreamSellerPayoutScheduleMetrics")
		}
		_ = received
	}
	return &sellersv1.AdvanceSellerPayoutScheduleWorkflowResponse{
		Outcome: &sellersv1.AdvanceSellerPayoutScheduleWorkflowResponse_Outcome{
			Status:  calculatePreferenceReply.GetResult().GetStatus(),
			Message: quoteGiftCardTransactionReply.GetResult().GetMessage(),
		},
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
			SellerPayoutScheduleId: req.GetSellerPayoutScheduleId(),
			Window:                 req.GetSellerPayoutSchedule().GetWindow(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CancelSellerPayoutScheduleWorkflow cancels a seller payout schedule.
func (s *sellerPayoutScheduleWorkflowHandlers) CancelSellerPayoutScheduleWorkflow(ctx context.Context, req *sellersv1.CancelSellerPayoutScheduleWorkflowRequest) (*sellersv1.CancelSellerPayoutScheduleWorkflowResponse, error) {
	if err := s.store.SaveSellerPayoutSchedule(ctx, req.GetContext().GetTenantId(), req.GetSellerPayoutSchedule()); err != nil {
		return nil, grpcx.Wrap(err, "save seller payout schedule")
	}
	s.log.InfoContext(ctx, "cancel seller payout schedule workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSellerPayoutSchedule().GetAudit().GetEtag())
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*sellersv1.EscalateSellerScorecardResponse, len(req.GetSellerPayoutSchedule().GetParticipants()))
	for i, participant := range req.GetSellerPayoutSchedule().GetParticipants() {
		group.Go(func() error {
			reply, err := s.sellerScorecardApprovalClient.EscalateSellerScorecard(groupCtx, &sellersv1.EscalateSellerScorecardRequest{
				Context: req.GetContext(),
				Details: &sellersv1.EscalateSellerScorecardRequest_Details{
					Approver: &identityv1.UserRef{
						Email:       participant.GetContact().GetEmail(),
						DisplayName: req.GetSellerPayoutSchedule().GetOrganizer().GetDisplayName(),
					},
				},
				SellerScorecard: &sellersv1.SellerScorecard{
					Audit: &commonv1.AuditInfo{
						CreatedAt: req.GetSellerPayoutSchedule().GetAudit().GetCreatedAt(),
						CreatedBy: req.GetSellerPayoutSchedule().GetAudit().GetCreatedBy(),
					},
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
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardApprovalService/EscalateSellerScorecard")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	ctx = tenancy.With(ctx, "email", req.GetDetails().GetNotify().GetEmail())
	watchLedgerAccountsStream, err := s.ledgerAccountClient.WatchLedgerAccounts(ctx, &ledgerv1.WatchLedgerAccountsRequest{
		Context: req.GetContext(),
		Subscriber: &commonv1.ContactInfo{
			Email: tenancy.Value(ctx, "email"),
			Name:  req.GetDetails().GetNotify().GetName(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountService/WatchLedgerAccounts")
	}
	for {
		received, err := watchLedgerAccountsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountService/WatchLedgerAccounts")
		}
		_ = received
	}
	listPendingGiftCardBatchesRequest := &giftcardsv1.ListPendingGiftCardBatchesRequest{
		Context: req.GetContext(),
		Filter:  &giftcardsv1.ListPendingGiftCardBatchesRequest_Filter{Labels: req.GetSellerPayoutSchedule().GetMetadata()},
	}
	var listPendingGiftCardBatchesReply *giftcardsv1.ListPendingGiftCardBatchesResponse
	if err := pipeline.Run(ctx, "acme.giftcards.v1.GiftCardBatchApprovalService/ListPendingGiftCardBatches", func(ctx context.Context) error {
		answer, err := s.giftCardBatchApprovalClient.ListPendingGiftCardBatches(ctx, listPendingGiftCardBatchesRequest)
		listPendingGiftCardBatchesReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchApprovalService/ListPendingGiftCardBatches")
	}
	summarizeBuildingsReply, err := s.buildingQueryClient.SummarizeBuildings(ctx, &workplacesv1.SummarizeBuildingsRequest{
		Context:     req.GetContext(),
		Reason:      req.GetReason(),
		Annotations: req.GetAnnotations(),
		Details: &workplacesv1.SummarizeBuildingsRequest_Details{
			Period:  req.GetSellerPayoutSchedule().GetWindow(),
			Filters: listPendingGiftCardBatchesReply.GetResult().GetDetails(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.workplaces.v1.BuildingQueryService/SummarizeBuildings")
	}
	_ = summarizeBuildingsReply
	return &sellersv1.CancelSellerPayoutScheduleWorkflowResponse{
		Result: &commonv1.OperationResult{
			Message: listPendingGiftCardBatchesReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Outcome: &sellersv1.CancelSellerPayoutScheduleWorkflowResponse_Outcome{
			Message: listPendingGiftCardBatchesReply.GetResult().GetMessage(),
		},
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
			SellerPayoutScheduleId: req.GetSellerPayoutScheduleId(),
			Location:               req.GetSellerPayoutSchedule().GetLocation(),
		},
	}, nil
}

// RetrySellerPayoutScheduleWorkflow retrys a seller payout schedule.
func (s *sellerPayoutScheduleWorkflowHandlers) RetrySellerPayoutScheduleWorkflow(ctx context.Context, req *sellersv1.RetrySellerPayoutScheduleWorkflowRequest) (*sellersv1.RetrySellerPayoutScheduleWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RetrySellerPayoutScheduleWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SaveSellerPayoutSchedule(ctx, req.GetContext().GetTenantId(), req.GetSellerPayoutSchedule()); err != nil {
		return nil, grpcx.Wrap(err, "save seller payout schedule")
	}
	s.log.InfoContext(ctx, "retry seller payout schedule workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSellerPayoutSchedule().GetAudit().GetEtag())
	listPendingSellersRequest := &sellersv1.ListPendingSellersRequest{
		Context: req.GetContext(),
		Filter: &sellersv1.ListPendingSellersRequest_Filter{
			Region: req.GetSellerPayoutSchedule().GetLocation().GetRegion(),
		},
	}
	var listPendingSellersReply *sellersv1.ListPendingSellersResponse
	if err := pipeline.Run(ctx, "acme.sellers.v1.SellerApprovalService/ListPendingSellers", func(ctx context.Context) error {
		answer, err := s.sellerApprovalClient.ListPendingSellers(ctx, listPendingSellersRequest)
		listPendingSellersReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerApprovalService/ListPendingSellers")
	}
	return &sellersv1.RetrySellerPayoutScheduleWorkflowResponse{
		Result: &commonv1.OperationResult{
			Status:       listPendingSellersReply.GetResult().GetStatus(),
			ReferenceIds: listPendingSellersReply.GetResult().GetReferenceIds(),
		},
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
			SellerPayoutScheduleId: req.GetSellerPayoutScheduleId(),
			Window:                 req.GetSellerPayoutSchedule().GetWindow(),
		},
	}, nil
}

// PauseSellerPayoutScheduleWorkflow pauses a seller payout schedule.
func (s *sellerPayoutScheduleWorkflowHandlers) PauseSellerPayoutScheduleWorkflow(ctx context.Context, req *sellersv1.PauseSellerPayoutScheduleWorkflowRequest) (*sellersv1.PauseSellerPayoutScheduleWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveSellerPayoutSchedule(ctx, req.GetContext().GetTenantId(), req.GetSellerPayoutSchedule()); err != nil {
		return nil, grpcx.Wrap(err, "save seller payout schedule")
	}
	s.log.InfoContext(ctx, "pause seller payout schedule workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSellerPayoutSchedule().GetAudit().GetEtag())
	submitSellerListingRequest := &sellersv1.SubmitSellerListingRequest{}
	submitSellerListingRequest.Context = req.GetContext()
	submitSellerListingRequest.SellerListing = &sellersv1.SellerListing{
		Locator: &sellersv1.SellerListing_Address{Address: &commonv1.PostalAddress{
			Locality:  req.GetSellerPayoutSchedule().GetLocation().GetLocality(),
			Recipient: req.GetSellerPayoutSchedule().GetLocation().GetRecipient(),
			Lines:     req.GetSellerPayoutSchedule().GetLocation().GetLines(),
		}},
		Owner: &identityv1.UserRef{
			DisplayName: req.GetSellerPayoutSchedule().GetOrganizer().GetDisplayName(),
		},
	}
	submitSellerListingReply, err := s.sellerListingApprovalClient.SubmitSellerListing(ctx, submitSellerListingRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingApprovalService/SubmitSellerListing")
	}
	reassignTenantReply, err := s.tenantAdminClient.ReassignTenant(ctx, &identityv1.ReassignTenantRequest{
		Context:  req.GetContext(),
		TenantId: req.GetContext().GetTenantId(),
		Details: &identityv1.ReassignTenantRequest_Details{
			Assignee: &identityv1.UserRef{
				Email:  submitSellerListingReply.GetSellerListing().GetOwner().GetEmail(),
				UserId: submitSellerListingReply.GetSellerListing().GetOwner().GetUserId(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantAdminService/ReassignTenant")
	}
	return &sellersv1.PauseSellerPayoutScheduleWorkflowResponse{
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
			Audit: &commonv1.AuditInfo{
				CreatedBy: submitSellerListingReply.GetSellerListing().GetAudit().GetCreatedBy(),
			},
			Location: &commonv1.PostalAddress{
				Region: reassignTenantReply.GetTenant().GetLocation().GetRegion(),
			},
			SellerPayoutScheduleId: req.GetSellerPayoutScheduleId(),
			Window:                 req.GetSellerPayoutSchedule().GetWindow(),
		},
		Outcome: &sellersv1.PauseSellerPayoutScheduleWorkflowResponse_Outcome{
			ReferenceId: reassignTenantReply.GetOutcome().GetReferenceId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ResumeSellerPayoutScheduleWorkflow resumes a seller payout schedule.
func (s *sellerPayoutScheduleWorkflowHandlers) ResumeSellerPayoutScheduleWorkflow(ctx context.Context, req *sellersv1.ResumeSellerPayoutScheduleWorkflowRequest) (*sellersv1.ResumeSellerPayoutScheduleWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveSellerPayoutSchedule(ctx, req.GetContext().GetTenantId(), req.GetSellerPayoutSchedule()); err != nil {
		return nil, grpcx.Wrap(err, "save seller payout schedule")
	}
	s.log.InfoContext(ctx, "resume seller payout schedule workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSellerPayoutSchedule().GetAudit().GetEtag())
	delegateGiftCardBatchApprovalRequest := &giftcardsv1.DelegateGiftCardBatchApprovalRequest{
		Context: req.GetContext(),
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			Audit: &commonv1.AuditInfo{
				CreatedBy: req.GetSellerPayoutSchedule().GetAudit().GetCreatedBy(),
			},
			BillingAddress: &commonv1.PostalAddress{
				CountryCode: req.GetSellerPayoutSchedule().GetLocation().GetCountryCode(),
			},
		},
	}
	delegateGiftCardBatchApprovalReply := new(giftcardsv1.DelegateGiftCardBatchApprovalResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", giftcardsGiftCardBatchApprovalServiceName, "DelegateGiftCardBatchApproval"), delegateGiftCardBatchApprovalRequest, delegateGiftCardBatchApprovalReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchApprovalService/DelegateGiftCardBatchApproval")
	}
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "sellers.resume_seller_payout_schedule_workflow.reconcile_seller_scorecards",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"seller_scorecard.audit.updated_by": delegateGiftCardBatchApprovalReply.GetGiftCardBatch().GetAudit().GetUpdatedBy(),
			"context.tenant_id":                 req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	return &sellersv1.ResumeSellerPayoutScheduleWorkflowResponse{
		Outcome: &sellersv1.ResumeSellerPayoutScheduleWorkflowResponse_Outcome{
			EffectiveAt: delegateGiftCardBatchApprovalReply.GetOutcome().GetEffectiveAt(),
		},
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
			SellerPayoutScheduleId: req.GetSellerPayoutScheduleId(),
			Location:               req.GetSellerPayoutSchedule().GetLocation(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// InspectSellerPayoutScheduleWorkflow returns a seller payout schedule.
func (s *sellerPayoutScheduleWorkflowHandlers) InspectSellerPayoutScheduleWorkflow(ctx context.Context, req *sellersv1.InspectSellerPayoutScheduleWorkflowRequest) (*sellersv1.InspectSellerPayoutScheduleWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected InspectSellerPayoutScheduleWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "seller_payout_schedule", req.GetSellerPayoutScheduleId(), "inspect_seller_payout_schedule_workflow"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "inspect seller payout schedule workflow", "tenant", req.GetContext().GetTenantId())
	baggage := make(map[string]string, len(req.GetContext().GetBaggage()))
	for key, value := range req.GetContext().GetBaggage() {
		baggage[key] = value
	}
	summarizeDocumentsRequest := &documentsv1.SummarizeDocumentsRequest{
		Context: req.GetContext(),
		Details: &documentsv1.SummarizeDocumentsRequest_Details{Filters: baggage},
	}
	summarizeDocumentsReply := new(documentsv1.SummarizeDocumentsResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", documentsDocumentQueryServiceName, "SummarizeDocuments"), summarizeDocumentsRequest, summarizeDocumentsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.documents.v1.DocumentQueryService/SummarizeDocuments")
	}
	streamSellerPayoutScheduleMetricsStream, err := s.sellerPayoutScheduleReportingClient.StreamSellerPayoutScheduleMetrics(ctx, &sellersv1.StreamSellerPayoutScheduleMetricsRequest{
		Context:    req.GetContext(),
		Subscriber: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		Filter: &sellersv1.StreamSellerPayoutScheduleMetricsRequest_Filter{
			Labels:  summarizeDocumentsReply.GetResult().GetDetails(),
			OwnerId: req.GetSellerPayoutScheduleId(),
		},
		Since: req.GetContext().GetDeadline(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerPayoutScheduleReportingService/StreamSellerPayoutScheduleMetrics")
	}
	for {
		received, err := streamSellerPayoutScheduleMetricsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerPayoutScheduleReportingService/StreamSellerPayoutScheduleMetrics")
		}
		_ = received
	}
	return &sellersv1.InspectSellerPayoutScheduleWorkflowResponse{
		Result: &commonv1.OperationResult{
			Details: summarizeDocumentsReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{SellerPayoutScheduleId: req.GetSellerPayoutScheduleId()},
	}, nil
}

// ListSellerPayoutScheduleWorkflows lists seller payout schedules a page at a time.
func (s *sellerPayoutScheduleWorkflowHandlers) ListSellerPayoutScheduleWorkflows(ctx context.Context, req *sellersv1.ListSellerPayoutScheduleWorkflowsRequest) (*sellersv1.ListSellerPayoutScheduleWorkflowsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ListSellerPayoutScheduleWorkflows", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "list seller payout schedule workflows", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "region", req.GetFilter().GetRegion())
	ctx = tenancy.With(ctx, "email", req.GetContext().GetActor().GetEmail())
	recallSellerScorecardRequest := &sellersv1.RecallSellerScorecardRequest{
		Context: req.GetContext(),
		SellerScorecard: &sellersv1.SellerScorecard{
			BillingAddress: &commonv1.PostalAddress{Region: tenancy.Value(ctx, "region")},
			IssuedAt:       req.GetFilter().GetCreatedAfter(),
		},
		Details: &sellersv1.RecallSellerScorecardRequest_Details{
			Approver: &identityv1.UserRef{
				Email:  tenancy.Value(ctx, "email"),
				UserId: req.GetContext().GetActor().GetUserId(),
			},
		},
	}
	recallSellerScorecardReply, err := s.meteredSellerScorecardApproval.RecallSellerScorecard(ctx, recallSellerScorecardRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardApprovalService/RecallSellerScorecard")
	}
	comparePreferenceOptionsRequest := &customersv1.ComparePreferenceOptionsRequest{
		Context: req.GetContext(),
		Details: &customersv1.ComparePreferenceOptionsRequest_Details{
			Destination: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
		Preference: &customersv1.Preference{
			Audit: &commonv1.AuditInfo{
				CreatedAt: recallSellerScorecardReply.GetSellerScorecard().GetAudit().GetCreatedAt(),
			},
		},
	}
	comparePreferenceOptionsReply, err := grpcx.Call(ctx, s.preferenceCalculationClient.ComparePreferenceOptions, comparePreferenceOptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceCalculationService/ComparePreferenceOptions")
	}
	return &sellersv1.ListSellerPayoutScheduleWorkflowsResponse{
		SellerPayoutSchedules: []*sellersv1.SellerPayoutSchedule{&sellersv1.SellerPayoutSchedule{
			Audit: &commonv1.AuditInfo{
				CreatedBy: recallSellerScorecardReply.GetSellerScorecard().GetAudit().GetCreatedBy(),
				Etag:      comparePreferenceOptionsReply.GetPreference().GetAudit().GetEtag(),
			},
			Participants: []*sellersv1.SellerPayoutScheduleParticipant{&sellersv1.SellerPayoutScheduleParticipant{
				Contact: &commonv1.ContactInfo{
					Email: recallSellerScorecardReply.GetSellerScorecard().GetCounterparty().GetEmail(),
				},
			}},
		}},
		Result: &commonv1.OperationResult{
			Details: comparePreferenceOptionsReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}

// SignalSellerPayoutScheduleWorkflow signals a seller payout schedule.
func (s *sellerPayoutScheduleWorkflowHandlers) SignalSellerPayoutScheduleWorkflow(ctx context.Context, req *sellersv1.SignalSellerPayoutScheduleWorkflowRequest) (*sellersv1.SignalSellerPayoutScheduleWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveSellerPayoutSchedule(ctx, req.GetContext().GetTenantId(), req.GetSellerPayoutSchedule()); err != nil {
		return nil, grpcx.Wrap(err, "save seller payout schedule")
	}
	s.log.InfoContext(ctx, "signal seller payout schedule workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSellerPayoutSchedule().GetAudit().GetEtag())
	suggestSearchQueriesRequest := buildSuggestSearchQueriesRequestForSignalSellerPayoutScheduleWorkflow(req)
	suggestSearchQueriesReply, err := s.searchQueryQueryClient.SuggestSearchQueries(ctx, suggestSearchQueriesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryQueryService/SuggestSearchQueries")
	}
	escalateSellerRequest := &sellersv1.EscalateSellerRequest{
		Context: req.GetContext(),
		Details: &sellersv1.EscalateSellerRequest_Details{
			Approver: &identityv1.UserRef{
				DisplayName: req.GetSellerPayoutSchedule().GetOrganizer().GetDisplayName(),
			},
		},
		Seller: &sellersv1.Seller{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetNotify().GetPreferredChannel(),
			},
			RelationshipManager: &identityv1.UserRef{
				Email: req.GetSellerPayoutSchedule().GetOrganizer().GetEmail(),
			},
		},
	}
	escalateSellerReply, err := s.sellerApprovalPort.EscalateSeller(ctx, escalateSellerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerApprovalService/EscalateSeller")
	}
	return &sellersv1.SignalSellerPayoutScheduleWorkflowResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: suggestSearchQueriesReply.GetResult().GetCorrelationId(),
			Details:       escalateSellerReply.GetResult().GetDetails(),
			Status:        "ok",
		},
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
			Location: &commonv1.PostalAddress{
				PostalCode: escalateSellerReply.GetSeller().GetPrimaryAddress().GetPostalCode(),
			},
			SellerPayoutScheduleId: req.GetSellerPayoutScheduleId(),
			Metadata:               req.GetSellerPayoutSchedule().GetMetadata(),
		},
	}, nil
}

// sellerPayoutScheduleWorkflowHandlersNormalizeVenueSteps are the ways to run
// acme.venues.v1.VenueValidationService/NormalizeVenue, by name.
var sellerPayoutScheduleWorkflowHandlersNormalizeVenueSteps = map[string]func(*sellerPayoutScheduleWorkflowHandlers, context.Context, *venuesv1.NormalizeVenueRequest) (*venuesv1.NormalizeVenueResponse, error){
	"default": (*sellerPayoutScheduleWorkflowHandlers).normalizeVenueStep,
	"audited": (*sellerPayoutScheduleWorkflowHandlers).normalizeVenueAuditedStep,
}

func (s *sellerPayoutScheduleWorkflowHandlers) normalizeVenueStep(ctx context.Context, request *venuesv1.NormalizeVenueRequest) (*venuesv1.NormalizeVenueResponse, error) {
	return s.venueValidationClient.NormalizeVenue(ctx, request)
}

func (s *sellerPayoutScheduleWorkflowHandlers) normalizeVenueAuditedStep(ctx context.Context, request *venuesv1.NormalizeVenueRequest) (*venuesv1.NormalizeVenueResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.venues.v1.VenueValidationService/NormalizeVenue")
	return s.venueValidationClient.NormalizeVenue(ctx, request)
}

// quoteGiftCardTransactionSender returns a function that sends one request to
// acme.giftcards.v1.GiftCardTransactionCalculationService/QuoteGiftCardTransaction.
func (s *sellerPayoutScheduleWorkflowHandlers) quoteGiftCardTransactionSender(request *giftcardsv1.QuoteGiftCardTransactionRequest) func(context.Context) (*giftcardsv1.QuoteGiftCardTransactionResponse, error) {
	return func(ctx context.Context) (*giftcardsv1.QuoteGiftCardTransactionResponse, error) {
		return s.giftCardTransactionCalculationClient.QuoteGiftCardTransaction(ctx, request)
	}
}
