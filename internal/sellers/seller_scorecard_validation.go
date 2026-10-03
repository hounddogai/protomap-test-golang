package sellers

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	documentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/documents/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	workplacesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/workplaces/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

// SellerScorecardValidationServer implements `acme.sellers.v1.SellerScorecardValidationService`.
// Checks seller scorecards against the rules before anything acts on them.
type SellerScorecardValidationServer struct {
	sellersv1.UnimplementedSellerScorecardValidationServiceServer
	store                                *Store
	log                                  *slog.Logger
	buildingClient                       workplacesv1.BuildingServiceClient
	searchIndexHistoryClient             searchv1.SearchIndexHistoryServiceClient
	meteredSellerScorecardCalculation    meteredSellerScorecardCalculation
	sellerScorecardCalculationClient     sellersv1.SellerScorecardCalculationServiceClient
	sellerPayoutScheduleValidationClient sellersv1.SellerPayoutScheduleValidationServiceClient
	conn                                 *grpc.ClientConn
	estimateSellerListingFn              func(context.Context, *sellersv1.EstimateSellerListingRequest, ...grpc.CallOption) (*sellersv1.EstimateSellerListingResponse, error)
	documentClient                       documentsv1.DocumentServiceClient
	giftCardCalculationOnce              sync.Once
	giftCardCalculationCached            giftcardsv1.GiftCardCalculationServiceClient
	clients                              *registry.Registry
	sellerListingCalculationClient       sellersv1.SellerListingCalculationServiceClient
	sellerScorecardCalculationOnce       sync.Once
	sellerScorecardCalculationCached     sellersv1.SellerScorecardCalculationServiceClient
}

// NewSellerScorecardValidationServer wires a SellerScorecardValidationServer.
func NewSellerScorecardValidationServer(deps platform.Deps) *SellerScorecardValidationServer {
	sellerListingCalculation := sellersv1.NewSellerListingCalculationServiceClient(deps.Conn)
	return &SellerScorecardValidationServer{
		store:                                NewStore(deps.DB),
		log:                                  deps.Logger.With("service", "acme.sellers.v1.SellerScorecardValidationService"),
		buildingClient:                       workplacesv1.NewBuildingServiceClient(deps.Conn),
		searchIndexHistoryClient:             searchv1.NewSearchIndexHistoryServiceClient(deps.Conn),
		meteredSellerScorecardCalculation:    meteredSellerScorecardCalculation{SellerScorecardCalculationServiceClient: sellersv1.NewSellerScorecardCalculationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		sellerScorecardCalculationClient:     sellersv1.NewSellerScorecardCalculationServiceClient(deps.Conn),
		sellerPayoutScheduleValidationClient: sellersv1.NewSellerPayoutScheduleValidationServiceClient(deps.Conn),
		conn:                                 deps.Conn,
		estimateSellerListingFn:              sellerListingCalculation.EstimateSellerListing,
		documentClient:                       documentsv1.NewDocumentServiceClient(deps.Conn),
		clients:                              deps.Clients,
		sellerListingCalculationClient:       sellersv1.NewSellerListingCalculationServiceClient(deps.Conn),
	}
}

// ValidateSellerScorecard validates a seller scorecard.
func (s *SellerScorecardValidationServer) ValidateSellerScorecard(ctx context.Context, req *sellersv1.ValidateSellerScorecardRequest) (*sellersv1.ValidateSellerScorecardResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveSellerScorecard(ctx, req.GetContext().GetTenantId(), req.GetSellerScorecard()); err != nil {
		return nil, grpcx.Wrap(err, "save seller scorecard")
	}
	s.log.InfoContext(ctx, "validate seller scorecard", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSellerScorecard().GetAudit().GetEtag())
	createBuildingRequest := &workplacesv1.CreateBuildingRequest{}
	createBuildingRequest.Context = req.GetContext()
	createBuildingRequest.Building = &workplacesv1.Building{
		Children: []*workplacesv1.Building{&workplacesv1.Building{
			Children: []*workplacesv1.Building{&workplacesv1.Building{
				Audit: &commonv1.AuditInfo{
					CreatedAt: req.GetSellerScorecard().GetAudit().GetCreatedAt(),
					CreatedBy: req.GetSellerScorecard().GetAudit().GetCreatedBy(),
				},
			}},
		}},
	}
	createBuildingReply, err := s.buildingClient.CreateBuilding(ctx, createBuildingRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.workplaces.v1.BuildingService/CreateBuilding")
	}
	ctx = tenancy.With(ctx, "region", req.GetSellerScorecard().GetBillingAddress().GetRegion())
	listSearchIndexVersionsRequest := &searchv1.ListSearchIndexVersionsRequest{
		Context: req.GetContext(),
		Filter: &searchv1.ListSearchIndexVersionsRequest_Filter{
			Region:       tenancy.Value(ctx, "region"),
			CreatedAfter: createBuildingReply.GetBuilding().GetAudit().GetCreatedAt(),
		},
	}
	type listSearchIndexVersionsResult struct {
		reply *searchv1.ListSearchIndexVersionsResponse
		err   error
	}
	listSearchIndexVersionsResults := make(chan listSearchIndexVersionsResult, 1)
	go func() {
		reply, err := s.searchIndexHistoryClient.ListSearchIndexVersions(ctx, listSearchIndexVersionsRequest)
		listSearchIndexVersionsResults <- listSearchIndexVersionsResult{reply, err}
	}()
	var listSearchIndexVersionsReply *searchv1.ListSearchIndexVersionsResponse
	select {
	case result := <-listSearchIndexVersionsResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.search.v1.SearchIndexHistoryService/ListSearchIndexVersions")
		}
		listSearchIndexVersionsReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &sellersv1.ValidateSellerScorecardResponse{
		SellerScorecard: &sellersv1.SellerScorecard{
			BillingAddress: &commonv1.PostalAddress{
				Lines:      createBuildingReply.GetBuilding().GetLocation().GetLines(),
				PostalCode: createBuildingReply.GetBuilding().GetLocation().GetPostalCode(),
			},
			SellerScorecardId: req.GetSellerScorecardId(),
			Counterparty:      req.GetSellerScorecard().GetCounterparty(),
		},
		Result: &commonv1.OperationResult{
			Status:  listSearchIndexVersionsReply.GetResult().GetStatus(),
			Message: listSearchIndexVersionsReply.GetResult().GetMessage(),
		},
	}, nil
}

// PreviewSellerScorecard previews a seller scorecard.
func (s *SellerScorecardValidationServer) PreviewSellerScorecard(ctx context.Context, req *sellersv1.PreviewSellerScorecardRequest) (*sellersv1.PreviewSellerScorecardResponse, error) {
	if err := s.store.SaveSellerScorecard(ctx, req.GetContext().GetTenantId(), req.GetSellerScorecard()); err != nil {
		return nil, grpcx.Wrap(err, "save seller scorecard")
	}
	s.log.InfoContext(ctx, "preview seller scorecard", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSellerScorecard().GetAudit().GetEtag())
	getSellerScorecardRatesRequest := &sellersv1.GetSellerScorecardRatesRequest{
		Context:           req.GetContext(),
		SellerScorecardId: req.GetSellerScorecard().GetSellerScorecardId(),
	}
	getSellerScorecardRatesReply, err := s.meteredSellerScorecardCalculation.GetSellerScorecardRates(ctx, getSellerScorecardRatesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardCalculationService/GetSellerScorecardRates")
	}
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := sellerScorecardValidationServerCalculateSellerScorecardSteps[mode]
	var iban string
	switch choice := req.GetSellerScorecard().GetSettlement().(type) {
	case *sellersv1.SellerScorecard_Card:
		iban = choice.Card.GetHolderName()
	case *sellersv1.SellerScorecard_Bank:
		iban = choice.Bank.GetIban()
	case *sellersv1.SellerScorecard_VoucherCode:
		iban = choice.VoucherCode
	}
	calculateSellerScorecardRequest := &sellersv1.CalculateSellerScorecardRequest{
		Context: req.GetContext(),
		SellerScorecard: &sellersv1.SellerScorecard{
			Settlement: &sellersv1.SellerScorecard_Bank{Bank: &sellersv1.SellerScorecardBankSettlement{Iban: iban}},
		},
	}
	calculateSellerScorecardReply, err := run(s, ctx, calculateSellerScorecardRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardCalculationService/CalculateSellerScorecard")
	}
	encoded, err := protojson.Marshal(req.GetSellerScorecard())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded sellersv1.SellerScorecard
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	validateSellerPayoutScheduleRequest := &sellersv1.ValidateSellerPayoutScheduleRequest{
		Context: req.GetContext(),
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
			Participants: []*sellersv1.SellerPayoutScheduleParticipant{&sellersv1.SellerPayoutScheduleParticipant{
				Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetContact().GetEmail()},
			}},
			Location: &commonv1.PostalAddress{
				CountryCode: (&decoded).GetBillingAddress().GetCountryCode(),
			},
			Audit: &commonv1.AuditInfo{
				Etag:      req.GetSellerScorecard().GetAudit().GetEtag(),
				CreatedAt: calculateSellerScorecardReply.GetSellerScorecard().GetAudit().GetCreatedAt(),
			},
		},
	}
	validateSellerPayoutScheduleReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*sellersv1.ValidateSellerPayoutScheduleResponse, error) {
		return s.sellerPayoutScheduleValidationClient.ValidateSellerPayoutSchedule(ctx, validateSellerPayoutScheduleRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerPayoutScheduleValidationService/ValidateSellerPayoutSchedule")
	}
	return &sellersv1.PreviewSellerScorecardResponse{
		SellerScorecard: &sellersv1.SellerScorecard{
			BillingAddress: &commonv1.PostalAddress{
				Locality: getSellerScorecardRatesReply.GetSellerScorecard().GetBillingAddress().GetLocality(),
			},
			SellerScorecardId: req.GetSellerScorecardId(),
			Notes:             req.GetSellerScorecard().GetNotes(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: calculateSellerScorecardReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Outcome: &sellersv1.PreviewSellerScorecardResponse_Outcome{
			Message: validateSellerPayoutScheduleReply.GetResult().GetMessage(),
		},
	}, nil
}

// NormalizeSellerScorecard normalizes a seller scorecard.
func (s *SellerScorecardValidationServer) NormalizeSellerScorecard(ctx context.Context, req *sellersv1.NormalizeSellerScorecardRequest) (*sellersv1.NormalizeSellerScorecardResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveSellerScorecard(ctx, req.GetContext().GetTenantId(), req.GetSellerScorecard()); err != nil {
		return nil, grpcx.Wrap(err, "save seller scorecard")
	}
	s.log.InfoContext(ctx, "normalize seller scorecard", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSellerScorecard().GetAudit().GetEtag())
	quoteSellerRequest := &sellersv1.QuoteSellerRequest{
		Context: req.GetContext(),
		Seller: &sellersv1.Seller{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: req.GetSellerScorecard().GetAudit().GetUpdatedBy(),
			},
		},
	}
	quoteSellerReply, err := grpcx.NewClient(s.conn, sellersv1.NewSellerCalculationServiceClient).QuoteSeller(ctx, quoteSellerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerCalculationService/QuoteSeller")
	}
	estimateSellerListingRequest := &sellersv1.EstimateSellerListingRequest{
		Context: req.GetContext(),
		SellerListing: &sellersv1.SellerListing{
			Locator: &sellersv1.SellerListing_Address{Address: &commonv1.PostalAddress{
				PostalCode: quoteSellerReply.GetSeller().GetPrimaryAddress().GetPostalCode(),
			}},
		},
	}
	estimateSellerListingReply, err := s.estimateSellerListingFn(ctx, estimateSellerListingRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingCalculationService/EstimateSellerListing")
	}
	return &sellersv1.NormalizeSellerScorecardResponse{
		SellerScorecard: &sellersv1.SellerScorecard{
			Audit: &commonv1.AuditInfo{
				CreatedAt: quoteSellerReply.GetSeller().GetAudit().GetCreatedAt(),
				UpdatedAt: estimateSellerListingReply.GetSellerListing().GetAudit().GetUpdatedAt(),
			},
			BillingAddress: &commonv1.PostalAddress{
				CountryCode: estimateSellerListingReply.GetSellerListing().GetAddress().GetCountryCode(),
			},
			SellerScorecardId: req.GetSellerScorecardId(),
			DueAt:             req.GetSellerScorecard().GetDueAt(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ExplainSellerScorecardDecision explains a seller scorecard.
func (s *SellerScorecardValidationServer) ExplainSellerScorecardDecision(ctx context.Context, req *sellersv1.ExplainSellerScorecardDecisionRequest) (*sellersv1.ExplainSellerScorecardDecisionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ExplainSellerScorecardDecision", "error", err)
		return nil, err
	}
	if err := s.store.SaveSellerScorecard(ctx, req.GetContext().GetTenantId(), req.GetSellerScorecard()); err != nil {
		return nil, grpcx.Wrap(err, "save seller scorecard")
	}
	s.log.InfoContext(ctx, "explain seller scorecard decision", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSellerScorecard().GetAudit().GetEtag())
	listDocumentsRequest := &documentsv1.ListDocumentsRequest{
		Context: req.GetContext(),
		Filter: &documentsv1.ListDocumentsRequest_Filter{
			Region:       req.GetSellerScorecard().GetBillingAddress().GetRegion(),
			CreatedAfter: req.GetSellerScorecard().GetAudit().GetCreatedAt(),
			OwnerId:      req.GetSellerScorecard().GetCounterparty().GetCustomerId(),
			Labels:       req.GetSellerScorecard().GetMetadata(),
		},
	}
	type listDocumentsResult struct {
		reply *documentsv1.ListDocumentsResponse
		err   error
	}
	listDocumentsResults := make(chan listDocumentsResult, 1)
	go func() {
		reply, err := s.documentClient.ListDocuments(ctx, listDocumentsRequest)
		listDocumentsResults <- listDocumentsResult{reply, err}
	}()
	var listDocumentsReply *documentsv1.ListDocumentsResponse
	select {
	case result := <-listDocumentsResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.documents.v1.DocumentService/ListDocuments")
		}
		listDocumentsReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	jobs := make(chan *sellersv1.SellerScorecardLine)
	failures := make(chan error, 4)
	workCtx, stopWork := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for line := range jobs {
				if _, err := s.sellerScorecardCalculationClient.RecalculateSellerScorecards(workCtx, &sellersv1.RecalculateSellerScorecardsRequest{
					Context: req.GetContext(),
					SellerScorecard: &sellersv1.SellerScorecard{
						Lines: []*sellersv1.SellerScorecardLine{&sellersv1.SellerScorecardLine{
							Tax: &sellersv1.SellerScorecardLineTax{
								Amount:       line.GetTax().GetAmount(),
								Jurisdiction: line.GetTax().GetJurisdiction(),
								Rate:         line.GetTax().GetRate(),
							},
							Sku: line.GetSku(),
						}},
					},
				}); err != nil {
					failures <- err
					stopWork()
					return
				}
			}
		}()
	}
	for _, queued := range req.GetSellerScorecard().GetLines() {
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
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardCalculationService/RecalculateSellerScorecards")
	}
	countSessionsRequest := &identityv1.CountSessionsRequest{
		Context: req.GetContext(),
		Filter: &identityv1.CountSessionsRequest_Filter{
			CreatedAfter: req.GetSellerScorecard().GetAudit().GetCreatedAt(),
			OwnerId:      req.GetSellerScorecard().GetSellerScorecardId(),
		},
	}
	countSessionsReply, err := grpcx.NewClient(s.conn, identityv1.NewSessionQueryServiceClient).CountSessions(ctx, countSessionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionQueryService/CountSessions")
	}
	return &sellersv1.ExplainSellerScorecardDecisionResponse{
		Outcome: &sellersv1.ExplainSellerScorecardDecisionResponse_Outcome{Message: listDocumentsReply.GetResult().GetMessage()},
		Result: &commonv1.OperationResult{
			Details:       listDocumentsReply.GetResult().GetDetails(),
			ReferenceIds:  countSessionsReply.GetResult().GetReferenceIds(),
			CorrelationId: countSessionsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		SellerScorecard: &sellersv1.SellerScorecard{
			SellerScorecardId: req.GetSellerScorecardId(),
			BillingAddress:    req.GetSellerScorecard().GetBillingAddress(),
		},
	}, nil
}

// ScoreSellerScorecard scores a seller scorecard.
func (s *SellerScorecardValidationServer) ScoreSellerScorecard(ctx context.Context, req *sellersv1.ScoreSellerScorecardRequest) (*sellersv1.ScoreSellerScorecardResponse, error) {
	if err := s.store.SaveSellerScorecard(ctx, req.GetContext().GetTenantId(), req.GetSellerScorecard()); err != nil {
		return nil, grpcx.Wrap(err, "save seller scorecard")
	}
	s.log.InfoContext(ctx, "score seller scorecard", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSellerScorecard().GetAudit().GetEtag())
	var holderName string
	switch choice := req.GetSellerScorecard().GetSettlement().(type) {
	case *sellersv1.SellerScorecard_Card:
		holderName = choice.Card.GetHolderName()
	case *sellersv1.SellerScorecard_Bank:
		holderName = choice.Bank.GetIban()
	case *sellersv1.SellerScorecard_VoucherCode:
		holderName = choice.VoucherCode
	}
	estimateGiftCardRequest := &giftcardsv1.EstimateGiftCardRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.EstimateGiftCardRequest_Details{
			Destination: &commonv1.PostalAddress{
				Recipient:   req.GetDetails().GetDestination().GetRecipient(),
				CountryCode: req.GetSellerScorecard().GetBillingAddress().GetCountryCode(),
				Region:      req.GetDetails().GetDestination().GetRegion(),
			},
		},
		GiftCard: &giftcardsv1.GiftCard{
			Funding: &giftcardsv1.GiftCard_Card{Card: &giftcardsv1.GiftCardCardFunding{HolderName: holderName}},
		},
	}
	estimateGiftCardReply, err := s.giftCardCalculationLazy().EstimateGiftCard(ctx, estimateGiftCardRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardCalculationService/EstimateGiftCard")
	}
	browseSessionsRequest := &identityv1.BrowseSessionsRequest{
		Context: req.GetContext(),
		Filter:  &identityv1.BrowseSessionsRequest_Filter{Region: req.GetDetails().GetDestination().GetRegion()},
	}
	browseSessionsReply := new(identityv1.BrowseSessionsResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", identitySessionQueryServiceName, "BrowseSessions"), browseSessionsRequest, browseSessionsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionQueryService/BrowseSessions")
	}
	sellerPayoutScheduleValidation, ok := s.clients.Get("acme.sellers.v1.SellerPayoutScheduleValidationService").(sellersv1.SellerPayoutScheduleValidationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.sellers.v1.SellerPayoutScheduleValidationService")
	}
	lintSellerPayoutSchedulesRequest := &sellersv1.LintSellerPayoutSchedulesRequest{
		Context: req.GetContext(),
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
			Participants: []*sellersv1.SellerPayoutScheduleParticipant{&sellersv1.SellerPayoutScheduleParticipant{
				Contact: &commonv1.ContactInfo{
					Name: estimateGiftCardReply.GetGiftCard().GetContact().GetName(),
				},
			}},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: estimateGiftCardReply.GetGiftCard().GetAudit().GetUpdatedAt(),
			},
			Organizer: &identityv1.UserRef{
				DisplayName: req.GetSellerScorecard().GetCounterparty().GetDisplayName(),
			},
		},
	}
	lintSellerPayoutSchedulesReply, err := sellerPayoutScheduleValidation.LintSellerPayoutSchedules(ctx, lintSellerPayoutSchedulesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerPayoutScheduleValidationService/LintSellerPayoutSchedules")
	}
	return &sellersv1.ScoreSellerScorecardResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: estimateGiftCardReply.GetResult().GetCorrelationId(),
			Status:        browseSessionsReply.GetResult().GetStatus(),
		},
		SellerScorecard: &sellersv1.SellerScorecard{
			Counterparty: &customersv1.CustomerRef{
				DisplayName: estimateGiftCardReply.GetGiftCard().GetHolder().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				Etag: lintSellerPayoutSchedulesReply.GetSellerPayoutSchedule().GetAudit().GetEtag(),
			},
			SellerScorecardId: req.GetSellerScorecardId(),
		},
		Breakdown: &sellersv1.ScoreSellerScorecardResponse_Breakdown{Total: req.GetSellerScorecard().GetTotal()},
	}, nil
}

// LintSellerScorecards lints a seller scorecard.
func (s *SellerScorecardValidationServer) LintSellerScorecards(ctx context.Context, req *sellersv1.LintSellerScorecardsRequest) (*sellersv1.LintSellerScorecardsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveSellerScorecard(ctx, req.GetContext().GetTenantId(), req.GetSellerScorecard()); err != nil {
		return nil, grpcx.Wrap(err, "save seller scorecard")
	}
	s.log.InfoContext(ctx, "lint seller scorecards", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSellerScorecard().GetAudit().GetEtag())
	compareSellerScorecardOptionsRequest := &sellersv1.CompareSellerScorecardOptionsRequest{
		Context: req.GetContext(),
		SellerScorecard: &sellersv1.SellerScorecard{
			Counterparty: &customersv1.CustomerRef{Email: req.GetDetails().GetContact().GetEmail()},
			BillingAddress: &commonv1.PostalAddress{
				Region: req.GetSellerScorecard().GetBillingAddress().GetRegion(),
			},
		},
	}
	compareSellerScorecardOptionsReply := new(sellersv1.CompareSellerScorecardOptionsResponse)
	if err := s.conn.Invoke(ctx, sellersSellerScorecardCalculationServiceCompareSellerScorecardOptionsMethod, compareSellerScorecardOptionsRequest, compareSellerScorecardOptionsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardCalculationService/CompareSellerScorecardOptions")
	}
	calculateSellerListingReply, err := s.sellerListingCalculationClient.CalculateSellerListing(ctx, &sellersv1.CalculateSellerListingRequest{
		Context: req.GetContext(),
		SellerListing: &sellersv1.SellerListing{
			Locator: &sellersv1.SellerListing_Address{Address: &commonv1.PostalAddress{
				CountryCode: compareSellerScorecardOptionsReply.GetSellerScorecard().GetBillingAddress().GetCountryCode(),
			}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingCalculationService/CalculateSellerListing")
	}
	giftCardBatchValidation, err := registry.Lookup[giftcardsv1.GiftCardBatchValidationServiceClient](s.clients, "acme.giftcards.v1.GiftCardBatchValidationService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	validateGiftCardBatchRequest := &giftcardsv1.ValidateGiftCardBatchRequest{
		Context: req.GetContext(),
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			Audit: &commonv1.AuditInfo{
				CreatedAt: calculateSellerListingReply.GetSellerListing().GetAudit().GetCreatedAt(),
			},
		},
		Details: &giftcardsv1.ValidateGiftCardBatchRequest_Details{
			Contact: &commonv1.ContactInfo{
				Phone: req.GetDetails().GetContact().GetPhone(),
				Email: compareSellerScorecardOptionsReply.GetSellerScorecard().GetCounterparty().GetEmail(),
			},
		},
	}
	validateGiftCardBatchReply, err := giftCardBatchValidation.ValidateGiftCardBatch(ctx, validateGiftCardBatchRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchValidationService/ValidateGiftCardBatch")
	}
	return &sellersv1.LintSellerScorecardsResponse{
		SellerScorecard: &sellersv1.SellerScorecard{
			Total: compareSellerScorecardOptionsReply.GetSellerScorecard().GetTotal(),
			BillingAddress: &commonv1.PostalAddress{
				Recipient: calculateSellerListingReply.GetSellerListing().GetAddress().GetRecipient(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: validateGiftCardBatchReply.GetGiftCardBatch().GetAudit().GetUpdatedAt(),
			},
			SellerScorecardId: req.GetSellerScorecardId(),
			Counterparty:      req.GetSellerScorecard().GetCounterparty(),
		},
		Outcome: &sellersv1.LintSellerScorecardsResponse_Outcome{Status: calculateSellerListingReply.GetResult().GetStatus()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetSellerScorecardRules returns a seller scorecard.
func (s *SellerScorecardValidationServer) GetSellerScorecardRules(ctx context.Context, req *sellersv1.GetSellerScorecardRulesRequest) (*sellersv1.GetSellerScorecardRulesResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "seller_scorecard", req.GetSellerScorecardId(), "get_seller_scorecard_rules"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get seller scorecard rules", "tenant", req.GetContext().GetTenantId())
	estimateSellerScorecardRequest := &sellersv1.EstimateSellerScorecardRequest{
		Context: req.GetContext(),
		SellerScorecard: &sellersv1.SellerScorecard{
			SellerScorecardId: req.GetSellerScorecardId(),
			Counterparty:      &customersv1.CustomerRef{Email: req.GetContext().GetActor().GetEmail()},
		},
	}
	estimateSellerScorecardReply, err := s.sellerScorecardCalculationLazy().EstimateSellerScorecard(ctx, estimateSellerScorecardRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardCalculationService/EstimateSellerScorecard")
	}
	step := &sellerScorecardValidationServerGetSellerScorecardRulesCreateDocumentStep{request: &documentsv1.CreateDocumentRequest{
		Context: req.GetContext(),
		Document: &documentsv1.Document{
			BillingAddress: &commonv1.PostalAddress{
				Region: estimateSellerScorecardReply.GetSellerScorecard().GetBillingAddress().GetRegion(),
			},
		},
	}}
	if err := s.runGetSellerScorecardRulesCreateDocument(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.documents.v1.DocumentService/CreateDocument")
	}
	createDocumentReply := step.reply
	return &sellersv1.GetSellerScorecardRulesResponse{
		SellerScorecard: &sellersv1.SellerScorecard{
			Audit: &commonv1.AuditInfo{
				Etag: estimateSellerScorecardReply.GetSellerScorecard().GetAudit().GetEtag(),
			},
			BillingAddress: &commonv1.PostalAddress{
				Region: createDocumentReply.GetDocument().GetBillingAddress().GetRegion(),
			},
			SellerScorecardId: req.GetSellerScorecardId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// sellerScorecardValidationServerCalculateSellerScorecardSteps are the ways to run
// acme.sellers.v1.SellerScorecardCalculationService/CalculateSellerScorecard, by name.
var sellerScorecardValidationServerCalculateSellerScorecardSteps = map[string]func(*SellerScorecardValidationServer, context.Context, *sellersv1.CalculateSellerScorecardRequest) (*sellersv1.CalculateSellerScorecardResponse, error){
	"default": (*SellerScorecardValidationServer).calculateSellerScorecardStep,
	"audited": (*SellerScorecardValidationServer).calculateSellerScorecardAuditedStep,
}

func (s *SellerScorecardValidationServer) calculateSellerScorecardStep(ctx context.Context, request *sellersv1.CalculateSellerScorecardRequest) (*sellersv1.CalculateSellerScorecardResponse, error) {
	return s.sellerScorecardCalculationClient.CalculateSellerScorecard(ctx, request)
}

func (s *SellerScorecardValidationServer) calculateSellerScorecardAuditedStep(ctx context.Context, request *sellersv1.CalculateSellerScorecardRequest) (*sellersv1.CalculateSellerScorecardResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.sellers.v1.SellerScorecardCalculationService/CalculateSellerScorecard")
	return s.sellerScorecardCalculationClient.CalculateSellerScorecard(ctx, request)
}

// giftCardCalculationLazy dials acme.giftcards.v1.GiftCardCalculationService the first time a handler needs it.
func (s *SellerScorecardValidationServer) giftCardCalculationLazy() giftcardsv1.GiftCardCalculationServiceClient {
	s.giftCardCalculationOnce.Do(func() { s.giftCardCalculationCached = giftcardsv1.NewGiftCardCalculationServiceClient(s.conn) })
	return s.giftCardCalculationCached
}

// sellerScorecardCalculationLazy dials acme.sellers.v1.SellerScorecardCalculationService the first time a handler needs
// it.
func (s *SellerScorecardValidationServer) sellerScorecardCalculationLazy() sellersv1.SellerScorecardCalculationServiceClient {
	s.sellerScorecardCalculationOnce.Do(func() {
		s.sellerScorecardCalculationCached = sellersv1.NewSellerScorecardCalculationServiceClient(s.conn)
	})
	return s.sellerScorecardCalculationCached
}

// sellerScorecardValidationServerGetSellerScorecardRulesCreateDocumentStep carries the request of one step of
// GetSellerScorecardRules and, once it ran, its reply.
type sellerScorecardValidationServerGetSellerScorecardRulesCreateDocumentStep struct {
	request *documentsv1.CreateDocumentRequest
	reply   *documentsv1.CreateDocumentResponse
}

func (s *SellerScorecardValidationServer) runGetSellerScorecardRulesCreateDocument(ctx context.Context, step *sellerScorecardValidationServerGetSellerScorecardRulesCreateDocumentStep) error {
	reply, err := s.documentClient.CreateDocument(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
