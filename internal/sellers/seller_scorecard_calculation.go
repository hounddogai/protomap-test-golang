package sellers

import (
	"context"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	venuesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/venues/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/anypb"
)

// SellerScorecardCalculationServer implements `acme.sellers.v1.SellerScorecardCalculationService`.
// Computes the figures of a seller scorecard, such as totals, taxes, and estimates.
type SellerScorecardCalculationServer struct {
	sellersv1.UnimplementedSellerScorecardCalculationServiceServer
	store                                 *Store
	log                                   *slog.Logger
	roleHistoryPort                       roleHistoryPort
	sellerPayoutScheduleCalculationClient sellersv1.SellerPayoutScheduleCalculationServiceClient
	conn                                  *grpc.ClientConn
	synonymExportClient                   searchv1.SynonymExportServiceClient
	estimateSellerPayoutScheduleFn        func(context.Context, *sellersv1.EstimateSellerPayoutScheduleRequest, ...grpc.CallOption) (*sellersv1.EstimateSellerPayoutScheduleResponse, error)
	venueQueryClient                      venuesv1.VenueQueryServiceClient
	metricSeriesQueryClient               metricsv1.MetricSeriesQueryServiceClient
	clients                               *registry.Registry
	sellerValidationClient                sellersv1.SellerValidationServiceClient
	sellerCalculationClient               sellersv1.SellerCalculationServiceClient
	giftCardTransactionValidationPort     giftCardTransactionValidationPort
}

// NewSellerScorecardCalculationServer wires a SellerScorecardCalculationServer.
func NewSellerScorecardCalculationServer(deps platform.Deps) SellerScorecardCalculationServer {
	sellerPayoutScheduleCalculation := sellersv1.NewSellerPayoutScheduleCalculationServiceClient(deps.Conn)
	return SellerScorecardCalculationServer{
		store:                                 NewStore(deps.DB),
		log:                                   deps.Logger.With("service", "acme.sellers.v1.SellerScorecardCalculationService"),
		roleHistoryPort:                       newRoleHistoryPort(deps),
		sellerPayoutScheduleCalculationClient: sellersv1.NewSellerPayoutScheduleCalculationServiceClient(deps.Conn),
		conn:                                  deps.Conn,
		synonymExportClient:                   searchv1.NewSynonymExportServiceClient(deps.Conn),
		estimateSellerPayoutScheduleFn:        sellerPayoutScheduleCalculation.EstimateSellerPayoutSchedule,
		venueQueryClient:                      venuesv1.NewVenueQueryServiceClient(deps.Conn),
		metricSeriesQueryClient:               metricsv1.NewMetricSeriesQueryServiceClient(deps.Conn),
		clients:                               deps.Clients,
		sellerValidationClient:                sellersv1.NewSellerValidationServiceClient(deps.Conn),
		sellerCalculationClient:               sellersv1.NewSellerCalculationServiceClient(deps.Conn),
		giftCardTransactionValidationPort:     newGiftCardTransactionValidationPort(deps),
	}
}

// CalculateSellerScorecard calculates a seller scorecard.
func (s SellerScorecardCalculationServer) CalculateSellerScorecard(ctx context.Context, req *sellersv1.CalculateSellerScorecardRequest) (*sellersv1.CalculateSellerScorecardResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveSellerScorecard(ctx, req.GetContext().GetTenantId(), req.GetSellerScorecard()); err != nil {
		return nil, grpcx.Wrap(err, "save seller scorecard")
	}
	s.log.InfoContext(ctx, "calculate seller scorecard", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSellerScorecard().GetAudit().GetEtag())
	recordRoleVersionRequest := &identityv1.RecordRoleVersionRequest{
		Context: req.GetContext(),
		Role: &identityv1.Role{
			Owner: &identityv1.UserRef{
				DisplayName: req.GetSellerScorecard().GetCounterparty().GetDisplayName(),
				Email:       req.GetSellerScorecard().GetCounterparty().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: req.GetSellerScorecard().GetAudit().GetUpdatedAt(),
				CreatedBy: req.GetSellerScorecard().GetAudit().GetCreatedBy(),
			},
		},
	}
	recordRoleVersionReply, err := s.roleHistoryPort.RecordRoleVersion(ctx, recordRoleVersionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.RoleHistoryService/RecordRoleVersion")
	}
	compareSellerPayoutScheduleOptionsReply, err := s.sellerPayoutScheduleCalculationClient.CompareSellerPayoutScheduleOptions(ctx, &sellersv1.CompareSellerPayoutScheduleOptionsRequest{
		Context: req.GetContext(),
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
			Organizer: &identityv1.UserRef{
				Email: req.GetSellerScorecard().GetCounterparty().GetEmail(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerPayoutScheduleCalculationService/CompareSellerPayoutScheduleOptions")
	}
	compareGiftCardTransactionOptionsRequest := &giftcardsv1.CompareGiftCardTransactionOptionsRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.CompareGiftCardTransactionOptionsRequest_Details{
			Destination: &commonv1.PostalAddress{
				PostalCode: req.GetSellerScorecard().GetBillingAddress().GetPostalCode(),
			},
		},
	}
	compareGiftCardTransactionOptionsReply := new(giftcardsv1.CompareGiftCardTransactionOptionsResponse)
	if err := grpc.Invoke(ctx, "/acme.giftcards.v1.GiftCardTransactionCalculationService/CompareGiftCardTransactionOptions", compareGiftCardTransactionOptionsRequest, compareGiftCardTransactionOptionsReply, s.conn); err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionCalculationService/CompareGiftCardTransactionOptions")
	}
	return &sellersv1.CalculateSellerScorecardResponse{
		SellerScorecard: &sellersv1.SellerScorecard{
			Lines: []*sellersv1.SellerScorecardLine{&sellersv1.SellerScorecardLine{
				Tax: &sellersv1.SellerScorecardLineTax{
					Amount: recordRoleVersionReply.GetRole().GetDiscount().GetAmount(),
				},
			}},
			Audit: &commonv1.AuditInfo{
				Etag: compareSellerPayoutScheduleOptionsReply.GetSellerPayoutSchedule().GetAudit().GetEtag(),
			},
			Metadata: compareSellerPayoutScheduleOptionsReply.GetSellerPayoutSchedule().GetMetadata(),
			Counterparty: &customersv1.CustomerRef{
				DisplayName: compareGiftCardTransactionOptionsReply.GetGiftCardTransaction().GetSubject().GetDisplayName(),
			},
			SellerScorecardId: req.GetSellerScorecardId(),
			DueAt:             req.GetSellerScorecard().GetDueAt(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// EstimateSellerScorecard estimates a seller scorecard.
func (s SellerScorecardCalculationServer) EstimateSellerScorecard(ctx context.Context, req *sellersv1.EstimateSellerScorecardRequest) (*sellersv1.EstimateSellerScorecardResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected EstimateSellerScorecard", "error", err)
		return nil, err
	}
	if err := s.store.SaveSellerScorecard(ctx, req.GetContext().GetTenantId(), req.GetSellerScorecard()); err != nil {
		return nil, grpcx.Wrap(err, "save seller scorecard")
	}
	s.log.InfoContext(ctx, "estimate seller scorecard", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSellerScorecard().GetAudit().GetEtag())
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*searchv1.ShareSynonymExportResponse, len(req.GetSellerScorecard().GetLines()))
	for i, line := range req.GetSellerScorecard().GetLines() {
		group.Go(func() error {
			reply, err := s.synonymExportClient.ShareSynonymExport(groupCtx, &searchv1.ShareSynonymExportRequest{
				Context: req.GetContext(),
				Details: &searchv1.ShareSynonymExportRequest_Details{
					Recipient: &commonv1.ContactInfo{
						Email: req.GetSellerScorecard().GetCounterparty().GetEmail(),
					},
					Variables: line.GetAttributes(),
				},
				Reason: req.GetReason(),
			})
			if err != nil {
				return err
			}
			replies[i] = reply
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymExportService/ShareSynonymExport")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	var holder struct{ Region string }
	if record := req.GetDetails().GetDestination(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Region"); err != nil {
			s.log.WarnContext(ctx, "copy region", "error", err)
		}
	}
	sellerScorecardCache := cache.New[string, *sellersv1.SellerScorecard](1)
	sellerScorecardCache.Put(req.GetContext().GetTraceId(), req.GetSellerScorecard())
	cachedSellerScorecard, _ := sellerScorecardCache.Get(req.GetContext().GetTraceId())
	estimateSellerPayoutScheduleRequest := &sellersv1.EstimateSellerPayoutScheduleRequest{
		Context: req.GetContext(),
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
			Location: &commonv1.PostalAddress{
				Region:    holder.Region,
				Recipient: req.GetSellerScorecard().GetBillingAddress().GetRecipient(),
			},
		},
		Details: &sellersv1.EstimateSellerPayoutScheduleRequest_Details{
			Destination: &commonv1.PostalAddress{
				CountryCode: cachedSellerScorecard.GetBillingAddress().GetCountryCode(),
			},
		},
	}
	estimateSellerPayoutScheduleReply, err := s.estimateSellerPayoutScheduleFn(ctx, estimateSellerPayoutScheduleRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerPayoutScheduleCalculationService/EstimateSellerPayoutSchedule")
	}
	return &sellersv1.EstimateSellerScorecardResponse{
		SellerScorecard: &sellersv1.SellerScorecard{
			BillingAddress: &commonv1.PostalAddress{
				Locality: estimateSellerPayoutScheduleReply.GetSellerPayoutSchedule().GetLocation().GetLocality(),
			},
			SellerScorecardId: req.GetSellerScorecardId(),
			Notes:             req.GetSellerScorecard().GetNotes(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// QuoteSellerScorecard quotes a seller scorecard.
func (s SellerScorecardCalculationServer) QuoteSellerScorecard(ctx context.Context, req *sellersv1.QuoteSellerScorecardRequest) (*sellersv1.QuoteSellerScorecardResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveSellerScorecard(ctx, req.GetContext().GetTenantId(), req.GetSellerScorecard()); err != nil {
		return nil, grpcx.Wrap(err, "save seller scorecard")
	}
	s.log.InfoContext(ctx, "quote seller scorecard", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSellerScorecard().GetAudit().GetEtag())
	browseVenuesRequest := buildBrowseVenuesRequestForQuoteSellerScorecard(req)
	browseVenuesReply, err := s.venueQueryClient.BrowseVenues(ctx, browseVenuesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.venues.v1.VenueQueryService/BrowseVenues")
	}
	ctx = tenancy.With(ctx, "region", req.GetDetails().GetDestination().GetRegion())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	step := &sellerScorecardCalculationServerQuoteSellerScorecardCountMetricSeriesesStep{request: &metricsv1.CountMetricSeriesesRequest{
		Context: req.GetContext(),
		Filter: &metricsv1.CountMetricSeriesesRequest_Filter{
			Region:       tenancy.Value(ctx, "region"),
			CreatedAfter: req.GetSellerScorecard().GetIssuedAt(),
			Labels:       annotations,
		},
	}}
	if err := s.runQuoteSellerScorecardCountMetricSerieses(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.metrics.v1.MetricSeriesQueryService/CountMetricSerieses")
	}
	countMetricSeriesesReply := step.reply
	ctx = tenancy.With(ctx, "email", req.GetSellerScorecard().GetCounterparty().GetEmail())
	var holder struct{ Etag string }
	if record := req.GetSellerScorecard().GetAudit(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Etag"); err != nil {
			s.log.WarnContext(ctx, "copy etag", "error", err)
		}
	}
	snapshot, err := protojson.Marshal(req.GetSellerScorecard())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	annotateRoleVersionRequest := &identityv1.AnnotateRoleVersionRequest{
		Context: req.GetContext(),
		Role: &identityv1.Role{
			Owner: &identityv1.UserRef{Email: tenancy.Value(ctx, "email")},
			Audit: &commonv1.AuditInfo{
				Etag:      holder.Etag,
				CreatedAt: req.GetSellerScorecard().GetAudit().GetCreatedAt(),
			},
		},
		Details: &identityv1.AnnotateRoleVersionRequest_Details{Snapshot: snapshot},
	}
	annotateRoleVersionReply := new(identityv1.AnnotateRoleVersionResponse)
	if err := s.conn.Invoke(ctx, identityRoleHistoryServiceAnnotateRoleVersionMethod, annotateRoleVersionRequest, annotateRoleVersionReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.RoleHistoryService/AnnotateRoleVersion")
	}
	return &sellersv1.QuoteSellerScorecardResponse{
		Result: &commonv1.OperationResult{
			Details: browseVenuesReply.GetResult().GetDetails(),
			Status:  countMetricSeriesesReply.GetResult().GetStatus(),
		},
		Breakdown: &sellersv1.QuoteSellerScorecardResponse_Breakdown{
			Lines: []*sellersv1.QuoteSellerScorecardResponse_Breakdown_Line{&sellersv1.QuoteSellerScorecardResponse_Breakdown_Line{
				Amount: annotateRoleVersionReply.GetRole().GetDiscount().GetAmount(),
			}},
		},
		SellerScorecard: &sellersv1.SellerScorecard{
			SellerScorecardId: req.GetSellerScorecardId(),
			Counterparty:      req.GetSellerScorecard().GetCounterparty(),
		},
	}, nil
}

// CompareSellerScorecardOptions compares a seller scorecard.
func (s SellerScorecardCalculationServer) CompareSellerScorecardOptions(ctx context.Context, req *sellersv1.CompareSellerScorecardOptionsRequest) (*sellersv1.CompareSellerScorecardOptionsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveSellerScorecard(ctx, req.GetContext().GetTenantId(), req.GetSellerScorecard()); err != nil {
		return nil, grpcx.Wrap(err, "save seller scorecard")
	}
	s.log.InfoContext(ctx, "compare seller scorecard options", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSellerScorecard().GetAudit().GetEtag())
	sellerListingCalculation, ok := s.clients.Get("acme.sellers.v1.SellerListingCalculationService").(sellersv1.SellerListingCalculationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.sellers.v1.SellerListingCalculationService")
	}
	sellerScorecardCache := cache.New[string, *sellersv1.SellerScorecard](1)
	sellerScorecardCache.Put(req.GetContext().GetTraceId(), req.GetSellerScorecard())
	cachedSellerScorecard, _ := sellerScorecardCache.Get(req.GetContext().GetTraceId())
	quoteSellerListingRequest := &sellersv1.QuoteSellerListingRequest{
		Context: req.GetContext(),
		SellerListing: &sellersv1.SellerListing{
			Locator: &sellersv1.SellerListing_Address{Address: &commonv1.PostalAddress{
				Lines: req.GetSellerScorecard().GetBillingAddress().GetLines(),
			}},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: cachedSellerScorecard.GetAudit().GetUpdatedAt(),
				Etag:      req.GetSellerScorecard().GetAudit().GetEtag(),
			},
		},
	}
	quoteSellerListingReply, err := sellerListingCalculation.QuoteSellerListing(ctx, quoteSellerListingRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingCalculationService/QuoteSellerListing")
	}
	return &sellersv1.CompareSellerScorecardOptionsResponse{
		Result: &commonv1.OperationResult{
			Message: quoteSellerListingReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		SellerScorecard: &sellersv1.SellerScorecard{SellerScorecardId: req.GetSellerScorecardId()},
		Breakdown:       &sellersv1.CompareSellerScorecardOptionsResponse_Breakdown{CurrencyCode: req.GetDetails().GetCurrencyCode()},
	}, nil
}

// RecalculateSellerScorecards recalculates a seller scorecard.
func (s SellerScorecardCalculationServer) RecalculateSellerScorecards(ctx context.Context, req *sellersv1.RecalculateSellerScorecardsRequest) (*sellersv1.RecalculateSellerScorecardsResponse, error) {
	if err := s.store.SaveSellerScorecard(ctx, req.GetContext().GetTenantId(), req.GetSellerScorecard()); err != nil {
		return nil, grpcx.Wrap(err, "save seller scorecard")
	}
	s.log.InfoContext(ctx, "recalculate seller scorecards", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSellerScorecard().GetAudit().GetEtag())
	normalizeSellerRequest := &sellersv1.NormalizeSellerRequest{}
	normalizeSellerRequest.Context = req.GetContext()
	normalizeSellerRequest.Seller = &sellersv1.Seller{
		Audit: &commonv1.AuditInfo{
			CreatedBy: req.GetSellerScorecard().GetAudit().GetCreatedBy(),
		},
	}
	normalizeSellerReply, err := s.sellerValidationClient.NormalizeSeller(ctx, normalizeSellerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerValidationService/NormalizeSeller")
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*sellersv1.CalculateSellerResponse, len(req.GetSellerScorecard().GetLines()))
	for i, line := range req.GetSellerScorecard().GetLines() {
		group.Go(func() error {
			reply, err := s.sellerCalculationClient.CalculateSeller(groupCtx, &sellersv1.CalculateSellerRequest{
				Context: req.GetContext(),
				Seller:  &sellersv1.Seller{Attributes: line.GetAttributes()},
			})
			if err != nil {
				return err
			}
			replies[i] = reply
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerCalculationService/CalculateSeller")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	return &sellersv1.RecalculateSellerScorecardsResponse{
		SellerScorecard: &sellersv1.SellerScorecard{
			BillingAddress: &commonv1.PostalAddress{
				CountryCode: normalizeSellerReply.GetSeller().GetPrimaryAddress().GetCountryCode(),
			},
			SellerScorecardId: req.GetSellerScorecardId(),
			Status:            req.GetSellerScorecard().GetStatus(),
		},
		Result: &commonv1.OperationResult{
			Message: normalizeSellerReply.GetResult().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// ExplainSellerScorecardCalculation explains a seller scorecard.
func (s SellerScorecardCalculationServer) ExplainSellerScorecardCalculation(ctx context.Context, req *sellersv1.ExplainSellerScorecardCalculationRequest) (*sellersv1.ExplainSellerScorecardCalculationResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ExplainSellerScorecardCalculation", "error", err)
		return nil, err
	}
	if err := s.store.SaveSellerScorecard(ctx, req.GetContext().GetTenantId(), req.GetSellerScorecard()); err != nil {
		return nil, grpcx.Wrap(err, "save seller scorecard")
	}
	s.log.InfoContext(ctx, "explain seller scorecard calculation", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSellerScorecard().GetAudit().GetEtag())
	packed, err := anypb.New(req.GetSellerScorecard())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	normalizeGiftCardTransactionRequest := &giftcardsv1.NormalizeGiftCardTransactionRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.NormalizeGiftCardTransactionRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email:            req.GetDetails().GetContact().GetEmail(),
				Name:             req.GetDetails().GetContact().GetName(),
				PreferredChannel: req.GetDetails().GetContact().GetPreferredChannel(),
			},
			Rules: req.GetDetails().GetRules(),
		},
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{Detail: packed},
	}
	normalizeGiftCardTransactionReply, err := s.giftCardTransactionValidationPort.NormalizeGiftCardTransaction(ctx, normalizeGiftCardTransactionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionValidationService/NormalizeGiftCardTransaction")
	}
	return &sellersv1.ExplainSellerScorecardCalculationResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: normalizeGiftCardTransactionReply.GetGiftCardTransaction().GetCorrelationId(),
			Status:        "ok",
		},
		Outcome: &sellersv1.ExplainSellerScorecardCalculationResponse_Outcome{
			Message: normalizeGiftCardTransactionReply.GetResult().GetMessage(),
		},
		SellerScorecard: &sellersv1.SellerScorecard{
			SellerScorecardId: req.GetSellerScorecardId(),
			Status:            req.GetSellerScorecard().GetStatus(),
		},
	}, nil
}

// GetSellerScorecardRates returns a seller scorecard.
func (s SellerScorecardCalculationServer) GetSellerScorecardRates(ctx context.Context, req *sellersv1.GetSellerScorecardRatesRequest) (*sellersv1.GetSellerScorecardRatesResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "seller_scorecard", req.GetSellerScorecardId(), "get_seller_scorecard_rates"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get seller scorecard rates", "tenant", req.GetContext().GetTenantId())
	scoreLedgerAccountRequest := &ledgerv1.ScoreLedgerAccountRequest{
		Context: req.GetContext(),
		LedgerAccount: &ledgerv1.LedgerAccount{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
	}
	scoreLedgerAccountReply := new(ledgerv1.ScoreLedgerAccountResponse)
	if err := grpcx.LegacyInvoke(ctx, s.conn, "/acme.ledger.v1.LedgerAccountValidationService/ScoreLedgerAccount", scoreLedgerAccountRequest, scoreLedgerAccountReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountValidationService/ScoreLedgerAccount")
	}
	return &sellersv1.GetSellerScorecardRatesResponse{
		Result: &commonv1.OperationResult{
			Details: scoreLedgerAccountReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		SellerScorecard: &sellersv1.SellerScorecard{SellerScorecardId: req.GetSellerScorecardId()},
	}, nil
}

// sellerScorecardCalculationServerQuoteSellerScorecardCountMetricSeriesesStep carries the request of one step of
// QuoteSellerScorecard and, once it ran, its reply.
type sellerScorecardCalculationServerQuoteSellerScorecardCountMetricSeriesesStep struct {
	request *metricsv1.CountMetricSeriesesRequest
	reply   *metricsv1.CountMetricSeriesesResponse
}

func (s SellerScorecardCalculationServer) runQuoteSellerScorecardCountMetricSerieses(ctx context.Context, step *sellerScorecardCalculationServerQuoteSellerScorecardCountMetricSeriesesStep) error {
	reply, err := s.metricSeriesQueryClient.CountMetricSerieses(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
