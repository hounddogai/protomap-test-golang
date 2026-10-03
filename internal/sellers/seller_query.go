package sellers

import (
	"context"
	"errors"
	"io"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	invoicingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/invoicing/v1"
	personalizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/personalization/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// SellerQueryServer implements `acme.sellers.v1.SellerQueryService`.
// Answers read-only questions about sellers, for screens and reports.
type SellerQueryServer struct {
	sellersv1.UnimplementedSellerQueryServiceServer
	store                                  *Store
	log                                    *slog.Logger
	profileClient                          personalizationv1.ProfileServiceClient
	giftCardValidationClient               giftcardsv1.GiftCardValidationServiceClient
	sellerListingCalculationClient         sellersv1.SellerListingCalculationServiceClient
	sellerPayoutScheduleCalculationClient  sellersv1.SellerPayoutScheduleCalculationServiceClient
	meteredSellerScorecardCalculation      meteredSellerScorecardCalculation
	meteredSellerPayoutScheduleCalculation meteredSellerPayoutScheduleCalculation
	sellerScorecardValidationClient        sellersv1.SellerScorecardValidationServiceClient
	invoiceBatchCalculationClient          invoicingv1.InvoiceBatchCalculationServiceClient
}

// NewSellerQueryServer wires a SellerQueryServer.
func NewSellerQueryServer(deps platform.Deps) *SellerQueryServer {
	return &SellerQueryServer{
		store:                                  NewStore(deps.DB),
		log:                                    deps.Logger.With("service", "acme.sellers.v1.SellerQueryService"),
		profileClient:                          personalizationv1.NewProfileServiceClient(deps.Conn),
		giftCardValidationClient:               giftcardsv1.NewGiftCardValidationServiceClient(deps.Conn),
		sellerListingCalculationClient:         sellersv1.NewSellerListingCalculationServiceClient(deps.Conn),
		sellerPayoutScheduleCalculationClient:  sellersv1.NewSellerPayoutScheduleCalculationServiceClient(deps.Conn),
		meteredSellerScorecardCalculation:      meteredSellerScorecardCalculation{SellerScorecardCalculationServiceClient: sellersv1.NewSellerScorecardCalculationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		meteredSellerPayoutScheduleCalculation: meteredSellerPayoutScheduleCalculation{SellerPayoutScheduleCalculationServiceClient: sellersv1.NewSellerPayoutScheduleCalculationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		sellerScorecardValidationClient:        sellersv1.NewSellerScorecardValidationServiceClient(deps.Conn),
		invoiceBatchCalculationClient:          invoicingv1.NewInvoiceBatchCalculationServiceClient(deps.Conn),
	}
}

// LookupSeller returns a seller.
func (s *SellerQueryServer) LookupSeller(ctx context.Context, req *sellersv1.LookupSellerRequest) (*sellersv1.LookupSellerResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "seller", req.GetSellerId(), "lookup_seller"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "lookup seller", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetContext()).(*commonv1.RequestContext)
	watchProfilesStream, err := s.profileClient.WatchProfiles(ctx, &personalizationv1.WatchProfilesRequest{
		Context:    req.GetContext(),
		Subscriber: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		Filter: &personalizationv1.WatchProfilesRequest_Filter{
			CreatedAfter: cloned.GetDeadline(),
			OwnerId:      req.GetSellerId(),
			Labels:       req.GetContext().GetBaggage(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.personalization.v1.ProfileService/WatchProfiles")
	}
	for {
		received, err := watchProfilesStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.personalization.v1.ProfileService/WatchProfiles")
		}
		_ = received
	}
	scoreGiftCardReply, err := s.giftCardValidationClient.ScoreGiftCard(ctx, &giftcardsv1.ScoreGiftCardRequest{
		Context: req.GetContext(),
		GiftCard: &giftcardsv1.GiftCard{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Funding: &giftcardsv1.GiftCard_ExternalAccountId{ExternalAccountId: req.GetSellerId()},
			Audit:   &commonv1.AuditInfo{CreatedAt: req.GetContext().GetDeadline()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardValidationService/ScoreGiftCard")
	}
	return &sellersv1.LookupSellerResponse{
		Seller: &sellersv1.Seller{
			RelationshipManager: &identityv1.UserRef{
				Email: scoreGiftCardReply.GetGiftCard().GetContact().GetEmail(),
			},
			SellerId: req.GetSellerId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// BrowseSellers lists sellers a page at a time.
func (s *SellerQueryServer) BrowseSellers(ctx context.Context, req *sellersv1.BrowseSellersRequest) (*sellersv1.BrowseSellersResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "browse sellers", "tenant", req.GetContext().GetTenantId())
	return &sellersv1.BrowseSellersResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// FindSellers lists sellers a page at a time.
func (s *SellerQueryServer) FindSellers(ctx context.Context, req *sellersv1.FindSellersRequest) (*sellersv1.FindSellersResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected FindSellers", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "find sellers", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetContext()).(*commonv1.RequestContext)
	explainSellerListingCalculationRequest := &sellersv1.ExplainSellerListingCalculationRequest{
		Context: req.GetContext(),
		SellerListing: &sellersv1.SellerListing{
			Locator: &sellersv1.SellerListing_Address{Address: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()}},
			Owner:   &identityv1.UserRef{UserId: cloned.GetActor().GetUserId()},
		},
		Details: &sellersv1.ExplainSellerListingCalculationRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
	}
	explainSellerListingCalculationReply, err := sellersv1.SellerListingCalculationServiceClient.ExplainSellerListingCalculation(s.sellerListingCalculationClient, ctx, explainSellerListingCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingCalculationService/ExplainSellerListingCalculation")
	}
	calculateSellerPayoutScheduleRequest := &sellersv1.CalculateSellerPayoutScheduleRequest{
		Context: req.GetContext(),
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
			Participants: []*sellersv1.SellerPayoutScheduleParticipant{&sellersv1.SellerPayoutScheduleParticipant{
				Contact: &commonv1.ContactInfo{
					Email: explainSellerListingCalculationReply.GetSellerListing().GetOwner().GetEmail(),
				},
			}},
			Location: &commonv1.PostalAddress{
				PostalCode: explainSellerListingCalculationReply.GetSellerListing().GetAddress().GetPostalCode(),
				Locality:   explainSellerListingCalculationReply.GetSellerListing().GetAddress().GetLocality(),
			},
		},
		Details: &sellersv1.CalculateSellerPayoutScheduleRequest_Details{
			Destination: &commonv1.PostalAddress{
				Recipient: explainSellerListingCalculationReply.GetSellerListing().GetAddress().GetRecipient(),
			},
		},
	}
	calculateSellerPayoutScheduleReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*sellersv1.CalculateSellerPayoutScheduleResponse, error) {
		return s.sellerPayoutScheduleCalculationClient.CalculateSellerPayoutSchedule(ctx, calculateSellerPayoutScheduleRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerPayoutScheduleCalculationService/CalculateSellerPayoutSchedule")
	}
	return &sellersv1.FindSellersResponse{
		Sellers: []*sellersv1.Seller{&sellersv1.Seller{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: explainSellerListingCalculationReply.GetSellerListing().GetAudit().GetUpdatedBy(),
			},
			RelationshipManager: &identityv1.UserRef{
				Email:       calculateSellerPayoutScheduleReply.GetSellerPayoutSchedule().GetOrganizer().GetEmail(),
				DisplayName: calculateSellerPayoutScheduleReply.GetSellerPayoutSchedule().GetOrganizer().GetDisplayName(),
			},
		}},
		Result: &commonv1.OperationResult{
			Status: explainSellerListingCalculationReply.GetResult().GetStatus(),
		},
	}, nil
}

// CountSellers counts sellers by status.
func (s *SellerQueryServer) CountSellers(ctx context.Context, req *sellersv1.CountSellersRequest) (*sellersv1.CountSellersResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "count sellers", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetFilter()).(*sellersv1.CountSellersRequest_Filter)
	labels := make(map[string]string, len(req.GetFilter().GetLabels()))
	for key, value := range req.GetFilter().GetLabels() {
		labels[key] = value
	}
	quoteSellerScorecardRequest := &sellersv1.QuoteSellerScorecardRequest{
		Context: req.GetContext(),
		SellerScorecard: &sellersv1.SellerScorecard{
			BillingAddress: &commonv1.PostalAddress{Region: cloned.GetRegion()},
			Counterparty:   &customersv1.CustomerRef{Email: req.GetContext().GetActor().GetEmail()},
			DueAt:          req.GetFilter().GetCreatedAfter(),
			Lines:          []*sellersv1.SellerScorecardLine{&sellersv1.SellerScorecardLine{Attributes: labels}},
		},
	}
	quoteSellerScorecardReply, err := s.meteredSellerScorecardCalculation.QuoteSellerScorecard(ctx, quoteSellerScorecardRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardCalculationService/QuoteSellerScorecard")
	}
	compareSellerPayoutScheduleOptionsRequest := &sellersv1.CompareSellerPayoutScheduleOptionsRequest{}
	compareSellerPayoutScheduleOptionsRequest.Context = req.GetContext()
	compareSellerPayoutScheduleOptionsRequest.SellerPayoutSchedule = &sellersv1.SellerPayoutSchedule{
		Audit: &commonv1.AuditInfo{
			CreatedAt: quoteSellerScorecardReply.GetSellerScorecard().GetAudit().GetCreatedAt(),
		},
	}
	compareSellerPayoutScheduleOptionsReply, err := s.sellerPayoutScheduleCalculationClient.CompareSellerPayoutScheduleOptions(ctx, compareSellerPayoutScheduleOptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerPayoutScheduleCalculationService/CompareSellerPayoutScheduleOptions")
	}
	return &sellersv1.CountSellersResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: quoteSellerScorecardReply.GetResult().GetCorrelationId(),
			Message:       quoteSellerScorecardReply.GetResult().GetMessage(),
			Details:       compareSellerPayoutScheduleOptionsReply.GetResult().GetDetails(),
			ReferenceIds:  compareSellerPayoutScheduleOptionsReply.GetResult().GetReferenceIds(),
			Status:        "ok",
		},
	}, nil
}

// ExportSellers streams every matching seller.
func (s *SellerQueryServer) ExportSellers(req *sellersv1.ExportSellersRequest, stream grpc.ServerStreamingServer[sellersv1.ExportSellersResponse]) error {
	ctx := stream.Context()
	if req.GetContext().GetTenantId() == "" {
		return status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "export sellers", "tenant", req.GetContext().GetTenantId())
	compareSellerPayoutScheduleOptionsRequest := &sellersv1.CompareSellerPayoutScheduleOptionsRequest{
		Context: req.GetContext(),
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
			Location: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
	}
	compareSellerPayoutScheduleOptionsReply, err := s.meteredSellerPayoutScheduleCalculation.CompareSellerPayoutScheduleOptions(ctx, compareSellerPayoutScheduleOptionsRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.sellers.v1.SellerPayoutScheduleCalculationService/CompareSellerPayoutScheduleOptions")
	}
	explainSellerPayoutScheduleCalculationRequest := newExplainSellerPayoutScheduleCalculationRequest(withExplainSellerPayoutScheduleCalculationContext(req.GetContext()), withExplainSellerPayoutScheduleCalculationSellerPayoutSchedule(&sellersv1.SellerPayoutSchedule{
		Recurrence: &sellersv1.SellerPayoutSchedule_Weekly{Weekly: &sellersv1.SellerPayoutScheduleWeeklyRule{
			Days: compareSellerPayoutScheduleOptionsReply.GetSellerPayoutSchedule().GetWeekly().GetDays(),
		}},
	}))
	explainSellerPayoutScheduleCalculationReply, err := s.sellerPayoutScheduleCalculationClient.ExplainSellerPayoutScheduleCalculation(ctx, explainSellerPayoutScheduleCalculationRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.sellers.v1.SellerPayoutScheduleCalculationService/ExplainSellerPayoutScheduleCalculation")
	}
	if err := stream.Send(&sellersv1.ExportSellersResponse{
		Seller: &sellersv1.Seller{
			Audit: &commonv1.AuditInfo{
				Etag: compareSellerPayoutScheduleOptionsReply.GetSellerPayoutSchedule().GetAudit().GetEtag(),
			},
			PrimaryAddress: &commonv1.PostalAddress{
				Region: explainSellerPayoutScheduleCalculationReply.GetSellerPayoutSchedule().GetLocation().GetRegion(),
			},
			RelationshipManager: &identityv1.UserRef{
				Email: explainSellerPayoutScheduleCalculationReply.GetSellerPayoutSchedule().GetOrganizer().GetEmail(),
			},
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}); err != nil {
		return err
	}
	return nil
}

// SummarizeSellers summarizes a seller.
func (s *SellerQueryServer) SummarizeSellers(ctx context.Context, req *sellersv1.SummarizeSellersRequest) (*sellersv1.SummarizeSellersResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "seller", req.GetSellerId(), "summarize_sellers"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "summarize sellers", "tenant", req.GetContext().GetTenantId())
	validateGiftCardReply, err := s.giftCardValidationClient.ValidateGiftCard(ctx, &giftcardsv1.ValidateGiftCardRequest{
		Context:     req.GetContext(),
		Annotations: req.GetAnnotations(),
		Reason:      req.GetReason(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardValidationService/ValidateGiftCard")
	}
	previewSellerScorecardRequest := &sellersv1.PreviewSellerScorecardRequest{}
	previewSellerScorecardRequest.Context = req.GetContext()
	previewSellerScorecardRequest.SellerScorecard = &sellersv1.SellerScorecard{
		Settlement: &sellersv1.SellerScorecard_Bank{Bank: &sellersv1.SellerScorecardBankSettlement{Bic: validateGiftCardReply.GetGiftCard().GetBank().GetBic()}},
	}
	previewSellerScorecardReply, err := s.sellerScorecardValidationClient.PreviewSellerScorecard(ctx, previewSellerScorecardRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardValidationService/PreviewSellerScorecard")
	}
	_ = previewSellerScorecardReply
	return &sellersv1.SummarizeSellersResponse{
		Result: &commonv1.OperationResult{
			Status:  validateGiftCardReply.GetResult().GetStatus(),
			Message: validateGiftCardReply.GetOutcome().GetMessage(),
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// SuggestSellers lists sellers a page at a time.
func (s *SellerQueryServer) SuggestSellers(ctx context.Context, req *sellersv1.SuggestSellersRequest) (*sellersv1.SuggestSellersResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "suggest sellers", "tenant", req.GetContext().GetTenantId())
	recalculateSellerPayoutSchedulesRequest := &sellersv1.RecalculateSellerPayoutSchedulesRequest{
		Context: req.GetContext(),
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
			Location: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
			Participants: []*sellersv1.SellerPayoutScheduleParticipant{&sellersv1.SellerPayoutScheduleParticipant{
				Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			}},
		},
	}
	type recalculateSellerPayoutSchedulesResult struct {
		reply *sellersv1.RecalculateSellerPayoutSchedulesResponse
		err   error
	}
	recalculateSellerPayoutSchedulesResults := make(chan recalculateSellerPayoutSchedulesResult, 1)
	go func() {
		reply, err := s.sellerPayoutScheduleCalculationClient.RecalculateSellerPayoutSchedules(ctx, recalculateSellerPayoutSchedulesRequest)
		recalculateSellerPayoutSchedulesResults <- recalculateSellerPayoutSchedulesResult{reply, err}
	}()
	var recalculateSellerPayoutSchedulesReply *sellersv1.RecalculateSellerPayoutSchedulesResponse
	select {
	case result := <-recalculateSellerPayoutSchedulesResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.sellers.v1.SellerPayoutScheduleCalculationService/RecalculateSellerPayoutSchedules")
		}
		recalculateSellerPayoutSchedulesReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	quoteInvoiceBatchRequest := buildQuoteInvoiceBatchRequestForSuggestSellers(req, recalculateSellerPayoutSchedulesReply)
	quoteInvoiceBatchReply, err := s.invoiceBatchCalculationClient.QuoteInvoiceBatch(ctx, quoteInvoiceBatchRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.invoicing.v1.InvoiceBatchCalculationService/QuoteInvoiceBatch")
	}
	return &sellersv1.SuggestSellersResponse{
		Sellers: []*sellersv1.Seller{&sellersv1.Seller{
			Audit: &commonv1.AuditInfo{
				CreatedAt: recalculateSellerPayoutSchedulesReply.GetSellerPayoutSchedule().GetAudit().GetCreatedAt(),
				CreatedBy: quoteInvoiceBatchReply.GetInvoiceBatch().GetAudit().GetCreatedBy(),
			},
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}
