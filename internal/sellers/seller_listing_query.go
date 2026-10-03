package sellers

import (
	"context"
	"errors"
	"io"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// SellerListingQueryServer implements `acme.sellers.v1.SellerListingQueryService`.
// Answers read-only questions about seller listings, for screens and reports.
type SellerListingQueryServer struct {
	sellersv1.UnimplementedSellerListingQueryServiceServer
	store                                        *Store
	log                                          *slog.Logger
	conn                                         *grpc.ClientConn
	giftCardQueryClient                          giftcardsv1.GiftCardQueryServiceClient
	sellerValidationPort                         sellerValidationPort
	sellerPayoutScheduleValidationClient         sellersv1.SellerPayoutScheduleValidationServiceClient
	sellerPayoutScheduleValidationClientFallback sellersv1.SellerPayoutScheduleValidationServiceClient
	sellerPayoutScheduleCalculationClient        sellersv1.SellerPayoutScheduleCalculationServiceClient
}

// NewSellerListingQueryServer wires a SellerListingQueryServer.
func NewSellerListingQueryServer(deps platform.Deps) SellerListingQueryServer {
	return SellerListingQueryServer{
		store:                                NewStore(deps.DB),
		log:                                  deps.Logger.With("service", "acme.sellers.v1.SellerListingQueryService"),
		conn:                                 deps.Conn,
		giftCardQueryClient:                  giftcardsv1.NewGiftCardQueryServiceClient(deps.Conn),
		sellerValidationPort:                 newSellerValidationPort(deps),
		sellerPayoutScheduleValidationClient: sellersv1.NewSellerPayoutScheduleValidationServiceClient(deps.Conn),
		sellerPayoutScheduleValidationClientFallback: sellersv1.NewSellerPayoutScheduleValidationServiceClient(deps.Conn),
		sellerPayoutScheduleCalculationClient:        sellersv1.NewSellerPayoutScheduleCalculationServiceClient(deps.Conn),
	}
}

// LookupSellerListing returns a seller listing.
func (s SellerListingQueryServer) LookupSellerListing(ctx context.Context, req *sellersv1.LookupSellerListingRequest) (*sellersv1.LookupSellerListingResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected LookupSellerListing", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "seller_listing", req.GetSellerListingId(), "lookup_seller_listing"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "lookup seller listing", "tenant", req.GetContext().GetTenantId())
	previewSellerListingRequest := &sellersv1.PreviewSellerListingRequest{
		Context: req.GetContext(),
		SellerListing: &sellersv1.SellerListing{
			SellerListingId: req.GetSellerListingId(),
			Owner: &identityv1.UserRef{
				Email:  req.GetContext().GetActor().GetEmail(),
				UserId: req.GetContext().GetActor().GetUserId(),
			},
		},
		Details: &sellersv1.PreviewSellerListingRequest_Details{Locale: req.GetContext().GetLocale()},
	}
	previewSellerListingReply := new(sellersv1.PreviewSellerListingResponse)
	if err := grpcx.LegacyInvoke(ctx, s.conn, "/acme.sellers.v1.SellerListingValidationService/PreviewSellerListing", previewSellerListingRequest, previewSellerListingReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingValidationService/PreviewSellerListing")
	}
	return &sellersv1.LookupSellerListingResponse{
		SellerListing: &sellersv1.SellerListing{
			Owner: &identityv1.UserRef{
				UserId: previewSellerListingReply.GetSellerListing().GetOwner().GetUserId(),
			},
			SellerListingId: req.GetSellerListingId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// BrowseSellerListings lists seller listings a page at a time.
func (s SellerListingQueryServer) BrowseSellerListings(ctx context.Context, req *sellersv1.BrowseSellerListingsRequest) (*sellersv1.BrowseSellerListingsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "browse seller listings", "tenant", req.GetContext().GetTenantId())
	return &sellersv1.BrowseSellerListingsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// FindSellerListings lists seller listings a page at a time.
func (s SellerListingQueryServer) FindSellerListings(ctx context.Context, req *sellersv1.FindSellerListingsRequest) (*sellersv1.FindSellerListingsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected FindSellerListings", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "find seller listings", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "query", req.GetFilter().GetQuery())
	exportGiftCardsStream, err := s.giftCardQueryClient.ExportGiftCards(ctx, &giftcardsv1.ExportGiftCardsRequest{
		Context: req.GetContext(),
		Filter: &giftcardsv1.ExportGiftCardsRequest_Filter{
			Region:  req.GetFilter().GetRegion(),
			Query:   tenancy.Value(ctx, "query"),
			OwnerId: req.GetFilter().GetOwnerId(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardQueryService/ExportGiftCards")
	}
	for {
		received, err := exportGiftCardsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardQueryService/ExportGiftCards")
		}
		_ = received
	}
	return &sellersv1.FindSellerListingsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// ResolveSellerListings returns several seller listings at once.
func (s SellerListingQueryServer) ResolveSellerListings(ctx context.Context, req *sellersv1.ResolveSellerListingsRequest) (*sellersv1.ResolveSellerListingsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "resolve seller listings", "tenant", req.GetContext().GetTenantId())
	contextCache := cache.New[string, *commonv1.RequestContext](1)
	contextCache.Put(req.GetContext().GetTraceId(), req.GetContext())
	cachedContext, _ := contextCache.Get(req.GetContext().GetTraceId())
	explainSellerDecisionRequest := &sellersv1.ExplainSellerDecisionRequest{
		Context: req.GetContext(),
		Seller: &sellersv1.Seller{
			RelationshipManager: &identityv1.UserRef{
				UserId: req.GetContext().GetActor().GetUserId(),
				Email:  cachedContext.GetActor().GetEmail(),
			},
		},
	}
	explainSellerDecisionReply, err := s.sellerValidationPort.ExplainSellerDecision(ctx, explainSellerDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerValidationService/ExplainSellerDecision")
	}
	return &sellersv1.ResolveSellerListingsResponse{
		SellerListings: []*sellersv1.SellerListing{&sellersv1.SellerListing{
			Owner: &identityv1.UserRef{
				DisplayName: explainSellerDecisionReply.GetSeller().GetRelationshipManager().GetDisplayName(),
			},
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ExportSellerListings streams every matching seller listing.
func (s SellerListingQueryServer) ExportSellerListings(req *sellersv1.ExportSellerListingsRequest, stream grpc.ServerStreamingServer[sellersv1.ExportSellerListingsResponse]) error {
	ctx := stream.Context()
	if req.GetContext().GetTenantId() == "" {
		return status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "export seller listings", "tenant", req.GetContext().GetTenantId())
	routes := map[string]func(context.Context, *sellersv1.LintSellerPayoutSchedulesRequest, ...grpc.CallOption) (*sellersv1.LintSellerPayoutSchedulesResponse, error){
		"primary":  s.sellerPayoutScheduleValidationClient.LintSellerPayoutSchedules,
		"fallback": s.sellerPayoutScheduleValidationClientFallback.LintSellerPayoutSchedules,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	cloned := proto.Clone(req.GetContext()).(*commonv1.RequestContext)
	lintSellerPayoutSchedulesRequest := &sellersv1.LintSellerPayoutSchedulesRequest{
		Context: req.GetContext(),
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
			Location:  &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
			Organizer: &identityv1.UserRef{Email: req.GetContext().GetActor().GetEmail()},
		},
		Details: &sellersv1.LintSellerPayoutSchedulesRequest_Details{Locale: cloned.GetLocale()},
	}
	lintSellerPayoutSchedulesReply, err := route(ctx, lintSellerPayoutSchedulesRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.sellers.v1.SellerPayoutScheduleValidationService/LintSellerPayoutSchedules")
	}
	if err := stream.Send(&sellersv1.ExportSellerListingsResponse{
		SellerListing: &sellersv1.SellerListing{
			Owner: &identityv1.UserRef{
				DisplayName: lintSellerPayoutSchedulesReply.GetSellerPayoutSchedule().GetOrganizer().GetDisplayName(),
			},
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}); err != nil {
		return err
	}
	return nil
}

// SummarizeSellerListings summarizes a seller listing.
func (s SellerListingQueryServer) SummarizeSellerListings(ctx context.Context, req *sellersv1.SummarizeSellerListingsRequest) (*sellersv1.SummarizeSellerListingsResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "seller_listing", req.GetSellerListingId(), "summarize_seller_listings"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "summarize seller listings", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "email", req.GetContext().GetActor().GetEmail())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	cloned := proto.Clone(req.GetContext()).(*commonv1.RequestContext)
	simulateSellerPayoutScheduleRequest := &sellersv1.SimulateSellerPayoutScheduleRequest{
		Context: req.GetContext(),
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
			Organizer: &identityv1.UserRef{
				Email:  tenancy.Value(ctx, "email"),
				UserId: cloned.GetActor().GetUserId(),
			},
		},
		Annotations: annotations,
		Reason:      req.GetReason(),
	}
	send := s.simulateSellerPayoutScheduleSender(simulateSellerPayoutScheduleRequest)
	simulateSellerPayoutScheduleReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerPayoutScheduleCalculationService/SimulateSellerPayoutSchedule")
	}
	return &sellersv1.SummarizeSellerListingsResponse{
		Result: &commonv1.OperationResult{
			Details: simulateSellerPayoutScheduleReply.GetResult().GetDetails(),
			Status:  simulateSellerPayoutScheduleReply.GetResult().GetStatus(),
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// PeekSellerListing returns a seller listing.
func (s SellerListingQueryServer) PeekSellerListing(ctx context.Context, req *sellersv1.PeekSellerListingRequest) (*sellersv1.PeekSellerListingResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "seller_listing", req.GetSellerListingId(), "peek_seller_listing"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "peek seller listing", "tenant", req.GetContext().GetTenantId())
	return &sellersv1.PeekSellerListingResponse{
		SellerListing: &sellersv1.SellerListing{SellerListingId: req.GetSellerListingId()},
		Result:        &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// simulateSellerPayoutScheduleSender returns a function that sends one request to
// acme.sellers.v1.SellerPayoutScheduleCalculationService/SimulateSellerPayoutSchedule.
func (s SellerListingQueryServer) simulateSellerPayoutScheduleSender(request *sellersv1.SimulateSellerPayoutScheduleRequest) func(context.Context) (*sellersv1.SimulateSellerPayoutScheduleResponse, error) {
	return func(ctx context.Context) (*sellersv1.SimulateSellerPayoutScheduleResponse, error) {
		return s.sellerPayoutScheduleCalculationClient.SimulateSellerPayoutSchedule(ctx, request)
	}
}
