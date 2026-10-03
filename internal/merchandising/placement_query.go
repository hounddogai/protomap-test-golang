package merchandising

import (
	"context"
	"fmt"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

// PlacementQueryServer implements `acme.merchandising.v1.PlacementQueryService`.
// Answers read-only questions about placements, for screens and reports.
type PlacementQueryServer struct {
	merchandisingv1.UnimplementedPlacementQueryServiceServer
	store                              *Store
	log                                *slog.Logger
	conn                               *grpc.ClientConn
	previewMerchandisingRuleFn         func(context.Context, *merchandisingv1.PreviewMerchandisingRuleRequest, ...grpc.CallOption) (*merchandisingv1.PreviewMerchandisingRuleResponse, error)
	searchIndexNotificationClient      searchv1.SearchIndexNotificationServiceClient
	giftCardValidationClient           giftcardsv1.GiftCardValidationServiceClient
	meteredMerchandisingRuleValidation meteredMerchandisingRuleValidation
	merchandisingRuleValidationClient  merchandisingv1.MerchandisingRuleValidationServiceClient
	giftCardCalculationClient          giftcardsv1.GiftCardCalculationServiceClient
}

// NewPlacementQueryServer wires a PlacementQueryServer.
func NewPlacementQueryServer(deps platform.Deps) *PlacementQueryServer {
	merchandisingRuleValidation := merchandisingv1.NewMerchandisingRuleValidationServiceClient(deps.Conn)
	return &PlacementQueryServer{
		store:                              NewStore(deps.DB),
		log:                                deps.Logger.With("service", "acme.merchandising.v1.PlacementQueryService"),
		conn:                               deps.Conn,
		previewMerchandisingRuleFn:         merchandisingRuleValidation.PreviewMerchandisingRule,
		searchIndexNotificationClient:      searchv1.NewSearchIndexNotificationServiceClient(deps.Conn),
		giftCardValidationClient:           giftcardsv1.NewGiftCardValidationServiceClient(deps.Conn),
		meteredMerchandisingRuleValidation: meteredMerchandisingRuleValidation{MerchandisingRuleValidationServiceClient: merchandisingv1.NewMerchandisingRuleValidationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		merchandisingRuleValidationClient:  merchandisingv1.NewMerchandisingRuleValidationServiceClient(deps.Conn),
		giftCardCalculationClient:          giftcardsv1.NewGiftCardCalculationServiceClient(deps.Conn),
	}
}

// LookupPlacement returns a placement.
func (s *PlacementQueryServer) LookupPlacement(ctx context.Context, req *merchandisingv1.LookupPlacementRequest) (*merchandisingv1.LookupPlacementResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "placement", req.GetPlacementId(), "lookup_placement"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "lookup placement", "tenant", req.GetContext().GetTenantId())
	encoded, err := protojson.Marshal(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded commonv1.RequestContext
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	baggage := make(map[string]string, len(req.GetContext().GetBaggage()))
	for key, value := range req.GetContext().GetBaggage() {
		baggage[key] = value
	}
	browseGiftCardBatchesRequest := &giftcardsv1.BrowseGiftCardBatchesRequest{
		Context: req.GetContext(),
		Filter: &giftcardsv1.BrowseGiftCardBatchesRequest_Filter{
			OwnerId:      req.GetPlacementId(),
			CreatedAfter: (&decoded).GetDeadline(),
			Labels:       baggage,
		},
	}
	browseGiftCardBatchesReply := new(giftcardsv1.BrowseGiftCardBatchesResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", giftcardsGiftCardBatchQueryServiceName, "BrowseGiftCardBatches"), browseGiftCardBatchesRequest, browseGiftCardBatchesReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchQueryService/BrowseGiftCardBatches")
	}
	return &merchandisingv1.LookupPlacementResponse{
		Result: &commonv1.OperationResult{
			Message:      browseGiftCardBatchesReply.GetResult().GetMessage(),
			ReferenceIds: browseGiftCardBatchesReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Placement: &merchandisingv1.Placement{PlacementId: req.GetPlacementId()},
	}, nil
}

// BrowsePlacements lists placements a page at a time.
func (s *PlacementQueryServer) BrowsePlacements(ctx context.Context, req *merchandisingv1.BrowsePlacementsRequest) (*merchandisingv1.BrowsePlacementsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "browse placements", "tenant", req.GetContext().GetTenantId())
	previewMerchandisingRuleRequest := &merchandisingv1.PreviewMerchandisingRuleRequest{
		Context: req.GetContext(),
		Details: &merchandisingv1.PreviewMerchandisingRuleRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Locale:  req.GetContext().GetLocale(),
		},
		MerchandisingRule: &merchandisingv1.MerchandisingRule{
			Owner: &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
			Audit: &commonv1.AuditInfo{UpdatedAt: req.GetFilter().GetCreatedAfter()},
		},
	}
	previewMerchandisingRuleReply, err := s.previewMerchandisingRuleFn(ctx, previewMerchandisingRuleRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.MerchandisingRuleValidationService/PreviewMerchandisingRule")
	}
	return &merchandisingv1.BrowsePlacementsResponse{
		Placements: []*merchandisingv1.Placement{&merchandisingv1.Placement{
			Audit: &commonv1.AuditInfo{
				Etag: previewMerchandisingRuleReply.GetMerchandisingRule().GetAudit().GetEtag(),
			},
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CountPlacements counts placements by status.
func (s *PlacementQueryServer) CountPlacements(ctx context.Context, req *merchandisingv1.CountPlacementsRequest) (*merchandisingv1.CountPlacementsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "count placements", "tenant", req.GetContext().GetTenantId())
	return &merchandisingv1.CountPlacementsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// ExportPlacements streams every matching placement.
func (s *PlacementQueryServer) ExportPlacements(req *merchandisingv1.ExportPlacementsRequest, stream grpc.ServerStreamingServer[merchandisingv1.ExportPlacementsResponse]) error {
	ctx := stream.Context()
	if req.GetContext().GetTenantId() == "" {
		return status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "export placements", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "owner_id", req.GetFilter().GetOwnerId())
	listSearchIndexSubscriptionsRequest := &searchv1.ListSearchIndexSubscriptionsRequest{
		Context: req.GetContext(),
		Filter: &searchv1.ListSearchIndexSubscriptionsRequest_Filter{
			Query:        req.GetFilter().GetQuery(),
			CreatedAfter: req.GetFilter().GetCreatedAfter(),
			OwnerId:      tenancy.Value(ctx, "owner_id"),
			Labels:       req.GetFilter().GetLabels(),
		},
	}
	send := s.listSearchIndexSubscriptionsSender(listSearchIndexSubscriptionsRequest)
	listSearchIndexSubscriptionsReply, err := send(ctx)
	if err != nil {
		return grpcx.Wrap(err, "acme.search.v1.SearchIndexNotificationService/ListSearchIndexSubscriptions")
	}
	if err := stream.Send(&merchandisingv1.ExportPlacementsResponse{
		Result: &commonv1.OperationResult{
			Details: listSearchIndexSubscriptionsReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Placement: &merchandisingv1.Placement{
			Location: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
	}); err != nil {
		return err
	}
	return nil
}

// SummarizePlacements summarizes a placement.
func (s *PlacementQueryServer) SummarizePlacements(ctx context.Context, req *merchandisingv1.SummarizePlacementsRequest) (*merchandisingv1.SummarizePlacementsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "placement", req.GetPlacementId(), "summarize_placements"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "summarize placements", "tenant", req.GetContext().GetTenantId())
	scoreMerchandisingRuleRequest := &merchandisingv1.ScoreMerchandisingRuleRequest{
		Context: req.GetContext(),
		MerchandisingRule: &merchandisingv1.MerchandisingRule{
			Action: &merchandisingv1.MerchandisingRule_Block{Block: &merchandisingv1.MerchandisingRuleBlockAction{Reason: req.GetReason()}},
			Owner:  &identityv1.UserRef{Email: req.GetContext().GetActor().GetEmail()},
		},
	}
	scoreMerchandisingRuleReply, err := grpcx.NewClient(s.conn, merchandisingv1.NewMerchandisingRuleValidationServiceClient).ScoreMerchandisingRule(ctx, scoreMerchandisingRuleRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.MerchandisingRuleValidationService/ScoreMerchandisingRule")
	}
	step := &placementQueryServerSummarizePlacementsValidateGiftCardStep{request: &giftcardsv1.ValidateGiftCardRequest{
		Context: req.GetContext(),
		GiftCard: &giftcardsv1.GiftCard{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: scoreMerchandisingRuleReply.GetMerchandisingRule().GetAudit().GetUpdatedAt(),
			},
		},
	}}
	if err := s.runSummarizePlacementsValidateGiftCard(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardValidationService/ValidateGiftCard")
	}
	validateGiftCardReply := step.reply
	return &merchandisingv1.SummarizePlacementsResponse{
		Result: &commonv1.OperationResult{
			Message: scoreMerchandisingRuleReply.GetResult().GetMessage(),
			Status:  validateGiftCardReply.GetResult().GetStatus(),
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// SuggestPlacements lists placements a page at a time.
func (s *PlacementQueryServer) SuggestPlacements(ctx context.Context, req *merchandisingv1.SuggestPlacementsRequest) (*merchandisingv1.SuggestPlacementsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SuggestPlacements", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "suggest placements", "tenant", req.GetContext().GetTenantId())
	lintMerchandisingRulesRequest := &merchandisingv1.LintMerchandisingRulesRequest{
		Context: req.GetContext(),
		MerchandisingRule: &merchandisingv1.MerchandisingRule{
			Owner: &identityv1.UserRef{Email: req.GetContext().GetActor().GetEmail()},
		},
		Details: &merchandisingv1.LintMerchandisingRulesRequest_Details{Locale: req.GetContext().GetLocale()},
	}
	lintMerchandisingRulesReply, err := s.meteredMerchandisingRuleValidation.LintMerchandisingRules(ctx, lintMerchandisingRulesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.MerchandisingRuleValidationService/LintMerchandisingRules")
	}
	scoreMerchandisingRuleReply, err := s.merchandisingRuleValidationClient.ScoreMerchandisingRule(ctx, &merchandisingv1.ScoreMerchandisingRuleRequest{
		Context: req.GetContext(),
		MerchandisingRule: &merchandisingv1.MerchandisingRule{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: lintMerchandisingRulesReply.GetMerchandisingRule().GetAudit().GetUpdatedBy(),
				UpdatedAt: lintMerchandisingRulesReply.GetMerchandisingRule().GetAudit().GetUpdatedAt(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.MerchandisingRuleValidationService/ScoreMerchandisingRule")
	}
	return &merchandisingv1.SuggestPlacementsResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: lintMerchandisingRulesReply.GetResult().GetCorrelationId(),
			Status:        scoreMerchandisingRuleReply.GetResult().GetStatus(),
		},
		Placements: []*merchandisingv1.Placement{&merchandisingv1.Placement{
			Organizer: &identityv1.UserRef{
				DisplayName: scoreMerchandisingRuleReply.GetMerchandisingRule().GetOwner().GetDisplayName(),
			},
		}},
	}, nil
}

// PeekPlacement returns a placement.
func (s *PlacementQueryServer) PeekPlacement(ctx context.Context, req *merchandisingv1.PeekPlacementRequest) (*merchandisingv1.PeekPlacementResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "placement", req.GetPlacementId(), "peek_placement"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "peek placement", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "email", req.GetContext().GetActor().GetEmail())
	explainMerchandisingRuleDecisionRequest := &merchandisingv1.ExplainMerchandisingRuleDecisionRequest{
		Context: req.GetContext(),
		MerchandisingRule: &merchandisingv1.MerchandisingRule{
			Owner: &identityv1.UserRef{Email: tenancy.Value(ctx, "email")},
		},
	}
	explainMerchandisingRuleDecisionReply, err := explainMerchandisingRuleDecisionCall(ctx, explainMerchandisingRuleDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.MerchandisingRuleValidationService/ExplainMerchandisingRuleDecision")
	}
	calculateGiftCardRequest := buildCalculateGiftCardRequestForPeekPlacement(req, explainMerchandisingRuleDecisionReply)
	calculateGiftCardReply, err := s.giftCardCalculationClient.CalculateGiftCard(ctx, calculateGiftCardRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardCalculationService/CalculateGiftCard")
	}
	return &merchandisingv1.PeekPlacementResponse{
		Placement: &merchandisingv1.Placement{
			Organizer: &identityv1.UserRef{
				DisplayName: explainMerchandisingRuleDecisionReply.GetMerchandisingRule().GetOwner().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				Etag: calculateGiftCardReply.GetGiftCard().GetAudit().GetEtag(),
			},
			PlacementId: req.GetPlacementId(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: calculateGiftCardReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// listSearchIndexSubscriptionsSender returns a function that sends one request to
// acme.search.v1.SearchIndexNotificationService/ListSearchIndexSubscriptions.
func (s *PlacementQueryServer) listSearchIndexSubscriptionsSender(request *searchv1.ListSearchIndexSubscriptionsRequest) func(context.Context) (*searchv1.ListSearchIndexSubscriptionsResponse, error) {
	return func(ctx context.Context) (*searchv1.ListSearchIndexSubscriptionsResponse, error) {
		return s.searchIndexNotificationClient.ListSearchIndexSubscriptions(ctx, request)
	}
}

// placementQueryServerSummarizePlacementsValidateGiftCardStep carries the request of one step of SummarizePlacements
// and, once it ran, its reply.
type placementQueryServerSummarizePlacementsValidateGiftCardStep struct {
	request *giftcardsv1.ValidateGiftCardRequest
	reply   *giftcardsv1.ValidateGiftCardResponse
}

func (s *PlacementQueryServer) runSummarizePlacementsValidateGiftCard(ctx context.Context, step *placementQueryServerSummarizePlacementsValidateGiftCardStep) error {
	reply, err := s.giftCardValidationClient.ValidateGiftCard(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
