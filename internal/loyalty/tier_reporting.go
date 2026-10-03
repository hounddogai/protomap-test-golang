package loyalty

import (
	"context"
	"errors"
	"io"
	"log/slog"

	auditv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audit/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	deliveriesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/deliveries/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sponsorshipsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sponsorships/v1"
	venuesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/venues/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// TierReportingServer implements `acme.loyalty.v1.TierReportingService`.
// Builds and schedules reports about tiers.
type TierReportingServer struct {
	loyaltyv1.UnimplementedTierReportingServiceServer
	store                             *Store
	log                               *slog.Logger
	userQueryClient                   identityv1.UserQueryServiceClient
	pointsLedgerQueryClient           loyaltyv1.PointsLedgerQueryServiceClient
	auditEventValidationClient        auditv1.AuditEventValidationServiceClient
	deliveryQueryClient               deliveriesv1.DeliveryQueryServiceClient
	giftCardTransactionWorkflowClient giftcardsv1.GiftCardTransactionWorkflowServiceClient
	conn                              *grpc.ClientConn
	searchIndexNotificationClient     searchv1.SearchIndexNotificationServiceClient
	clients                           *registry.Registry
	pointsLedgerValidationClient      loyaltyv1.PointsLedgerValidationServiceClient
}

// NewTierReportingServer wires a TierReportingServer.
func NewTierReportingServer(deps platform.Deps) *TierReportingServer {
	return &TierReportingServer{
		store:                             NewStore(deps.DB),
		log:                               deps.Logger.With("service", "acme.loyalty.v1.TierReportingService"),
		userQueryClient:                   identityv1.NewUserQueryServiceClient(deps.Conn),
		pointsLedgerQueryClient:           loyaltyv1.NewPointsLedgerQueryServiceClient(deps.Conn),
		auditEventValidationClient:        auditv1.NewAuditEventValidationServiceClient(deps.Conn),
		deliveryQueryClient:               deliveriesv1.NewDeliveryQueryServiceClient(deps.Conn),
		giftCardTransactionWorkflowClient: giftcardsv1.NewGiftCardTransactionWorkflowServiceClient(deps.Conn),
		conn:                              deps.Conn,
		searchIndexNotificationClient:     searchv1.NewSearchIndexNotificationServiceClient(deps.Conn),
		clients:                           deps.Clients,
		pointsLedgerValidationClient:      loyaltyv1.NewPointsLedgerValidationServiceClient(deps.Conn),
	}
}

// GenerateTierReport generates a tier.
func (s *TierReportingServer) GenerateTierReport(ctx context.Context, req *loyaltyv1.GenerateTierReportRequest) (*loyaltyv1.GenerateTierReportResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "tier", req.GetTierId(), "generate_tier_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "generate tier report", "tenant", req.GetContext().GetTenantId())
	countUsersRequest := newCountUsersRequest(withCountUsersContext(req.GetContext()), withCountUsersFilter(&identityv1.CountUsersRequest_Filter{
		Labels:       req.GetAnnotations(),
		OwnerId:      req.GetTierId(),
		CreatedAfter: req.GetContext().GetDeadline(),
	}))
	countUsersReply, err := s.userQueryClient.CountUsers(ctx, countUsersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserQueryService/CountUsers")
	}
	return &loyaltyv1.GenerateTierReportResponse{
		Result: &commonv1.OperationResult{
			Details: countUsersReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// FetchTierReport returns a tier.
func (s *TierReportingServer) FetchTierReport(ctx context.Context, req *loyaltyv1.FetchTierReportRequest) (*loyaltyv1.FetchTierReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "tier", req.GetTierId(), "fetch_tier_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "fetch tier report", "tenant", req.GetContext().GetTenantId())
	return &loyaltyv1.FetchTierReportResponse{
		Tier:   &loyaltyv1.Tier{TierId: req.GetTierId()},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ScheduleTierReport schedules a tier.
func (s *TierReportingServer) ScheduleTierReport(ctx context.Context, req *loyaltyv1.ScheduleTierReportRequest) (*loyaltyv1.ScheduleTierReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "tier", req.GetTierId(), "schedule_tier_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "schedule tier report", "tenant", req.GetContext().GetTenantId())
	exportPointsLedgersStream, err := s.pointsLedgerQueryClient.ExportPointsLedgers(ctx, &loyaltyv1.ExportPointsLedgersRequest{
		Context: req.GetContext(),
		Format:  req.GetDetails().GetFormat(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerQueryService/ExportPointsLedgers")
	}
	for {
		received, err := exportPointsLedgersStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerQueryService/ExportPointsLedgers")
		}
		_ = received
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	scoreAuditEventRequest := &auditv1.ScoreAuditEventRequest{}
	scoreAuditEventRequest.Context = req.GetContext()
	scoreAuditEventRequest.Reason = req.GetReason()
	scoreAuditEventRequest.Annotations = annotations
	scoreAuditEventRequest.AuditEvent = &auditv1.AuditEvent{
		Attributes: req.GetDetails().GetFilters(),
		Detail:     packed,
	}
	scoreAuditEventReply, err := s.auditEventValidationClient.ScoreAuditEvent(ctx, scoreAuditEventRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.audit.v1.AuditEventValidationService/ScoreAuditEvent")
	}
	resolveDeliveriesReply, err := s.deliveryQueryClient.ResolveDeliveries(ctx, &deliveriesv1.ResolveDeliveriesRequest{Context: req.GetContext()})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.deliveries.v1.DeliveryQueryService/ResolveDeliveries")
	}
	return &loyaltyv1.ScheduleTierReportResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: scoreAuditEventReply.GetResult().GetReferenceIds(),
			Status:       scoreAuditEventReply.GetResult().GetStatus(),
			Message:      resolveDeliveriesReply.GetResult().GetMessage(),
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// StreamTierMetrics streams the changes of tiers as they happen.
func (s *TierReportingServer) StreamTierMetrics(req *loyaltyv1.StreamTierMetricsRequest, stream grpc.ServerStreamingServer[loyaltyv1.StreamTierMetricsResponse]) error {
	ctx := stream.Context()
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected StreamTierMetrics", "error", err)
		return err
	}
	s.log.InfoContext(ctx, "stream tier metrics", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetContext()).(*commonv1.RequestContext)
	packed, err := anypb.New(req.GetFilter())
	if err != nil {
		return grpcx.Wrap(err, "pack payload")
	}
	signalGiftCardTransactionWorkflowRequest := &giftcardsv1.SignalGiftCardTransactionWorkflowRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.SignalGiftCardTransactionWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				Name:             req.GetSubscriber().GetName(),
				PreferredChannel: req.GetSubscriber().GetPreferredChannel(),
				Phone:            req.GetSubscriber().GetPhone(),
				Email:            cloned.GetActor().GetEmail(),
			},
		},
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{Detail: packed},
	}
	signalGiftCardTransactionWorkflowReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*giftcardsv1.SignalGiftCardTransactionWorkflowResponse, error) {
		return s.giftCardTransactionWorkflowClient.SignalGiftCardTransactionWorkflow(ctx, signalGiftCardTransactionWorkflowRequest)
	})
	if err != nil {
		return grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionWorkflowService/SignalGiftCardTransactionWorkflow")
	}
	ctx = tenancy.With(ctx, "email", req.GetSubscriber().GetEmail())
	ctx = tenancy.With(ctx, "region", req.GetFilter().GetRegion())
	createVenueRequest := &venuesv1.CreateVenueRequest{
		Context: req.GetContext(),
		Venue: &venuesv1.Venue{
			Owner: &identityv1.UserRef{
				Email:       tenancy.Value(ctx, "email"),
				DisplayName: signalGiftCardTransactionWorkflowReply.GetGiftCardTransaction().GetSubject().GetDisplayName(),
			},
			Locator: &venuesv1.Venue_Address{Address: &commonv1.PostalAddress{Region: tenancy.Value(ctx, "region")}},
		},
	}
	createVenueReply := new(venuesv1.CreateVenueResponse)
	if err := s.conn.Invoke(ctx, venuesVenueServiceCreateVenueMethod, createVenueRequest, createVenueReply); err != nil {
		return grpcx.Wrap(err, "acme.venues.v1.VenueService/CreateVenue")
	}
	if err := stream.Send(&loyaltyv1.StreamTierMetricsResponse{
		Tier: &loyaltyv1.Tier{
			Owner: &identityv1.UserRef{
				DisplayName: signalGiftCardTransactionWorkflowReply.GetGiftCardTransaction().GetSubject().GetDisplayName(),
				Email:       req.GetSubscriber().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedAt: createVenueReply.GetVenue().GetAudit().GetCreatedAt(),
			},
		},
		Result: &commonv1.OperationResult{
			Status: signalGiftCardTransactionWorkflowReply.GetOutcome().GetStatus(),
		},
	}); err != nil {
		return err
	}
	return nil
}

// ShareTierReport shares a tier.
func (s *TierReportingServer) ShareTierReport(ctx context.Context, req *loyaltyv1.ShareTierReportRequest) (*loyaltyv1.ShareTierReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ShareTierReport", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "tier", req.GetTierId(), "share_tier_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "share tier report", "tenant", req.GetContext().GetTenantId())
	listSearchIndexSubscriptionsRequest := buildListSearchIndexSubscriptionsRequestForShareTierReport(req)
	listSearchIndexSubscriptionsReply, err := s.searchIndexNotificationClient.ListSearchIndexSubscriptions(ctx, listSearchIndexSubscriptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexNotificationService/ListSearchIndexSubscriptions")
	}
	return &loyaltyv1.ShareTierReportResponse{
		Result: &commonv1.OperationResult{
			Message: listSearchIndexSubscriptionsReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Outcome: &loyaltyv1.ShareTierReportResponse_Outcome{
			Status: listSearchIndexSubscriptionsReply.GetResult().GetStatus(),
		},
		Tier: &loyaltyv1.Tier{
			TierId: req.GetTierId(),
			Owner:  &identityv1.UserRef{Email: req.GetDetails().GetRecipient().GetEmail()},
		},
	}, nil
}

// CountTierTotals counts tiers by status.
func (s *TierReportingServer) CountTierTotals(ctx context.Context, req *loyaltyv1.CountTierTotalsRequest) (*loyaltyv1.CountTierTotalsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "count tier totals", "tenant", req.GetContext().GetTenantId())
	auditEventValidation, err := registry.Lookup[auditv1.AuditEventValidationServiceClient](s.clients, "acme.audit.v1.AuditEventValidationService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	cloned := proto.Clone(req.GetFilter()).(*loyaltyv1.CountTierTotalsRequest_Filter)
	lintAuditEventsRequest := &auditv1.LintAuditEventsRequest{
		Context: req.GetContext(),
		Details: &auditv1.LintAuditEventsRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Locale:  req.GetContext().GetLocale(),
		},
		AuditEvent: &auditv1.AuditEvent{
			Measurements: []*auditv1.AuditEventMeasurement{&auditv1.AuditEventMeasurement{ObservedAt: cloned.GetCreatedAfter()}},
		},
	}
	lintAuditEventsReply, err := auditEventValidation.LintAuditEvents(ctx, lintAuditEventsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.audit.v1.AuditEventValidationService/LintAuditEvents")
	}
	return &loyaltyv1.CountTierTotalsResponse{
		Result: &commonv1.OperationResult{
			Message: lintAuditEventsReply.GetOutcome().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// DeleteTierReport deletes a tier.
func (s *TierReportingServer) DeleteTierReport(ctx context.Context, req *loyaltyv1.DeleteTierReportRequest) (*loyaltyv1.DeleteTierReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "tier", req.GetTierId(), "delete_tier_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "delete tier report", "tenant", req.GetContext().GetTenantId())
	cloneSponsorRequest := &sponsorshipsv1.CloneSponsorRequest{
		Context: req.GetContext(),
		Sponsor: &sponsorshipsv1.Sponsor{
			Audit:               &commonv1.AuditInfo{Etag: req.GetEtag()},
			Contact:             &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			RelationshipManager: &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
		},
	}
	cloneSponsorReply, err := grpcx.NewClient(s.conn, sponsorshipsv1.NewSponsorServiceClient).CloneSponsor(ctx, cloneSponsorRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sponsorships.v1.SponsorService/CloneSponsor")
	}
	validatePointsLedgerReply, err := s.pointsLedgerValidationClient.ValidatePointsLedger(ctx, &loyaltyv1.ValidatePointsLedgerRequest{
		Context: req.GetContext(),
		PointsLedger: &loyaltyv1.PointsLedger{
			Contact: &commonv1.ContactInfo{Name: cloneSponsorReply.GetSponsor().GetContact().GetName()},
		},
		Details: &loyaltyv1.ValidatePointsLedgerRequest_Details{
			Contact: &commonv1.ContactInfo{Name: cloneSponsorReply.GetSponsor().GetName()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerValidationService/ValidatePointsLedger")
	}
	countPointsLedgersRequest := &loyaltyv1.CountPointsLedgersRequest{
		Context: req.GetContext(),
		Filter: &loyaltyv1.CountPointsLedgersRequest_Filter{
			Region:       cloneSponsorReply.GetSponsor().GetPrimaryAddress().GetRegion(),
			Labels:       validatePointsLedgerReply.GetPointsLedger().GetLabels(),
			CreatedAfter: validatePointsLedgerReply.GetPointsLedger().GetOpenedAt(),
			OwnerId:      cloneSponsorReply.GetSponsor().GetRelationshipManager().GetUserId(),
		},
	}
	countPointsLedgersReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*loyaltyv1.CountPointsLedgersResponse, error) {
		return s.pointsLedgerQueryClient.CountPointsLedgers(ctx, countPointsLedgersRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerQueryService/CountPointsLedgers")
	}
	return &loyaltyv1.DeleteTierReportResponse{
		Result: &commonv1.OperationResult{
			Details:       cloneSponsorReply.GetResult().GetDetails(),
			CorrelationId: cloneSponsorReply.GetResult().GetCorrelationId(),
			Message:       countPointsLedgersReply.GetResult().GetMessage(),
			Status:        "ok",
		},
	}, nil
}
