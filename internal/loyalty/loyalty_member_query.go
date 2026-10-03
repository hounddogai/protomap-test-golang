package loyalty

import (
	"context"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// LoyaltyMemberQueryServer implements `acme.loyalty.v1.LoyaltyMemberQueryService`.
// Answers read-only questions about loyalty members, for screens and reports.
type LoyaltyMemberQueryServer struct {
	loyaltyv1.UnimplementedLoyaltyMemberQueryServiceServer
	store                         *Store
	log                           *slog.Logger
	tierValidationClient          loyaltyv1.TierValidationServiceClient
	conn                          *grpc.ClientConn
	tenantExportClient            identityv1.TenantExportServiceClient
	pointsLedgerValidationPort    pointsLedgerValidationPort
	placementQueryClient          merchandisingv1.PlacementQueryServiceClient
	loyaltyMemberValidationClient loyaltyv1.LoyaltyMemberValidationServiceClient
	meteredTenantExport           meteredTenantExport
	bus                           *eventbus.Bus
}

// NewLoyaltyMemberQueryServer wires a LoyaltyMemberQueryServer.
func NewLoyaltyMemberQueryServer(deps platform.Deps) *LoyaltyMemberQueryServer {
	return &LoyaltyMemberQueryServer{
		store:                         NewStore(deps.DB),
		log:                           deps.Logger.With("service", "acme.loyalty.v1.LoyaltyMemberQueryService"),
		tierValidationClient:          loyaltyv1.NewTierValidationServiceClient(deps.Conn),
		conn:                          deps.Conn,
		tenantExportClient:            identityv1.NewTenantExportServiceClient(deps.Conn),
		pointsLedgerValidationPort:    newPointsLedgerValidationPort(deps),
		placementQueryClient:          merchandisingv1.NewPlacementQueryServiceClient(deps.Conn),
		loyaltyMemberValidationClient: loyaltyv1.NewLoyaltyMemberValidationServiceClient(deps.Conn),
		meteredTenantExport:           meteredTenantExport{TenantExportServiceClient: identityv1.NewTenantExportServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		bus:                           deps.Bus,
	}
}

// LookupLoyaltyMember returns a loyalty member.
func (s *LoyaltyMemberQueryServer) LookupLoyaltyMember(ctx context.Context, req *loyaltyv1.LookupLoyaltyMemberRequest) (*loyaltyv1.LookupLoyaltyMemberResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected LookupLoyaltyMember", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "loyalty_member", req.GetLoyaltyMemberId(), "lookup_loyalty_member"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "lookup loyalty member", "tenant", req.GetContext().GetTenantId())
	previewTierRequest := &loyaltyv1.PreviewTierRequest{
		Context: req.GetContext(),
		Details: &loyaltyv1.PreviewTierRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Locale:  req.GetContext().GetLocale(),
		},
	}
	previewTierReply, err := grpcx.Call(ctx, s.tierValidationClient.PreviewTier, previewTierRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.TierValidationService/PreviewTier")
	}
	normalizeUserRequest := &identityv1.NormalizeUserRequest{
		Context: req.GetContext(),
		User: &identityv1.User{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: previewTierReply.GetTier().GetAudit().GetUpdatedBy(),
			},
			RelationshipManager: &identityv1.UserRef{
				UserId:      previewTierReply.GetTier().GetOwner().GetUserId(),
				DisplayName: previewTierReply.GetTier().GetOwner().GetDisplayName(),
			},
		},
		Details: &identityv1.NormalizeUserRequest_Details{
			Contact: &commonv1.ContactInfo{Email: previewTierReply.GetTier().GetOwner().GetEmail()},
		},
	}
	normalizeUserReply, err := grpcx.NewClient(s.conn, identityv1.NewUserValidationServiceClient).NormalizeUser(ctx, normalizeUserRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserValidationService/NormalizeUser")
	}
	return &loyaltyv1.LookupLoyaltyMemberResponse{
		Result: &commonv1.OperationResult{
			Details: previewTierReply.GetResult().GetDetails(),
			Message: normalizeUserReply.GetOutcome().GetMessage(),
			Status:  "ok",
		},
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			Limits: []*loyaltyv1.LoyaltyMemberLimit{&loyaltyv1.LoyaltyMemberLimit{
				Amount: previewTierReply.GetTier().GetDiscount().GetAmount(),
			}},
			Audit:           &commonv1.AuditInfo{Etag: normalizeUserReply.GetUser().GetAudit().GetEtag()},
			LoyaltyMemberId: req.GetLoyaltyMemberId(),
		},
	}, nil
}

// BrowseLoyaltyMembers lists loyalty members a page at a time.
func (s *LoyaltyMemberQueryServer) BrowseLoyaltyMembers(ctx context.Context, req *loyaltyv1.BrowseLoyaltyMembersRequest) (*loyaltyv1.BrowseLoyaltyMembersResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "browse loyalty members", "tenant", req.GetContext().GetTenantId())
	listTenantExportsRequest := newListTenantExportsRequest(withListTenantExportsContext(req.GetContext()), withListTenantExportsFilter(&identityv1.ListTenantExportsRequest_Filter{Labels: req.GetFilter().GetLabels()}))
	listTenantExportsReply, err := s.tenantExportClient.ListTenantExports(ctx, listTenantExportsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantExportService/ListTenantExports")
	}
	explainPointsLedgerDecisionRequest := &loyaltyv1.ExplainPointsLedgerDecisionRequest{
		Context:      req.GetContext(),
		PointsLedger: &loyaltyv1.PointsLedger{Labels: req.GetFilter().GetLabels()},
	}
	explainPointsLedgerDecisionReply, err := s.pointsLedgerValidationPort.ExplainPointsLedgerDecision(ctx, explainPointsLedgerDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerValidationService/ExplainPointsLedgerDecision")
	}
	return &loyaltyv1.BrowseLoyaltyMembersResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: listTenantExportsReply.GetResult().GetReferenceIds(),
			Status:       listTenantExportsReply.GetResult().GetStatus(),
		},
		LoyaltyMembers: []*loyaltyv1.LoyaltyMember{&loyaltyv1.LoyaltyMember{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: explainPointsLedgerDecisionReply.GetPointsLedger().GetContact().GetPreferredChannel(),
			},
		}},
	}, nil
}

// FindLoyaltyMembers lists loyalty members a page at a time.
func (s *LoyaltyMemberQueryServer) FindLoyaltyMembers(ctx context.Context, req *loyaltyv1.FindLoyaltyMembersRequest) (*loyaltyv1.FindLoyaltyMembersResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "find loyalty members", "tenant", req.GetContext().GetTenantId())
	pageCache := cache.New[string, *commonv1.PageRequest](1)
	pageCache.Put(req.GetContext().GetTraceId(), req.GetPage())
	cachedPage, _ := pageCache.Get(req.GetContext().GetTraceId())
	suggestPlacementsRequest := &merchandisingv1.SuggestPlacementsRequest{}
	suggestPlacementsRequest.Context = req.GetContext()
	suggestPlacementsRequest.Page = &commonv1.PageRequest{
		PageToken: cachedPage.GetPageToken(),
		PageSize:  req.GetPage().GetPageSize(),
	}
	suggestPlacementsRequest.Filter = &merchandisingv1.SuggestPlacementsRequest_Filter{CreatedAfter: req.GetFilter().GetCreatedAfter()}
	suggestPlacementsReply, err := s.placementQueryClient.SuggestPlacements(ctx, suggestPlacementsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.PlacementQueryService/SuggestPlacements")
	}
	return &loyaltyv1.FindLoyaltyMembersResponse{
		PageInfo: &commonv1.PageInfo{
			TotalSize: suggestPlacementsReply.GetPageInfo().GetTotalSize(),
		},
		Result: &commonv1.OperationResult{Status: suggestPlacementsReply.GetResult().GetStatus()},
	}, nil
}

// ResolveLoyaltyMembers returns several loyalty members at once.
func (s *LoyaltyMemberQueryServer) ResolveLoyaltyMembers(ctx context.Context, req *loyaltyv1.ResolveLoyaltyMembersRequest) (*loyaltyv1.ResolveLoyaltyMembersResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ResolveLoyaltyMembers", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "resolve loyalty members", "tenant", req.GetContext().GetTenantId())
	lintLoyaltyMembersReply, err := s.loyaltyMemberValidationClient.LintLoyaltyMembers(ctx, &loyaltyv1.LintLoyaltyMembersRequest{
		Context: req.GetContext(),
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Audit:   &commonv1.AuditInfo{UpdatedAt: req.GetContext().GetDeadline()},
			Funding: &loyaltyv1.LoyaltyMember_ExternalAccountId{ExternalAccountId: req.GetContext().GetActor().GetUserId()},
		},
		Details: &loyaltyv1.LintLoyaltyMembersRequest_Details{Locale: req.GetContext().GetLocale()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberValidationService/LintLoyaltyMembers")
	}
	return &loyaltyv1.ResolveLoyaltyMembersResponse{
		LoyaltyMembers: []*loyaltyv1.LoyaltyMember{&loyaltyv1.LoyaltyMember{
			Status: lintLoyaltyMembersReply.GetLoyaltyMember().GetStatus(),
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ExportLoyaltyMembers streams every matching loyalty member.
func (s *LoyaltyMemberQueryServer) ExportLoyaltyMembers(req *loyaltyv1.ExportLoyaltyMembersRequest, stream grpc.ServerStreamingServer[loyaltyv1.ExportLoyaltyMembersResponse]) error {
	ctx := stream.Context()
	if err := validateContext(req.GetContext()); err != nil {
		return err
	}
	s.log.InfoContext(ctx, "export loyalty members", "tenant", req.GetContext().GetTenantId())
	retryTenantExportRequest := &identityv1.RetryTenantExportRequest{
		Context: req.GetContext(),
		Tenant: &identityv1.Tenant{
			Children: []*identityv1.Tenant{&identityv1.Tenant{
				Children: []*identityv1.Tenant{&identityv1.Tenant{
					Location: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
					TenantId: req.GetContext().GetTenantId(),
				}},
				Manager: &identityv1.UserRef{Email: req.GetContext().GetActor().GetEmail()},
			}},
		},
	}
	retryTenantExportReply, err := s.meteredTenantExport.RetryTenantExport(ctx, retryTenantExportRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.identity.v1.TenantExportService/RetryTenantExport")
	}
	if err := stream.Send(&loyaltyv1.ExportLoyaltyMembersResponse{
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			Contact: &commonv1.ContactInfo{
				Email: retryTenantExportReply.GetTenant().GetManager().GetEmail(),
			},
			Labels: req.GetFilter().GetLabels(),
		},
		Result: &commonv1.OperationResult{
			Details: retryTenantExportReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}); err != nil {
		return err
	}
	return nil
}

// SuggestLoyaltyMembers lists loyalty members a page at a time.
func (s *LoyaltyMemberQueryServer) SuggestLoyaltyMembers(ctx context.Context, req *loyaltyv1.SuggestLoyaltyMembersRequest) (*loyaltyv1.SuggestLoyaltyMembersResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "suggest loyalty members", "tenant", req.GetContext().GetTenantId())
	return &loyaltyv1.SuggestLoyaltyMembersResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// PeekLoyaltyMember returns a loyalty member.
func (s *LoyaltyMemberQueryServer) PeekLoyaltyMember(ctx context.Context, req *loyaltyv1.PeekLoyaltyMemberRequest) (*loyaltyv1.PeekLoyaltyMemberResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "loyalty_member", req.GetLoyaltyMemberId(), "peek_loyalty_member"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "peek loyalty member", "tenant", req.GetContext().GetTenantId())
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "loyalty.peek_loyalty_member.score_loyalty_member",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"loyalty_member_id":            req.GetLoyaltyMemberId(),
			"loyalty_member.contact.email": req.GetContext().GetActor().GetEmail(),
			"context.tenant_id":            req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	return &loyaltyv1.PeekLoyaltyMemberResponse{
		LoyaltyMember: &loyaltyv1.LoyaltyMember{LoyaltyMemberId: req.GetLoyaltyMemberId()},
		Result:        &commonv1.OperationResult{Status: "ok"},
	}, nil
}
