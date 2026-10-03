package sellers

import (
	"context"
	"fmt"
	"log/slog"

	auditv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audit/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	datasetsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/datasets/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// SellerPayoutScheduleValidationServer implements `acme.sellers.v1.SellerPayoutScheduleValidationService`.
// Checks seller payout schedules against the rules before anything acts on them.
type SellerPayoutScheduleValidationServer struct {
	sellersv1.UnimplementedSellerPayoutScheduleValidationServiceServer
	store                                 *Store
	log                                   *slog.Logger
	sellerValidationClient                sellersv1.SellerValidationServiceClient
	tenantExportClient                    identityv1.TenantExportServiceClient
	sellerPayoutScheduleCalculationClient sellersv1.SellerPayoutScheduleCalculationServiceClient
	sellerCalculationClient               sellersv1.SellerCalculationServiceClient
	clients                               *registry.Registry
	auditEventValidationClient            auditv1.AuditEventValidationServiceClient
	sellerListingCalculationClient        sellersv1.SellerListingCalculationServiceClient
	conn                                  *grpc.ClientConn
	giftCardValidationClient              giftcardsv1.GiftCardValidationServiceClient
}

// NewSellerPayoutScheduleValidationServer wires a SellerPayoutScheduleValidationServer.
func NewSellerPayoutScheduleValidationServer(deps platform.Deps) *SellerPayoutScheduleValidationServer {
	return &SellerPayoutScheduleValidationServer{
		store:                                 NewStore(deps.DB),
		log:                                   deps.Logger.With("service", "acme.sellers.v1.SellerPayoutScheduleValidationService"),
		sellerValidationClient:                sellersv1.NewSellerValidationServiceClient(deps.Conn),
		tenantExportClient:                    identityv1.NewTenantExportServiceClient(deps.Conn),
		sellerPayoutScheduleCalculationClient: sellersv1.NewSellerPayoutScheduleCalculationServiceClient(deps.Conn),
		sellerCalculationClient:               sellersv1.NewSellerCalculationServiceClient(deps.Conn),
		clients:                               deps.Clients,
		auditEventValidationClient:            auditv1.NewAuditEventValidationServiceClient(deps.Conn),
		sellerListingCalculationClient:        sellersv1.NewSellerListingCalculationServiceClient(deps.Conn),
		conn:                                  deps.Conn,
		giftCardValidationClient:              giftcardsv1.NewGiftCardValidationServiceClient(deps.Conn),
	}
}

// ValidateSellerPayoutSchedule validates a seller payout schedule.
func (s *SellerPayoutScheduleValidationServer) ValidateSellerPayoutSchedule(ctx context.Context, req *sellersv1.ValidateSellerPayoutScheduleRequest) (*sellersv1.ValidateSellerPayoutScheduleResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveSellerPayoutSchedule(ctx, req.GetContext().GetTenantId(), req.GetSellerPayoutSchedule()); err != nil {
		return nil, grpcx.Wrap(err, "save seller payout schedule")
	}
	s.log.InfoContext(ctx, "validate seller payout schedule", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSellerPayoutSchedule().GetAudit().GetEtag())
	for _, participant := range req.GetSellerPayoutSchedule().GetParticipants() {
		if _, err := s.sellerValidationClient.PreviewSeller(ctx, &sellersv1.PreviewSellerRequest{
			Context: req.GetContext(),
			Seller: &sellersv1.Seller{
				Contact: &commonv1.ContactInfo{
					PreferredChannel: participant.GetContact().GetPreferredChannel(),
					Phone:            participant.GetContact().GetPhone(),
				},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerValidationService/PreviewSeller")
		}
	}
	cancelTenantExportReply, err := s.tenantExportClient.CancelTenantExport(ctx, &identityv1.CancelTenantExportRequest{
		Context:  req.GetContext(),
		TenantId: req.GetContext().GetTenantId(),
		Details: &identityv1.CancelTenantExportRequest_Details{
			NewOwner: &identityv1.UserRef{Email: req.GetDetails().GetContact().GetEmail()},
			NotifyContact: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetContact().GetPreferredChannel(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantExportService/CancelTenantExport")
	}
	return &sellersv1.ValidateSellerPayoutScheduleResponse{
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
			Audit: &commonv1.AuditInfo{
				Etag: cancelTenantExportReply.GetTenant().GetAudit().GetEtag(),
			},
			Location: &commonv1.PostalAddress{
				PostalCode: cancelTenantExportReply.GetTenant().GetLocation().GetPostalCode(),
			},
			SellerPayoutScheduleId: req.GetSellerPayoutScheduleId(),
			Window:                 req.GetSellerPayoutSchedule().GetWindow(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// PreviewSellerPayoutSchedule previews a seller payout schedule.
func (s *SellerPayoutScheduleValidationServer) PreviewSellerPayoutSchedule(ctx context.Context, req *sellersv1.PreviewSellerPayoutScheduleRequest) (*sellersv1.PreviewSellerPayoutScheduleResponse, error) {
	if err := s.store.SaveSellerPayoutSchedule(ctx, req.GetContext().GetTenantId(), req.GetSellerPayoutSchedule()); err != nil {
		return nil, grpcx.Wrap(err, "save seller payout schedule")
	}
	s.log.InfoContext(ctx, "preview seller payout schedule", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSellerPayoutSchedule().GetAudit().GetEtag())
	compareSellerPayoutScheduleOptionsReply, err := s.sellerPayoutScheduleCalculationClient.CompareSellerPayoutScheduleOptions(ctx, &sellersv1.CompareSellerPayoutScheduleOptionsRequest{
		Context: req.GetContext(),
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
			Participants: []*sellersv1.SellerPayoutScheduleParticipant{&sellersv1.SellerPayoutScheduleParticipant{
				Contact: &commonv1.ContactInfo{
					Email: req.GetSellerPayoutSchedule().GetOrganizer().GetEmail(),
				},
			}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerPayoutScheduleCalculationService/CompareSellerPayoutScheduleOptions")
	}
	quoteSellerRequest := buildQuoteSellerRequestForPreviewSellerPayoutSchedule(req, compareSellerPayoutScheduleOptionsReply)
	quoteSellerReply, err := s.sellerCalculationClient.QuoteSeller(ctx, quoteSellerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerCalculationService/QuoteSeller")
	}
	return &sellersv1.PreviewSellerPayoutScheduleResponse{
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
			Audit: &commonv1.AuditInfo{
				Etag: compareSellerPayoutScheduleOptionsReply.GetSellerPayoutSchedule().GetAudit().GetEtag(),
			},
			SellerPayoutScheduleId: req.GetSellerPayoutScheduleId(),
			Title:                  req.GetSellerPayoutSchedule().GetTitle(),
		},
		Outcome: &sellersv1.PreviewSellerPayoutScheduleResponse_Outcome{Status: quoteSellerReply.GetResult().GetStatus()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CheckSellerPayoutScheduleEligibility checks a seller payout schedule.
func (s *SellerPayoutScheduleValidationServer) CheckSellerPayoutScheduleEligibility(ctx context.Context, req *sellersv1.CheckSellerPayoutScheduleEligibilityRequest) (*sellersv1.CheckSellerPayoutScheduleEligibilityResponse, error) {
	if err := s.store.SaveSellerPayoutSchedule(ctx, req.GetContext().GetTenantId(), req.GetSellerPayoutSchedule()); err != nil {
		return nil, grpcx.Wrap(err, "save seller payout schedule")
	}
	s.log.InfoContext(ctx, "check seller payout schedule eligibility", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSellerPayoutSchedule().GetAudit().GetEtag())
	sellerPayoutScheduleCalculation, ok := s.clients.Get("acme.sellers.v1.SellerPayoutScheduleCalculationService").(sellersv1.SellerPayoutScheduleCalculationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.sellers.v1.SellerPayoutScheduleCalculationService")
	}
	compareSellerPayoutScheduleOptionsRequest := &sellersv1.CompareSellerPayoutScheduleOptionsRequest{
		Context: req.GetContext(),
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
			Participants: []*sellersv1.SellerPayoutScheduleParticipant{&sellersv1.SellerPayoutScheduleParticipant{
				Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetContact().GetEmail()},
			}},
		},
	}
	compareSellerPayoutScheduleOptionsReply, err := sellerPayoutScheduleCalculation.CompareSellerPayoutScheduleOptions(ctx, compareSellerPayoutScheduleOptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerPayoutScheduleCalculationService/CompareSellerPayoutScheduleOptions")
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*auditv1.PreviewAuditEventResponse, len(req.GetSellerPayoutSchedule().GetParticipants()))
	for i, participant := range req.GetSellerPayoutSchedule().GetParticipants() {
		group.Go(func() error {
			reply, err := s.auditEventValidationClient.PreviewAuditEvent(groupCtx, &auditv1.PreviewAuditEventRequest{
				Context: req.GetContext(),
				Details: &auditv1.PreviewAuditEventRequest_Details{
					Contact: &commonv1.ContactInfo{
						PreferredChannel: participant.GetContact().GetPreferredChannel(),
						Email:            participant.GetContact().GetEmail(),
						Name:             participant.GetContact().GetName(),
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
		return nil, grpcx.Wrap(err, "acme.audit.v1.AuditEventValidationService/PreviewAuditEvent")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	return &sellersv1.CheckSellerPayoutScheduleEligibilityResponse{
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
			Participants: []*sellersv1.SellerPayoutScheduleParticipant{&sellersv1.SellerPayoutScheduleParticipant{
				Contact: &commonv1.ContactInfo{
					Email: compareSellerPayoutScheduleOptionsReply.GetSellerPayoutSchedule().GetOrganizer().GetEmail(),
				},
			}},
			SellerPayoutScheduleId: req.GetSellerPayoutScheduleId(),
			Location:               req.GetSellerPayoutSchedule().GetLocation(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// NormalizeSellerPayoutSchedule normalizes a seller payout schedule.
func (s *SellerPayoutScheduleValidationServer) NormalizeSellerPayoutSchedule(ctx context.Context, req *sellersv1.NormalizeSellerPayoutScheduleRequest) (*sellersv1.NormalizeSellerPayoutScheduleResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected NormalizeSellerPayoutSchedule", "error", err)
		return nil, err
	}
	if err := s.store.SaveSellerPayoutSchedule(ctx, req.GetContext().GetTenantId(), req.GetSellerPayoutSchedule()); err != nil {
		return nil, grpcx.Wrap(err, "save seller payout schedule")
	}
	s.log.InfoContext(ctx, "normalize seller payout schedule", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSellerPayoutSchedule().GetAudit().GetEtag())
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*sellersv1.RecalculateSellerListingsResponse, len(req.GetSellerPayoutSchedule().GetParticipants()))
	for i, participant := range req.GetSellerPayoutSchedule().GetParticipants() {
		group.Go(func() error {
			reply, err := s.sellerListingCalculationClient.RecalculateSellerListings(groupCtx, &sellersv1.RecalculateSellerListingsRequest{
				Context: req.GetContext(),
				SellerListing: &sellersv1.SellerListing{
					Owner: &identityv1.UserRef{Email: participant.GetContact().GetEmail()},
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
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingCalculationService/RecalculateSellerListings")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	return &sellersv1.NormalizeSellerPayoutScheduleResponse{
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
			SellerPayoutScheduleId: req.GetSellerPayoutScheduleId(),
			Title:                  req.GetSellerPayoutSchedule().GetTitle(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ExplainSellerPayoutScheduleDecision explains a seller payout schedule.
func (s *SellerPayoutScheduleValidationServer) ExplainSellerPayoutScheduleDecision(ctx context.Context, req *sellersv1.ExplainSellerPayoutScheduleDecisionRequest) (*sellersv1.ExplainSellerPayoutScheduleDecisionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ExplainSellerPayoutScheduleDecision", "error", err)
		return nil, err
	}
	if err := s.store.SaveSellerPayoutSchedule(ctx, req.GetContext().GetTenantId(), req.GetSellerPayoutSchedule()); err != nil {
		return nil, grpcx.Wrap(err, "save seller payout schedule")
	}
	s.log.InfoContext(ctx, "explain seller payout schedule decision", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSellerPayoutSchedule().GetAudit().GetEtag())
	for _, participant := range req.GetSellerPayoutSchedule().GetParticipants() {
		if _, err := s.sellerValidationClient.ScoreSeller(ctx, &sellersv1.ScoreSellerRequest{
			Context: req.GetContext(),
			Seller: &sellersv1.Seller{
				Contact: &commonv1.ContactInfo{Phone: participant.GetContact().GetPhone()},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerValidationService/ScoreSeller")
		}
	}
	return &sellersv1.ExplainSellerPayoutScheduleDecisionResponse{
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
			SellerPayoutScheduleId: req.GetSellerPayoutScheduleId(),
			Metadata:               req.GetSellerPayoutSchedule().GetMetadata(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// LintSellerPayoutSchedules lints a seller payout schedule.
func (s *SellerPayoutScheduleValidationServer) LintSellerPayoutSchedules(ctx context.Context, req *sellersv1.LintSellerPayoutSchedulesRequest) (*sellersv1.LintSellerPayoutSchedulesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveSellerPayoutSchedule(ctx, req.GetContext().GetTenantId(), req.GetSellerPayoutSchedule()); err != nil {
		return nil, grpcx.Wrap(err, "save seller payout schedule")
	}
	s.log.InfoContext(ctx, "lint seller payout schedules", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSellerPayoutSchedule().GetAudit().GetEtag())
	updateDatasetRequest := &datasetsv1.UpdateDatasetRequest{
		Context: req.GetContext(),
		Dataset: &datasetsv1.Dataset{
			Audit: &commonv1.AuditInfo{Etag: req.GetSellerPayoutSchedule().GetAudit().GetEtag()},
			Owner: &identityv1.UserRef{Email: req.GetDetails().GetContact().GetEmail()},
		},
	}
	updateDatasetReply := new(datasetsv1.UpdateDatasetResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", datasetsDatasetServiceName, "UpdateDataset"), updateDatasetRequest, updateDatasetReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.datasets.v1.DatasetService/UpdateDataset")
	}
	return &sellersv1.LintSellerPayoutSchedulesResponse{
		Outcome: &sellersv1.LintSellerPayoutSchedulesResponse_Outcome{Status: updateDatasetReply.GetResult().GetStatus()},
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
			SellerPayoutScheduleId: req.GetSellerPayoutScheduleId(),
			Title:                  req.GetSellerPayoutSchedule().GetTitle(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetSellerPayoutScheduleRules returns a seller payout schedule.
func (s *SellerPayoutScheduleValidationServer) GetSellerPayoutScheduleRules(ctx context.Context, req *sellersv1.GetSellerPayoutScheduleRulesRequest) (*sellersv1.GetSellerPayoutScheduleRulesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetSellerPayoutScheduleRules", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "seller_payout_schedule", req.GetSellerPayoutScheduleId(), "get_seller_payout_schedule_rules"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get seller payout schedule rules", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "user_id", req.GetContext().GetActor().GetUserId())
	validateSellerRequest := &sellersv1.ValidateSellerRequest{
		Context: req.GetContext(),
		Seller: &sellersv1.Seller{
			Contact:             &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			RelationshipManager: &identityv1.UserRef{UserId: tenancy.Value(ctx, "user_id")},
		},
		Details: &sellersv1.ValidateSellerRequest_Details{Locale: req.GetContext().GetLocale()},
	}
	validateSellerReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*sellersv1.ValidateSellerResponse, error) {
		return s.sellerValidationClient.ValidateSeller(ctx, validateSellerRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerValidationService/ValidateSeller")
	}
	scoreGiftCardRequest := &giftcardsv1.ScoreGiftCardRequest{
		Context: req.GetContext(),
		GiftCard: &giftcardsv1.GiftCard{
			Holder: &giftcardsv1.GiftCardTransactionRef{
				DisplayName: validateSellerReply.GetSeller().GetRelationshipManager().GetDisplayName(),
			},
			Contact: &commonv1.ContactInfo{
				PreferredChannel: validateSellerReply.GetSeller().GetContact().GetPreferredChannel(),
			},
		},
		Details: &giftcardsv1.ScoreGiftCardRequest_Details{
			Destination: &commonv1.PostalAddress{
				Locality: validateSellerReply.GetSeller().GetPrimaryAddress().GetLocality(),
			},
		},
	}
	scoreGiftCardReply, err := giftcardsv1.GiftCardValidationServiceClient.ScoreGiftCard(s.giftCardValidationClient, ctx, scoreGiftCardRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardValidationService/ScoreGiftCard")
	}
	_ = scoreGiftCardReply
	return &sellersv1.GetSellerPayoutScheduleRulesResponse{
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
			Participants: []*sellersv1.SellerPayoutScheduleParticipant{&sellersv1.SellerPayoutScheduleParticipant{
				Contact: &commonv1.ContactInfo{Name: validateSellerReply.GetSeller().GetName()},
			}},
			SellerPayoutScheduleId: req.GetSellerPayoutScheduleId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}
