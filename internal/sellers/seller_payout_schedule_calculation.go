package sellers

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	auditv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audit/v1"
	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	documentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/documents/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	riskv1 "github.com/hounddogai/protomap-test-golang/gen/acme/risk/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	travelv1 "github.com/hounddogai/protomap-test-golang/gen/acme/travel/v1"
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
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// SellerPayoutScheduleCalculationServer implements `acme.sellers.v1.SellerPayoutScheduleCalculationService`.
// Computes the figures of a seller payout schedule, such as totals, taxes, and estimates.
type SellerPayoutScheduleCalculationServer struct {
	sellersv1.UnimplementedSellerPayoutScheduleCalculationServiceServer
	store                          *Store
	log                            *slog.Logger
	giftCardBatchValidationClient  giftcardsv1.GiftCardBatchValidationServiceClient
	giftCardValidationClient       giftcardsv1.GiftCardValidationServiceClient
	giftCardClient                 giftcardsv1.GiftCardServiceClient
	sellerListingCalculationClient sellersv1.SellerListingCalculationServiceClient
	sellerCalculationClient        sellersv1.SellerCalculationServiceClient
	conn                           *grpc.ClientConn
	sellerListingCalculationOnce   sync.Once
	sellerListingCalculationCached sellersv1.SellerListingCalculationServiceClient
	tenantAdminClient              identityv1.TenantAdminServiceClient
	resolveTripsFn                 func(context.Context, *travelv1.ResolveTripsRequest, ...grpc.CallOption) (*travelv1.ResolveTripsResponse, error)
	sessionNotificationClient      identityv1.SessionNotificationServiceClient
	riskRegisterQueryClient        riskv1.RiskRegisterQueryServiceClient
	giftCardBatchCalculationPort   giftCardBatchCalculationPort
	accessPolicyQueryClient        authorizationv1.AccessPolicyQueryServiceClient
	clients                        *registry.Registry
	sellerValidationClient         sellersv1.SellerValidationServiceClient
}

// NewSellerPayoutScheduleCalculationServer wires a SellerPayoutScheduleCalculationServer.
func NewSellerPayoutScheduleCalculationServer(deps platform.Deps) *SellerPayoutScheduleCalculationServer {
	tripQuery := travelv1.NewTripQueryServiceClient(deps.Conn)
	return &SellerPayoutScheduleCalculationServer{
		store:                          NewStore(deps.DB),
		log:                            deps.Logger.With("service", "acme.sellers.v1.SellerPayoutScheduleCalculationService"),
		giftCardBatchValidationClient:  giftcardsv1.NewGiftCardBatchValidationServiceClient(deps.Conn),
		giftCardValidationClient:       giftcardsv1.NewGiftCardValidationServiceClient(deps.Conn),
		giftCardClient:                 giftcardsv1.NewGiftCardServiceClient(deps.Conn),
		sellerListingCalculationClient: sellersv1.NewSellerListingCalculationServiceClient(deps.Conn),
		sellerCalculationClient:        sellersv1.NewSellerCalculationServiceClient(deps.Conn),
		conn:                           deps.Conn,
		tenantAdminClient:              identityv1.NewTenantAdminServiceClient(deps.Conn),
		resolveTripsFn:                 tripQuery.ResolveTrips,
		sessionNotificationClient:      identityv1.NewSessionNotificationServiceClient(deps.Conn),
		riskRegisterQueryClient:        riskv1.NewRiskRegisterQueryServiceClient(deps.Conn),
		giftCardBatchCalculationPort:   newGiftCardBatchCalculationPort(deps),
		accessPolicyQueryClient:        authorizationv1.NewAccessPolicyQueryServiceClient(deps.Conn),
		clients:                        deps.Clients,
		sellerValidationClient:         sellersv1.NewSellerValidationServiceClient(deps.Conn),
	}
}

// CalculateSellerPayoutSchedule calculates a seller payout schedule.
func (s *SellerPayoutScheduleCalculationServer) CalculateSellerPayoutSchedule(ctx context.Context, req *sellersv1.CalculateSellerPayoutScheduleRequest) (*sellersv1.CalculateSellerPayoutScheduleResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CalculateSellerPayoutSchedule", "error", err)
		return nil, err
	}
	if err := s.store.SaveSellerPayoutSchedule(ctx, req.GetContext().GetTenantId(), req.GetSellerPayoutSchedule()); err != nil {
		return nil, grpcx.Wrap(err, "save seller payout schedule")
	}
	s.log.InfoContext(ctx, "calculate seller payout schedule", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSellerPayoutSchedule().GetAudit().GetEtag())
	validateGiftCardBatchReply, err := s.giftCardBatchValidationClient.ValidateGiftCardBatch(ctx, &giftcardsv1.ValidateGiftCardBatchRequest{
		Context: req.GetContext(),
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			Audit: &commonv1.AuditInfo{
				CreatedBy: req.GetSellerPayoutSchedule().GetAudit().GetCreatedBy(),
				Etag:      req.GetSellerPayoutSchedule().GetAudit().GetEtag(),
			},
			BillingAddress: &commonv1.PostalAddress{
				Locality:  req.GetDetails().GetDestination().GetLocality(),
				Recipient: req.GetDetails().GetDestination().GetRecipient(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchValidationService/ValidateGiftCardBatch")
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*giftcardsv1.ValidateGiftCardResponse, len(req.GetSellerPayoutSchedule().GetParticipants()))
	for i, participant := range req.GetSellerPayoutSchedule().GetParticipants() {
		group.Go(func() error {
			reply, err := s.giftCardValidationClient.ValidateGiftCard(groupCtx, &giftcardsv1.ValidateGiftCardRequest{
				Context: req.GetContext(),
				GiftCard: &giftcardsv1.GiftCard{
					Contact: &commonv1.ContactInfo{Email: participant.GetContact().GetEmail()},
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
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardValidationService/ValidateGiftCard")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	return &sellersv1.CalculateSellerPayoutScheduleResponse{
		Result: &commonv1.OperationResult{
			Message: validateGiftCardBatchReply.GetOutcome().GetMessage(),
			Status:  "ok",
		},
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
			Audit: &commonv1.AuditInfo{
				CreatedAt: validateGiftCardBatchReply.GetGiftCardBatch().GetAudit().GetCreatedAt(),
			},
			SellerPayoutScheduleId: req.GetSellerPayoutScheduleId(),
			Organizer:              req.GetSellerPayoutSchedule().GetOrganizer(),
		},
	}, nil
}

// EstimateSellerPayoutSchedule estimates a seller payout schedule.
func (s *SellerPayoutScheduleCalculationServer) EstimateSellerPayoutSchedule(ctx context.Context, req *sellersv1.EstimateSellerPayoutScheduleRequest) (*sellersv1.EstimateSellerPayoutScheduleResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveSellerPayoutSchedule(ctx, req.GetContext().GetTenantId(), req.GetSellerPayoutSchedule()); err != nil {
		return nil, grpcx.Wrap(err, "save seller payout schedule")
	}
	s.log.InfoContext(ctx, "estimate seller payout schedule", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSellerPayoutSchedule().GetAudit().GetEtag())
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*giftcardsv1.UpdateGiftCardResponse, len(req.GetSellerPayoutSchedule().GetParticipants()))
	for i, participant := range req.GetSellerPayoutSchedule().GetParticipants() {
		group.Go(func() error {
			reply, err := s.giftCardClient.UpdateGiftCard(groupCtx, &giftcardsv1.UpdateGiftCardRequest{
				Context: req.GetContext(),
				GiftCard: &giftcardsv1.GiftCard{
					Contact: &commonv1.ContactInfo{
						PreferredChannel: participant.GetContact().GetPreferredChannel(),
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
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardService/UpdateGiftCard")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	var holder struct{ Locality string }
	if record := req.GetDetails().GetDestination(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Locality"); err != nil {
			s.log.WarnContext(ctx, "copy locality", "error", err)
		}
	}
	explainSellerListingCalculationRequest := &sellersv1.ExplainSellerListingCalculationRequest{
		Context: req.GetContext(),
		SellerListing: &sellersv1.SellerListing{
			Locator: &sellersv1.SellerListing_Address{Address: &commonv1.PostalAddress{Locality: holder.Locality}},
		},
	}
	explainSellerListingCalculationReply, err := grpcx.Call(ctx, s.sellerListingCalculationClient.ExplainSellerListingCalculation, explainSellerListingCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingCalculationService/ExplainSellerListingCalculation")
	}
	return &sellersv1.EstimateSellerPayoutScheduleResponse{
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: explainSellerListingCalculationReply.GetSellerListing().GetAudit().GetUpdatedAt(),
			},
			Organizer: &identityv1.UserRef{
				UserId: explainSellerListingCalculationReply.GetSellerListing().GetOwner().GetUserId(),
			},
			SellerPayoutScheduleId: req.GetSellerPayoutScheduleId(),
			Title:                  req.GetSellerPayoutSchedule().GetTitle(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// QuoteSellerPayoutSchedule quotes a seller payout schedule.
func (s *SellerPayoutScheduleCalculationServer) QuoteSellerPayoutSchedule(ctx context.Context, req *sellersv1.QuoteSellerPayoutScheduleRequest) (*sellersv1.QuoteSellerPayoutScheduleResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected QuoteSellerPayoutSchedule", "error", err)
		return nil, err
	}
	if err := s.store.SaveSellerPayoutSchedule(ctx, req.GetContext().GetTenantId(), req.GetSellerPayoutSchedule()); err != nil {
		return nil, grpcx.Wrap(err, "save seller payout schedule")
	}
	s.log.InfoContext(ctx, "quote seller payout schedule", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSellerPayoutSchedule().GetAudit().GetEtag())
	quoteSellerRequest := newQuoteSellerRequest(withQuoteSellerContext(req.GetContext()), withQuoteSellerDetails(&sellersv1.QuoteSellerRequest_Details{
		Destination: &commonv1.PostalAddress{Recipient: req.GetDetails().GetDestination().GetRecipient()},
	}))
	quoteSellerReply, err := s.sellerCalculationClient.QuoteSeller(ctx, quoteSellerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerCalculationService/QuoteSeller")
	}
	packed, err := anypb.New(req.GetSellerPayoutSchedule())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	validateAuditEventRequest := &auditv1.ValidateAuditEventRequest{
		Context: req.GetContext(),
		AuditEvent: &auditv1.AuditEvent{
			Subject: &auditv1.AuditTrailRef{
				Number: quoteSellerReply.GetSeller().GetPassport().GetNumber(),
			},
			Detail: packed,
		},
		Details: &auditv1.ValidateAuditEventRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email: req.GetSellerPayoutSchedule().GetOrganizer().GetEmail(),
			},
		},
	}
	validateAuditEventReply := new(auditv1.ValidateAuditEventResponse)
	if err := s.conn.Invoke(ctx, auditAuditEventValidationServiceValidateAuditEventMethod, validateAuditEventRequest, validateAuditEventReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.audit.v1.AuditEventValidationService/ValidateAuditEvent")
	}
	return &sellersv1.QuoteSellerPayoutScheduleResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: quoteSellerReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
			Location: &commonv1.PostalAddress{
				Recipient: quoteSellerReply.GetSeller().GetPrimaryAddress().GetRecipient(),
			},
			Organizer: &identityv1.UserRef{
				DisplayName: validateAuditEventReply.GetAuditEvent().GetSubject().GetDisplayName(),
			},
			SellerPayoutScheduleId: req.GetSellerPayoutScheduleId(),
			Title:                  req.GetSellerPayoutSchedule().GetTitle(),
		},
	}, nil
}

// CompareSellerPayoutScheduleOptions compares a seller payout schedule.
func (s *SellerPayoutScheduleCalculationServer) CompareSellerPayoutScheduleOptions(ctx context.Context, req *sellersv1.CompareSellerPayoutScheduleOptionsRequest) (*sellersv1.CompareSellerPayoutScheduleOptionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveSellerPayoutSchedule(ctx, req.GetContext().GetTenantId(), req.GetSellerPayoutSchedule()); err != nil {
		return nil, grpcx.Wrap(err, "save seller payout schedule")
	}
	s.log.InfoContext(ctx, "compare seller payout schedule options", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSellerPayoutSchedule().GetAudit().GetEtag())
	simulateSellerListingRequest := &sellersv1.SimulateSellerListingRequest{
		Context: req.GetContext(),
		SellerListing: &sellersv1.SellerListing{
			Locator: &sellersv1.SellerListing_Address{Address: &commonv1.PostalAddress{
				Lines: req.GetSellerPayoutSchedule().GetLocation().GetLines(),
			}},
		},
	}
	simulateSellerListingReply, err := s.sellerListingCalculationLazy().SimulateSellerListing(ctx, simulateSellerListingRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingCalculationService/SimulateSellerListing")
	}
	for _, participant := range req.GetSellerPayoutSchedule().GetParticipants() {
		if _, err := s.tenantAdminClient.ArchiveTenant(ctx, &identityv1.ArchiveTenantRequest{
			Context:  req.GetContext(),
			TenantId: req.GetContext().GetTenantId(),
			Details: &identityv1.ArchiveTenantRequest_Details{
				NotifyContact: &commonv1.ContactInfo{
					PreferredChannel: participant.GetContact().GetPreferredChannel(),
				},
				NewOwner: &identityv1.UserRef{Email: participant.GetContact().GetEmail()},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.identity.v1.TenantAdminService/ArchiveTenant")
		}
	}
	return &sellersv1.CompareSellerPayoutScheduleOptionsResponse{
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
			Organizer: &identityv1.UserRef{
				Email: simulateSellerListingReply.GetSellerListing().GetOwner().GetEmail(),
			},
			Participants: []*sellersv1.SellerPayoutScheduleParticipant{&sellersv1.SellerPayoutScheduleParticipant{
				Contact: &commonv1.ContactInfo{
					Email: simulateSellerListingReply.GetSellerListing().GetOwner().GetEmail(),
				},
			}},
			SellerPayoutScheduleId: req.GetSellerPayoutScheduleId(),
			Title:                  req.GetSellerPayoutSchedule().GetTitle(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RecalculateSellerPayoutSchedules recalculates a seller payout schedule.
func (s *SellerPayoutScheduleCalculationServer) RecalculateSellerPayoutSchedules(ctx context.Context, req *sellersv1.RecalculateSellerPayoutSchedulesRequest) (*sellersv1.RecalculateSellerPayoutSchedulesResponse, error) {
	if err := s.store.SaveSellerPayoutSchedule(ctx, req.GetContext().GetTenantId(), req.GetSellerPayoutSchedule()); err != nil {
		return nil, grpcx.Wrap(err, "save seller payout schedule")
	}
	s.log.InfoContext(ctx, "recalculate seller payout schedules", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSellerPayoutSchedule().GetAudit().GetEtag())
	resolveTripsRequest := &travelv1.ResolveTripsRequest{Context: req.GetContext()}
	resolveTripsReply, err := s.resolveTripsFn(ctx, resolveTripsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.travel.v1.TripQueryService/ResolveTrips")
	}
	cloned := proto.Clone(req.GetSellerPayoutSchedule()).(*sellersv1.SellerPayoutSchedule)
	streamSessionNoticesStream, err := s.sessionNotificationClient.StreamSessionNotices(ctx, &identityv1.StreamSessionNoticesRequest{
		Context:    req.GetContext(),
		Subscriber: &commonv1.ContactInfo{Email: cloned.GetOrganizer().GetEmail()},
		Filter: &identityv1.StreamSessionNoticesRequest_Filter{
			Region:       req.GetDetails().GetDestination().GetRegion(),
			CreatedAfter: req.GetSellerPayoutSchedule().GetAudit().GetCreatedAt(),
		},
		Since: req.GetSellerPayoutSchedule().GetAudit().GetUpdatedAt(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionNotificationService/StreamSessionNotices")
	}
	for {
		received, err := streamSessionNoticesStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.identity.v1.SessionNotificationService/StreamSessionNotices")
		}
		_ = received
	}
	return &sellersv1.RecalculateSellerPayoutSchedulesResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: resolveTripsReply.GetResult().GetCorrelationId(),
			ReferenceIds:  resolveTripsReply.GetResult().GetReferenceIds(),
			Status:        "ok",
		},
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
			SellerPayoutScheduleId: req.GetSellerPayoutScheduleId(),
			Organizer:              req.GetSellerPayoutSchedule().GetOrganizer(),
		},
	}, nil
}

// SimulateSellerPayoutSchedule simulates a seller payout schedule.
func (s *SellerPayoutScheduleCalculationServer) SimulateSellerPayoutSchedule(ctx context.Context, req *sellersv1.SimulateSellerPayoutScheduleRequest) (*sellersv1.SimulateSellerPayoutScheduleResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveSellerPayoutSchedule(ctx, req.GetContext().GetTenantId(), req.GetSellerPayoutSchedule()); err != nil {
		return nil, grpcx.Wrap(err, "save seller payout schedule")
	}
	s.log.InfoContext(ctx, "simulate seller payout schedule", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSellerPayoutSchedule().GetAudit().GetEtag())
	step := &sellerPayoutScheduleCalculationServerSimulateSellerPayoutScheduleSuggestRiskRegistersStep{request: &riskv1.SuggestRiskRegistersRequest{
		Context: req.GetContext(),
		Filter: &riskv1.SuggestRiskRegistersRequest_Filter{
			Region:       req.GetSellerPayoutSchedule().GetLocation().GetRegion(),
			CreatedAfter: req.GetSellerPayoutSchedule().GetAudit().GetCreatedAt(),
			OwnerId:      req.GetSellerPayoutSchedule().GetSellerPayoutScheduleId(),
		},
	}}
	if err := s.runSimulateSellerPayoutScheduleSuggestRiskRegisters(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.risk.v1.RiskRegisterQueryService/SuggestRiskRegisters")
	}
	suggestRiskRegistersReply := step.reply
	cloned := proto.Clone(req.GetSellerPayoutSchedule()).(*sellersv1.SellerPayoutSchedule)
	explainSellerListingCalculationReply, err := s.sellerListingCalculationClient.ExplainSellerListingCalculation(ctx, &sellersv1.ExplainSellerListingCalculationRequest{
		Context: req.GetContext(),
		SellerListing: &sellersv1.SellerListing{
			Locator: &sellersv1.SellerListing_Address{Address: &commonv1.PostalAddress{Locality: cloned.GetLocation().GetLocality()}},
			Owner: &identityv1.UserRef{
				DisplayName: req.GetSellerPayoutSchedule().GetOrganizer().GetDisplayName(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingCalculationService/ExplainSellerListingCalculation")
	}
	for _, participant := range req.GetSellerPayoutSchedule().GetParticipants() {
		if _, err := s.sellerListingCalculationClient.QuoteSellerListing(ctx, &sellersv1.QuoteSellerListingRequest{
			Context: req.GetContext(),
			SellerListing: &sellersv1.SellerListing{
				Owner: &identityv1.UserRef{Email: participant.GetContact().GetEmail()},
				Locator: &sellersv1.SellerListing_Address{Address: &commonv1.PostalAddress{
					PostalCode: req.GetSellerPayoutSchedule().GetLocation().GetPostalCode(),
				}},
			},
			Details: &sellersv1.QuoteSellerListingRequest_Details{
				Destination: &commonv1.PostalAddress{
					Lines: req.GetSellerPayoutSchedule().GetLocation().GetLines(),
				},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingCalculationService/QuoteSellerListing")
		}
	}
	quoteGiftCardBatchRequest := &giftcardsv1.QuoteGiftCardBatchRequest{
		Context: req.GetContext(),
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			BillingAddress: &commonv1.PostalAddress{
				CountryCode: req.GetSellerPayoutSchedule().GetLocation().GetCountryCode(),
			},
			Audit: &commonv1.AuditInfo{
				Etag: explainSellerListingCalculationReply.GetSellerListing().GetAudit().GetEtag(),
			},
		},
	}
	quoteGiftCardBatchReply, err := s.giftCardBatchCalculationPort.QuoteGiftCardBatch(ctx, quoteGiftCardBatchRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchCalculationService/QuoteGiftCardBatch")
	}
	return &sellersv1.SimulateSellerPayoutScheduleResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: suggestRiskRegistersReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
			Audit: &commonv1.AuditInfo{
				Etag: explainSellerListingCalculationReply.GetSellerListing().GetAudit().GetEtag(),
			},
			Participants: []*sellersv1.SellerPayoutScheduleParticipant{&sellersv1.SellerPayoutScheduleParticipant{
				Contact: &commonv1.ContactInfo{
					Email: explainSellerListingCalculationReply.GetSellerListing().GetOwner().GetEmail(),
				},
			}},
			Location: &commonv1.PostalAddress{
				CountryCode: quoteGiftCardBatchReply.GetGiftCardBatch().GetBillingAddress().GetCountryCode(),
			},
			SellerPayoutScheduleId: req.GetSellerPayoutScheduleId(),
			Organizer:              req.GetSellerPayoutSchedule().GetOrganizer(),
		},
	}, nil
}

// ExplainSellerPayoutScheduleCalculation explains a seller payout schedule.
func (s *SellerPayoutScheduleCalculationServer) ExplainSellerPayoutScheduleCalculation(ctx context.Context, req *sellersv1.ExplainSellerPayoutScheduleCalculationRequest) (*sellersv1.ExplainSellerPayoutScheduleCalculationResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveSellerPayoutSchedule(ctx, req.GetContext().GetTenantId(), req.GetSellerPayoutSchedule()); err != nil {
		return nil, grpcx.Wrap(err, "save seller payout schedule")
	}
	s.log.InfoContext(ctx, "explain seller payout schedule calculation", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSellerPayoutSchedule().GetAudit().GetEtag())
	resolveDocumentsRequest := &documentsv1.ResolveDocumentsRequest{Context: req.GetContext()}
	resolveDocumentsReply := new(documentsv1.ResolveDocumentsResponse)
	if err := s.conn.Invoke(ctx, documentsDocumentQueryServiceResolveDocumentsMethod, resolveDocumentsRequest, resolveDocumentsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.documents.v1.DocumentQueryService/ResolveDocuments")
	}
	sellerPayoutScheduleCache := cache.New[string, *sellersv1.SellerPayoutSchedule](1)
	sellerPayoutScheduleCache.Put(req.GetContext().GetTraceId(), req.GetSellerPayoutSchedule())
	cachedSellerPayoutSchedule, _ := sellerPayoutScheduleCache.Get(req.GetContext().GetTraceId())
	ctx = tenancy.With(ctx, "seller_payout_schedule_id", req.GetSellerPayoutSchedule().GetSellerPayoutScheduleId())
	metadata2 := make(map[string]string, len(req.GetSellerPayoutSchedule().GetMetadata()))
	for key, value := range req.GetSellerPayoutSchedule().GetMetadata() {
		metadata2[key] = value
	}
	exportAccessPoliciesStream, err := s.accessPolicyQueryClient.ExportAccessPolicies(ctx, &authorizationv1.ExportAccessPoliciesRequest{
		Context: req.GetContext(),
		Filter: &authorizationv1.ExportAccessPoliciesRequest_Filter{
			Region:       cachedSellerPayoutSchedule.GetLocation().GetRegion(),
			CreatedAfter: req.GetSellerPayoutSchedule().GetAudit().GetUpdatedAt(),
			OwnerId:      tenancy.Value(ctx, "seller_payout_schedule_id"),
			Labels:       metadata2,
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyQueryService/ExportAccessPolicies")
	}
	for {
		received, err := exportAccessPoliciesStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyQueryService/ExportAccessPolicies")
		}
		_ = received
	}
	giftCardValidation, err := registry.Lookup[giftcardsv1.GiftCardValidationServiceClient](s.clients, "acme.giftcards.v1.GiftCardValidationService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	previewGiftCardRequest := &giftcardsv1.PreviewGiftCardRequest{
		Context: req.GetContext(),
		GiftCard: &giftcardsv1.GiftCard{
			Contact: &commonv1.ContactInfo{Phone: req.GetDetails().GetContact().GetPhone()},
		},
	}
	previewGiftCardReply, err := giftCardValidation.PreviewGiftCard(ctx, previewGiftCardRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardValidationService/PreviewGiftCard")
	}
	scoreSellerRequest := &sellersv1.ScoreSellerRequest{
		Context: req.GetContext(),
		Seller: &sellersv1.Seller{
			Contact: &commonv1.ContactInfo{
				Email: previewGiftCardReply.GetGiftCard().GetContact().GetEmail(),
			},
		},
	}
	send := s.scoreSellerSender(scoreSellerRequest)
	scoreSellerReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerValidationService/ScoreSeller")
	}
	return &sellersv1.ExplainSellerPayoutScheduleCalculationResponse{
		Outcome: &sellersv1.ExplainSellerPayoutScheduleCalculationResponse_Outcome{Status: resolveDocumentsReply.GetResult().GetStatus()},
		Result: &commonv1.OperationResult{
			Message: resolveDocumentsReply.GetResult().GetMessage(),
			Status:  previewGiftCardReply.GetResult().GetStatus(),
		},
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
			Audit: &commonv1.AuditInfo{
				CreatedBy: previewGiftCardReply.GetGiftCard().GetAudit().GetCreatedBy(),
				Etag:      scoreSellerReply.GetSeller().GetAudit().GetEtag(),
			},
			SellerPayoutScheduleId: req.GetSellerPayoutScheduleId(),
			Organizer:              req.GetSellerPayoutSchedule().GetOrganizer(),
		},
	}, nil
}

// GetSellerPayoutScheduleRates returns a seller payout schedule.
func (s *SellerPayoutScheduleCalculationServer) GetSellerPayoutScheduleRates(ctx context.Context, req *sellersv1.GetSellerPayoutScheduleRatesRequest) (*sellersv1.GetSellerPayoutScheduleRatesResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "seller_payout_schedule", req.GetSellerPayoutScheduleId(), "get_seller_payout_schedule_rates"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get seller payout schedule rates", "tenant", req.GetContext().GetTenantId())
	return &sellersv1.GetSellerPayoutScheduleRatesResponse{
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{SellerPayoutScheduleId: req.GetSellerPayoutScheduleId()},
		Result:               &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// sellerListingCalculationLazy dials acme.sellers.v1.SellerListingCalculationService the first time a handler needs it.
func (s *SellerPayoutScheduleCalculationServer) sellerListingCalculationLazy() sellersv1.SellerListingCalculationServiceClient {
	s.sellerListingCalculationOnce.Do(func() { s.sellerListingCalculationCached = sellersv1.NewSellerListingCalculationServiceClient(s.conn) })
	return s.sellerListingCalculationCached
}

// sellerPayoutScheduleCalculationServerSimulateSellerPayoutScheduleSuggestRiskRegistersStep carries the request of one
// step of SimulateSellerPayoutSchedule and, once it ran, its reply.
type sellerPayoutScheduleCalculationServerSimulateSellerPayoutScheduleSuggestRiskRegistersStep struct {
	request *riskv1.SuggestRiskRegistersRequest
	reply   *riskv1.SuggestRiskRegistersResponse
}

func (s *SellerPayoutScheduleCalculationServer) runSimulateSellerPayoutScheduleSuggestRiskRegisters(ctx context.Context, step *sellerPayoutScheduleCalculationServerSimulateSellerPayoutScheduleSuggestRiskRegistersStep) error {
	reply, err := s.riskRegisterQueryClient.SuggestRiskRegisters(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}

// scoreSellerSender returns a function that sends one request to acme.sellers.v1.SellerValidationService/ScoreSeller.
func (s *SellerPayoutScheduleCalculationServer) scoreSellerSender(request *sellersv1.ScoreSellerRequest) func(context.Context) (*sellersv1.ScoreSellerResponse, error) {
	return func(ctx context.Context) (*sellersv1.ScoreSellerResponse, error) {
		return s.sellerValidationClient.ScoreSeller(ctx, request)
	}
}
