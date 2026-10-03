package merchandising

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	corporateeventsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/corporateevents/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	devicesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/devices/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// MerchandisingRuleValidationServer implements `acme.merchandising.v1.MerchandisingRuleValidationService`.
// Checks merchandising rules against the rules before anything acts on them.
type MerchandisingRuleValidationServer struct {
	*merchandisingRuleValidationHandlers
}

// merchandisingRuleValidationHandlers holds the handlers of MerchandisingRuleValidationServer, with what they use.
type merchandisingRuleValidationHandlers struct {
	merchandisingv1.UnimplementedMerchandisingRuleValidationServiceServer
	store                            *Store
	log                              *slog.Logger
	giftCardBatchQueryClient         giftcardsv1.GiftCardBatchQueryServiceClient
	sessionNotificationClient        identityv1.SessionNotificationServiceClient
	placementValidationClient        merchandisingv1.PlacementValidationServiceClient
	conn                             *grpc.ClientConn
	userValidationOnce               sync.Once
	userValidationCached             identityv1.UserValidationServiceClient
	managedDeviceClient              devicesv1.ManagedDeviceServiceClient
	managedDeviceClientFallback      devicesv1.ManagedDeviceServiceClient
	bus                              *eventbus.Bus
	sellerQueryClient                sellersv1.SellerQueryServiceClient
	corporateEventQueryPort          corporateEventQueryPort
	giftCardBatchCalculationClient   giftcardsv1.GiftCardBatchCalculationServiceClient
	placementValidationPort          placementValidationPort
	clients                          *registry.Registry
	sellerScorecardCalculationClient sellersv1.SellerScorecardCalculationServiceClient
}

// NewMerchandisingRuleValidationServer wires a MerchandisingRuleValidationServer.
func NewMerchandisingRuleValidationServer(deps platform.Deps) *MerchandisingRuleValidationServer {
	return &MerchandisingRuleValidationServer{merchandisingRuleValidationHandlers: &merchandisingRuleValidationHandlers{
		store:                            NewStore(deps.DB),
		log:                              deps.Logger.With("service", "acme.merchandising.v1.MerchandisingRuleValidationService"),
		giftCardBatchQueryClient:         giftcardsv1.NewGiftCardBatchQueryServiceClient(deps.Conn),
		sessionNotificationClient:        identityv1.NewSessionNotificationServiceClient(deps.Conn),
		placementValidationClient:        merchandisingv1.NewPlacementValidationServiceClient(deps.Conn),
		conn:                             deps.Conn,
		managedDeviceClient:              devicesv1.NewManagedDeviceServiceClient(deps.Conn),
		managedDeviceClientFallback:      devicesv1.NewManagedDeviceServiceClient(deps.Conn),
		bus:                              deps.Bus,
		sellerQueryClient:                sellersv1.NewSellerQueryServiceClient(deps.Conn),
		corporateEventQueryPort:          newCorporateEventQueryPort(deps),
		giftCardBatchCalculationClient:   giftcardsv1.NewGiftCardBatchCalculationServiceClient(deps.Conn),
		placementValidationPort:          newPlacementValidationPort(deps),
		clients:                          deps.Clients,
		sellerScorecardCalculationClient: sellersv1.NewSellerScorecardCalculationServiceClient(deps.Conn),
	}}
}

// ValidateMerchandisingRule validates a merchandising rule.
func (s *merchandisingRuleValidationHandlers) ValidateMerchandisingRule(ctx context.Context, req *merchandisingv1.ValidateMerchandisingRuleRequest) (*merchandisingv1.ValidateMerchandisingRuleResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveMerchandisingRule(ctx, req.GetContext().GetTenantId(), req.GetMerchandisingRule()); err != nil {
		return nil, grpcx.Wrap(err, "save merchandising rule")
	}
	s.log.InfoContext(ctx, "validate merchandising rule", "tenant", req.GetContext().GetTenantId(), "etag", req.GetMerchandisingRule().GetAudit().GetEtag())
	exportGiftCardBatchesStream, err := s.giftCardBatchQueryClient.ExportGiftCardBatches(ctx, &giftcardsv1.ExportGiftCardBatchesRequest{
		Context: req.GetContext(),
		Filter: &giftcardsv1.ExportGiftCardBatchesRequest_Filter{
			CreatedAfter: req.GetMerchandisingRule().GetAudit().GetUpdatedAt(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchQueryService/ExportGiftCardBatches")
	}
	for {
		received, err := exportGiftCardBatchesStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchQueryService/ExportGiftCardBatches")
		}
		_ = received
	}
	packed, err := anypb.New(req.GetMerchandisingRule())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	subscribeSessionUpdatesReply, err := s.sessionNotificationClient.SubscribeSessionUpdates(ctx, &identityv1.SubscribeSessionUpdatesRequest{
		Context: req.GetContext(),
		Details: &identityv1.SubscribeSessionUpdatesRequest_Details{
			Recipient: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetContact().GetPreferredChannel(),
				Name:             req.GetDetails().GetContact().GetName(),
			},
			Payload: packed,
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionNotificationService/SubscribeSessionUpdates")
	}
	return &merchandisingv1.ValidateMerchandisingRuleResponse{
		Result: &commonv1.OperationResult{
			Message: subscribeSessionUpdatesReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		MerchandisingRule: &merchandisingv1.MerchandisingRule{
			MerchandisingRuleId: req.GetMerchandisingRuleId(),
			Status:              req.GetMerchandisingRule().GetStatus(),
		},
	}, nil
}

// PreviewMerchandisingRule previews a merchandising rule.
func (s *merchandisingRuleValidationHandlers) PreviewMerchandisingRule(ctx context.Context, req *merchandisingv1.PreviewMerchandisingRuleRequest) (*merchandisingv1.PreviewMerchandisingRuleResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected PreviewMerchandisingRule", "error", err)
		return nil, err
	}
	if err := s.store.SaveMerchandisingRule(ctx, req.GetContext().GetTenantId(), req.GetMerchandisingRule()); err != nil {
		return nil, grpcx.Wrap(err, "save merchandising rule")
	}
	s.log.InfoContext(ctx, "preview merchandising rule", "tenant", req.GetContext().GetTenantId(), "etag", req.GetMerchandisingRule().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetMerchandisingRule()).(*merchandisingv1.MerchandisingRule)
	validatePlacementRequest := &merchandisingv1.ValidatePlacementRequest{
		Context: req.GetContext(),
		Placement: &merchandisingv1.Placement{
			Participants: []*merchandisingv1.PlacementParticipant{&merchandisingv1.PlacementParticipant{
				Contact: &commonv1.ContactInfo{
					Phone: req.GetDetails().GetContact().GetPhone(),
					Email: req.GetDetails().GetContact().GetEmail(),
					Name:  req.GetDetails().GetContact().GetName(),
				},
			}},
			Audit: &commonv1.AuditInfo{Etag: cloned.GetAudit().GetEtag()},
		},
	}
	send := s.validatePlacementSender(validatePlacementRequest)
	validatePlacementReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.PlacementValidationService/ValidatePlacement")
	}
	ctx = tenancy.With(ctx, "etag", req.GetMerchandisingRule().GetAudit().GetEtag())
	scoreUserRequest := &identityv1.ScoreUserRequest{
		Context: req.GetContext(),
		User: &identityv1.User{
			RelationshipManager: &identityv1.UserRef{
				UserId:      req.GetMerchandisingRule().GetOwner().GetUserId(),
				DisplayName: validatePlacementReply.GetPlacement().GetOrganizer().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{Etag: tenancy.Value(ctx, "etag")},
			Name:  req.GetDetails().GetContact().GetName(),
		},
	}
	scoreUserReply, err := s.userValidationLazy().ScoreUser(ctx, scoreUserRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserValidationService/ScoreUser")
	}
	return &merchandisingv1.PreviewMerchandisingRuleResponse{
		MerchandisingRule: &merchandisingv1.MerchandisingRule{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: validatePlacementReply.GetPlacement().GetAudit().GetUpdatedAt(),
			},
			Owner: &identityv1.UserRef{
				UserId: scoreUserReply.GetUser().GetRelationshipManager().GetUserId(),
			},
			MerchandisingRuleId: req.GetMerchandisingRuleId(),
			Description:         req.GetMerchandisingRule().GetDescription(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CheckMerchandisingRuleEligibility checks a merchandising rule.
func (s *merchandisingRuleValidationHandlers) CheckMerchandisingRuleEligibility(ctx context.Context, req *merchandisingv1.CheckMerchandisingRuleEligibilityRequest) (*merchandisingv1.CheckMerchandisingRuleEligibilityResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CheckMerchandisingRuleEligibility", "error", err)
		return nil, err
	}
	if err := s.store.SaveMerchandisingRule(ctx, req.GetContext().GetTenantId(), req.GetMerchandisingRule()); err != nil {
		return nil, grpcx.Wrap(err, "save merchandising rule")
	}
	s.log.InfoContext(ctx, "check merchandising rule eligibility", "tenant", req.GetContext().GetTenantId(), "etag", req.GetMerchandisingRule().GetAudit().GetEtag())
	scorePlacementReply, err := s.placementValidationClient.ScorePlacement(ctx, &merchandisingv1.ScorePlacementRequest{
		Context: req.GetContext(),
		Placement: &merchandisingv1.Placement{
			Participants: []*merchandisingv1.PlacementParticipant{&merchandisingv1.PlacementParticipant{
				Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetContact().GetEmail()},
			}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.PlacementValidationService/ScorePlacement")
	}
	routes := map[string]func(context.Context, *devicesv1.UpdateManagedDeviceRequest, ...grpc.CallOption) (*devicesv1.UpdateManagedDeviceResponse, error){
		"primary":  s.managedDeviceClient.UpdateManagedDevice,
		"fallback": s.managedDeviceClientFallback.UpdateManagedDevice,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	updateManagedDeviceRequest := &devicesv1.UpdateManagedDeviceRequest{
		Context: req.GetContext(),
		ManagedDevice: &devicesv1.ManagedDevice{
			Audit: &commonv1.AuditInfo{
				CreatedAt: scorePlacementReply.GetPlacement().GetAudit().GetCreatedAt(),
			},
			Locator: &devicesv1.ManagedDevice_Address{Address: &commonv1.PostalAddress{
				Recipient: scorePlacementReply.GetPlacement().GetLocation().GetRecipient(),
			}},
		},
	}
	updateManagedDeviceReply, err := route(ctx, updateManagedDeviceRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.devices.v1.ManagedDeviceService/UpdateManagedDevice")
	}
	_ = updateManagedDeviceReply
	return &merchandisingv1.CheckMerchandisingRuleEligibilityResponse{
		Outcome: &merchandisingv1.CheckMerchandisingRuleEligibilityResponse_Outcome{Message: scorePlacementReply.GetResult().GetMessage()},
		MerchandisingRule: &merchandisingv1.MerchandisingRule{
			Owner: &identityv1.UserRef{
				DisplayName: scorePlacementReply.GetPlacement().GetOrganizer().GetDisplayName(),
			},
			MerchandisingRuleId: req.GetMerchandisingRuleId(),
			Name:                req.GetMerchandisingRule().GetName(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// NormalizeMerchandisingRule normalizes a merchandising rule.
func (s *merchandisingRuleValidationHandlers) NormalizeMerchandisingRule(ctx context.Context, req *merchandisingv1.NormalizeMerchandisingRuleRequest) (*merchandisingv1.NormalizeMerchandisingRuleResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveMerchandisingRule(ctx, req.GetContext().GetTenantId(), req.GetMerchandisingRule()); err != nil {
		return nil, grpcx.Wrap(err, "save merchandising rule")
	}
	s.log.InfoContext(ctx, "normalize merchandising rule", "tenant", req.GetContext().GetTenantId(), "etag", req.GetMerchandisingRule().GetAudit().GetEtag())
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "merchandising.normalize_merchandising_rule.explain_placement_decision",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"placement.participants.contact.email": req.GetDetails().GetContact().GetEmail(),
			"placement.organizer.display_name":     req.GetMerchandisingRule().GetOwner().GetDisplayName(),
			"placement.audit.created_by":           req.GetMerchandisingRule().GetAudit().GetCreatedBy(),
			"context.tenant_id":                    req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	summarizeSellersRequest := &sellersv1.SummarizeSellersRequest{
		Context:     req.GetContext(),
		Reason:      req.GetMerchandisingRule().GetBlock().GetReason(),
		Annotations: annotations,
	}
	summarizeSellersReply, err := summarizeSellersCall(ctx, summarizeSellersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerQueryService/SummarizeSellers")
	}
	step := &merchandisingRuleValidationHandlersNormalizeMerchandisingRuleBrowseSellersStep{request: &sellersv1.BrowseSellersRequest{
		Context: req.GetContext(),
		Filter: &sellersv1.BrowseSellersRequest_Filter{
			CreatedAfter: req.GetMerchandisingRule().GetAudit().GetUpdatedAt(),
		},
	}}
	if err := s.runNormalizeMerchandisingRuleBrowseSellers(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerQueryService/BrowseSellers")
	}
	browseSellersReply := step.reply
	lintPlacementsRequest := &merchandisingv1.LintPlacementsRequest{}
	lintPlacementsRequest.Context = req.GetContext()
	lintPlacementsRequest.Placement = &merchandisingv1.Placement{
		Participants: []*merchandisingv1.PlacementParticipant{&merchandisingv1.PlacementParticipant{
			Contact: &commonv1.ContactInfo{Email: req.GetMerchandisingRule().GetOwner().GetEmail()},
		}},
		Organizer: &identityv1.UserRef{
			DisplayName: req.GetMerchandisingRule().GetOwner().GetDisplayName(),
		},
	}
	lintPlacementsRequest.Details = &merchandisingv1.LintPlacementsRequest_Details{
		Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetContact().GetEmail()},
	}
	lintPlacementsReply, err := s.placementValidationClient.LintPlacements(ctx, lintPlacementsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.PlacementValidationService/LintPlacements")
	}
	return &merchandisingv1.NormalizeMerchandisingRuleResponse{
		Result: &commonv1.OperationResult{
			Status:       summarizeSellersReply.GetResult().GetStatus(),
			Details:      summarizeSellersReply.GetResult().GetDetails(),
			ReferenceIds: lintPlacementsReply.GetResult().GetReferenceIds(),
		},
		Outcome: &merchandisingv1.NormalizeMerchandisingRuleResponse_Outcome{
			Status:      browseSellersReply.GetResult().GetStatus(),
			ReferenceId: lintPlacementsReply.GetOutcome().GetReferenceId(),
		},
		MerchandisingRule: &merchandisingv1.MerchandisingRule{
			MerchandisingRuleId: req.GetMerchandisingRuleId(),
			Owner:               req.GetMerchandisingRule().GetOwner(),
		},
	}, nil
}

// ExplainMerchandisingRuleDecision explains a merchandising rule.
func (s *merchandisingRuleValidationHandlers) ExplainMerchandisingRuleDecision(ctx context.Context, req *merchandisingv1.ExplainMerchandisingRuleDecisionRequest) (*merchandisingv1.ExplainMerchandisingRuleDecisionResponse, error) {
	if err := s.store.SaveMerchandisingRule(ctx, req.GetContext().GetTenantId(), req.GetMerchandisingRule()); err != nil {
		return nil, grpcx.Wrap(err, "save merchandising rule")
	}
	s.log.InfoContext(ctx, "explain merchandising rule decision", "tenant", req.GetContext().GetTenantId(), "etag", req.GetMerchandisingRule().GetAudit().GetEtag())
	findCorporateEventsRequest := &corporateeventsv1.FindCorporateEventsRequest{
		Context: req.GetContext(),
		Filter: &corporateeventsv1.FindCorporateEventsRequest_Filter{
			CreatedAfter: req.GetMerchandisingRule().GetAudit().GetCreatedAt(),
		},
	}
	findCorporateEventsReply, err := s.corporateEventQueryPort.FindCorporateEvents(ctx, findCorporateEventsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.corporateevents.v1.CorporateEventQueryService/FindCorporateEvents")
	}
	var holder struct{ Description string }
	if record := req.GetMerchandisingRule(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Description"); err != nil {
			s.log.WarnContext(ctx, "copy description", "error", err)
		}
	}
	estimateGiftCardBatchRequest := &giftcardsv1.EstimateGiftCardBatchRequest{
		Context: req.GetContext(),
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			Lines: []*giftcardsv1.GiftCardBatchLine{&giftcardsv1.GiftCardBatchLine{
				Amount:      req.GetMerchandisingRule().GetDiscount().GetAmount(),
				Description: holder.Description,
			}},
			Audit: &commonv1.AuditInfo{Etag: req.GetMerchandisingRule().GetAudit().GetEtag()},
		},
	}
	estimateGiftCardBatchReply, err := grpcx.Call(ctx, s.giftCardBatchCalculationClient.EstimateGiftCardBatch, estimateGiftCardBatchRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchCalculationService/EstimateGiftCardBatch")
	}
	return &merchandisingv1.ExplainMerchandisingRuleDecisionResponse{
		Result: &commonv1.OperationResult{
			Status:        findCorporateEventsReply.GetResult().GetStatus(),
			CorrelationId: findCorporateEventsReply.GetResult().GetCorrelationId(),
			Details:       estimateGiftCardBatchReply.GetResult().GetDetails(),
			Message:       estimateGiftCardBatchReply.GetResult().GetMessage(),
		},
		MerchandisingRule: &merchandisingv1.MerchandisingRule{
			MerchandisingRuleId: req.GetMerchandisingRuleId(),
			Effective:           req.GetMerchandisingRule().GetEffective(),
		},
	}, nil
}

// ScoreMerchandisingRule scores a merchandising rule.
func (s *merchandisingRuleValidationHandlers) ScoreMerchandisingRule(ctx context.Context, req *merchandisingv1.ScoreMerchandisingRuleRequest) (*merchandisingv1.ScoreMerchandisingRuleResponse, error) {
	if err := s.store.SaveMerchandisingRule(ctx, req.GetContext().GetTenantId(), req.GetMerchandisingRule()); err != nil {
		return nil, grpcx.Wrap(err, "save merchandising rule")
	}
	s.log.InfoContext(ctx, "score merchandising rule", "tenant", req.GetContext().GetTenantId(), "etag", req.GetMerchandisingRule().GetAudit().GetEtag())
	estimateSellerScorecardRequest := &sellersv1.EstimateSellerScorecardRequest{
		Context: req.GetContext(),
		SellerScorecard: &sellersv1.SellerScorecard{
			Lines: []*sellersv1.SellerScorecardLine{&sellersv1.SellerScorecardLine{
				Tax: &sellersv1.SellerScorecardLineTax{
					Amount: req.GetMerchandisingRule().GetDiscount().GetAmount(),
				},
			}},
			BillingAddress: &commonv1.PostalAddress{
				CountryCode: req.GetDetails().GetDestination().GetCountryCode(),
			},
		},
	}
	estimateSellerScorecardReply, err := grpcx.NewClient(s.conn, sellersv1.NewSellerScorecardCalculationServiceClient).EstimateSellerScorecard(ctx, estimateSellerScorecardRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardCalculationService/EstimateSellerScorecard")
	}
	normalizePlacementRequest := &merchandisingv1.NormalizePlacementRequest{
		Context: req.GetContext(),
		Placement: &merchandisingv1.Placement{
			Audit: &commonv1.AuditInfo{
				CreatedBy: estimateSellerScorecardReply.GetSellerScorecard().GetAudit().GetCreatedBy(),
			},
		},
	}
	normalizePlacementReply, err := s.placementValidationPort.NormalizePlacement(ctx, normalizePlacementRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.PlacementValidationService/NormalizePlacement")
	}
	previewGiftCardBatchRequest := &giftcardsv1.PreviewGiftCardBatchRequest{
		Context: req.GetContext(),
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			BillingAddress: &commonv1.PostalAddress{
				CountryCode: normalizePlacementReply.GetPlacement().GetLocation().GetCountryCode(),
			},
		},
	}
	previewGiftCardBatchReply, err := previewGiftCardBatchCall(ctx, previewGiftCardBatchRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchValidationService/PreviewGiftCardBatch")
	}
	summarizeGiftCardBatchesRequest := &giftcardsv1.SummarizeGiftCardBatchesRequest{
		Context: req.GetContext(),
		Reason:  req.GetMerchandisingRule().GetBlock().GetReason(),
	}
	summarizeGiftCardBatchesReply, err := grpcx.Call(ctx, s.giftCardBatchQueryClient.SummarizeGiftCardBatches, summarizeGiftCardBatchesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchQueryService/SummarizeGiftCardBatches")
	}
	return &merchandisingv1.ScoreMerchandisingRuleResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds:  estimateSellerScorecardReply.GetResult().GetReferenceIds(),
			Status:        normalizePlacementReply.GetResult().GetStatus(),
			CorrelationId: previewGiftCardBatchReply.GetResult().GetCorrelationId(),
			Message:       summarizeGiftCardBatchesReply.GetResult().GetMessage(),
		},
		MerchandisingRule: &merchandisingv1.MerchandisingRule{
			Audit: &commonv1.AuditInfo{
				CreatedAt: normalizePlacementReply.GetPlacement().GetAudit().GetCreatedAt(),
			},
			MerchandisingRuleId: req.GetMerchandisingRuleId(),
			Version:             req.GetMerchandisingRule().GetVersion(),
		},
	}, nil
}

// LintMerchandisingRules lints a merchandising rule.
func (s *merchandisingRuleValidationHandlers) LintMerchandisingRules(ctx context.Context, req *merchandisingv1.LintMerchandisingRulesRequest) (*merchandisingv1.LintMerchandisingRulesResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveMerchandisingRule(ctx, req.GetContext().GetTenantId(), req.GetMerchandisingRule()); err != nil {
		return nil, grpcx.Wrap(err, "save merchandising rule")
	}
	s.log.InfoContext(ctx, "lint merchandising rules", "tenant", req.GetContext().GetTenantId(), "etag", req.GetMerchandisingRule().GetAudit().GetEtag())
	placementValidation, err := registry.Lookup[merchandisingv1.PlacementValidationServiceClient](s.clients, "acme.merchandising.v1.PlacementValidationService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	normalizePlacementRequest := &merchandisingv1.NormalizePlacementRequest{
		Context: req.GetContext(),
		Placement: &merchandisingv1.Placement{
			Audit: &commonv1.AuditInfo{
				CreatedBy: req.GetMerchandisingRule().GetAudit().GetCreatedBy(),
			},
		},
	}
	normalizePlacementReply, err := placementValidation.NormalizePlacement(ctx, normalizePlacementRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.PlacementValidationService/NormalizePlacement")
	}
	estimateSellerScorecardReply, err := s.sellerScorecardCalculationClient.EstimateSellerScorecard(ctx, &sellersv1.EstimateSellerScorecardRequest{
		Context: req.GetContext(),
		SellerScorecard: &sellersv1.SellerScorecard{
			Counterparty: &customersv1.CustomerRef{Email: req.GetDetails().GetContact().GetEmail()},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: req.GetMerchandisingRule().GetAudit().GetUpdatedAt(),
			},
			Lines: []*sellersv1.SellerScorecardLine{&sellersv1.SellerScorecardLine{
				Tax: &sellersv1.SellerScorecardLineTax{
					Amount: req.GetMerchandisingRule().GetDiscount().GetAmount(),
				},
			}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardCalculationService/EstimateSellerScorecard")
	}
	previewPlacementRequest := &merchandisingv1.PreviewPlacementRequest{
		Context: req.GetContext(),
		Placement: &merchandisingv1.Placement{
			Participants: []*merchandisingv1.PlacementParticipant{&merchandisingv1.PlacementParticipant{
				Contact: &commonv1.ContactInfo{
					Email: estimateSellerScorecardReply.GetSellerScorecard().GetCounterparty().GetEmail(),
				},
			}},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: estimateSellerScorecardReply.GetSellerScorecard().GetAudit().GetUpdatedAt(),
			},
		},
	}
	previewPlacementReply := new(merchandisingv1.PreviewPlacementResponse)
	if err := grpcx.LegacyInvoke(ctx, s.conn, "/acme.merchandising.v1.PlacementValidationService/PreviewPlacement", previewPlacementRequest, previewPlacementReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.PlacementValidationService/PreviewPlacement")
	}
	return &merchandisingv1.LintMerchandisingRulesResponse{
		MerchandisingRule: &merchandisingv1.MerchandisingRule{
			Owner: &identityv1.UserRef{
				Email:       normalizePlacementReply.GetPlacement().GetOrganizer().GetEmail(),
				DisplayName: previewPlacementReply.GetPlacement().GetOrganizer().GetDisplayName(),
			},
			MerchandisingRuleId: req.GetMerchandisingRuleId(),
			Status:              req.GetMerchandisingRule().GetStatus(),
		},
		Outcome: &merchandisingv1.LintMerchandisingRulesResponse_Outcome{Message: normalizePlacementReply.GetOutcome().GetMessage()},
		Result: &commonv1.OperationResult{
			ReferenceIds:  estimateSellerScorecardReply.GetResult().GetReferenceIds(),
			CorrelationId: previewPlacementReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// GetMerchandisingRuleRules returns a merchandising rule.
func (s *merchandisingRuleValidationHandlers) GetMerchandisingRuleRules(ctx context.Context, req *merchandisingv1.GetMerchandisingRuleRulesRequest) (*merchandisingv1.GetMerchandisingRuleRulesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetMerchandisingRuleRules", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "merchandising_rule", req.GetMerchandisingRuleId(), "get_merchandising_rule_rules"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get merchandising rule rules", "tenant", req.GetContext().GetTenantId())
	return &merchandisingv1.GetMerchandisingRuleRulesResponse{
		MerchandisingRule: &merchandisingv1.MerchandisingRule{MerchandisingRuleId: req.GetMerchandisingRuleId()},
		Result:            &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// validatePlacementSender returns a function that sends one request to
// acme.merchandising.v1.PlacementValidationService/ValidatePlacement.
func (s *merchandisingRuleValidationHandlers) validatePlacementSender(request *merchandisingv1.ValidatePlacementRequest) func(context.Context) (*merchandisingv1.ValidatePlacementResponse, error) {
	return func(ctx context.Context) (*merchandisingv1.ValidatePlacementResponse, error) {
		return s.placementValidationClient.ValidatePlacement(ctx, request)
	}
}

// userValidationLazy dials acme.identity.v1.UserValidationService the first time a handler needs it.
func (s *merchandisingRuleValidationHandlers) userValidationLazy() identityv1.UserValidationServiceClient {
	s.userValidationOnce.Do(func() { s.userValidationCached = identityv1.NewUserValidationServiceClient(s.conn) })
	return s.userValidationCached
}

// merchandisingRuleValidationHandlersNormalizeMerchandisingRuleBrowseSellersStep carries the request of one step of
// NormalizeMerchandisingRule and, once it ran, its reply.
type merchandisingRuleValidationHandlersNormalizeMerchandisingRuleBrowseSellersStep struct {
	request *sellersv1.BrowseSellersRequest
	reply   *sellersv1.BrowseSellersResponse
}

func (s *merchandisingRuleValidationHandlers) runNormalizeMerchandisingRuleBrowseSellers(ctx context.Context, step *merchandisingRuleValidationHandlersNormalizeMerchandisingRuleBrowseSellersStep) error {
	reply, err := s.sellerQueryClient.BrowseSellers(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
