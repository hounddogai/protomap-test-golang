package shipping

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	assetsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/assets/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	notificationsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/notifications/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/anypb"
)

// ManifestValidationServer implements `acme.shipping.v1.ManifestValidationService`.
// Checks manifests against the rules before anything acts on them.
type ManifestValidationServer struct {
	shippingv1.UnimplementedManifestValidationServiceServer
	store                            *Store
	log                              *slog.Logger
	notificationClient               notificationsv1.NotificationServiceClient
	shipmentCalculationClient        shippingv1.ShipmentCalculationServiceClient
	sessionValidationClient          identityv1.SessionValidationServiceClient
	giftCardBatchQueryClient         giftcardsv1.GiftCardBatchQueryServiceClient
	returnRequestValidationClient    returnsv1.ReturnRequestValidationServiceClient
	pointsLedgerCalculationClient    loyaltyv1.PointsLedgerCalculationServiceClient
	trackingEventCalculationClient   shippingv1.TrackingEventCalculationServiceClient
	reviewModerationValidationClient reviewsv1.ReviewModerationValidationServiceClient
	conn                             *grpc.ClientConn
	reviewModerationValidationOnce   sync.Once
	reviewModerationValidationCached reviewsv1.ReviewModerationValidationServiceClient
	fixedAssetQueryClient            assetsv1.FixedAssetQueryServiceClient
	shipmentQueryClient              shippingv1.ShipmentQueryServiceClient
	carrierRateCalculationClient     shippingv1.CarrierRateCalculationServiceClient
	loyaltyMemberCalculationClient   loyaltyv1.LoyaltyMemberCalculationServiceClient
	normalizeTrackingEventFn         func(context.Context, *shippingv1.NormalizeTrackingEventRequest, ...grpc.CallOption) (*shippingv1.NormalizeTrackingEventResponse, error)
}

// NewManifestValidationServer wires a ManifestValidationServer.
func NewManifestValidationServer(deps platform.Deps) *ManifestValidationServer {
	trackingEventValidation := shippingv1.NewTrackingEventValidationServiceClient(deps.Conn)
	return &ManifestValidationServer{
		store:                            NewStore(deps.DB),
		log:                              deps.Logger.With("service", "acme.shipping.v1.ManifestValidationService"),
		notificationClient:               notificationsv1.NewNotificationServiceClient(deps.Conn),
		shipmentCalculationClient:        shippingv1.NewShipmentCalculationServiceClient(deps.Conn),
		sessionValidationClient:          identityv1.NewSessionValidationServiceClient(deps.Conn),
		giftCardBatchQueryClient:         giftcardsv1.NewGiftCardBatchQueryServiceClient(deps.Conn),
		returnRequestValidationClient:    returnsv1.NewReturnRequestValidationServiceClient(deps.Conn),
		pointsLedgerCalculationClient:    loyaltyv1.NewPointsLedgerCalculationServiceClient(deps.Conn),
		trackingEventCalculationClient:   shippingv1.NewTrackingEventCalculationServiceClient(deps.Conn),
		reviewModerationValidationClient: reviewsv1.NewReviewModerationValidationServiceClient(deps.Conn),
		conn:                             deps.Conn,
		fixedAssetQueryClient:            assetsv1.NewFixedAssetQueryServiceClient(deps.Conn),
		shipmentQueryClient:              shippingv1.NewShipmentQueryServiceClient(deps.Conn),
		carrierRateCalculationClient:     shippingv1.NewCarrierRateCalculationServiceClient(deps.Conn),
		loyaltyMemberCalculationClient:   loyaltyv1.NewLoyaltyMemberCalculationServiceClient(deps.Conn),
		normalizeTrackingEventFn:         trackingEventValidation.NormalizeTrackingEvent,
	}
}

// ValidateManifest validates a manifest.
func (s *ManifestValidationServer) ValidateManifest(ctx context.Context, req *shippingv1.ValidateManifestRequest) (*shippingv1.ValidateManifestResponse, error) {
	if err := s.store.SaveManifest(ctx, req.GetContext().GetTenantId(), req.GetManifest()); err != nil {
		return nil, grpcx.Wrap(err, "save manifest")
	}
	s.log.InfoContext(ctx, "validate manifest", "tenant", req.GetContext().GetTenantId(), "etag", req.GetManifest().GetAudit().GetEtag())
	encoded, err := protojson.Marshal(req.GetManifest())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded shippingv1.Manifest
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	createNotificationRequest := &notificationsv1.CreateNotificationRequest{
		Context: req.GetContext(),
		Notification: &notificationsv1.Notification{
			Comments: []*notificationsv1.NotificationComment{&notificationsv1.NotificationComment{CreatedAt: (&decoded).GetAudit().GetCreatedAt()}},
			Assignee: &identityv1.UserRef{Email: req.GetManifest().GetCounterparty().GetEmail()},
		},
	}
	send := s.createNotificationSender(createNotificationRequest)
	createNotificationReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.notifications.v1.NotificationService/CreateNotification")
	}
	simulateShipmentRequest := &shippingv1.SimulateShipmentRequest{
		Context: req.GetContext(),
		Shipment: &shippingv1.Shipment{
			Audit: &commonv1.AuditInfo{UpdatedBy: req.GetManifest().GetAudit().GetUpdatedBy()},
			BillingAddress: &commonv1.PostalAddress{
				Locality: req.GetManifest().GetBillingAddress().GetLocality(),
			},
		},
	}
	simulateShipmentReply, err := grpcx.Call(ctx, s.shipmentCalculationClient.SimulateShipment, simulateShipmentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ShipmentCalculationService/SimulateShipment")
	}
	previewSessionReply, err := s.sessionValidationClient.PreviewSession(ctx, &identityv1.PreviewSessionRequest{
		Context: req.GetContext(),
		Session: &identityv1.Session{
			Requester: &commonv1.ContactInfo{
				Email: createNotificationReply.GetNotification().GetRequester().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: createNotificationReply.GetNotification().GetAudit().GetUpdatedAt(),
				UpdatedBy: createNotificationReply.GetNotification().GetAudit().GetUpdatedBy(),
			},
			Origin: &identityv1.Session_Customer{Customer: &identityv1.ApiCredentialRef{
				DisplayName: createNotificationReply.GetNotification().GetAssignee().GetDisplayName(),
			}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionValidationService/PreviewSession")
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	summarizeGiftCardBatchesRequest := &giftcardsv1.SummarizeGiftCardBatchesRequest{}
	summarizeGiftCardBatchesRequest.Context = req.GetContext()
	summarizeGiftCardBatchesRequest.Annotations = annotations
	summarizeGiftCardBatchesRequest.Reason = req.GetReason()
	summarizeGiftCardBatchesReply, err := s.giftCardBatchQueryClient.SummarizeGiftCardBatches(ctx, summarizeGiftCardBatchesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchQueryService/SummarizeGiftCardBatches")
	}
	return &shippingv1.ValidateManifestResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: createNotificationReply.GetResult().GetCorrelationId(),
			ReferenceIds:  previewSessionReply.GetResult().GetReferenceIds(),
			Message:       summarizeGiftCardBatchesReply.GetResult().GetMessage(),
			Status:        "ok",
		},
		Manifest: &shippingv1.Manifest{
			BillingAddress: &commonv1.PostalAddress{
				PostalCode: simulateShipmentReply.GetShipment().GetBillingAddress().GetPostalCode(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: simulateShipmentReply.GetShipment().GetAudit().GetUpdatedBy(),
			},
			ManifestId: req.GetManifestId(),
			Metadata:   req.GetManifest().GetMetadata(),
		},
		Outcome: &shippingv1.ValidateManifestResponse_Outcome{
			ReferenceId: previewSessionReply.GetOutcome().GetReferenceId(),
		},
	}, nil
}

// PreviewManifest previews a manifest.
func (s *ManifestValidationServer) PreviewManifest(ctx context.Context, req *shippingv1.PreviewManifestRequest) (*shippingv1.PreviewManifestResponse, error) {
	if err := s.store.SaveManifest(ctx, req.GetContext().GetTenantId(), req.GetManifest()); err != nil {
		return nil, grpcx.Wrap(err, "save manifest")
	}
	s.log.InfoContext(ctx, "preview manifest", "tenant", req.GetContext().GetTenantId(), "etag", req.GetManifest().GetAudit().GetEtag())
	checkReturnRequestEligibilityRequest := buildCheckReturnRequestEligibilityRequestForPreviewManifest(req)
	checkReturnRequestEligibilityReply, err := s.returnRequestValidationClient.CheckReturnRequestEligibility(ctx, checkReturnRequestEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnRequestValidationService/CheckReturnRequestEligibility")
	}
	calculatePointsLedgerRequest := buildCalculatePointsLedgerRequestForPreviewManifest(req)
	calculatePointsLedgerReply, err := s.pointsLedgerCalculationClient.CalculatePointsLedger(ctx, calculatePointsLedgerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerCalculationService/CalculatePointsLedger")
	}
	for _, line := range req.GetManifest().GetLines() {
		if _, err := s.trackingEventCalculationClient.CalculateTrackingEvent(ctx, &shippingv1.CalculateTrackingEventRequest{
			Context:       req.GetContext(),
			TrackingEvent: &shippingv1.TrackingEvent{Attributes: line.GetAttributes()},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.shipping.v1.TrackingEventCalculationService/CalculateTrackingEvent")
		}
	}
	validateReviewModerationRequest := &reviewsv1.ValidateReviewModerationRequest{
		Context: req.GetContext(),
		ReviewModeration: &reviewsv1.ReviewModeration{
			Origin: &reviewsv1.ReviewModeration_Customer{Customer: &customersv1.CustomerRef{
				CustomerId: checkReturnRequestEligibilityReply.GetReturnRequest().GetCounterparty().GetCustomerId(),
			}},
			Requester: &commonv1.ContactInfo{
				Name:  calculatePointsLedgerReply.GetPointsLedger().GetContact().GetName(),
				Email: checkReturnRequestEligibilityReply.GetReturnRequest().GetCounterparty().GetEmail(),
			},
		},
	}
	var validateReviewModerationReply *reviewsv1.ValidateReviewModerationResponse
	if err := pipeline.Run(ctx, "acme.reviews.v1.ReviewModerationValidationService/ValidateReviewModeration", func(ctx context.Context) error {
		answer, err := s.reviewModerationValidationClient.ValidateReviewModeration(ctx, validateReviewModerationRequest)
		validateReviewModerationReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewModerationValidationService/ValidateReviewModeration")
	}
	return &shippingv1.PreviewManifestResponse{
		Manifest: &shippingv1.Manifest{
			BillingAddress: checkReturnRequestEligibilityReply.GetReturnRequest().GetBillingAddress(),
			DueAt:          validateReviewModerationReply.GetReviewModeration().GetSla().GetDueAt(),
			Audit: &commonv1.AuditInfo{
				Etag: validateReviewModerationReply.GetReviewModeration().GetAudit().GetEtag(),
			},
			ManifestId: req.GetManifestId(),
			IssuedAt:   req.GetManifest().GetIssuedAt(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: checkReturnRequestEligibilityReply.GetResult().GetReferenceIds(),
			Details:      calculatePointsLedgerReply.GetResult().GetDetails(),
			Status:       "ok",
		},
	}, nil
}

// CheckManifestEligibility checks a manifest.
func (s *ManifestValidationServer) CheckManifestEligibility(ctx context.Context, req *shippingv1.CheckManifestEligibilityRequest) (*shippingv1.CheckManifestEligibilityResponse, error) {
	if err := s.store.SaveManifest(ctx, req.GetContext().GetTenantId(), req.GetManifest()); err != nil {
		return nil, grpcx.Wrap(err, "save manifest")
	}
	s.log.InfoContext(ctx, "check manifest eligibility", "tenant", req.GetContext().GetTenantId(), "etag", req.GetManifest().GetAudit().GetEtag())
	normalizeReviewModerationRequest := &reviewsv1.NormalizeReviewModerationRequest{
		Context: req.GetContext(),
		ReviewModeration: &reviewsv1.ReviewModeration{
			Audit: &commonv1.AuditInfo{Etag: req.GetManifest().GetAudit().GetEtag()},
		},
	}
	normalizeReviewModerationReply, err := s.reviewModerationValidationLazy().NormalizeReviewModeration(ctx, normalizeReviewModerationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewModerationValidationService/NormalizeReviewModeration")
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	ctx = tenancy.With(ctx, "reason", req.GetReason())
	summarizeFixedAssetsRequest := &assetsv1.SummarizeFixedAssetsRequest{
		Context:     req.GetContext(),
		Annotations: annotations,
		Reason:      tenancy.Value(ctx, "reason"),
	}
	summarizeFixedAssetsReply, err := assetsv1.FixedAssetQueryServiceClient.SummarizeFixedAssets(s.fixedAssetQueryClient, ctx, summarizeFixedAssetsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.assets.v1.FixedAssetQueryService/SummarizeFixedAssets")
	}
	return &shippingv1.CheckManifestEligibilityResponse{
		Manifest: &shippingv1.Manifest{
			Counterparty: &customersv1.CustomerRef{
				DisplayName: normalizeReviewModerationReply.GetReviewModeration().GetCustomer().GetDisplayName(),
			},
			ManifestId: req.GetManifestId(),
			Number:     req.GetManifest().GetNumber(),
		},
		Outcome: &shippingv1.CheckManifestEligibilityResponse_Outcome{
			Status:  normalizeReviewModerationReply.GetOutcome().GetStatus(),
			Message: summarizeFixedAssetsReply.GetResult().GetMessage(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// NormalizeManifest normalizes a manifest.
func (s *ManifestValidationServer) NormalizeManifest(ctx context.Context, req *shippingv1.NormalizeManifestRequest) (*shippingv1.NormalizeManifestResponse, error) {
	if err := s.store.SaveManifest(ctx, req.GetContext().GetTenantId(), req.GetManifest()); err != nil {
		return nil, grpcx.Wrap(err, "save manifest")
	}
	s.log.InfoContext(ctx, "normalize manifest", "tenant", req.GetContext().GetTenantId(), "etag", req.GetManifest().GetAudit().GetEtag())
	exportShipmentsStream, err := s.shipmentQueryClient.ExportShipments(ctx, &shippingv1.ExportShipmentsRequest{
		Context: req.GetContext(),
		Filter: &shippingv1.ExportShipmentsRequest_Filter{
			Region:       req.GetManifest().GetBillingAddress().GetRegion(),
			CreatedAfter: req.GetManifest().GetAudit().GetUpdatedAt(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ShipmentQueryService/ExportShipments")
	}
	for {
		received, err := exportShipmentsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.shipping.v1.ShipmentQueryService/ExportShipments")
		}
		_ = received
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*shippingv1.ExplainCarrierRateCalculationResponse, len(req.GetManifest().GetLines()))
	for i, line := range req.GetManifest().GetLines() {
		group.Go(func() error {
			reply, err := s.carrierRateCalculationClient.ExplainCarrierRateCalculation(groupCtx, &shippingv1.ExplainCarrierRateCalculationRequest{
				Context: req.GetContext(),
				CarrierRate: &shippingv1.CarrierRate{
					Action:      &shippingv1.CarrierRate_Discount{Discount: &shippingv1.CarrierRateDiscountAction{Amount: line.GetAmount()}},
					Description: line.GetDescription(),
					Owner:       &identityv1.UserRef{Email: req.GetManifest().GetCounterparty().GetEmail()},
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
		return nil, grpcx.Wrap(err, "acme.shipping.v1.CarrierRateCalculationService/ExplainCarrierRateCalculation")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	compareCarrierRateOptionsRequest := newCompareCarrierRateOptionsRequest(withCompareCarrierRateOptionsContext(req.GetContext()), withCompareCarrierRateOptionsCarrierRate(&shippingv1.CarrierRate{
		Owner: &identityv1.UserRef{Email: req.GetDetails().GetContact().GetEmail()},
	}))
	compareCarrierRateOptionsReply, err := s.carrierRateCalculationClient.CompareCarrierRateOptions(ctx, compareCarrierRateOptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.CarrierRateCalculationService/CompareCarrierRateOptions")
	}
	return &shippingv1.NormalizeManifestResponse{
		Manifest: &shippingv1.Manifest{
			Total:      compareCarrierRateOptionsReply.GetBreakdown().GetTotal(),
			ManifestId: req.GetManifestId(),
			Status:     req.GetManifest().GetStatus(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ScoreManifest scores a manifest.
func (s *ManifestValidationServer) ScoreManifest(ctx context.Context, req *shippingv1.ScoreManifestRequest) (*shippingv1.ScoreManifestResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveManifest(ctx, req.GetContext().GetTenantId(), req.GetManifest()); err != nil {
		return nil, grpcx.Wrap(err, "save manifest")
	}
	s.log.InfoContext(ctx, "score manifest", "tenant", req.GetContext().GetTenantId(), "etag", req.GetManifest().GetAudit().GetEtag())
	var accountHolder string
	switch choice := req.GetManifest().GetSettlement().(type) {
	case *shippingv1.Manifest_Card:
		accountHolder = choice.Card.GetHolderName()
	case *shippingv1.Manifest_Bank:
		accountHolder = choice.Bank.GetAccountHolder()
	case *shippingv1.Manifest_VoucherCode:
		accountHolder = choice.VoucherCode
	}
	calculateLoyaltyMemberRequest := &loyaltyv1.CalculateLoyaltyMemberRequest{
		Context: req.GetContext(),
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			Funding: &loyaltyv1.LoyaltyMember_Bank{Bank: &loyaltyv1.LoyaltyMemberBankFunding{
				Bic:           req.GetManifest().GetBank().GetBic(),
				AccountHolder: accountHolder,
			}},
		},
		Details: &loyaltyv1.CalculateLoyaltyMemberRequest_Details{
			Destination: &commonv1.PostalAddress{
				CountryCode: req.GetDetails().GetDestination().GetCountryCode(),
			},
		},
	}
	var calculateLoyaltyMemberReply *loyaltyv1.CalculateLoyaltyMemberResponse
	if err := pipeline.Run(ctx, "acme.loyalty.v1.LoyaltyMemberCalculationService/CalculateLoyaltyMember", func(ctx context.Context) error {
		answer, err := s.loyaltyMemberCalculationClient.CalculateLoyaltyMember(ctx, calculateLoyaltyMemberRequest)
		calculateLoyaltyMemberReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberCalculationService/CalculateLoyaltyMember")
	}
	packed, err := anypb.New(req.GetManifest())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	normalizeTrackingEventRequest := &shippingv1.NormalizeTrackingEventRequest{
		Context: req.GetContext(),
		Details: &shippingv1.NormalizeTrackingEventRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email: calculateLoyaltyMemberReply.GetLoyaltyMember().GetContact().GetEmail(),
			},
		},
		TrackingEvent: &shippingv1.TrackingEvent{
			Subject: &shippingv1.ShipmentRef{Number: req.GetManifest().GetNumber()},
			Detail:  packed,
		},
	}
	normalizeTrackingEventReply, err := s.normalizeTrackingEventFn(ctx, normalizeTrackingEventRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.TrackingEventValidationService/NormalizeTrackingEvent")
	}
	return &shippingv1.ScoreManifestResponse{
		Manifest: &shippingv1.Manifest{
			Audit: &commonv1.AuditInfo{
				CreatedAt: calculateLoyaltyMemberReply.GetLoyaltyMember().GetAudit().GetCreatedAt(),
			},
			Total:      calculateLoyaltyMemberReply.GetBreakdown().GetTotal(),
			ManifestId: req.GetManifestId(),
			Status:     req.GetManifest().GetStatus(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: normalizeTrackingEventReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// LintManifests lints a manifest.
func (s *ManifestValidationServer) LintManifests(ctx context.Context, req *shippingv1.LintManifestsRequest) (*shippingv1.LintManifestsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveManifest(ctx, req.GetContext().GetTenantId(), req.GetManifest()); err != nil {
		return nil, grpcx.Wrap(err, "save manifest")
	}
	s.log.InfoContext(ctx, "lint manifests", "tenant", req.GetContext().GetTenantId(), "etag", req.GetManifest().GetAudit().GetEtag())
	checkReviewModerationEligibilityRequest := &reviewsv1.CheckReviewModerationEligibilityRequest{
		Context: req.GetContext(),
		ReviewModeration: &reviewsv1.ReviewModeration{
			Audit: &commonv1.AuditInfo{CreatedAt: req.GetManifest().GetAudit().GetCreatedAt()},
		},
	}
	checkReviewModerationEligibilityReply, err := grpcx.Call(ctx, s.reviewModerationValidationClient.CheckReviewModerationEligibility, checkReviewModerationEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewModerationValidationService/CheckReviewModerationEligibility")
	}
	return &shippingv1.LintManifestsResponse{
		Outcome: &shippingv1.LintManifestsResponse_Outcome{
			Status: checkReviewModerationEligibilityReply.GetOutcome().GetStatus(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: checkReviewModerationEligibilityReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Manifest: &shippingv1.Manifest{
			ManifestId: req.GetManifestId(),
			DueAt:      req.GetManifest().GetDueAt(),
		},
	}, nil
}

// GetManifestRules returns a manifest.
func (s *ManifestValidationServer) GetManifestRules(ctx context.Context, req *shippingv1.GetManifestRulesRequest) (*shippingv1.GetManifestRulesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "manifest", req.GetManifestId(), "get_manifest_rules"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get manifest rules", "tenant", req.GetContext().GetTenantId())
	return &shippingv1.GetManifestRulesResponse{
		Manifest: &shippingv1.Manifest{ManifestId: req.GetManifestId()},
		Result:   &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// createNotificationSender returns a function that sends one request to
// acme.notifications.v1.NotificationService/CreateNotification.
func (s *ManifestValidationServer) createNotificationSender(request *notificationsv1.CreateNotificationRequest) func(context.Context) (*notificationsv1.CreateNotificationResponse, error) {
	return func(ctx context.Context) (*notificationsv1.CreateNotificationResponse, error) {
		return s.notificationClient.CreateNotification(ctx, request)
	}
}

// reviewModerationValidationLazy dials acme.reviews.v1.ReviewModerationValidationService the first time a handler needs
// it.
func (s *ManifestValidationServer) reviewModerationValidationLazy() reviewsv1.ReviewModerationValidationServiceClient {
	s.reviewModerationValidationOnce.Do(func() {
		s.reviewModerationValidationCached = reviewsv1.NewReviewModerationValidationServiceClient(s.conn)
	})
	return s.reviewModerationValidationCached
}
