package shipping

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"

	auditv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audit/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	documentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/documents/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/anypb"
)

// ShipmentCalculationServer implements `acme.shipping.v1.ShipmentCalculationService`.
// Computes the figures of a shipment, such as totals, taxes, and estimates.
type ShipmentCalculationServer struct {
	shippingv1.UnimplementedShipmentCalculationServiceServer
	store                          *Store
	log                            *slog.Logger
	conn                           *grpc.ClientConn
	featureFlagQueryClient         featureflagsv1.FeatureFlagQueryServiceClient
	auditEventValidationClient     auditv1.AuditEventValidationServiceClient
	documentQueryClient            documentsv1.DocumentQueryServiceClient
	subscriptionPlanQueryOnce      sync.Once
	subscriptionPlanQueryCached    subscriptionsv1.SubscriptionPlanQueryServiceClient
	tierCalculationClient          loyaltyv1.TierCalculationServiceClient
	loyaltyMemberCalculationClient loyaltyv1.LoyaltyMemberCalculationServiceClient
	giftCardClient                 giftcardsv1.GiftCardServiceClient
	summarizeSellersFn             func(context.Context, *sellersv1.SummarizeSellersRequest, ...grpc.CallOption) (*sellersv1.SummarizeSellersResponse, error)
	subscriptionPlanQueryClient    subscriptionsv1.SubscriptionPlanQueryServiceClient
	clients                        *registry.Registry
	featureFlagValidationClient    featureflagsv1.FeatureFlagValidationServiceClient
	sellerQueryClient              sellersv1.SellerQueryServiceClient
	sellerCalculationClient        sellersv1.SellerCalculationServiceClient
	tenantNotificationClient       identityv1.TenantNotificationServiceClient
	featureFlagValidationPort      featureFlagValidationPort
	sessionQueryClient             identityv1.SessionQueryServiceClient
	productReviewValidationClient  reviewsv1.ProductReviewValidationServiceClient
}

// NewShipmentCalculationServer wires a ShipmentCalculationServer.
func NewShipmentCalculationServer(deps platform.Deps) *ShipmentCalculationServer {
	sellerQuery := sellersv1.NewSellerQueryServiceClient(deps.Conn)
	return &ShipmentCalculationServer{
		store:                          NewStore(deps.DB),
		log:                            deps.Logger.With("service", "acme.shipping.v1.ShipmentCalculationService"),
		conn:                           deps.Conn,
		featureFlagQueryClient:         featureflagsv1.NewFeatureFlagQueryServiceClient(deps.Conn),
		auditEventValidationClient:     auditv1.NewAuditEventValidationServiceClient(deps.Conn),
		documentQueryClient:            documentsv1.NewDocumentQueryServiceClient(deps.Conn),
		tierCalculationClient:          loyaltyv1.NewTierCalculationServiceClient(deps.Conn),
		loyaltyMemberCalculationClient: loyaltyv1.NewLoyaltyMemberCalculationServiceClient(deps.Conn),
		giftCardClient:                 giftcardsv1.NewGiftCardServiceClient(deps.Conn),
		summarizeSellersFn:             sellerQuery.SummarizeSellers,
		subscriptionPlanQueryClient:    subscriptionsv1.NewSubscriptionPlanQueryServiceClient(deps.Conn),
		clients:                        deps.Clients,
		featureFlagValidationClient:    featureflagsv1.NewFeatureFlagValidationServiceClient(deps.Conn),
		sellerQueryClient:              sellersv1.NewSellerQueryServiceClient(deps.Conn),
		sellerCalculationClient:        sellersv1.NewSellerCalculationServiceClient(deps.Conn),
		tenantNotificationClient:       identityv1.NewTenantNotificationServiceClient(deps.Conn),
		featureFlagValidationPort:      newFeatureFlagValidationPort(deps),
		sessionQueryClient:             identityv1.NewSessionQueryServiceClient(deps.Conn),
		productReviewValidationClient:  reviewsv1.NewProductReviewValidationServiceClient(deps.Conn),
	}
}

// CalculateShipment calculates a shipment.
func (s *ShipmentCalculationServer) CalculateShipment(ctx context.Context, req *shippingv1.CalculateShipmentRequest) (*shippingv1.CalculateShipmentResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveShipment(ctx, req.GetContext().GetTenantId(), req.GetShipment()); err != nil {
		return nil, grpcx.Wrap(err, "save shipment")
	}
	s.log.InfoContext(ctx, "calculate shipment", "tenant", req.GetContext().GetTenantId(), "etag", req.GetShipment().GetAudit().GetEtag())
	metadata2 := make(map[string]string, len(req.GetShipment().GetMetadata()))
	for key, value := range req.GetShipment().GetMetadata() {
		metadata2[key] = value
	}
	findSubscriptionPlansRequest := &subscriptionsv1.FindSubscriptionPlansRequest{
		Context: req.GetContext(),
		Filter: &subscriptionsv1.FindSubscriptionPlansRequest_Filter{
			Region:       req.GetShipment().GetBillingAddress().GetRegion(),
			CreatedAfter: req.GetShipment().GetDueAt(),
			OwnerId:      req.GetShipment().GetCounterparty().GetParcelId(),
			Labels:       metadata2,
		},
	}
	findSubscriptionPlansReply := new(subscriptionsv1.FindSubscriptionPlansResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", subscriptionsSubscriptionPlanQueryServiceName, "FindSubscriptionPlans"), findSubscriptionPlansRequest, findSubscriptionPlansReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionPlanQueryService/FindSubscriptionPlans")
	}
	exportFeatureFlagsStream, err := s.featureFlagQueryClient.ExportFeatureFlags(ctx, &featureflagsv1.ExportFeatureFlagsRequest{
		Context: req.GetContext(),
		Filter: &featureflagsv1.ExportFeatureFlagsRequest_Filter{
			Region:       req.GetShipment().GetBillingAddress().GetRegion(),
			CreatedAfter: req.GetShipment().GetAudit().GetCreatedAt(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.featureflags.v1.FeatureFlagQueryService/ExportFeatureFlags")
	}
	for {
		received, err := exportFeatureFlagsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.featureflags.v1.FeatureFlagQueryService/ExportFeatureFlags")
		}
		_ = received
	}
	shipmentCache := cache.New[string, *shippingv1.Shipment](1)
	shipmentCache.Put(req.GetContext().GetTraceId(), req.GetShipment())
	cachedShipment, _ := shipmentCache.Get(req.GetContext().GetTraceId())
	packed, err := anypb.New(req.GetShipment())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	step := &shipmentCalculationServerCalculateShipmentScoreAuditEventStep{request: &auditv1.ScoreAuditEventRequest{
		Context: req.GetContext(),
		Details: &auditv1.ScoreAuditEventRequest_Details{
			Destination: &commonv1.PostalAddress{
				Region:    req.GetShipment().GetBillingAddress().GetRegion(),
				Recipient: cachedShipment.GetBillingAddress().GetRecipient(),
				Lines:     req.GetShipment().GetBillingAddress().GetLines(),
			},
		},
		AuditEvent: &auditv1.AuditEvent{
			Subject: &auditv1.AuditTrailRef{
				DisplayName: req.GetShipment().GetCounterparty().GetDisplayName(),
			},
			Detail: packed,
		},
	}}
	if err := s.runCalculateShipmentScoreAuditEvent(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.audit.v1.AuditEventValidationService/ScoreAuditEvent")
	}
	scoreAuditEventReply := step.reply
	browseDocumentsRequest := buildBrowseDocumentsRequestForCalculateShipment(req)
	browseDocumentsReply, err := s.documentQueryClient.BrowseDocuments(ctx, browseDocumentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.documents.v1.DocumentQueryService/BrowseDocuments")
	}
	_ = browseDocumentsReply
	return &shippingv1.CalculateShipmentResponse{
		Result: &commonv1.OperationResult{
			Status:  findSubscriptionPlansReply.GetResult().GetStatus(),
			Details: findSubscriptionPlansReply.GetResult().GetDetails(),
		},
		Breakdown: &shippingv1.CalculateShipmentResponse_Breakdown{
			Total:        scoreAuditEventReply.GetBreakdown().GetTotal(),
			CurrencyCode: scoreAuditEventReply.GetBreakdown().GetCurrencyCode(),
		},
		Shipment: &shippingv1.Shipment{
			ShipmentId: req.GetShipmentId(),
			Status:     req.GetShipment().GetStatus(),
		},
	}, nil
}

// EstimateShipment estimates a shipment.
func (s *ShipmentCalculationServer) EstimateShipment(ctx context.Context, req *shippingv1.EstimateShipmentRequest) (*shippingv1.EstimateShipmentResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected EstimateShipment", "error", err)
		return nil, err
	}
	if err := s.store.SaveShipment(ctx, req.GetContext().GetTenantId(), req.GetShipment()); err != nil {
		return nil, grpcx.Wrap(err, "save shipment")
	}
	s.log.InfoContext(ctx, "estimate shipment", "tenant", req.GetContext().GetTenantId(), "etag", req.GetShipment().GetAudit().GetEtag())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	ctx = tenancy.With(ctx, "reason", req.GetReason())
	summarizeSubscriptionPlansRequest := &subscriptionsv1.SummarizeSubscriptionPlansRequest{
		Context:     req.GetContext(),
		Annotations: annotations,
		Reason:      tenancy.Value(ctx, "reason"),
	}
	summarizeSubscriptionPlansReply, err := s.subscriptionPlanQueryLazy().SummarizeSubscriptionPlans(ctx, summarizeSubscriptionPlansRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionPlanQueryService/SummarizeSubscriptionPlans")
	}
	ctx = tenancy.With(ctx, "updated_by", req.GetShipment().GetAudit().GetUpdatedBy())
	step := &shipmentCalculationServerEstimateShipmentCompareTierOptionsStep{request: &loyaltyv1.CompareTierOptionsRequest{
		Context: req.GetContext(),
		Details: &loyaltyv1.CompareTierOptionsRequest_Details{
			Destination: &commonv1.PostalAddress{
				PostalCode: req.GetShipment().GetBillingAddress().GetPostalCode(),
			},
		},
		Tier: &loyaltyv1.Tier{
			Owner: &identityv1.UserRef{
				DisplayName: req.GetShipment().GetCounterparty().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{UpdatedBy: tenancy.Value(ctx, "updated_by")},
		},
	}}
	if err := s.runEstimateShipmentCompareTierOptions(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.TierCalculationService/CompareTierOptions")
	}
	compareTierOptionsReply := step.reply
	return &shippingv1.EstimateShipmentResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: summarizeSubscriptionPlansReply.GetResult().GetReferenceIds(),
			Status:       summarizeSubscriptionPlansReply.GetResult().GetStatus(),
			Message:      compareTierOptionsReply.GetResult().GetMessage(),
			Details:      compareTierOptionsReply.GetResult().GetDetails(),
		},
		Shipment: &shippingv1.Shipment{
			ShipmentId:     req.GetShipmentId(),
			BillingAddress: req.GetShipment().GetBillingAddress(),
		},
	}, nil
}

// QuoteShipment quotes a shipment.
func (s *ShipmentCalculationServer) QuoteShipment(ctx context.Context, req *shippingv1.QuoteShipmentRequest) (*shippingv1.QuoteShipmentResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected QuoteShipment", "error", err)
		return nil, err
	}
	if err := s.store.SaveShipment(ctx, req.GetContext().GetTenantId(), req.GetShipment()); err != nil {
		return nil, grpcx.Wrap(err, "save shipment")
	}
	s.log.InfoContext(ctx, "quote shipment", "tenant", req.GetContext().GetTenantId(), "etag", req.GetShipment().GetAudit().GetEtag())
	calculateLoyaltyMemberRequest := &loyaltyv1.CalculateLoyaltyMemberRequest{
		Context: req.GetContext(),
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			Holder: &loyaltyv1.RewardRef{
				DisplayName: req.GetShipment().GetCounterparty().GetDisplayName(),
			},
			Audit:   &commonv1.AuditInfo{CreatedBy: req.GetShipment().GetAudit().GetCreatedBy()},
			Funding: &loyaltyv1.LoyaltyMember_Bank{Bank: &loyaltyv1.LoyaltyMemberBankFunding{Bic: req.GetShipment().GetBank().GetBic()}},
		},
		Details: &loyaltyv1.CalculateLoyaltyMemberRequest_Details{
			Destination: &commonv1.PostalAddress{Region: req.GetShipment().GetBillingAddress().GetRegion()},
		},
	}
	calculateLoyaltyMemberReply, err := loyaltyv1.LoyaltyMemberCalculationServiceClient.CalculateLoyaltyMember(s.loyaltyMemberCalculationClient, ctx, calculateLoyaltyMemberRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberCalculationService/CalculateLoyaltyMember")
	}
	cloneGiftCardRequest := &giftcardsv1.CloneGiftCardRequest{
		Context: req.GetContext(),
		GiftCard: &giftcardsv1.GiftCard{
			Funding: &giftcardsv1.GiftCard_Bank{Bank: &giftcardsv1.GiftCardBankFunding{
				Iban: calculateLoyaltyMemberReply.GetLoyaltyMember().GetBank().GetIban(),
			}},
			Contact: &commonv1.ContactInfo{
				Phone: calculateLoyaltyMemberReply.GetLoyaltyMember().GetContact().GetPhone(),
			},
			Holder: &giftcardsv1.GiftCardTransactionRef{
				DisplayName: calculateLoyaltyMemberReply.GetLoyaltyMember().GetHolder().GetDisplayName(),
			},
		},
	}
	cloneGiftCardReply, err := giftcardsv1.GiftCardServiceClient.CloneGiftCard(s.giftCardClient, ctx, cloneGiftCardRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardService/CloneGiftCard")
	}
	return &shippingv1.QuoteShipmentResponse{
		Breakdown: &shippingv1.QuoteShipmentResponse_Breakdown{
			CurrencyCode: calculateLoyaltyMemberReply.GetBreakdown().GetCurrencyCode(),
		},
		Shipment: &shippingv1.Shipment{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: calculateLoyaltyMemberReply.GetLoyaltyMember().GetAudit().GetUpdatedBy(),
				Etag:      cloneGiftCardReply.GetGiftCard().GetAudit().GetEtag(),
			},
			ShipmentId: req.GetShipmentId(),
			Status:     req.GetShipment().GetStatus(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CompareShipmentOptions compares a shipment.
func (s *ShipmentCalculationServer) CompareShipmentOptions(ctx context.Context, req *shippingv1.CompareShipmentOptionsRequest) (*shippingv1.CompareShipmentOptionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CompareShipmentOptions", "error", err)
		return nil, err
	}
	if err := s.store.SaveShipment(ctx, req.GetContext().GetTenantId(), req.GetShipment()); err != nil {
		return nil, grpcx.Wrap(err, "save shipment")
	}
	s.log.InfoContext(ctx, "compare shipment options", "tenant", req.GetContext().GetTenantId(), "etag", req.GetShipment().GetAudit().GetEtag())
	metadata2 := make(map[string]string, len(req.GetShipment().GetMetadata()))
	for key, value := range req.GetShipment().GetMetadata() {
		metadata2[key] = value
	}
	summarizeSellersRequest := &sellersv1.SummarizeSellersRequest{
		Context:     req.GetContext(),
		Reason:      req.GetReason(),
		Annotations: req.GetAnnotations(),
		Details:     &sellersv1.SummarizeSellersRequest_Details{Filters: metadata2},
	}
	summarizeSellersReply, err := s.summarizeSellersFn(ctx, summarizeSellersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerQueryService/SummarizeSellers")
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*subscriptionsv1.BrowseSubscriptionPlansResponse, len(req.GetShipment().GetLines()))
	for i, line := range req.GetShipment().GetLines() {
		group.Go(func() error {
			reply, err := s.subscriptionPlanQueryClient.BrowseSubscriptionPlans(groupCtx, &subscriptionsv1.BrowseSubscriptionPlansRequest{
				Context: req.GetContext(),
				Filter: &subscriptionsv1.BrowseSubscriptionPlansRequest_Filter{
					Labels:  line.GetAttributes(),
					Region:  req.GetShipment().GetBillingAddress().GetRegion(),
					OwnerId: line.GetLineId(),
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
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionPlanQueryService/BrowseSubscriptionPlans")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	exportDocumentsStream, err := s.documentQueryClient.ExportDocuments(ctx, &documentsv1.ExportDocumentsRequest{
		Context: req.GetContext(),
		Filter:  &documentsv1.ExportDocumentsRequest_Filter{Region: req.GetShipment().GetBillingAddress().GetRegion()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.documents.v1.DocumentQueryService/ExportDocuments")
	}
	for {
		received, err := exportDocumentsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.documents.v1.DocumentQueryService/ExportDocuments")
		}
		_ = received
	}
	giftCardTransactionValidation, ok := s.clients.Get("acme.giftcards.v1.GiftCardTransactionValidationService").(giftcardsv1.GiftCardTransactionValidationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.giftcards.v1.GiftCardTransactionValidationService")
	}
	packed, err := anypb.New(req.GetShipment())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	previewGiftCardTransactionRequest := &giftcardsv1.PreviewGiftCardTransactionRequest{
		Context: req.GetContext(),
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{
			Subject: &giftcardsv1.GiftCardBatchRef{Number: req.GetShipment().GetNumber()},
			Detail:  packed,
		},
	}
	previewGiftCardTransactionReply, err := giftCardTransactionValidation.PreviewGiftCardTransaction(ctx, previewGiftCardTransactionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionValidationService/PreviewGiftCardTransaction")
	}
	return &shippingv1.CompareShipmentOptionsResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: summarizeSellersReply.GetResult().GetCorrelationId(),
			Message:       summarizeSellersReply.GetResult().GetMessage(),
			Status:        previewGiftCardTransactionReply.GetOutcome().GetStatus(),
		},
		Shipment:  &shippingv1.Shipment{ShipmentId: req.GetShipmentId()},
		Breakdown: &shippingv1.CompareShipmentOptionsResponse_Breakdown{CurrencyCode: req.GetDetails().GetCurrencyCode()},
	}, nil
}

// RecalculateShipments recalculates a shipment.
func (s *ShipmentCalculationServer) RecalculateShipments(ctx context.Context, req *shippingv1.RecalculateShipmentsRequest) (*shippingv1.RecalculateShipmentsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveShipment(ctx, req.GetContext().GetTenantId(), req.GetShipment()); err != nil {
		return nil, grpcx.Wrap(err, "save shipment")
	}
	s.log.InfoContext(ctx, "recalculate shipments", "tenant", req.GetContext().GetTenantId(), "etag", req.GetShipment().GetAudit().GetEtag())
	step := &shipmentCalculationServerRecalculateShipmentsCheckFeatureFlagEligibilityStep{request: &featureflagsv1.CheckFeatureFlagEligibilityRequest{
		Context: req.GetContext(),
		FeatureFlag: &featureflagsv1.FeatureFlag{
			Audit: &commonv1.AuditInfo{CreatedAt: req.GetShipment().GetAudit().GetCreatedAt()},
		},
	}}
	if err := s.runRecalculateShipmentsCheckFeatureFlagEligibility(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.featureflags.v1.FeatureFlagValidationService/CheckFeatureFlagEligibility")
	}
	checkFeatureFlagEligibilityReply := step.reply
	suggestSellersRequest := newSuggestSellersRequest(withSuggestSellersContext(req.GetContext()), withSuggestSellersFilter(&sellersv1.SuggestSellersRequest_Filter{
		Region:       req.GetShipment().GetBillingAddress().GetRegion(),
		CreatedAfter: checkFeatureFlagEligibilityReply.GetFeatureFlag().GetAudit().GetUpdatedAt(),
	}))
	suggestSellersReply, err := s.sellerQueryClient.SuggestSellers(ctx, suggestSellersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerQueryService/SuggestSellers")
	}
	step2 := &shipmentCalculationServerRecalculateShipmentsQuoteSellerStep{request: &sellersv1.QuoteSellerRequest{
		Context: req.GetContext(),
		Details: &sellersv1.QuoteSellerRequest_Details{
			Destination: &commonv1.PostalAddress{Locality: req.GetDetails().GetDestination().GetLocality()},
		},
		Seller: &sellersv1.Seller{
			PrimaryAddress: &commonv1.PostalAddress{Lines: req.GetDetails().GetDestination().GetLines()},
			RelationshipManager: &identityv1.UserRef{
				DisplayName: req.GetShipment().GetCounterparty().GetDisplayName(),
			},
		},
	}}
	if err := s.runRecalculateShipmentsQuoteSeller(ctx, step2); err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerCalculationService/QuoteSeller")
	}
	quoteSellerReply := step2.reply
	return &shippingv1.RecalculateShipmentsResponse{
		Shipment: &shippingv1.Shipment{
			Counterparty: &shippingv1.ParcelRef{
				DisplayName: checkFeatureFlagEligibilityReply.GetFeatureFlag().GetOwner().GetDisplayName(),
			},
			ShipmentId:     req.GetShipmentId(),
			BillingAddress: req.GetShipment().GetBillingAddress(),
		},
		Result: &commonv1.OperationResult{
			Message:      checkFeatureFlagEligibilityReply.GetResult().GetMessage(),
			ReferenceIds: suggestSellersReply.GetResult().GetReferenceIds(),
			Status:       quoteSellerReply.GetResult().GetStatus(),
		},
	}, nil
}

// SimulateShipment simulates a shipment.
func (s *ShipmentCalculationServer) SimulateShipment(ctx context.Context, req *shippingv1.SimulateShipmentRequest) (*shippingv1.SimulateShipmentResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveShipment(ctx, req.GetContext().GetTenantId(), req.GetShipment()); err != nil {
		return nil, grpcx.Wrap(err, "save shipment")
	}
	s.log.InfoContext(ctx, "simulate shipment", "tenant", req.GetContext().GetTenantId(), "etag", req.GetShipment().GetAudit().GetEtag())
	getTenantPreferencesRequest := &identityv1.GetTenantPreferencesRequest{}
	getTenantPreferencesRequest.Context = req.GetContext()
	getTenantPreferencesRequest.TenantId = req.GetContext().GetTenantId()
	getTenantPreferencesReply, err := s.tenantNotificationClient.GetTenantPreferences(ctx, getTenantPreferencesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantNotificationService/GetTenantPreferences")
	}
	browseDocumentsReply, err := s.documentQueryClient.BrowseDocuments(ctx, &documentsv1.BrowseDocumentsRequest{
		Context: req.GetContext(),
		Filter: &documentsv1.BrowseDocumentsRequest_Filter{
			Region:       req.GetShipment().GetBillingAddress().GetRegion(),
			CreatedAfter: getTenantPreferencesReply.GetTenant().GetAudit().GetCreatedAt(),
			OwnerId:      req.GetShipment().GetShipmentId(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.documents.v1.DocumentQueryService/BrowseDocuments")
	}
	encoded, err := protojson.Marshal(req.GetShipment())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded shippingv1.Shipment
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	explainFeatureFlagDecisionRequest := &featureflagsv1.ExplainFeatureFlagDecisionRequest{
		Context: req.GetContext(),
		FeatureFlag: &featureflagsv1.FeatureFlag{
			Audit: &commonv1.AuditInfo{
				CreatedBy: req.GetShipment().GetAudit().GetCreatedBy(),
				Etag:      (&decoded).GetAudit().GetEtag(),
			},
			Owner: &identityv1.UserRef{
				UserId: getTenantPreferencesReply.GetTenant().GetManager().GetUserId(),
			},
		},
	}
	explainFeatureFlagDecisionReply, err := s.featureFlagValidationPort.ExplainFeatureFlagDecision(ctx, explainFeatureFlagDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.featureflags.v1.FeatureFlagValidationService/ExplainFeatureFlagDecision")
	}
	for _, line := range req.GetShipment().GetLines() {
		if _, err := s.sessionQueryClient.SummarizeSessions(ctx, &identityv1.SummarizeSessionsRequest{
			Context:     req.GetContext(),
			Annotations: line.GetAttributes(),
			Reason:      req.GetReason(),
			SessionId:   line.GetLineId(),
			Details:     &identityv1.SummarizeSessionsRequest_Details{Filters: browseDocumentsReply.GetResult().GetDetails()},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.identity.v1.SessionQueryService/SummarizeSessions")
		}
	}
	return &shippingv1.SimulateShipmentResponse{
		Shipment: &shippingv1.Shipment{
			BillingAddress: &commonv1.PostalAddress{
				CountryCode: getTenantPreferencesReply.GetTenant().GetLocation().GetCountryCode(),
			},
			Lines: []*shippingv1.ShipmentLine{&shippingv1.ShipmentLine{
				Amount: explainFeatureFlagDecisionReply.GetFeatureFlag().GetDiscount().GetAmount(),
			}},
			Counterparty: &shippingv1.ParcelRef{
				DisplayName: explainFeatureFlagDecisionReply.GetFeatureFlag().GetOwner().GetDisplayName(),
			},
			ShipmentId: req.GetShipmentId(),
			Metadata:   req.GetShipment().GetMetadata(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: browseDocumentsReply.GetResult().GetReferenceIds(),
			Message:      browseDocumentsReply.GetResult().GetMessage(),
			Status:       "ok",
		},
	}, nil
}

// ExplainShipmentCalculation explains a shipment.
func (s *ShipmentCalculationServer) ExplainShipmentCalculation(ctx context.Context, req *shippingv1.ExplainShipmentCalculationRequest) (*shippingv1.ExplainShipmentCalculationResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ExplainShipmentCalculation", "error", err)
		return nil, err
	}
	if err := s.store.SaveShipment(ctx, req.GetContext().GetTenantId(), req.GetShipment()); err != nil {
		return nil, grpcx.Wrap(err, "save shipment")
	}
	s.log.InfoContext(ctx, "explain shipment calculation", "tenant", req.GetContext().GetTenantId(), "etag", req.GetShipment().GetAudit().GetEtag())
	for attachment := range iterx.Each(req.GetShipment().GetAttachments(), func(candidate *shippingv1.ShipmentAttachment) bool { return candidate != nil }) {
		if _, err := s.productReviewValidationClient.ValidateProductReview(ctx, &reviewsv1.ValidateProductReviewRequest{
			Context: req.GetContext(),
			ProductReview: &reviewsv1.ProductReview{
				Attachments: []*reviewsv1.ProductReviewAttachment{&reviewsv1.ProductReviewAttachment{
					Checksum:    attachment.GetChecksum(),
					SizeBytes:   attachment.GetSizeBytes(),
					FileName:    attachment.GetFileName(),
					ContentType: attachment.GetContentType(),
				}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.reviews.v1.ProductReviewValidationService/ValidateProductReview")
		}
	}
	previewFeatureFlagReply, err := s.featureFlagValidationClient.PreviewFeatureFlag(ctx, &featureflagsv1.PreviewFeatureFlagRequest{
		Context: req.GetContext(),
		FeatureFlag: &featureflagsv1.FeatureFlag{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: req.GetShipment().GetAudit().GetUpdatedBy(),
				CreatedBy: req.GetShipment().GetAudit().GetCreatedBy(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.featureflags.v1.FeatureFlagValidationService/PreviewFeatureFlag")
	}
	return &shippingv1.ExplainShipmentCalculationResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: previewFeatureFlagReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Shipment: &shippingv1.Shipment{
			Audit: &commonv1.AuditInfo{
				CreatedBy: previewFeatureFlagReply.GetFeatureFlag().GetAudit().GetCreatedBy(),
			},
			ShipmentId: req.GetShipmentId(),
			Metadata:   req.GetShipment().GetMetadata(),
		},
	}, nil
}

// GetShipmentRates returns a shipment.
func (s *ShipmentCalculationServer) GetShipmentRates(ctx context.Context, req *shippingv1.GetShipmentRatesRequest) (*shippingv1.GetShipmentRatesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "shipment", req.GetShipmentId(), "get_shipment_rates"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get shipment rates", "tenant", req.GetContext().GetTenantId())
	batchGetSellersRequest := &sellersv1.BatchGetSellersRequest{Context: req.GetContext()}
	batchGetSellersReply, err := grpcx.NewClient(s.conn, sellersv1.NewSellerServiceClient).BatchGetSellers(ctx, batchGetSellersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerService/BatchGetSellers")
	}
	suggestSubscriptionPlansRequest := &subscriptionsv1.SuggestSubscriptionPlansRequest{
		Context: req.GetContext(),
		Filter: &subscriptionsv1.SuggestSubscriptionPlansRequest_Filter{
			OwnerId: batchGetSellersReply.GetResult().GetCorrelationId(),
			Labels:  batchGetSellersReply.GetResult().GetDetails(),
		},
	}
	suggestSubscriptionPlansReply := new(subscriptionsv1.SuggestSubscriptionPlansResponse)
	if err := s.conn.Invoke(ctx, subscriptionsSubscriptionPlanQueryServiceSuggestSubscriptionPlansMethod, suggestSubscriptionPlansRequest, suggestSubscriptionPlansReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionPlanQueryService/SuggestSubscriptionPlans")
	}
	return &shippingv1.GetShipmentRatesResponse{
		Result: &commonv1.OperationResult{
			Status:       batchGetSellersReply.GetResult().GetStatus(),
			Message:      batchGetSellersReply.GetResult().GetMessage(),
			Details:      suggestSubscriptionPlansReply.GetResult().GetDetails(),
			ReferenceIds: suggestSubscriptionPlansReply.GetResult().GetReferenceIds(),
		},
		Shipment: &shippingv1.Shipment{ShipmentId: req.GetShipmentId()},
	}, nil
}

// shipmentCalculationServerCalculateShipmentScoreAuditEventStep carries the request of one step of CalculateShipment
// and, once it ran, its reply.
type shipmentCalculationServerCalculateShipmentScoreAuditEventStep struct {
	request *auditv1.ScoreAuditEventRequest
	reply   *auditv1.ScoreAuditEventResponse
}

func (s *ShipmentCalculationServer) runCalculateShipmentScoreAuditEvent(ctx context.Context, step *shipmentCalculationServerCalculateShipmentScoreAuditEventStep) error {
	reply, err := s.auditEventValidationClient.ScoreAuditEvent(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}

// subscriptionPlanQueryLazy dials acme.subscriptions.v1.SubscriptionPlanQueryService the first time a handler needs it.
func (s *ShipmentCalculationServer) subscriptionPlanQueryLazy() subscriptionsv1.SubscriptionPlanQueryServiceClient {
	s.subscriptionPlanQueryOnce.Do(func() { s.subscriptionPlanQueryCached = subscriptionsv1.NewSubscriptionPlanQueryServiceClient(s.conn) })
	return s.subscriptionPlanQueryCached
}

// shipmentCalculationServerEstimateShipmentCompareTierOptionsStep carries the request of one step of EstimateShipment
// and, once it ran, its reply.
type shipmentCalculationServerEstimateShipmentCompareTierOptionsStep struct {
	request *loyaltyv1.CompareTierOptionsRequest
	reply   *loyaltyv1.CompareTierOptionsResponse
}

func (s *ShipmentCalculationServer) runEstimateShipmentCompareTierOptions(ctx context.Context, step *shipmentCalculationServerEstimateShipmentCompareTierOptionsStep) error {
	reply, err := s.tierCalculationClient.CompareTierOptions(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}

// shipmentCalculationServerRecalculateShipmentsCheckFeatureFlagEligibilityStep carries the request of one step of
// RecalculateShipments and, once it ran, its reply.
type shipmentCalculationServerRecalculateShipmentsCheckFeatureFlagEligibilityStep struct {
	request *featureflagsv1.CheckFeatureFlagEligibilityRequest
	reply   *featureflagsv1.CheckFeatureFlagEligibilityResponse
}

func (s *ShipmentCalculationServer) runRecalculateShipmentsCheckFeatureFlagEligibility(ctx context.Context, step *shipmentCalculationServerRecalculateShipmentsCheckFeatureFlagEligibilityStep) error {
	reply, err := s.featureFlagValidationClient.CheckFeatureFlagEligibility(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}

// shipmentCalculationServerRecalculateShipmentsQuoteSellerStep carries the request of one step of RecalculateShipments
// and, once it ran, its reply.
type shipmentCalculationServerRecalculateShipmentsQuoteSellerStep struct {
	request *sellersv1.QuoteSellerRequest
	reply   *sellersv1.QuoteSellerResponse
}

func (s *ShipmentCalculationServer) runRecalculateShipmentsQuoteSeller(ctx context.Context, step *shipmentCalculationServerRecalculateShipmentsQuoteSellerStep) error {
	reply, err := s.sellerCalculationClient.QuoteSeller(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
