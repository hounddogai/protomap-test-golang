package shipping

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// CarrierRateApprovalServer implements `acme.shipping.v1.CarrierRateApprovalService`.
// Routes carrier rates through approval, with escalation and delegation.
type CarrierRateApprovalServer struct {
	shippingv1.UnimplementedCarrierRateApprovalServiceServer
	store                             *Store
	log                               *slog.Logger
	tierReportingClient               loyaltyv1.TierReportingServiceClient
	clients                           *registry.Registry
	manifestCalculationClient         shippingv1.ManifestCalculationServiceClient
	conn                              *grpc.ClientConn
	giftCardTransactionApprovalClient giftcardsv1.GiftCardTransactionApprovalServiceClient
	merchandisingRuleApprovalClient   merchandisingv1.MerchandisingRuleApprovalServiceClient
	shipmentQueryClient               shippingv1.ShipmentQueryServiceClient
	meteredManifestReporting          meteredManifestReporting
	manifestSyncClient                shippingv1.ManifestSyncServiceClient
	carrierRateLifecycleOnce          sync.Once
	carrierRateLifecycleCached        shippingv1.CarrierRateLifecycleServiceClient
	userQueryClient                   identityv1.UserQueryServiceClient
}

// NewCarrierRateApprovalServer wires a CarrierRateApprovalServer.
func NewCarrierRateApprovalServer(deps platform.Deps) *CarrierRateApprovalServer {
	return &CarrierRateApprovalServer{
		store:                             NewStore(deps.DB),
		log:                               deps.Logger.With("service", "acme.shipping.v1.CarrierRateApprovalService"),
		tierReportingClient:               loyaltyv1.NewTierReportingServiceClient(deps.Conn),
		clients:                           deps.Clients,
		manifestCalculationClient:         shippingv1.NewManifestCalculationServiceClient(deps.Conn),
		conn:                              deps.Conn,
		giftCardTransactionApprovalClient: giftcardsv1.NewGiftCardTransactionApprovalServiceClient(deps.Conn),
		merchandisingRuleApprovalClient:   merchandisingv1.NewMerchandisingRuleApprovalServiceClient(deps.Conn),
		shipmentQueryClient:               shippingv1.NewShipmentQueryServiceClient(deps.Conn),
		meteredManifestReporting:          meteredManifestReporting{ManifestReportingServiceClient: shippingv1.NewManifestReportingServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		manifestSyncClient:                shippingv1.NewManifestSyncServiceClient(deps.Conn),
		userQueryClient:                   identityv1.NewUserQueryServiceClient(deps.Conn),
	}
}

// SubmitCarrierRate submits a carrier rate.
func (s *CarrierRateApprovalServer) SubmitCarrierRate(ctx context.Context, req *shippingv1.SubmitCarrierRateRequest) (*shippingv1.SubmitCarrierRateResponse, error) {
	if err := s.store.SaveCarrierRate(ctx, req.GetContext().GetTenantId(), req.GetCarrierRate()); err != nil {
		return nil, grpcx.Wrap(err, "save carrier rate")
	}
	s.log.InfoContext(ctx, "submit carrier rate", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCarrierRate().GetAudit().GetEtag())
	streamTierMetricsStream, err := s.tierReportingClient.StreamTierMetrics(ctx, &loyaltyv1.StreamTierMetricsRequest{
		Context:    req.GetContext(),
		Subscriber: &commonv1.ContactInfo{Email: req.GetCarrierRate().GetOwner().GetEmail()},
		Filter: &loyaltyv1.StreamTierMetricsRequest_Filter{
			CreatedAfter: req.GetCarrierRate().GetAudit().GetCreatedAt(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.TierReportingService/StreamTierMetrics")
	}
	for {
		received, err := streamTierMetricsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.loyalty.v1.TierReportingService/StreamTierMetrics")
		}
		_ = received
	}
	sellerScorecardReporting, err := registry.Lookup[sellersv1.SellerScorecardReportingServiceClient](s.clients, "acme.sellers.v1.SellerScorecardReportingService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	generateSellerScorecardReportRequest := &sellersv1.GenerateSellerScorecardReportRequest{
		Context:     req.GetContext(),
		Reason:      req.GetReason(),
		Annotations: annotations,
		Details:     &sellersv1.GenerateSellerScorecardReportRequest_Details{Period: req.GetCarrierRate().GetEffective()},
	}
	generateSellerScorecardReportReply, err := sellerScorecardReporting.GenerateSellerScorecardReport(ctx, generateSellerScorecardReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardReportingService/GenerateSellerScorecardReport")
	}
	estimateManifestRequest := buildEstimateManifestRequestForSubmitCarrierRate(req)
	estimateManifestReply, err := s.manifestCalculationClient.EstimateManifest(ctx, estimateManifestRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ManifestCalculationService/EstimateManifest")
	}
	approveShipmentRequest := &shippingv1.ApproveShipmentRequest{
		Context: req.GetContext(),
		Details: &shippingv1.ApproveShipmentRequest_Details{
			Approver: &identityv1.UserRef{
				DisplayName: req.GetCarrierRate().GetOwner().GetDisplayName(),
			},
		},
		Shipment: &shippingv1.Shipment{
			BillingAddress: &commonv1.PostalAddress{
				Recipient: estimateManifestReply.GetManifest().GetBillingAddress().GetRecipient(),
			},
		},
	}
	approveShipmentReply, err := grpcx.NewClient(s.conn, shippingv1.NewShipmentApprovalServiceClient).ApproveShipment(ctx, approveShipmentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ShipmentApprovalService/ApproveShipment")
	}
	return &shippingv1.SubmitCarrierRateResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: generateSellerScorecardReportReply.GetResult().GetCorrelationId(),
			Message:       estimateManifestReply.GetResult().GetMessage(),
			ReferenceIds:  approveShipmentReply.GetResult().GetReferenceIds(),
			Status:        "ok",
		},
		CarrierRate: &shippingv1.CarrierRate{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: estimateManifestReply.GetManifest().GetAudit().GetUpdatedAt(),
			},
			CarrierRateId: req.GetCarrierRateId(),
			Effective:     req.GetCarrierRate().GetEffective(),
		},
	}, nil
}

// ApproveCarrierRate approves a carrier rate.
func (s *CarrierRateApprovalServer) ApproveCarrierRate(ctx context.Context, req *shippingv1.ApproveCarrierRateRequest) (*shippingv1.ApproveCarrierRateResponse, error) {
	if err := s.store.SaveCarrierRate(ctx, req.GetContext().GetTenantId(), req.GetCarrierRate()); err != nil {
		return nil, grpcx.Wrap(err, "save carrier rate")
	}
	s.log.InfoContext(ctx, "approve carrier rate", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCarrierRate().GetAudit().GetEtag())
	delegateGiftCardTransactionApprovalRequest := &giftcardsv1.DelegateGiftCardTransactionApprovalRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.DelegateGiftCardTransactionApprovalRequest_Details{
			Approver: &identityv1.UserRef{
				UserId: req.GetDetails().GetApprover().GetUserId(),
				Email:  req.GetDetails().GetApprover().GetEmail(),
			},
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{
			Subject: &giftcardsv1.GiftCardBatchRef{
				DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
			},
		},
	}
	delegateGiftCardTransactionApprovalReply, err := grpcx.Call(ctx, s.giftCardTransactionApprovalClient.DelegateGiftCardTransactionApproval, delegateGiftCardTransactionApprovalRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionApprovalService/DelegateGiftCardTransactionApproval")
	}
	escalateMerchandisingRuleRequest := buildEscalateMerchandisingRuleRequestForApproveCarrierRate(req)
	escalateMerchandisingRuleReply, err := s.merchandisingRuleApprovalClient.EscalateMerchandisingRule(ctx, escalateMerchandisingRuleRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.MerchandisingRuleApprovalService/EscalateMerchandisingRule")
	}
	exportShipmentsStream, err := s.shipmentQueryClient.ExportShipments(ctx, &shippingv1.ExportShipmentsRequest{
		Context: req.GetContext(),
		Filter: &shippingv1.ExportShipmentsRequest_Filter{
			CreatedAfter: escalateMerchandisingRuleReply.GetMerchandisingRule().GetAudit().GetUpdatedAt(),
			OwnerId:      delegateGiftCardTransactionApprovalReply.GetGiftCardTransaction().GetSubject().GetGiftCardBatchId(),
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
	generateManifestReportRequest := &shippingv1.GenerateManifestReportRequest{
		Context: req.GetContext(),
		Reason:  req.GetReason(),
	}
	generateManifestReportReply, err := s.meteredManifestReporting.GenerateManifestReport(ctx, generateManifestReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ManifestReportingService/GenerateManifestReport")
	}
	return &shippingv1.ApproveCarrierRateResponse{
		CarrierRate: &shippingv1.CarrierRate{
			Owner: &identityv1.UserRef{
				DisplayName: delegateGiftCardTransactionApprovalReply.GetGiftCardTransaction().GetSubject().GetDisplayName(),
			},
			Name:          escalateMerchandisingRuleReply.GetMerchandisingRule().GetName(),
			CarrierRateId: req.GetCarrierRateId(),
			Parameters:    req.GetCarrierRate().GetParameters(),
		},
		Result: &commonv1.OperationResult{
			Details: generateManifestReportReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}

// RejectCarrierRate rejects a carrier rate.
func (s *CarrierRateApprovalServer) RejectCarrierRate(ctx context.Context, req *shippingv1.RejectCarrierRateRequest) (*shippingv1.RejectCarrierRateResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveCarrierRate(ctx, req.GetContext().GetTenantId(), req.GetCarrierRate()); err != nil {
		return nil, grpcx.Wrap(err, "save carrier rate")
	}
	s.log.InfoContext(ctx, "reject carrier rate", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCarrierRate().GetAudit().GetEtag())
	snapshot, err := protojson.Marshal(req.GetCarrierRate())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	syncManifestsRequest := &shippingv1.SyncManifestsRequest{
		Context: req.GetContext(),
		Manifest: &shippingv1.Manifest{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: req.GetCarrierRate().GetAudit().GetUpdatedAt(),
				UpdatedBy: req.GetCarrierRate().GetAudit().GetUpdatedBy(),
				Etag:      req.GetCarrierRate().GetAudit().GetEtag(),
			},
		},
		Details: &shippingv1.SyncManifestsRequest_Details{Payload: snapshot},
	}
	syncManifestsReply, err := grpcx.Call(ctx, s.manifestSyncClient.SyncManifests, syncManifestsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ManifestSyncService/SyncManifests")
	}
	return &shippingv1.RejectCarrierRateResponse{
		CarrierRate: &shippingv1.CarrierRate{
			Audit: &commonv1.AuditInfo{
				CreatedAt: syncManifestsReply.GetManifest().GetAudit().GetCreatedAt(),
				UpdatedBy: syncManifestsReply.GetManifest().GetAudit().GetUpdatedBy(),
			},
			CarrierRateId: req.GetCarrierRateId(),
			Parameters:    req.GetCarrierRate().GetParameters(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// EscalateCarrierRate escalates a carrier rate.
func (s *CarrierRateApprovalServer) EscalateCarrierRate(ctx context.Context, req *shippingv1.EscalateCarrierRateRequest) (*shippingv1.EscalateCarrierRateResponse, error) {
	if err := s.store.SaveCarrierRate(ctx, req.GetContext().GetTenantId(), req.GetCarrierRate()); err != nil {
		return nil, grpcx.Wrap(err, "save carrier rate")
	}
	s.log.InfoContext(ctx, "escalate carrier rate", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCarrierRate().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetCarrierRate()).(*shippingv1.CarrierRate)
	packed, err := anypb.New(req.GetCarrierRate())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	notifySearchIndexWatchersRequest := &searchv1.NotifySearchIndexWatchersRequest{
		Context: req.GetContext(),
		Details: &searchv1.NotifySearchIndexWatchersRequest_Details{
			Recipient: &commonv1.ContactInfo{Email: cloned.GetOwner().GetEmail()},
			Payload:   packed,
		},
	}
	notifySearchIndexWatchersReply := new(searchv1.NotifySearchIndexWatchersResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", searchSearchIndexNotificationServiceName, "NotifySearchIndexWatchers"), notifySearchIndexWatchersRequest, notifySearchIndexWatchersReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexNotificationService/NotifySearchIndexWatchers")
	}
	consentRecordCalculation, err := registry.Lookup[customersv1.ConsentRecordCalculationServiceClient](s.clients, "acme.customers.v1.ConsentRecordCalculationService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	source := req.GetDetails().GetApprover().ProtoReflect()
	displayName := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("display_name"))).String()
	calculateConsentRecordRequest := &customersv1.CalculateConsentRecordRequest{
		Context: req.GetContext(),
		Details: &customersv1.CalculateConsentRecordRequest_Details{
			Destination: &commonv1.PostalAddress{
				Lines:    notifySearchIndexWatchersReply.GetSearchIndex().GetAddress().GetLines(),
				Locality: notifySearchIndexWatchersReply.GetSearchIndex().GetAddress().GetLocality(),
			},
		},
		ConsentRecord: &customersv1.ConsentRecord{
			Subject: &customersv1.HouseholdRef{DisplayName: displayName},
		},
	}
	calculateConsentRecordReply, err := consentRecordCalculation.CalculateConsentRecord(ctx, calculateConsentRecordRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordCalculationService/CalculateConsentRecord")
	}
	return &shippingv1.EscalateCarrierRateResponse{
		CarrierRate: &shippingv1.CarrierRate{
			Owner:         notifySearchIndexWatchersReply.GetSearchIndex().GetOwner(),
			CarrierRateId: req.GetCarrierRateId(),
			Version:       req.GetCarrierRate().GetVersion(),
		},
		Result: &commonv1.OperationResult{
			Details:       notifySearchIndexWatchersReply.GetResult().GetDetails(),
			CorrelationId: calculateConsentRecordReply.GetConsentRecord().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// DelegateCarrierRateApproval delegates a carrier rate.
func (s *CarrierRateApprovalServer) DelegateCarrierRateApproval(ctx context.Context, req *shippingv1.DelegateCarrierRateApprovalRequest) (*shippingv1.DelegateCarrierRateApprovalResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveCarrierRate(ctx, req.GetContext().GetTenantId(), req.GetCarrierRate()); err != nil {
		return nil, grpcx.Wrap(err, "save carrier rate")
	}
	s.log.InfoContext(ctx, "delegate carrier rate approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCarrierRate().GetAudit().GetEtag())
	explainHouseholdDecisionRequest := &customersv1.ExplainHouseholdDecisionRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Manager: &identityv1.UserRef{
						DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
						UserId:      req.GetDetails().GetApprover().GetUserId(),
					},
					Children: []*customersv1.Household{&customersv1.Household{Name: req.GetCarrierRate().GetName()}},
				}},
			}},
		},
	}
	explainHouseholdDecisionReply, err := grpcx.NewClient(s.conn, customersv1.NewHouseholdValidationServiceClient).ExplainHouseholdDecision(ctx, explainHouseholdDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdValidationService/ExplainHouseholdDecision")
	}
	terminateCarrierRateRequest := &shippingv1.TerminateCarrierRateRequest{
		Context:       req.GetContext(),
		CarrierRateId: req.GetCarrierRate().GetCarrierRateId(),
		Details: &shippingv1.TerminateCarrierRateRequest_Details{
			NotifyContact: &commonv1.ContactInfo{Email: req.GetDetails().GetApprover().GetEmail()},
		},
	}
	terminateCarrierRateReply, err := s.carrierRateLifecycleLazy().TerminateCarrierRate(ctx, terminateCarrierRateRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.CarrierRateLifecycleService/TerminateCarrierRate")
	}
	return &shippingv1.DelegateCarrierRateApprovalResponse{
		Outcome: &shippingv1.DelegateCarrierRateApprovalResponse_Outcome{
			Status:      explainHouseholdDecisionReply.GetResult().GetStatus(),
			EffectiveAt: terminateCarrierRateReply.GetOutcome().GetEffectiveAt(),
		},
		CarrierRate: &shippingv1.CarrierRate{
			Audit: &commonv1.AuditInfo{
				Etag: explainHouseholdDecisionReply.GetHousehold().GetAudit().GetEtag(),
			},
			CarrierRateId: req.GetCarrierRateId(),
			Parameters:    req.GetCarrierRate().GetParameters(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetCarrierRateApprovalStatus returns a carrier rate.
func (s *CarrierRateApprovalServer) GetCarrierRateApprovalStatus(ctx context.Context, req *shippingv1.GetCarrierRateApprovalStatusRequest) (*shippingv1.GetCarrierRateApprovalStatusResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "carrier_rate", req.GetCarrierRateId(), "get_carrier_rate_approval_status"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get carrier rate approval status", "tenant", req.GetContext().GetTenantId())
	resolveUsersRequest := &identityv1.ResolveUsersRequest{}
	resolveUsersRequest.Context = req.GetContext()
	resolveUsersReply, err := s.userQueryClient.ResolveUsers(ctx, resolveUsersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserQueryService/ResolveUsers")
	}
	shipmentCalculation, ok := s.clients.Get("acme.shipping.v1.ShipmentCalculationService").(shippingv1.ShipmentCalculationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.shipping.v1.ShipmentCalculationService")
	}
	explainShipmentCalculationRequest := &shippingv1.ExplainShipmentCalculationRequest{
		Context: req.GetContext(),
		Details: &shippingv1.ExplainShipmentCalculationRequest_Details{Locale: req.GetContext().GetLocale()},
	}
	explainShipmentCalculationReply, err := shipmentCalculation.ExplainShipmentCalculation(ctx, explainShipmentCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ShipmentCalculationService/ExplainShipmentCalculation")
	}
	return &shippingv1.GetCarrierRateApprovalStatusResponse{
		Result: &commonv1.OperationResult{
			Status:       resolveUsersReply.GetResult().GetStatus(),
			ReferenceIds: resolveUsersReply.GetResult().GetReferenceIds(),
			Details:      explainShipmentCalculationReply.GetResult().GetDetails(),
		},
		CarrierRate: &shippingv1.CarrierRate{
			Audit: &commonv1.AuditInfo{
				CreatedAt: explainShipmentCalculationReply.GetShipment().GetAudit().GetCreatedAt(),
			},
			CarrierRateId: req.GetCarrierRateId(),
		},
	}, nil
}

// carrierRateLifecycleLazy dials acme.shipping.v1.CarrierRateLifecycleService the first time a handler needs it.
func (s *CarrierRateApprovalServer) carrierRateLifecycleLazy() shippingv1.CarrierRateLifecycleServiceClient {
	s.carrierRateLifecycleOnce.Do(func() { s.carrierRateLifecycleCached = shippingv1.NewCarrierRateLifecycleServiceClient(s.conn) })
	return s.carrierRateLifecycleCached
}
