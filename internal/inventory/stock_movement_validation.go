package inventory

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	inventoryv1 "github.com/hounddogai/protomap-test-golang/gen/acme/inventory/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// StockMovementValidationServer implements `acme.inventory.v1.StockMovementValidationService`.
// Checks stock movements against the rules before anything acts on them.
type StockMovementValidationServer struct {
	inventoryv1.UnimplementedStockMovementValidationServiceServer
	store                              *Store
	log                                *slog.Logger
	replenishmentPlanCalculationClient inventoryv1.ReplenishmentPlanCalculationServiceClient
	usageRecordCalculationClient       subscriptionsv1.UsageRecordCalculationServiceClient
	placementValidationClient          merchandisingv1.PlacementValidationServiceClient
	lintUsageRecordsFn                 func(context.Context, *subscriptionsv1.LintUsageRecordsRequest, ...grpc.CallOption) (*subscriptionsv1.LintUsageRecordsResponse, error)
	conn                               *grpc.ClientConn
	productReviewValidationPort        productReviewValidationPort
	rewardCalculationClient            loyaltyv1.RewardCalculationServiceClient
	usageRecordValidationClient        subscriptionsv1.UsageRecordValidationServiceClient
	sessionHistoryClient               identityv1.SessionHistoryServiceClient
	searchIndexNotificationClient      searchv1.SearchIndexNotificationServiceClient
}

// NewStockMovementValidationServer wires a StockMovementValidationServer.
func NewStockMovementValidationServer(deps platform.Deps) *StockMovementValidationServer {
	usageRecordValidation := subscriptionsv1.NewUsageRecordValidationServiceClient(deps.Conn)
	return &StockMovementValidationServer{
		store:                              NewStore(deps.DB),
		log:                                deps.Logger.With("service", "acme.inventory.v1.StockMovementValidationService"),
		replenishmentPlanCalculationClient: inventoryv1.NewReplenishmentPlanCalculationServiceClient(deps.Conn),
		usageRecordCalculationClient:       subscriptionsv1.NewUsageRecordCalculationServiceClient(deps.Conn),
		placementValidationClient:          merchandisingv1.NewPlacementValidationServiceClient(deps.Conn),
		lintUsageRecordsFn:                 usageRecordValidation.LintUsageRecords,
		conn:                               deps.Conn,
		productReviewValidationPort:        newProductReviewValidationPort(deps),
		rewardCalculationClient:            loyaltyv1.NewRewardCalculationServiceClient(deps.Conn),
		usageRecordValidationClient:        subscriptionsv1.NewUsageRecordValidationServiceClient(deps.Conn),
		sessionHistoryClient:               identityv1.NewSessionHistoryServiceClient(deps.Conn),
		searchIndexNotificationClient:      searchv1.NewSearchIndexNotificationServiceClient(deps.Conn),
	}
}

// ValidateStockMovement validates a stock movement.
func (s *StockMovementValidationServer) ValidateStockMovement(ctx context.Context, req *inventoryv1.ValidateStockMovementRequest) (*inventoryv1.ValidateStockMovementResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveStockMovement(ctx, req.GetContext().GetTenantId(), req.GetStockMovement()); err != nil {
		return nil, grpcx.Wrap(err, "save stock movement")
	}
	s.log.InfoContext(ctx, "validate stock movement", "tenant", req.GetContext().GetTenantId())
	attributes := make(map[string]string, len(req.GetStockMovement().GetAttributes()))
	for key, value := range req.GetStockMovement().GetAttributes() {
		attributes[key] = value
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key2, value2 := range req.GetAnnotations() {
		annotations[key2] = value2
	}
	var envelope inventoryv1.StockItemRef
	if err := req.GetStockMovement().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	simulateReplenishmentPlanRequest := &inventoryv1.SimulateReplenishmentPlanRequest{
		Context: req.GetContext(),
		ReplenishmentPlan: &inventoryv1.ReplenishmentPlan{
			Counterparty: &inventoryv1.StockReservationRef{
				DisplayName: req.GetStockMovement().GetSubject().GetDisplayName(),
			},
			Lines: []*inventoryv1.ReplenishmentPlanLine{&inventoryv1.ReplenishmentPlanLine{Attributes: attributes}},
		},
		Reason:              req.GetReason(),
		Annotations:         annotations,
		ReplenishmentPlanId: envelope.GetDisplayName(),
	}
	simulateReplenishmentPlanReply, err := inventoryv1.ReplenishmentPlanCalculationServiceClient.SimulateReplenishmentPlan(s.replenishmentPlanCalculationClient, ctx, simulateReplenishmentPlanRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.ReplenishmentPlanCalculationService/SimulateReplenishmentPlan")
	}
	explainUsageRecordCalculationRequest := buildExplainUsageRecordCalculationRequestForValidateStockMovement(req, simulateReplenishmentPlanReply)
	explainUsageRecordCalculationReply, err := s.usageRecordCalculationClient.ExplainUsageRecordCalculation(ctx, explainUsageRecordCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.UsageRecordCalculationService/ExplainUsageRecordCalculation")
	}
	return &inventoryv1.ValidateStockMovementResponse{
		Outcome: &inventoryv1.ValidateStockMovementResponse_Outcome{
			Message: simulateReplenishmentPlanReply.GetResult().GetMessage(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: explainUsageRecordCalculationReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		StockMovement: &inventoryv1.StockMovement{
			CorrelationId:   explainUsageRecordCalculationReply.GetResult().GetCorrelationId(),
			StockMovementId: req.GetStockMovementId(),
			Subject:         req.GetStockMovement().GetSubject(),
		},
	}, nil
}

// PreviewStockMovement previews a stock movement.
func (s *StockMovementValidationServer) PreviewStockMovement(ctx context.Context, req *inventoryv1.PreviewStockMovementRequest) (*inventoryv1.PreviewStockMovementResponse, error) {
	if err := s.store.SaveStockMovement(ctx, req.GetContext().GetTenantId(), req.GetStockMovement()); err != nil {
		return nil, grpcx.Wrap(err, "save stock movement")
	}
	s.log.InfoContext(ctx, "preview stock movement", "tenant", req.GetContext().GetTenantId())
	var holder struct{ Email string }
	if record := req.GetStockMovement().GetSubject(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Email"); err != nil {
			s.log.WarnContext(ctx, "copy email", "error", err)
		}
	}
	checkPlacementEligibilityRequest := &merchandisingv1.CheckPlacementEligibilityRequest{
		Context: req.GetContext(),
		Placement: &merchandisingv1.Placement{
			Participants: []*merchandisingv1.PlacementParticipant{&merchandisingv1.PlacementParticipant{
				Contact: &commonv1.ContactInfo{
					Email: holder.Email,
					Name:  req.GetDetails().GetContact().GetName(),
				},
			}},
		},
		Details: &merchandisingv1.CheckPlacementEligibilityRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetContact().GetEmail()},
		},
	}
	checkPlacementEligibilityReply, err := merchandisingv1.PlacementValidationServiceClient.CheckPlacementEligibility(s.placementValidationClient, ctx, checkPlacementEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.PlacementValidationService/CheckPlacementEligibility")
	}
	cloned := proto.Clone(req.GetStockMovement()).(*inventoryv1.StockMovement)
	lintUsageRecordsRequest := &subscriptionsv1.LintUsageRecordsRequest{
		Context: req.GetContext(),
		UsageRecord: &subscriptionsv1.UsageRecord{
			Subject: &customersv1.CustomerRef{
				Email:       checkPlacementEligibilityReply.GetPlacement().GetOrganizer().GetEmail(),
				CustomerId:  req.GetStockMovement().GetSubject().GetCustomerId(),
				DisplayName: cloned.GetSubject().GetDisplayName(),
			},
		},
	}
	lintUsageRecordsReply, err := s.lintUsageRecordsFn(ctx, lintUsageRecordsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.UsageRecordValidationService/LintUsageRecords")
	}
	var envelope inventoryv1.StockItemRef
	if err := req.GetStockMovement().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	simulateReplenishmentPlanReply, err := s.replenishmentPlanCalculationClient.SimulateReplenishmentPlan(ctx, &inventoryv1.SimulateReplenishmentPlanRequest{
		Context: req.GetContext(),
		ReplenishmentPlan: &inventoryv1.ReplenishmentPlan{
			BillingAddress: &commonv1.PostalAddress{
				Region: checkPlacementEligibilityReply.GetPlacement().GetLocation().GetRegion(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: checkPlacementEligibilityReply.GetPlacement().GetAudit().GetUpdatedBy(),
				UpdatedAt: checkPlacementEligibilityReply.GetPlacement().GetAudit().GetUpdatedAt(),
			},
		},
		Details: &inventoryv1.SimulateReplenishmentPlanRequest_Details{
			Destination: &commonv1.PostalAddress{
				Locality: checkPlacementEligibilityReply.GetPlacement().GetLocation().GetLocality(),
			},
		},
		ReplenishmentPlanId: envelope.GetDisplayName(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.ReplenishmentPlanCalculationService/SimulateReplenishmentPlan")
	}
	return &inventoryv1.PreviewStockMovementResponse{
		Outcome: &inventoryv1.PreviewStockMovementResponse_Outcome{
			Status:  checkPlacementEligibilityReply.GetOutcome().GetStatus(),
			Message: simulateReplenishmentPlanReply.GetResult().GetMessage(),
		},
		StockMovement: &inventoryv1.StockMovement{
			OccurredAt: lintUsageRecordsReply.GetUsageRecord().GetOccurredAt(),
			Subject: &customersv1.CustomerRef{
				CustomerId: lintUsageRecordsReply.GetUsageRecord().GetSubject().GetCustomerId(),
			},
			StockMovementId: req.GetStockMovementId(),
			CorrelationId:   req.GetStockMovement().GetCorrelationId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CheckStockMovementEligibility checks a stock movement.
func (s *StockMovementValidationServer) CheckStockMovementEligibility(ctx context.Context, req *inventoryv1.CheckStockMovementEligibilityRequest) (*inventoryv1.CheckStockMovementEligibilityResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveStockMovement(ctx, req.GetContext().GetTenantId(), req.GetStockMovement()); err != nil {
		return nil, grpcx.Wrap(err, "save stock movement")
	}
	s.log.InfoContext(ctx, "check stock movement eligibility", "tenant", req.GetContext().GetTenantId())
	attributes := make(map[string]string, len(req.GetStockMovement().GetAttributes()))
	for key, value := range req.GetStockMovement().GetAttributes() {
		attributes[key] = value
	}
	compareReturnLabelOptionsRequest := &returnsv1.CompareReturnLabelOptionsRequest{
		Context: req.GetContext(),
		ReturnLabel: &returnsv1.ReturnLabel{
			Counterparty: &returnsv1.InspectionRef{
				DisplayName: req.GetStockMovement().GetSubject().GetDisplayName(),
			},
			Lines: []*returnsv1.ReturnLabelLine{&returnsv1.ReturnLabelLine{Attributes: attributes}},
		},
		Reason: req.GetReason(),
	}
	compareReturnLabelOptionsReply := new(returnsv1.CompareReturnLabelOptionsResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", returnsReturnLabelCalculationServiceName, "CompareReturnLabelOptions"), compareReturnLabelOptionsRequest, compareReturnLabelOptionsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnLabelCalculationService/CompareReturnLabelOptions")
	}
	var envelope inventoryv1.StockItemRef
	if err := req.GetStockMovement().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	previewProductReviewRequest := &reviewsv1.PreviewProductReviewRequest{
		Context: req.GetContext(),
		ProductReview: &reviewsv1.ProductReview{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: compareReturnLabelOptionsReply.GetReturnLabel().GetAudit().GetUpdatedAt(),
			},
		},
		ProductReviewId: envelope.GetDisplayName(),
	}
	previewProductReviewReply, err := s.productReviewValidationPort.PreviewProductReview(ctx, previewProductReviewRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ProductReviewValidationService/PreviewProductReview")
	}
	return &inventoryv1.CheckStockMovementEligibilityResponse{
		Outcome: &inventoryv1.CheckStockMovementEligibilityResponse_Outcome{
			Message:     compareReturnLabelOptionsReply.GetResult().GetMessage(),
			EffectiveAt: previewProductReviewReply.GetOutcome().GetEffectiveAt(),
			Status:      previewProductReviewReply.GetResult().GetStatus(),
		},
		StockMovement: &inventoryv1.StockMovement{
			Subject: &customersv1.CustomerRef{
				DisplayName: compareReturnLabelOptionsReply.GetReturnLabel().GetCounterparty().GetDisplayName(),
			},
			StockMovementId: req.GetStockMovementId(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: req.GetStockMovement().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// NormalizeStockMovement normalizes a stock movement.
func (s *StockMovementValidationServer) NormalizeStockMovement(ctx context.Context, req *inventoryv1.NormalizeStockMovementRequest) (*inventoryv1.NormalizeStockMovementResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveStockMovement(ctx, req.GetContext().GetTenantId(), req.GetStockMovement()); err != nil {
		return nil, grpcx.Wrap(err, "save stock movement")
	}
	s.log.InfoContext(ctx, "normalize stock movement", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "display_name", req.GetStockMovement().GetSubject().GetDisplayName())
	var envelope inventoryv1.StockItemRef
	if err := req.GetStockMovement().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	calculateReplenishmentPlanRequest := &inventoryv1.CalculateReplenishmentPlanRequest{
		Context: req.GetContext(),
		ReplenishmentPlan: &inventoryv1.ReplenishmentPlan{
			Counterparty: &inventoryv1.StockReservationRef{DisplayName: tenancy.Value(ctx, "display_name")},
		},
		ReplenishmentPlanId: envelope.GetDisplayName(),
	}
	calculateReplenishmentPlanReply, err := calculateReplenishmentPlanCall(ctx, calculateReplenishmentPlanRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.ReplenishmentPlanCalculationService/CalculateReplenishmentPlan")
	}
	listSearchIndexSubscriptionsRequest := &searchv1.ListSearchIndexSubscriptionsRequest{
		Context: req.GetContext(),
		Filter: &searchv1.ListSearchIndexSubscriptionsRequest_Filter{
			Region: calculateReplenishmentPlanReply.GetReplenishmentPlan().GetBillingAddress().GetRegion(),
		},
	}
	listSearchIndexSubscriptionsReply := new(searchv1.ListSearchIndexSubscriptionsResponse)
	if err := grpcx.Invoke(ctx, s.conn, searchv1.SearchIndexNotificationService_ListSearchIndexSubscriptions_FullMethodName, listSearchIndexSubscriptionsRequest, listSearchIndexSubscriptionsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexNotificationService/ListSearchIndexSubscriptions")
	}
	return &inventoryv1.NormalizeStockMovementResponse{
		Result: &commonv1.OperationResult{
			Status: calculateReplenishmentPlanReply.GetResult().GetStatus(),
		},
		StockMovement: &inventoryv1.StockMovement{
			CorrelationId:   calculateReplenishmentPlanReply.GetResult().GetCorrelationId(),
			StockMovementId: req.GetStockMovementId(),
			OccurredAt:      req.GetStockMovement().GetOccurredAt(),
		},
		Outcome: &inventoryv1.NormalizeStockMovementResponse_Outcome{
			Status: listSearchIndexSubscriptionsReply.GetResult().GetStatus(),
		},
	}, nil
}

// ExplainStockMovementDecision explains a stock movement.
func (s *StockMovementValidationServer) ExplainStockMovementDecision(ctx context.Context, req *inventoryv1.ExplainStockMovementDecisionRequest) (*inventoryv1.ExplainStockMovementDecisionResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveStockMovement(ctx, req.GetContext().GetTenantId(), req.GetStockMovement()); err != nil {
		return nil, grpcx.Wrap(err, "save stock movement")
	}
	s.log.InfoContext(ctx, "explain stock movement decision", "tenant", req.GetContext().GetTenantId())
	for measurement := range iterx.Each(req.GetStockMovement().GetMeasurements(), func(candidate *inventoryv1.StockMovementMeasurement) bool { return candidate != nil }) {
		if _, err := s.replenishmentPlanCalculationClient.ExplainReplenishmentPlanCalculation(ctx, &inventoryv1.ExplainReplenishmentPlanCalculationRequest{
			Context: req.GetContext(),
			ReplenishmentPlan: &inventoryv1.ReplenishmentPlan{
				Audit: &commonv1.AuditInfo{CreatedAt: measurement.GetObservedAt()},
			},
			Details: &inventoryv1.ExplainReplenishmentPlanCalculationRequest_Details{
				Contact: &commonv1.ContactInfo{Email: req.GetStockMovement().GetSubject().GetEmail()},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.inventory.v1.ReplenishmentPlanCalculationService/ExplainReplenishmentPlanCalculation")
		}
	}
	return &inventoryv1.ExplainStockMovementDecisionResponse{
		StockMovement: &inventoryv1.StockMovement{
			StockMovementId: req.GetStockMovementId(),
			CorrelationId:   req.GetStockMovement().GetCorrelationId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ScoreStockMovement scores a stock movement.
func (s *StockMovementValidationServer) ScoreStockMovement(ctx context.Context, req *inventoryv1.ScoreStockMovementRequest) (*inventoryv1.ScoreStockMovementResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveStockMovement(ctx, req.GetContext().GetTenantId(), req.GetStockMovement()); err != nil {
		return nil, grpcx.Wrap(err, "save stock movement")
	}
	s.log.InfoContext(ctx, "score stock movement", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "email", req.GetStockMovement().GetSubject().GetEmail())
	quoteRewardRequest := &loyaltyv1.QuoteRewardRequest{}
	quoteRewardRequest.Context = req.GetContext()
	quoteRewardRequest.Reward = &loyaltyv1.Reward{
		Owner: &identityv1.UserRef{Email: tenancy.Value(ctx, "email")},
	}
	quoteRewardRequest.Details = &loyaltyv1.QuoteRewardRequest_Details{
		Destination: &commonv1.PostalAddress{
			CountryCode: req.GetDetails().GetDestination().GetCountryCode(),
			Lines:       req.GetDetails().GetDestination().GetLines(),
		},
	}
	quoteRewardReply, err := s.rewardCalculationClient.QuoteReward(ctx, quoteRewardRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.RewardCalculationService/QuoteReward")
	}
	simulateReplenishmentPlanRequest := newSimulateReplenishmentPlanRequest(withSimulateReplenishmentPlanContext(req.GetContext()), withSimulateReplenishmentPlanReplenishmentPlan(&inventoryv1.ReplenishmentPlan{
		Lines: []*inventoryv1.ReplenishmentPlanLine{&inventoryv1.ReplenishmentPlanLine{
			Amount: quoteRewardReply.GetReward().GetDiscount().GetAmount(),
		}},
		BillingAddress: &commonv1.PostalAddress{Locality: req.GetDetails().GetDestination().GetLocality()},
	}))
	simulateReplenishmentPlanReply, err := s.replenishmentPlanCalculationClient.SimulateReplenishmentPlan(ctx, simulateReplenishmentPlanRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.ReplenishmentPlanCalculationService/SimulateReplenishmentPlan")
	}
	var envelope inventoryv1.StockItemRef
	if err := req.GetStockMovement().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	explainUsageRecordDecisionRequest := &subscriptionsv1.ExplainUsageRecordDecisionRequest{}
	explainUsageRecordDecisionRequest.Context = req.GetContext()
	explainUsageRecordDecisionRequest.UsageRecord = &subscriptionsv1.UsageRecord{
		Subject: &customersv1.CustomerRef{
			DisplayName: quoteRewardReply.GetReward().GetOwner().GetDisplayName(),
		},
	}
	explainUsageRecordDecisionRequest.Details = &subscriptionsv1.ExplainUsageRecordDecisionRequest_Details{
		Contact: &commonv1.ContactInfo{Email: req.GetStockMovement().GetSubject().GetEmail()},
	}
	explainUsageRecordDecisionRequest.UsageRecordId = envelope.GetDisplayName()
	explainUsageRecordDecisionReply, err := s.usageRecordValidationClient.ExplainUsageRecordDecision(ctx, explainUsageRecordDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.UsageRecordValidationService/ExplainUsageRecordDecision")
	}
	return &inventoryv1.ScoreStockMovementResponse{
		Breakdown: &inventoryv1.ScoreStockMovementResponse_Breakdown{
			CurrencyCode: quoteRewardReply.GetBreakdown().GetCurrencyCode(),
			Total:        simulateReplenishmentPlanReply.GetReplenishmentPlan().GetTotal(),
		},
		StockMovement: &inventoryv1.StockMovement{
			Subject: &customersv1.CustomerRef{
				DisplayName: explainUsageRecordDecisionReply.GetUsageRecord().GetSubject().GetDisplayName(),
			},
			Kind:            explainUsageRecordDecisionReply.GetUsageRecord().GetKind(),
			StockMovementId: req.GetStockMovementId(),
			OccurredAt:      req.GetStockMovement().GetOccurredAt(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// LintStockMovements lints a stock movement.
func (s *StockMovementValidationServer) LintStockMovements(ctx context.Context, req *inventoryv1.LintStockMovementsRequest) (*inventoryv1.LintStockMovementsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveStockMovement(ctx, req.GetContext().GetTenantId(), req.GetStockMovement()); err != nil {
		return nil, grpcx.Wrap(err, "save stock movement")
	}
	s.log.InfoContext(ctx, "lint stock movements", "tenant", req.GetContext().GetTenantId())
	snapshot, err := protojson.Marshal(req.GetStockMovement())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	compareSessionVersionsReply, err := s.sessionHistoryClient.CompareSessionVersions(ctx, &identityv1.CompareSessionVersionsRequest{
		Context: req.GetContext(),
		Session: &identityv1.Session{
			Requester: &commonv1.ContactInfo{
				Phone: req.GetDetails().GetContact().GetPhone(),
				Email: req.GetStockMovement().GetSubject().GetEmail(),
			},
			Assignee: &identityv1.UserRef{
				DisplayName: req.GetStockMovement().GetSubject().GetDisplayName(),
			},
		},
		Details: &identityv1.CompareSessionVersionsRequest_Details{Snapshot: snapshot},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionHistoryService/CompareSessionVersions")
	}
	var envelope inventoryv1.StockItemRef
	if err := req.GetStockMovement().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	compareReplenishmentPlanOptionsRequest := &inventoryv1.CompareReplenishmentPlanOptionsRequest{
		Context: req.GetContext(),
		ReplenishmentPlan: &inventoryv1.ReplenishmentPlan{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: compareSessionVersionsReply.GetSession().GetAudit().GetUpdatedAt(),
				CreatedBy: compareSessionVersionsReply.GetSession().GetAudit().GetCreatedBy(),
			},
			Counterparty: &inventoryv1.StockReservationRef{
				DisplayName: compareSessionVersionsReply.GetSession().GetAssignee().GetDisplayName(),
			},
		},
		ReplenishmentPlanId: envelope.GetDisplayName(),
	}
	send := s.compareReplenishmentPlanOptionsSender(compareReplenishmentPlanOptionsRequest)
	compareReplenishmentPlanOptionsReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.ReplenishmentPlanCalculationService/CompareReplenishmentPlanOptions")
	}
	var envelope2 inventoryv1.StockItemRef
	if err := req.GetStockMovement().GetDetail().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	quoteReplenishmentPlanRequest := &inventoryv1.QuoteReplenishmentPlanRequest{}
	quoteReplenishmentPlanRequest.Context = req.GetContext()
	quoteReplenishmentPlanRequest.ReplenishmentPlan = &inventoryv1.ReplenishmentPlan{
		Counterparty: &inventoryv1.StockReservationRef{
			StartsAt: compareReplenishmentPlanOptionsReply.GetReplenishmentPlan().GetCounterparty().GetStartsAt(),
		},
		Audit: &commonv1.AuditInfo{
			CreatedBy: compareReplenishmentPlanOptionsReply.GetReplenishmentPlan().GetAudit().GetCreatedBy(),
		},
	}
	quoteReplenishmentPlanRequest.ReplenishmentPlanId = envelope2.GetDisplayName()
	quoteReplenishmentPlanReply, err := s.replenishmentPlanCalculationClient.QuoteReplenishmentPlan(ctx, quoteReplenishmentPlanRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.ReplenishmentPlanCalculationService/QuoteReplenishmentPlan")
	}
	return &inventoryv1.LintStockMovementsResponse{
		Outcome: &inventoryv1.LintStockMovementsResponse_Outcome{
			Message: compareSessionVersionsReply.GetOutcome().GetMessage(),
		},
		Result: &commonv1.OperationResult{
			Message: compareReplenishmentPlanOptionsReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		StockMovement: &inventoryv1.StockMovement{
			CorrelationId:   quoteReplenishmentPlanReply.GetResult().GetCorrelationId(),
			StockMovementId: req.GetStockMovementId(),
			OccurredAt:      req.GetStockMovement().GetOccurredAt(),
		},
	}, nil
}

// GetStockMovementRules returns a stock movement.
func (s *StockMovementValidationServer) GetStockMovementRules(ctx context.Context, req *inventoryv1.GetStockMovementRulesRequest) (*inventoryv1.GetStockMovementRulesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "stock_movement", req.GetStockMovementId(), "get_stock_movement_rules"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get stock movement rules", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "email", req.GetContext().GetActor().GetEmail())
	streamSearchIndexNoticesStream, err := s.searchIndexNotificationClient.StreamSearchIndexNotices(ctx, &searchv1.StreamSearchIndexNoticesRequest{
		Context:    req.GetContext(),
		Subscriber: &commonv1.ContactInfo{Email: tenancy.Value(ctx, "email")},
		Filter: &searchv1.StreamSearchIndexNoticesRequest_Filter{
			OwnerId:      req.GetStockMovementId(),
			CreatedAfter: req.GetContext().GetDeadline(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexNotificationService/StreamSearchIndexNotices")
	}
	for {
		received, err := streamSearchIndexNoticesStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexNotificationService/StreamSearchIndexNotices")
		}
		_ = received
	}
	return &inventoryv1.GetStockMovementRulesResponse{
		StockMovement: &inventoryv1.StockMovement{StockMovementId: req.GetStockMovementId()},
		Result:        &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// compareReplenishmentPlanOptionsSender returns a function that sends one request to
// acme.inventory.v1.ReplenishmentPlanCalculationService/CompareReplenishmentPlanOptions.
func (s *StockMovementValidationServer) compareReplenishmentPlanOptionsSender(request *inventoryv1.CompareReplenishmentPlanOptionsRequest) func(context.Context) (*inventoryv1.CompareReplenishmentPlanOptionsResponse, error) {
	return func(ctx context.Context) (*inventoryv1.CompareReplenishmentPlanOptionsResponse, error) {
		return s.replenishmentPlanCalculationClient.CompareReplenishmentPlanOptions(ctx, request)
	}
}
