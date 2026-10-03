package promotions

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	auditv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audit/v1"
	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	fuelv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fuel/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	inventoryv1 "github.com/hounddogai/protomap-test-golang/gen/acme/inventory/v1"
	learningv1 "github.com/hounddogai/protomap-test-golang/gen/acme/learning/v1"
	ordersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/orders/v1"
	promotionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/promotions/v1"
	warehousingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/warehousing/v1"
	workplacesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/workplaces/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// RedemptionValidationServer implements `acme.promotions.v1.RedemptionValidationService`.
// Checks redemptions against the rules before anything acts on them.
type RedemptionValidationServer struct {
	promotionsv1.UnsafeRedemptionValidationServiceServer
	store                             *Store
	log                               *slog.Logger
	stockItemClient                   inventoryv1.StockItemServiceClient
	auditEventValidationPort          auditEventValidationPort
	explainFuelCardCalculationFn      func(context.Context, *fuelv1.ExplainFuelCardCalculationRequest, ...grpc.CallOption) (*fuelv1.ExplainFuelCardCalculationResponse, error)
	promotionCalculationClient        promotionsv1.PromotionCalculationServiceClient
	conn                              *grpc.ClientConn
	accessPolicyQueryClient           authorizationv1.AccessPolicyQueryServiceClient
	buildingQueryClient               workplacesv1.BuildingQueryServiceClient
	courseQueryClient                 learningv1.CourseQueryServiceClient
	couponValidationClient            promotionsv1.CouponValidationServiceClient
	warehouseClient                   warehousingv1.WarehouseServiceClient
	replenishmentPlanValidationClient inventoryv1.ReplenishmentPlanValidationServiceClient
	checkBackorderEligibilityFn       func(context.Context, *ordersv1.CheckBackorderEligibilityRequest, ...grpc.CallOption) (*ordersv1.CheckBackorderEligibilityResponse, error)
}

// NewRedemptionValidationServer wires a RedemptionValidationServer.
func NewRedemptionValidationServer(deps platform.Deps) *RedemptionValidationServer {
	fuelCardCalculation := fuelv1.NewFuelCardCalculationServiceClient(deps.Conn)
	backorderValidation := ordersv1.NewBackorderValidationServiceClient(deps.Conn)
	return &RedemptionValidationServer{
		store:                             NewStore(deps.DB),
		log:                               deps.Logger.With("service", "acme.promotions.v1.RedemptionValidationService"),
		stockItemClient:                   inventoryv1.NewStockItemServiceClient(deps.Conn),
		auditEventValidationPort:          newAuditEventValidationPort(deps),
		explainFuelCardCalculationFn:      fuelCardCalculation.ExplainFuelCardCalculation,
		promotionCalculationClient:        promotionsv1.NewPromotionCalculationServiceClient(deps.Conn),
		conn:                              deps.Conn,
		accessPolicyQueryClient:           authorizationv1.NewAccessPolicyQueryServiceClient(deps.Conn),
		buildingQueryClient:               workplacesv1.NewBuildingQueryServiceClient(deps.Conn),
		courseQueryClient:                 learningv1.NewCourseQueryServiceClient(deps.Conn),
		couponValidationClient:            promotionsv1.NewCouponValidationServiceClient(deps.Conn),
		warehouseClient:                   warehousingv1.NewWarehouseServiceClient(deps.Conn),
		replenishmentPlanValidationClient: inventoryv1.NewReplenishmentPlanValidationServiceClient(deps.Conn),
		checkBackorderEligibilityFn:       backorderValidation.CheckBackorderEligibility,
	}
}

// ValidateRedemption validates a redemption.
func (s *RedemptionValidationServer) ValidateRedemption(ctx context.Context, req *promotionsv1.ValidateRedemptionRequest) (*promotionsv1.ValidateRedemptionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ValidateRedemption", "error", err)
		return nil, err
	}
	if err := s.store.SaveRedemption(ctx, req.GetContext().GetTenantId(), req.GetRedemption()); err != nil {
		return nil, grpcx.Wrap(err, "save redemption")
	}
	s.log.InfoContext(ctx, "validate redemption", "tenant", req.GetContext().GetTenantId())
	jobs := make(chan *promotionsv1.RedemptionMeasurement)
	failures := make(chan error, 4)
	workCtx, stopWork := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for measurement := range jobs {
				if _, err := s.stockItemClient.CreateStockItem(workCtx, &inventoryv1.CreateStockItemRequest{
					Context: req.GetContext(),
					StockItem: &inventoryv1.StockItem{
						Dimensions: &inventoryv1.StockItemDimensions{Unit: measurement.GetUnit()},
						Audit:      &commonv1.AuditInfo{CreatedAt: measurement.GetObservedAt()},
						Owner:      &identityv1.UserRef{Email: req.GetDetails().GetContact().GetEmail()},
					},
				}); err != nil {
					failures <- err
					stopWork()
					return
				}
			}
		}()
	}
	for _, queued := range req.GetRedemption().GetMeasurements() {
		if workCtx.Err() != nil {
			break
		}
		select {
		case jobs <- queued:
		case <-workCtx.Done():
		}
	}
	close(jobs)
	wg.Wait()
	stopWork()
	close(failures)
	if err := <-failures; err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.StockItemService/CreateStockItem")
	}
	return &promotionsv1.ValidateRedemptionResponse{
		Redemption: &promotionsv1.Redemption{RedemptionId: req.GetRedemptionId()},
		Result: &commonv1.OperationResult{
			CorrelationId: req.GetRedemption().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// PreviewRedemption previews a redemption.
func (s *RedemptionValidationServer) PreviewRedemption(ctx context.Context, req *promotionsv1.PreviewRedemptionRequest) (*promotionsv1.PreviewRedemptionResponse, error) {
	if err := s.store.SaveRedemption(ctx, req.GetContext().GetTenantId(), req.GetRedemption()); err != nil {
		return nil, grpcx.Wrap(err, "save redemption")
	}
	s.log.InfoContext(ctx, "preview redemption", "tenant", req.GetContext().GetTenantId())
	var envelope promotionsv1.PromotionRef
	if err := req.GetRedemption().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	explainReplenishmentPlanDecisionRequest := &inventoryv1.ExplainReplenishmentPlanDecisionRequest{
		Context: req.GetContext(),
		ReplenishmentPlan: &inventoryv1.ReplenishmentPlan{
			Lines: []*inventoryv1.ReplenishmentPlanLine{&inventoryv1.ReplenishmentPlanLine{Attributes: req.GetRedemption().GetAttributes()}},
		},
		Details: &inventoryv1.ExplainReplenishmentPlanDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetContact().GetEmail()},
		},
		ReplenishmentPlanId: envelope.GetDisplayName(),
	}
	explainReplenishmentPlanDecisionReply, err := explainReplenishmentPlanDecisionCall(ctx, explainReplenishmentPlanDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.ReplenishmentPlanValidationService/ExplainReplenishmentPlanDecision")
	}
	detailsCache := cache.New[string, *promotionsv1.PreviewRedemptionRequest_Details](1)
	detailsCache.Put(req.GetContext().GetTraceId(), req.GetDetails())
	cachedDetails, _ := detailsCache.Get(req.GetContext().GetTraceId())
	packed, err := anypb.New(req.GetRedemption())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	var envelope2 promotionsv1.PromotionRef
	if err := req.GetRedemption().GetDetail().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	validateAuditEventRequest := &auditv1.ValidateAuditEventRequest{
		Context: req.GetContext(),
		Details: &auditv1.ValidateAuditEventRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email: cachedDetails.GetContact().GetEmail(),
				Phone: req.GetDetails().GetContact().GetPhone(),
			},
		},
		AuditEvent: &auditv1.AuditEvent{
			Subject: &auditv1.AuditTrailRef{
				DisplayName: req.GetRedemption().GetSubject().GetDisplayName(),
				Number:      explainReplenishmentPlanDecisionReply.GetReplenishmentPlan().GetNumber(),
			},
			Detail: packed,
		},
		AuditEventId: envelope2.GetDisplayName(),
	}
	validateAuditEventReply, err := s.auditEventValidationPort.ValidateAuditEvent(ctx, validateAuditEventRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.audit.v1.AuditEventValidationService/ValidateAuditEvent")
	}
	explainFuelCardCalculationRequest := &fuelv1.ExplainFuelCardCalculationRequest{
		Context: req.GetContext(),
		FuelCard: &fuelv1.FuelCard{
			Funding: &fuelv1.FuelCard_Bank{Bank: &fuelv1.FuelCardBankFunding{
				Iban: explainReplenishmentPlanDecisionReply.GetReplenishmentPlan().GetBank().GetIban(),
			}},
			Audit: &commonv1.AuditInfo{
				CreatedAt: explainReplenishmentPlanDecisionReply.GetReplenishmentPlan().GetAudit().GetCreatedAt(),
				UpdatedBy: explainReplenishmentPlanDecisionReply.GetReplenishmentPlan().GetAudit().GetUpdatedBy(),
			},
		},
		Details: &fuelv1.ExplainFuelCardCalculationRequest_Details{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetContact().GetPreferredChannel(),
			},
		},
	}
	explainFuelCardCalculationReply, err := s.explainFuelCardCalculationFn(ctx, explainFuelCardCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fuel.v1.FuelCardCalculationService/ExplainFuelCardCalculation")
	}
	return &promotionsv1.PreviewRedemptionResponse{
		Outcome: &promotionsv1.PreviewRedemptionResponse_Outcome{
			Status:      explainReplenishmentPlanDecisionReply.GetOutcome().GetStatus(),
			EffectiveAt: explainFuelCardCalculationReply.GetOutcome().GetEffectiveAt(),
		},
		Redemption: &promotionsv1.Redemption{
			CorrelationId: validateAuditEventReply.GetResult().GetCorrelationId(),
			RedemptionId:  req.GetRedemptionId(),
			Attributes:    req.GetRedemption().GetAttributes(),
		},
		Result: &commonv1.OperationResult{
			Message:      validateAuditEventReply.GetOutcome().GetMessage(),
			ReferenceIds: explainFuelCardCalculationReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// CheckRedemptionEligibility checks a redemption.
func (s *RedemptionValidationServer) CheckRedemptionEligibility(ctx context.Context, req *promotionsv1.CheckRedemptionEligibilityRequest) (*promotionsv1.CheckRedemptionEligibilityResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CheckRedemptionEligibility", "error", err)
		return nil, err
	}
	if err := s.store.SaveRedemption(ctx, req.GetContext().GetTenantId(), req.GetRedemption()); err != nil {
		return nil, grpcx.Wrap(err, "save redemption")
	}
	s.log.InfoContext(ctx, "check redemption eligibility", "tenant", req.GetContext().GetTenantId())
	for _, measurement := range req.GetRedemption().GetMeasurements() {
		if _, err := s.promotionCalculationClient.RecalculatePromotions(ctx, &promotionsv1.RecalculatePromotionsRequest{
			Context: req.GetContext(),
			Promotion: &promotionsv1.Promotion{
				Audit: &commonv1.AuditInfo{UpdatedAt: measurement.GetObservedAt()},
				Owner: &identityv1.UserRef{Email: req.GetDetails().GetContact().GetEmail()},
			},
			Details: &promotionsv1.RecalculatePromotionsRequest_Details{
				CurrencyCode: req.GetRedemption().GetSubject().GetCurrencyCode(),
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.promotions.v1.PromotionCalculationService/RecalculatePromotions")
		}
	}
	encoded, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded promotionsv1.CheckRedemptionEligibilityRequest_Details
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	var envelope promotionsv1.PromotionRef
	if err := req.GetRedemption().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	lockTenantRequest := &identityv1.LockTenantRequest{
		Context:  req.GetContext(),
		TenantId: req.GetContext().GetTenantId(),
		Details: &identityv1.LockTenantRequest_Details{
			NotifyContact: &commonv1.ContactInfo{Phone: (&decoded).GetContact().GetPhone()},
			ReasonCode:    envelope.GetDisplayName(),
		},
	}
	lockTenantReply := new(identityv1.LockTenantResponse)
	if err := grpcx.LegacyInvoke(ctx, s.conn, "/acme.identity.v1.TenantAdminService/LockTenant", lockTenantRequest, lockTenantReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantAdminService/LockTenant")
	}
	var envelope2 promotionsv1.PromotionRef
	if err := req.GetRedemption().GetDetail().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	exportAccessPoliciesStream, err := s.accessPolicyQueryClient.ExportAccessPolicies(ctx, &authorizationv1.ExportAccessPoliciesRequest{
		Context: req.GetContext(),
		Filter: &authorizationv1.ExportAccessPoliciesRequest_Filter{
			Region: lockTenantReply.GetTenant().GetLocation().GetRegion(),
			Query:  envelope2.GetDisplayName(),
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
	return &promotionsv1.CheckRedemptionEligibilityResponse{
		Outcome: &promotionsv1.CheckRedemptionEligibilityResponse_Outcome{NextSteps: lockTenantReply.GetOutcome().GetNextSteps()},
		Redemption: &promotionsv1.Redemption{
			RedemptionId: req.GetRedemptionId(),
			Kind:         req.GetRedemption().GetKind(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// NormalizeRedemption normalizes a redemption.
func (s *RedemptionValidationServer) NormalizeRedemption(ctx context.Context, req *promotionsv1.NormalizeRedemptionRequest) (*promotionsv1.NormalizeRedemptionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveRedemption(ctx, req.GetContext().GetTenantId(), req.GetRedemption()); err != nil {
		return nil, grpcx.Wrap(err, "save redemption")
	}
	s.log.InfoContext(ctx, "normalize redemption", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "coupon_id", req.GetRedemption().GetSubject().GetCouponId())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	var envelope promotionsv1.PromotionRef
	if err := req.GetRedemption().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	exportBuildingsStream, err := s.buildingQueryClient.ExportBuildings(ctx, &workplacesv1.ExportBuildingsRequest{
		Context: req.GetContext(),
		Filter: &workplacesv1.ExportBuildingsRequest_Filter{
			CreatedAfter: req.GetRedemption().GetOccurredAt(),
			OwnerId:      tenancy.Value(ctx, "coupon_id"),
			Labels:       annotations,
			Query:        envelope.GetDisplayName(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.workplaces.v1.BuildingQueryService/ExportBuildings")
	}
	for {
		received, err := exportBuildingsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.workplaces.v1.BuildingQueryService/ExportBuildings")
		}
		_ = received
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*promotionsv1.QuotePromotionResponse, len(req.GetRedemption().GetMeasurements()))
	for i, measurement := range req.GetRedemption().GetMeasurements() {
		group.Go(func() error {
			reply, err := s.promotionCalculationClient.QuotePromotion(groupCtx, &promotionsv1.QuotePromotionRequest{
				Context: req.GetContext(),
				Promotion: &promotionsv1.Promotion{
					Audit:  &commonv1.AuditInfo{UpdatedAt: measurement.GetObservedAt()},
					Owner:  &identityv1.UserRef{Email: req.GetDetails().GetContact().GetEmail()},
					Action: &promotionsv1.Promotion_Block{Block: &promotionsv1.PromotionBlockAction{Reason: req.GetReason()}},
				},
				Details: &promotionsv1.QuotePromotionRequest_Details{
					CurrencyCode: req.GetRedemption().GetSubject().GetCurrencyCode(),
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
		return nil, grpcx.Wrap(err, "acme.promotions.v1.PromotionCalculationService/QuotePromotion")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	return &promotionsv1.NormalizeRedemptionResponse{
		Redemption: &promotionsv1.Redemption{
			RedemptionId: req.GetRedemptionId(),
			Source:       req.GetRedemption().GetSource(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ExplainRedemptionDecision explains a redemption.
func (s *RedemptionValidationServer) ExplainRedemptionDecision(ctx context.Context, req *promotionsv1.ExplainRedemptionDecisionRequest) (*promotionsv1.ExplainRedemptionDecisionResponse, error) {
	if err := s.store.SaveRedemption(ctx, req.GetContext().GetTenantId(), req.GetRedemption()); err != nil {
		return nil, grpcx.Wrap(err, "save redemption")
	}
	s.log.InfoContext(ctx, "explain redemption decision", "tenant", req.GetContext().GetTenantId())
	var envelope promotionsv1.PromotionRef
	if err := req.GetRedemption().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	step := &redemptionValidationServerExplainRedemptionDecisionQuotePromotionStep{request: &promotionsv1.QuotePromotionRequest{
		Context: req.GetContext(),
		Promotion: &promotionsv1.Promotion{
			Action: &promotionsv1.Promotion_Block{Block: &promotionsv1.PromotionBlockAction{Reason: req.GetReason()}},
			Owner: &identityv1.UserRef{
				Email:       req.GetDetails().GetContact().GetEmail(),
				DisplayName: req.GetRedemption().GetSubject().GetDisplayName(),
			},
		},
		Annotations: req.GetAnnotations(),
		PromotionId: envelope.GetDisplayName(),
	}}
	if err := s.runExplainRedemptionDecisionQuotePromotion(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.PromotionCalculationService/QuotePromotion")
	}
	quotePromotionReply := step.reply
	browseCoursesRequest := &learningv1.BrowseCoursesRequest{
		Context: req.GetContext(),
		Filter: &learningv1.BrowseCoursesRequest_Filter{
			CreatedAfter: quotePromotionReply.GetPromotion().GetAudit().GetCreatedAt(),
			OwnerId:      req.GetRedemption().GetRedemptionId(),
			Labels:       quotePromotionReply.GetPromotion().GetParameters(),
		},
	}
	send := s.browseCoursesSender(browseCoursesRequest)
	browseCoursesReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.learning.v1.CourseQueryService/BrowseCourses")
	}
	return &promotionsv1.ExplainRedemptionDecisionResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds:  quotePromotionReply.GetResult().GetReferenceIds(),
			CorrelationId: browseCoursesReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Outcome: &promotionsv1.ExplainRedemptionDecisionResponse_Outcome{Status: browseCoursesReply.GetResult().GetStatus()},
		Redemption: &promotionsv1.Redemption{
			RedemptionId: req.GetRedemptionId(),
			Attributes:   req.GetRedemption().GetAttributes(),
		},
	}, nil
}

// ScoreRedemption scores a redemption.
func (s *RedemptionValidationServer) ScoreRedemption(ctx context.Context, req *promotionsv1.ScoreRedemptionRequest) (*promotionsv1.ScoreRedemptionResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveRedemption(ctx, req.GetContext().GetTenantId(), req.GetRedemption()); err != nil {
		return nil, grpcx.Wrap(err, "save redemption")
	}
	s.log.InfoContext(ctx, "score redemption", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetRedemption()).(*promotionsv1.Redemption)
	var envelope promotionsv1.PromotionRef
	if err := req.GetRedemption().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	lintCouponsRequest := &promotionsv1.LintCouponsRequest{}
	lintCouponsRequest.Context = req.GetContext()
	lintCouponsRequest.Coupon = &promotionsv1.Coupon{
		CouponId:     req.GetRedemption().GetSubject().GetCouponId(),
		CurrencyCode: req.GetRedemption().GetSubject().GetCurrencyCode(),
		Holder:       &promotionsv1.PromotionRef{DisplayName: cloned.GetSubject().GetDisplayName()},
	}
	lintCouponsRequest.Details = &promotionsv1.LintCouponsRequest_Details{
		Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
	}
	lintCouponsRequest.CouponId = envelope.GetDisplayName()
	lintCouponsReply, err := s.couponValidationClient.LintCoupons(ctx, lintCouponsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CouponValidationService/LintCoupons")
	}
	ctx = tenancy.With(ctx, "locality", req.GetDetails().GetDestination().GetLocality())
	var envelope2 promotionsv1.PromotionRef
	if err := req.GetRedemption().GetDetail().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	createWarehouseRequest := &warehousingv1.CreateWarehouseRequest{
		Context: req.GetContext(),
		Warehouse: &warehousingv1.Warehouse{
			Locator: &warehousingv1.Warehouse_Address{Address: &commonv1.PostalAddress{Locality: tenancy.Value(ctx, "locality")}},
			Audit: &commonv1.AuditInfo{
				CreatedAt: lintCouponsReply.GetCoupon().GetAudit().GetCreatedAt(),
			},
			WarehouseId: envelope2.GetDisplayName(),
		},
	}
	createWarehouseReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*warehousingv1.CreateWarehouseResponse, error) {
		return s.warehouseClient.CreateWarehouse(ctx, createWarehouseRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.WarehouseService/CreateWarehouse")
	}
	_ = createWarehouseReply
	return &promotionsv1.ScoreRedemptionResponse{
		Redemption: &promotionsv1.Redemption{
			CorrelationId: lintCouponsReply.GetResult().GetCorrelationId(),
			RedemptionId:  req.GetRedemptionId(),
		},
		Result: &commonv1.OperationResult{
			Message: lintCouponsReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Breakdown: &promotionsv1.ScoreRedemptionResponse_Breakdown{CurrencyCode: req.GetDetails().GetCurrencyCode()},
	}, nil
}

// LintRedemptions lints a redemption.
func (s *RedemptionValidationServer) LintRedemptions(ctx context.Context, req *promotionsv1.LintRedemptionsRequest) (*promotionsv1.LintRedemptionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected LintRedemptions", "error", err)
		return nil, err
	}
	if err := s.store.SaveRedemption(ctx, req.GetContext().GetTenantId(), req.GetRedemption()); err != nil {
		return nil, grpcx.Wrap(err, "save redemption")
	}
	s.log.InfoContext(ctx, "lint redemptions", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetDetails()).(*promotionsv1.LintRedemptionsRequest_Details)
	var envelope promotionsv1.PromotionRef
	if err := req.GetRedemption().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	step := &redemptionValidationServerLintRedemptionsExplainReplenishmentPlanDecisionStep{request: &inventoryv1.ExplainReplenishmentPlanDecisionRequest{
		Context: req.GetContext(),
		Details: &inventoryv1.ExplainReplenishmentPlanDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{
				Phone: req.GetDetails().GetContact().GetPhone(),
				Email: cloned.GetContact().GetEmail(),
				Name:  req.GetDetails().GetContact().GetName(),
			},
		},
		ReplenishmentPlanId: envelope.GetDisplayName(),
	}}
	if err := s.runLintRedemptionsExplainReplenishmentPlanDecision(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.ReplenishmentPlanValidationService/ExplainReplenishmentPlanDecision")
	}
	explainReplenishmentPlanDecisionReply := step.reply
	return &promotionsv1.LintRedemptionsResponse{
		Outcome: &promotionsv1.LintRedemptionsResponse_Outcome{
			ReferenceId: explainReplenishmentPlanDecisionReply.GetOutcome().GetReferenceId(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: explainReplenishmentPlanDecisionReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Redemption: &promotionsv1.Redemption{
			RedemptionId: req.GetRedemptionId(),
			Kind:         req.GetRedemption().GetKind(),
		},
	}, nil
}

// GetRedemptionRules returns a redemption.
func (s *RedemptionValidationServer) GetRedemptionRules(ctx context.Context, req *promotionsv1.GetRedemptionRulesRequest) (*promotionsv1.GetRedemptionRulesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "redemption", req.GetRedemptionId(), "get_redemption_rules"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get redemption rules", "tenant", req.GetContext().GetTenantId())
	source := req.GetContext().GetActor().ProtoReflect()
	userId := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("user_id"))).String()
	checkBackorderEligibilityRequest := &ordersv1.CheckBackorderEligibilityRequest{
		Context: req.GetContext(),
		Backorder: &ordersv1.Backorder{
			Assignee:  &identityv1.UserRef{UserId: userId},
			Requester: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Origin:    &ordersv1.Backorder_ExternalReference{ExternalReference: req.GetRedemptionId()},
		},
		Details: &ordersv1.CheckBackorderEligibilityRequest_Details{Locale: req.GetContext().GetLocale()},
	}
	checkBackorderEligibilityReply, err := s.checkBackorderEligibilityFn(ctx, checkBackorderEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.BackorderValidationService/CheckBackorderEligibility")
	}
	return &promotionsv1.GetRedemptionRulesResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: checkBackorderEligibilityReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Redemption: &promotionsv1.Redemption{RedemptionId: req.GetRedemptionId()},
	}, nil
}

// redemptionValidationServerExplainRedemptionDecisionQuotePromotionStep carries the request of one step of
// ExplainRedemptionDecision and, once it ran, its reply.
type redemptionValidationServerExplainRedemptionDecisionQuotePromotionStep struct {
	request *promotionsv1.QuotePromotionRequest
	reply   *promotionsv1.QuotePromotionResponse
}

func (s *RedemptionValidationServer) runExplainRedemptionDecisionQuotePromotion(ctx context.Context, step *redemptionValidationServerExplainRedemptionDecisionQuotePromotionStep) error {
	reply, err := s.promotionCalculationClient.QuotePromotion(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}

// browseCoursesSender returns a function that sends one request to acme.learning.v1.CourseQueryService/BrowseCourses.
func (s *RedemptionValidationServer) browseCoursesSender(request *learningv1.BrowseCoursesRequest) func(context.Context) (*learningv1.BrowseCoursesResponse, error) {
	return func(ctx context.Context) (*learningv1.BrowseCoursesResponse, error) {
		return s.courseQueryClient.BrowseCourses(ctx, request)
	}
}

// redemptionValidationServerLintRedemptionsExplainReplenishmentPlanDecisionStep carries the request of one step of
// LintRedemptions and, once it ran, its reply.
type redemptionValidationServerLintRedemptionsExplainReplenishmentPlanDecisionStep struct {
	request *inventoryv1.ExplainReplenishmentPlanDecisionRequest
	reply   *inventoryv1.ExplainReplenishmentPlanDecisionResponse
}

func (s *RedemptionValidationServer) runLintRedemptionsExplainReplenishmentPlanDecision(ctx context.Context, step *redemptionValidationServerLintRedemptionsExplainReplenishmentPlanDecisionStep) error {
	reply, err := s.replenishmentPlanValidationClient.ExplainReplenishmentPlanDecision(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
