package fraud

import (
	"context"
	"log/slog"

	admissionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/admissions/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	deliveriesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/deliveries/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	fulfillmentv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fulfillment/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	inventoryv1 "github.com/hounddogai/protomap-test-golang/gen/acme/inventory/v1"
	licensingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/licensing/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// FraudRuleCalculationServer implements `acme.fraud.v1.FraudRuleCalculationService`.
// Computes the figures of a fraud rule, such as totals, taxes, and estimates.
type FraudRuleCalculationServer struct {
	fraudv1.UnimplementedFraudRuleCalculationServiceServer
	store                                 *Store
	log                                   *slog.Logger
	userClient                            identityv1.UserServiceClient
	riskAssessmentCalculationClient       fraudv1.RiskAssessmentCalculationServiceClient
	conn                                  *grpc.ClientConn
	userValidationClient                  identityv1.UserValidationServiceClient
	meteredStockMovementValidation        meteredStockMovementValidation
	deliveryClient                        deliveriesv1.DeliveryServiceClient
	softwareLicenseQueryClient            licensingv1.SoftwareLicenseQueryServiceClient
	fulfillmentQueryClient                fulfillmentv1.FulfillmentQueryServiceClient
	fulfillmentQueryClientFallback        fulfillmentv1.FulfillmentQueryServiceClient
	sellerPayoutScheduleCalculationClient sellersv1.SellerPayoutScheduleCalculationServiceClient
	riskAssessmentValidationClient        fraudv1.RiskAssessmentValidationServiceClient
	carrierRateValidationClient           shippingv1.CarrierRateValidationServiceClient
	admissionQueryClient                  admissionsv1.AdmissionQueryServiceClient
}

// NewFraudRuleCalculationServer wires a FraudRuleCalculationServer.
func NewFraudRuleCalculationServer(deps platform.Deps) *FraudRuleCalculationServer {
	return &FraudRuleCalculationServer{
		store:                                 NewStore(deps.DB),
		log:                                   deps.Logger.With("service", "acme.fraud.v1.FraudRuleCalculationService"),
		userClient:                            identityv1.NewUserServiceClient(deps.Conn),
		riskAssessmentCalculationClient:       fraudv1.NewRiskAssessmentCalculationServiceClient(deps.Conn),
		conn:                                  deps.Conn,
		userValidationClient:                  identityv1.NewUserValidationServiceClient(deps.Conn),
		meteredStockMovementValidation:        meteredStockMovementValidation{StockMovementValidationServiceClient: inventoryv1.NewStockMovementValidationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		deliveryClient:                        deliveriesv1.NewDeliveryServiceClient(deps.Conn),
		softwareLicenseQueryClient:            licensingv1.NewSoftwareLicenseQueryServiceClient(deps.Conn),
		fulfillmentQueryClient:                fulfillmentv1.NewFulfillmentQueryServiceClient(deps.Conn),
		fulfillmentQueryClientFallback:        fulfillmentv1.NewFulfillmentQueryServiceClient(deps.Conn),
		sellerPayoutScheduleCalculationClient: sellersv1.NewSellerPayoutScheduleCalculationServiceClient(deps.Conn),
		riskAssessmentValidationClient:        fraudv1.NewRiskAssessmentValidationServiceClient(deps.Conn),
		carrierRateValidationClient:           shippingv1.NewCarrierRateValidationServiceClient(deps.Conn),
		admissionQueryClient:                  admissionsv1.NewAdmissionQueryServiceClient(deps.Conn),
	}
}

// CalculateFraudRule calculates a fraud rule.
func (s *FraudRuleCalculationServer) CalculateFraudRule(ctx context.Context, req *fraudv1.CalculateFraudRuleRequest) (*fraudv1.CalculateFraudRuleResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveFraudRule(ctx, req.GetContext().GetTenantId(), req.GetFraudRule()); err != nil {
		return nil, grpcx.Wrap(err, "save fraud rule")
	}
	s.log.InfoContext(ctx, "calculate fraud rule", "tenant", req.GetContext().GetTenantId(), "etag", req.GetFraudRule().GetAudit().GetEtag())
	getUserRequest := &identityv1.GetUserRequest{}
	getUserRequest.Context = req.GetContext()
	getUserRequest.UserId = req.GetFraudRule().GetOwner().GetUserId()
	getUserReply, err := s.userClient.GetUser(ctx, getUserRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserService/GetUser")
	}
	calculateRiskAssessmentRequest := buildCalculateRiskAssessmentRequestForCalculateFraudRule(req)
	calculateRiskAssessmentReply, err := s.riskAssessmentCalculationClient.CalculateRiskAssessment(ctx, calculateRiskAssessmentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.RiskAssessmentCalculationService/CalculateRiskAssessment")
	}
	return &fraudv1.CalculateFraudRuleResponse{
		FraudRule: &fraudv1.FraudRule{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: getUserReply.GetUser().GetAudit().GetUpdatedAt(),
				CreatedAt: getUserReply.GetUser().GetAudit().GetCreatedAt(),
			},
			FraudRuleId: req.GetFraudRuleId(),
			Version:     req.GetFraudRule().GetVersion(),
		},
		Result: &commonv1.OperationResult{
			Status: calculateRiskAssessmentReply.GetResult().GetStatus(),
		},
	}, nil
}

// EstimateFraudRule estimates a fraud rule.
func (s *FraudRuleCalculationServer) EstimateFraudRule(ctx context.Context, req *fraudv1.EstimateFraudRuleRequest) (*fraudv1.EstimateFraudRuleResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveFraudRule(ctx, req.GetContext().GetTenantId(), req.GetFraudRule()); err != nil {
		return nil, grpcx.Wrap(err, "save fraud rule")
	}
	s.log.InfoContext(ctx, "estimate fraud rule", "tenant", req.GetContext().GetTenantId(), "etag", req.GetFraudRule().GetAudit().GetEtag())
	simulateRiskAssessmentRequest := &fraudv1.SimulateRiskAssessmentRequest{
		Context: req.GetContext(),
		RiskAssessment: &fraudv1.RiskAssessment{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: req.GetFraudRule().GetAudit().GetUpdatedBy(),
				CreatedAt: req.GetFraudRule().GetAudit().GetCreatedAt(),
			},
			Assignee: &identityv1.UserRef{Email: req.GetFraudRule().GetOwner().GetEmail()},
		},
	}
	simulateRiskAssessmentReply := new(fraudv1.SimulateRiskAssessmentResponse)
	if err := grpcx.LegacyInvoke(ctx, s.conn, "/acme.fraud.v1.RiskAssessmentCalculationService/SimulateRiskAssessment", simulateRiskAssessmentRequest, simulateRiskAssessmentReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.RiskAssessmentCalculationService/SimulateRiskAssessment")
	}
	packed, err := anypb.New(req.GetFraudRule())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	sendSearchQueryTestNoticeRequest := &searchv1.SendSearchQueryTestNoticeRequest{
		Context: req.GetContext(),
		Details: &searchv1.SendSearchQueryTestNoticeRequest_Details{
			Recipient: &commonv1.ContactInfo{
				Phone:            simulateRiskAssessmentReply.GetRiskAssessment().GetRequester().GetPhone(),
				Email:            simulateRiskAssessmentReply.GetRiskAssessment().GetRequester().GetEmail(),
				PreferredChannel: simulateRiskAssessmentReply.GetRiskAssessment().GetRequester().GetPreferredChannel(),
			},
			Payload: packed,
		},
		Reason: req.GetFraudRule().GetBlock().GetReason(),
	}
	sendSearchQueryTestNoticeReply := new(searchv1.SendSearchQueryTestNoticeResponse)
	if err := s.conn.Invoke(ctx, searchSearchQueryNotificationServiceSendSearchQueryTestNoticeMethod, sendSearchQueryTestNoticeRequest, sendSearchQueryTestNoticeReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryNotificationService/SendSearchQueryTestNotice")
	}
	scoreUserRequest := &identityv1.ScoreUserRequest{
		Context: req.GetContext(),
		User: &identityv1.User{
			Contact: &commonv1.ContactInfo{
				Name: simulateRiskAssessmentReply.GetRiskAssessment().GetRequester().GetName(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: simulateRiskAssessmentReply.GetRiskAssessment().GetAudit().GetUpdatedAt(),
			},
			RelationshipManager: &identityv1.UserRef{
				Email: simulateRiskAssessmentReply.GetRiskAssessment().GetRequester().GetEmail(),
			},
		},
	}
	scoreUserReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*identityv1.ScoreUserResponse, error) {
		return s.userValidationClient.ScoreUser(ctx, scoreUserRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserValidationService/ScoreUser")
	}
	scoreStockMovementRequest := &inventoryv1.ScoreStockMovementRequest{
		Context: req.GetContext(),
		Details: &inventoryv1.ScoreStockMovementRequest_Details{
			Destination: &commonv1.PostalAddress{
				Locality: scoreUserReply.GetUser().GetPrimaryAddress().GetLocality(),
			},
		},
		StockMovement: &inventoryv1.StockMovement{
			Subject: &customersv1.CustomerRef{
				Email: simulateRiskAssessmentReply.GetRiskAssessment().GetRequester().GetEmail(),
			},
		},
	}
	scoreStockMovementReply, err := s.meteredStockMovementValidation.ScoreStockMovement(ctx, scoreStockMovementRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.StockMovementValidationService/ScoreStockMovement")
	}
	return &fraudv1.EstimateFraudRuleResponse{
		FraudRule: &fraudv1.FraudRule{
			Audit: &commonv1.AuditInfo{
				CreatedBy: simulateRiskAssessmentReply.GetRiskAssessment().GetAudit().GetCreatedBy(),
				UpdatedBy: simulateRiskAssessmentReply.GetRiskAssessment().GetAudit().GetUpdatedBy(),
			},
			Owner: &identityv1.UserRef{
				UserId: scoreUserReply.GetUser().GetRelationshipManager().GetUserId(),
			},
			FraudRuleId: req.GetFraudRuleId(),
			Parameters:  req.GetFraudRule().GetParameters(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: sendSearchQueryTestNoticeReply.GetResult().GetReferenceIds(),
			Status:       scoreStockMovementReply.GetResult().GetStatus(),
		},
		Breakdown: &fraudv1.EstimateFraudRuleResponse_Breakdown{
			CurrencyCode: scoreStockMovementReply.GetBreakdown().GetCurrencyCode(),
		},
	}, nil
}

// QuoteFraudRule quotes a fraud rule.
func (s *FraudRuleCalculationServer) QuoteFraudRule(ctx context.Context, req *fraudv1.QuoteFraudRuleRequest) (*fraudv1.QuoteFraudRuleResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveFraudRule(ctx, req.GetContext().GetTenantId(), req.GetFraudRule()); err != nil {
		return nil, grpcx.Wrap(err, "save fraud rule")
	}
	s.log.InfoContext(ctx, "quote fraud rule", "tenant", req.GetContext().GetTenantId(), "etag", req.GetFraudRule().GetAudit().GetEtag())
	createDeliveryReply, err := s.deliveryClient.CreateDelivery(ctx, &deliveriesv1.CreateDeliveryRequest{
		Context: req.GetContext(),
		Delivery: &deliveriesv1.Delivery{
			Counterparty: &deliveriesv1.DeliveryExceptionRef{DisplayName: req.GetFraudRule().GetOwner().GetDisplayName()},
			Audit:        &commonv1.AuditInfo{UpdatedAt: req.GetFraudRule().GetAudit().GetUpdatedAt()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.deliveries.v1.DeliveryService/CreateDelivery")
	}
	return &fraudv1.QuoteFraudRuleResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: createDeliveryReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		FraudRule: &fraudv1.FraudRule{
			FraudRuleId: req.GetFraudRuleId(),
			Parameters:  req.GetFraudRule().GetParameters(),
		},
	}, nil
}

// CompareFraudRuleOptions compares a fraud rule.
func (s *FraudRuleCalculationServer) CompareFraudRuleOptions(ctx context.Context, req *fraudv1.CompareFraudRuleOptionsRequest) (*fraudv1.CompareFraudRuleOptionsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveFraudRule(ctx, req.GetContext().GetTenantId(), req.GetFraudRule()); err != nil {
		return nil, grpcx.Wrap(err, "save fraud rule")
	}
	s.log.InfoContext(ctx, "compare fraud rule options", "tenant", req.GetContext().GetTenantId(), "etag", req.GetFraudRule().GetAudit().GetEtag())
	recalculateRiskAssessmentsRequest := &fraudv1.RecalculateRiskAssessmentsRequest{
		Context: req.GetContext(),
		RiskAssessment: &fraudv1.RiskAssessment{
			Assignee: &identityv1.UserRef{Email: req.GetFraudRule().GetOwner().GetEmail()},
			Audit: &commonv1.AuditInfo{
				CreatedAt: req.GetFraudRule().GetAudit().GetCreatedAt(),
				Etag:      req.GetFraudRule().GetAudit().GetEtag(),
				UpdatedAt: req.GetFraudRule().GetAudit().GetUpdatedAt(),
			},
		},
	}
	type recalculateRiskAssessmentsResult struct {
		reply *fraudv1.RecalculateRiskAssessmentsResponse
		err   error
	}
	recalculateRiskAssessmentsResults := make(chan recalculateRiskAssessmentsResult, 1)
	go func() {
		reply, err := s.riskAssessmentCalculationClient.RecalculateRiskAssessments(ctx, recalculateRiskAssessmentsRequest)
		recalculateRiskAssessmentsResults <- recalculateRiskAssessmentsResult{reply, err}
	}()
	var recalculateRiskAssessmentsReply *fraudv1.RecalculateRiskAssessmentsResponse
	select {
	case result := <-recalculateRiskAssessmentsResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.fraud.v1.RiskAssessmentCalculationService/RecalculateRiskAssessments")
		}
		recalculateRiskAssessmentsReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	validateReturnLabelRequest := &returnsv1.ValidateReturnLabelRequest{
		Context: req.GetContext(),
		Details: &returnsv1.ValidateReturnLabelRequest_Details{
			Contact: &commonv1.ContactInfo{
				Name: recalculateRiskAssessmentsReply.GetRiskAssessment().GetRequester().GetName(),
			},
		},
	}
	validateReturnLabelReply := new(returnsv1.ValidateReturnLabelResponse)
	if err := grpcx.Invoke(ctx, s.conn, returnsv1.ReturnLabelValidationService_ValidateReturnLabel_FullMethodName, validateReturnLabelRequest, validateReturnLabelReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnLabelValidationService/ValidateReturnLabel")
	}
	suggestSoftwareLicensesReply, err := s.softwareLicenseQueryClient.SuggestSoftwareLicenses(ctx, &licensingv1.SuggestSoftwareLicensesRequest{
		Context: req.GetContext(),
		Filter: &licensingv1.SuggestSoftwareLicensesRequest_Filter{
			Region:       validateReturnLabelReply.GetReturnLabel().GetBillingAddress().GetRegion(),
			CreatedAfter: validateReturnLabelReply.GetReturnLabel().GetAudit().GetUpdatedAt(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.licensing.v1.SoftwareLicenseQueryService/SuggestSoftwareLicenses")
	}
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := fraudRuleCalculationServerQuoteRiskAssessmentSteps[mode]
	quoteRiskAssessmentRequest := &fraudv1.QuoteRiskAssessmentRequest{
		Context: req.GetContext(),
		RiskAssessment: &fraudv1.RiskAssessment{
			Requester: &commonv1.ContactInfo{
				Email: recalculateRiskAssessmentsReply.GetRiskAssessment().GetAssignee().GetEmail(),
			},
		},
	}
	quoteRiskAssessmentReply, err := run(s, ctx, quoteRiskAssessmentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.RiskAssessmentCalculationService/QuoteRiskAssessment")
	}
	_ = quoteRiskAssessmentReply
	return &fraudv1.CompareFraudRuleOptionsResponse{
		FraudRule: &fraudv1.FraudRule{
			Owner: &identityv1.UserRef{
				Email: recalculateRiskAssessmentsReply.GetRiskAssessment().GetRequester().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{
				Etag: validateReturnLabelReply.GetReturnLabel().GetAudit().GetEtag(),
			},
			FraudRuleId: req.GetFraudRuleId(),
			Status:      req.GetFraudRule().GetStatus(),
		},
		Result: &commonv1.OperationResult{
			Message: validateReturnLabelReply.GetResult().GetMessage(),
			Status:  suggestSoftwareLicensesReply.GetResult().GetStatus(),
		},
	}, nil
}

// RecalculateFraudRules recalculates a fraud rule.
func (s *FraudRuleCalculationServer) RecalculateFraudRules(ctx context.Context, req *fraudv1.RecalculateFraudRulesRequest) (*fraudv1.RecalculateFraudRulesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveFraudRule(ctx, req.GetContext().GetTenantId(), req.GetFraudRule()); err != nil {
		return nil, grpcx.Wrap(err, "save fraud rule")
	}
	s.log.InfoContext(ctx, "recalculate fraud rules", "tenant", req.GetContext().GetTenantId(), "etag", req.GetFraudRule().GetAudit().GetEtag())
	routes := map[string]func(context.Context, *fulfillmentv1.SummarizeFulfillmentsRequest, ...grpc.CallOption) (*fulfillmentv1.SummarizeFulfillmentsResponse, error){
		"primary":  s.fulfillmentQueryClient.SummarizeFulfillments,
		"fallback": s.fulfillmentQueryClientFallback.SummarizeFulfillments,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	var reason string
	switch choice := req.GetFraudRule().GetAction().(type) {
	case *fraudv1.FraudRule_Route:
		reason = choice.Route.GetTarget()
	case *fraudv1.FraudRule_Block:
		reason = choice.Block.GetReason()
	}
	summarizeFulfillmentsRequest := &fulfillmentv1.SummarizeFulfillmentsRequest{
		Context: req.GetContext(),
		Reason:  reason,
	}
	summarizeFulfillmentsReply, err := route(ctx, summarizeFulfillmentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.FulfillmentQueryService/SummarizeFulfillments")
	}
	ctx = tenancy.With(ctx, "country_code", req.GetDetails().GetDestination().GetCountryCode())
	quoteSellerPayoutScheduleReply, err := s.sellerPayoutScheduleCalculationClient.QuoteSellerPayoutSchedule(ctx, &sellersv1.QuoteSellerPayoutScheduleRequest{
		Context: req.GetContext(),
		Details: &sellersv1.QuoteSellerPayoutScheduleRequest_Details{
			Destination: &commonv1.PostalAddress{CountryCode: tenancy.Value(ctx, "country_code")},
		},
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
			Location: &commonv1.PostalAddress{
				Recipient: req.GetDetails().GetDestination().GetRecipient(),
				Region:    req.GetDetails().GetDestination().GetRegion(),
			},
			Audit: &commonv1.AuditInfo{CreatedAt: req.GetFraudRule().GetAudit().GetCreatedAt()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerPayoutScheduleCalculationService/QuoteSellerPayoutSchedule")
	}
	return &fraudv1.RecalculateFraudRulesResponse{
		Result: &commonv1.OperationResult{
			Message:       summarizeFulfillmentsReply.GetResult().GetMessage(),
			CorrelationId: quoteSellerPayoutScheduleReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		FraudRule: &fraudv1.FraudRule{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: quoteSellerPayoutScheduleReply.GetSellerPayoutSchedule().GetAudit().GetUpdatedAt(),
			},
			FraudRuleId: req.GetFraudRuleId(),
			Description: req.GetFraudRule().GetDescription(),
		},
	}, nil
}

// SimulateFraudRule simulates a fraud rule.
func (s *FraudRuleCalculationServer) SimulateFraudRule(ctx context.Context, req *fraudv1.SimulateFraudRuleRequest) (*fraudv1.SimulateFraudRuleResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SimulateFraudRule", "error", err)
		return nil, err
	}
	if err := s.store.SaveFraudRule(ctx, req.GetContext().GetTenantId(), req.GetFraudRule()); err != nil {
		return nil, grpcx.Wrap(err, "save fraud rule")
	}
	s.log.InfoContext(ctx, "simulate fraud rule", "tenant", req.GetContext().GetTenantId(), "etag", req.GetFraudRule().GetAudit().GetEtag())
	var holder struct{ Recipient string }
	if record := req.GetDetails().GetDestination(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Recipient"); err != nil {
			s.log.WarnContext(ctx, "copy recipient", "error", err)
		}
	}
	scoreUserRequest := &identityv1.ScoreUserRequest{
		Context: req.GetContext(),
		User: &identityv1.User{
			PrimaryAddress: &commonv1.PostalAddress{
				Recipient:  holder.Recipient,
				PostalCode: req.GetDetails().GetDestination().GetPostalCode(),
			},
			Audit: &commonv1.AuditInfo{Etag: req.GetFraudRule().GetAudit().GetEtag()},
		},
	}
	var scoreUserReply *identityv1.ScoreUserResponse
	if err := pipeline.Run(ctx, "acme.identity.v1.UserValidationService/ScoreUser", func(ctx context.Context) error {
		answer, err := s.userValidationClient.ScoreUser(ctx, scoreUserRequest)
		scoreUserReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserValidationService/ScoreUser")
	}
	normalizeRiskAssessmentRequest := &fraudv1.NormalizeRiskAssessmentRequest{}
	normalizeRiskAssessmentRequest.Context = req.GetContext()
	normalizeRiskAssessmentRequest.RiskAssessment = &fraudv1.RiskAssessment{
		Requester: &commonv1.ContactInfo{
			PreferredChannel: scoreUserReply.GetUser().GetContact().GetPreferredChannel(),
		},
	}
	normalizeRiskAssessmentReply, err := s.riskAssessmentValidationClient.NormalizeRiskAssessment(ctx, normalizeRiskAssessmentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.RiskAssessmentValidationService/NormalizeRiskAssessment")
	}
	return &fraudv1.SimulateFraudRuleResponse{
		FraudRule: &fraudv1.FraudRule{
			Owner: &identityv1.UserRef{
				UserId:      scoreUserReply.GetUser().GetUserId(),
				Email:       normalizeRiskAssessmentReply.GetRiskAssessment().GetCustomer().GetEmail(),
				DisplayName: normalizeRiskAssessmentReply.GetRiskAssessment().GetAssignee().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedAt: scoreUserReply.GetUser().GetAudit().GetCreatedAt(),
			},
			FraudRuleId: req.GetFraudRuleId(),
			Version:     req.GetFraudRule().GetVersion(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ExplainFraudRuleCalculation explains a fraud rule.
func (s *FraudRuleCalculationServer) ExplainFraudRuleCalculation(ctx context.Context, req *fraudv1.ExplainFraudRuleCalculationRequest) (*fraudv1.ExplainFraudRuleCalculationResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveFraudRule(ctx, req.GetContext().GetTenantId(), req.GetFraudRule()); err != nil {
		return nil, grpcx.Wrap(err, "save fraud rule")
	}
	s.log.InfoContext(ctx, "explain fraud rule calculation", "tenant", req.GetContext().GetTenantId(), "etag", req.GetFraudRule().GetAudit().GetEtag())
	for condition := range iterx.Each(req.GetFraudRule().GetConditions(), func(candidate *fraudv1.FraudRuleCondition) bool { return candidate != nil }) {
		if _, err := s.carrierRateValidationClient.LintCarrierRates(ctx, &shippingv1.LintCarrierRatesRequest{
			Context: req.GetContext(),
			CarrierRate: &shippingv1.CarrierRate{
				Conditions: []*shippingv1.CarrierRateCondition{&shippingv1.CarrierRateCondition{
					Attribute: condition.GetAttribute(),
					Operator:  condition.GetOperator(),
				}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.shipping.v1.CarrierRateValidationService/LintCarrierRates")
		}
	}
	estimateRiskAssessmentRequest := &fraudv1.EstimateRiskAssessmentRequest{
		Context: req.GetContext(),
		RiskAssessment: &fraudv1.RiskAssessment{
			Requester: &commonv1.ContactInfo{Email: req.GetFraudRule().GetOwner().GetEmail()},
			Assignee:  &identityv1.UserRef{UserId: req.GetFraudRule().GetOwner().GetUserId()},
		},
	}
	estimateRiskAssessmentReply, err := grpcx.NewClient(s.conn, fraudv1.NewRiskAssessmentCalculationServiceClient).EstimateRiskAssessment(ctx, estimateRiskAssessmentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.RiskAssessmentCalculationService/EstimateRiskAssessment")
	}
	var reason string
	switch choice := req.GetFraudRule().GetAction().(type) {
	case *fraudv1.FraudRule_Route:
		reason = choice.Route.GetTarget()
	case *fraudv1.FraudRule_Block:
		reason = choice.Block.GetReason()
	}
	summarizeAdmissionsRequest := &admissionsv1.SummarizeAdmissionsRequest{
		Context: req.GetContext(),
		Reason:  reason,
	}
	summarizeAdmissionsReply, err := grpcx.Call(ctx, s.admissionQueryClient.SummarizeAdmissions, summarizeAdmissionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.admissions.v1.AdmissionQueryService/SummarizeAdmissions")
	}
	return &fraudv1.ExplainFraudRuleCalculationResponse{
		FraudRule: &fraudv1.FraudRule{
			Owner: &identityv1.UserRef{
				Email:       estimateRiskAssessmentReply.GetRiskAssessment().GetAssignee().GetEmail(),
				DisplayName: estimateRiskAssessmentReply.GetRiskAssessment().GetCustomer().GetDisplayName(),
			},
			FraudRuleId: req.GetFraudRuleId(),
			Status:      req.GetFraudRule().GetStatus(),
		},
		Result: &commonv1.OperationResult{
			Message:      summarizeAdmissionsReply.GetResult().GetMessage(),
			ReferenceIds: summarizeAdmissionsReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// GetFraudRuleRates returns a fraud rule.
func (s *FraudRuleCalculationServer) GetFraudRuleRates(ctx context.Context, req *fraudv1.GetFraudRuleRatesRequest) (*fraudv1.GetFraudRuleRatesResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "fraud_rule", req.GetFraudRuleId(), "get_fraud_rule_rates"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get fraud rule rates", "tenant", req.GetContext().GetTenantId())
	estimateSellerPayoutScheduleRequest := &sellersv1.EstimateSellerPayoutScheduleRequest{
		Context: req.GetContext(),
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
			Participants: []*sellersv1.SellerPayoutScheduleParticipant{&sellersv1.SellerPayoutScheduleParticipant{
				Contact:       &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
				ParticipantId: req.GetFraudRuleId(),
			}},
			Organizer: &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
		},
	}
	estimateSellerPayoutScheduleReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*sellersv1.EstimateSellerPayoutScheduleResponse, error) {
		return s.sellerPayoutScheduleCalculationClient.EstimateSellerPayoutSchedule(ctx, estimateSellerPayoutScheduleRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerPayoutScheduleCalculationService/EstimateSellerPayoutSchedule")
	}
	source := req.GetContext().GetActor().ProtoReflect()
	email := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("email"))).String()
	restoreTenantRequest := &identityv1.RestoreTenantRequest{
		Context:  req.GetContext(),
		TenantId: req.GetContext().GetTenantId(),
		Details: &identityv1.RestoreTenantRequest_Details{
			NewOwner: &identityv1.UserRef{
				Email:       estimateSellerPayoutScheduleReply.GetSellerPayoutSchedule().GetOrganizer().GetEmail(),
				DisplayName: estimateSellerPayoutScheduleReply.GetSellerPayoutSchedule().GetOrganizer().GetDisplayName(),
				UserId:      estimateSellerPayoutScheduleReply.GetSellerPayoutSchedule().GetOrganizer().GetUserId(),
			},
			NotifyContact: &commonv1.ContactInfo{Email: email},
		},
	}
	restoreTenantReply := new(identityv1.RestoreTenantResponse)
	if err := grpcx.Invoke(ctx, s.conn, identityv1.TenantAdminService_RestoreTenant_FullMethodName, restoreTenantRequest, restoreTenantReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantAdminService/RestoreTenant")
	}
	return &fraudv1.GetFraudRuleRatesResponse{
		FraudRule: &fraudv1.FraudRule{
			Owner: &identityv1.UserRef{
				DisplayName: estimateSellerPayoutScheduleReply.GetSellerPayoutSchedule().GetOrganizer().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: restoreTenantReply.GetTenant().GetAudit().GetUpdatedBy(),
			},
			FraudRuleId: req.GetFraudRuleId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// fraudRuleCalculationServerQuoteRiskAssessmentSteps are the ways to run
// acme.fraud.v1.RiskAssessmentCalculationService/QuoteRiskAssessment, by name.
var fraudRuleCalculationServerQuoteRiskAssessmentSteps = map[string]func(*FraudRuleCalculationServer, context.Context, *fraudv1.QuoteRiskAssessmentRequest) (*fraudv1.QuoteRiskAssessmentResponse, error){
	"default": (*FraudRuleCalculationServer).quoteRiskAssessmentStep,
	"audited": (*FraudRuleCalculationServer).quoteRiskAssessmentAuditedStep,
}

func (s *FraudRuleCalculationServer) quoteRiskAssessmentStep(ctx context.Context, request *fraudv1.QuoteRiskAssessmentRequest) (*fraudv1.QuoteRiskAssessmentResponse, error) {
	return s.riskAssessmentCalculationClient.QuoteRiskAssessment(ctx, request)
}

func (s *FraudRuleCalculationServer) quoteRiskAssessmentAuditedStep(ctx context.Context, request *fraudv1.QuoteRiskAssessmentRequest) (*fraudv1.QuoteRiskAssessmentResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.fraud.v1.RiskAssessmentCalculationService/QuoteRiskAssessment")
	return s.riskAssessmentCalculationClient.QuoteRiskAssessment(ctx, request)
}
