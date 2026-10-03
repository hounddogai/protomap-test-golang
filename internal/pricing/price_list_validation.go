package pricing

import (
	"context"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	notificationsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/notifications/v1"
	pricingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/pricing/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// PriceListValidationServer implements `acme.pricing.v1.PriceListValidationService`.
// Checks price lists against the rules before anything acts on them.
type PriceListValidationServer struct {
	pricingv1.UnimplementedPriceListValidationServiceServer
	store                        *Store
	log                          *slog.Logger
	priceQuoteCalculationClient  pricingv1.PriceQuoteCalculationServiceClient
	clients                      *registry.Registry
	fraudSignalValidationClient  fraudv1.FraudSignalValidationServiceClient
	notificationQueryClient      notificationsv1.NotificationQueryServiceClient
	conn                         *grpc.ClientConn
	notificationValidationClient notificationsv1.NotificationValidationServiceClient
	bus                          *eventbus.Bus
}

// NewPriceListValidationServer wires a PriceListValidationServer.
func NewPriceListValidationServer(deps platform.Deps) *PriceListValidationServer {
	return &PriceListValidationServer{
		store:                        NewStore(deps.DB),
		log:                          deps.Logger.With("service", "acme.pricing.v1.PriceListValidationService"),
		priceQuoteCalculationClient:  pricingv1.NewPriceQuoteCalculationServiceClient(deps.Conn),
		clients:                      deps.Clients,
		fraudSignalValidationClient:  fraudv1.NewFraudSignalValidationServiceClient(deps.Conn),
		notificationQueryClient:      notificationsv1.NewNotificationQueryServiceClient(deps.Conn),
		conn:                         deps.Conn,
		notificationValidationClient: notificationsv1.NewNotificationValidationServiceClient(deps.Conn),
		bus:                          deps.Bus,
	}
}

// ValidatePriceList validates a price list.
func (s *PriceListValidationServer) ValidatePriceList(ctx context.Context, req *pricingv1.ValidatePriceListRequest) (*pricingv1.ValidatePriceListResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ValidatePriceList", "error", err)
		return nil, err
	}
	if err := s.store.SavePriceList(ctx, req.GetContext().GetTenantId(), req.GetPriceList()); err != nil {
		return nil, grpcx.Wrap(err, "save price list")
	}
	s.log.InfoContext(ctx, "validate price list", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPriceList().GetAudit().GetEtag())
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := priceListValidationServerSimulatePriceQuoteSteps[mode]
	simulatePriceQuoteRequest := &pricingv1.SimulatePriceQuoteRequest{
		Context: req.GetContext(),
		PriceQuote: &pricingv1.PriceQuote{
			Audit: &commonv1.AuditInfo{UpdatedBy: req.GetPriceList().GetAudit().GetUpdatedBy()},
		},
	}
	simulatePriceQuoteReply, err := run(s, ctx, simulatePriceQuoteRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceQuoteCalculationService/SimulatePriceQuote")
	}
	return &pricingv1.ValidatePriceListResponse{
		Outcome: &pricingv1.ValidatePriceListResponse_Outcome{Message: simulatePriceQuoteReply.GetResult().GetMessage()},
		PriceList: &pricingv1.PriceList{
			PriceListId: req.GetPriceListId(),
			Status:      req.GetPriceList().GetStatus(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// PreviewPriceList previews a price list.
func (s *PriceListValidationServer) PreviewPriceList(ctx context.Context, req *pricingv1.PreviewPriceListRequest) (*pricingv1.PreviewPriceListResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePriceList(ctx, req.GetContext().GetTenantId(), req.GetPriceList()); err != nil {
		return nil, grpcx.Wrap(err, "save price list")
	}
	s.log.InfoContext(ctx, "preview price list", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPriceList().GetAudit().GetEtag())
	priceQuoteCalculation, err := registry.Lookup[pricingv1.PriceQuoteCalculationServiceClient](s.clients, "acme.pricing.v1.PriceQuoteCalculationService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	explainPriceQuoteCalculationRequest := &pricingv1.ExplainPriceQuoteCalculationRequest{
		Context: req.GetContext(),
		PriceQuote: &pricingv1.PriceQuote{
			Lines: []*pricingv1.PriceQuoteLine{&pricingv1.PriceQuoteLine{
				Tax: &pricingv1.PriceQuoteLineTax{Amount: req.GetPriceList().GetDiscount().GetAmount()},
			}},
		},
	}
	explainPriceQuoteCalculationReply, err := priceQuoteCalculation.ExplainPriceQuoteCalculation(ctx, explainPriceQuoteCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceQuoteCalculationService/ExplainPriceQuoteCalculation")
	}
	return &pricingv1.PreviewPriceListResponse{
		Outcome: &pricingv1.PreviewPriceListResponse_Outcome{
			NextSteps: explainPriceQuoteCalculationReply.GetOutcome().GetNextSteps(),
		},
		PriceList: &pricingv1.PriceList{
			PriceListId: req.GetPriceListId(),
			Description: req.GetPriceList().GetDescription(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CheckPriceListEligibility checks a price list.
func (s *PriceListValidationServer) CheckPriceListEligibility(ctx context.Context, req *pricingv1.CheckPriceListEligibilityRequest) (*pricingv1.CheckPriceListEligibilityResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SavePriceList(ctx, req.GetContext().GetTenantId(), req.GetPriceList()); err != nil {
		return nil, grpcx.Wrap(err, "save price list")
	}
	s.log.InfoContext(ctx, "check price list eligibility", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPriceList().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "email", req.GetPriceList().GetOwner().GetEmail())
	normalizeFraudSignalRequest := newNormalizeFraudSignalRequest(withNormalizeFraudSignalContext(req.GetContext()), withNormalizeFraudSignalDetails(&fraudv1.NormalizeFraudSignalRequest_Details{
		Contact: &commonv1.ContactInfo{
			Email:            tenancy.Value(ctx, "email"),
			Name:             req.GetDetails().GetContact().GetName(),
			PreferredChannel: req.GetDetails().GetContact().GetPreferredChannel(),
		},
	}))
	normalizeFraudSignalReply, err := s.fraudSignalValidationClient.NormalizeFraudSignal(ctx, normalizeFraudSignalRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.FraudSignalValidationService/NormalizeFraudSignal")
	}
	findNotificationsRequest := &notificationsv1.FindNotificationsRequest{
		Context: req.GetContext(),
		Filter: &notificationsv1.FindNotificationsRequest_Filter{
			CreatedAfter: normalizeFraudSignalReply.GetOutcome().GetEffectiveAt(),
			OwnerId:      req.GetPriceList().GetOwner().GetUserId(),
			Labels:       req.GetPriceList().GetParameters(),
		},
	}
	findNotificationsReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*notificationsv1.FindNotificationsResponse, error) {
		return s.notificationQueryClient.FindNotifications(ctx, findNotificationsRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.notifications.v1.NotificationQueryService/FindNotifications")
	}
	return &pricingv1.CheckPriceListEligibilityResponse{
		Outcome: &pricingv1.CheckPriceListEligibilityResponse_Outcome{Status: normalizeFraudSignalReply.GetOutcome().GetStatus()},
		Result: &commonv1.OperationResult{
			CorrelationId: findNotificationsReply.GetResult().GetCorrelationId(),
			Details:       findNotificationsReply.GetResult().GetDetails(),
			Status:        "ok",
		},
		PriceList: &pricingv1.PriceList{
			PriceListId: req.GetPriceListId(),
			Effective:   req.GetPriceList().GetEffective(),
		},
	}, nil
}

// NormalizePriceList normalizes a price list.
func (s *PriceListValidationServer) NormalizePriceList(ctx context.Context, req *pricingv1.NormalizePriceListRequest) (*pricingv1.NormalizePriceListResponse, error) {
	if err := s.store.SavePriceList(ctx, req.GetContext().GetTenantId(), req.GetPriceList()); err != nil {
		return nil, grpcx.Wrap(err, "save price list")
	}
	s.log.InfoContext(ctx, "normalize price list", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPriceList().GetAudit().GetEtag())
	priceQuoteCalculation, err := registry.Lookup[pricingv1.PriceQuoteCalculationServiceClient](s.clients, "acme.pricing.v1.PriceQuoteCalculationService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	calculatePriceQuoteRequest := &pricingv1.CalculatePriceQuoteRequest{
		Context: req.GetContext(),
		PriceQuote: &pricingv1.PriceQuote{
			Audit: &commonv1.AuditInfo{CreatedAt: req.GetPriceList().GetAudit().GetCreatedAt()},
		},
	}
	calculatePriceQuoteReply, err := priceQuoteCalculation.CalculatePriceQuote(ctx, calculatePriceQuoteRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceQuoteCalculationService/CalculatePriceQuote")
	}
	return &pricingv1.NormalizePriceListResponse{
		PriceList: &pricingv1.PriceList{
			PriceListId: calculatePriceQuoteReply.GetPriceQuote().GetCounterparty().GetPriceListId(),
			Effective:   req.GetPriceList().GetEffective(),
			Status:      req.GetPriceList().GetStatus(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ExplainPriceListDecision explains a price list.
func (s *PriceListValidationServer) ExplainPriceListDecision(ctx context.Context, req *pricingv1.ExplainPriceListDecisionRequest) (*pricingv1.ExplainPriceListDecisionResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePriceList(ctx, req.GetContext().GetTenantId(), req.GetPriceList()); err != nil {
		return nil, grpcx.Wrap(err, "save price list")
	}
	s.log.InfoContext(ctx, "explain price list decision", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPriceList().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "email", req.GetDetails().GetContact().GetEmail())
	quoteFraudRuleRequest := &fraudv1.QuoteFraudRuleRequest{
		Context: req.GetContext(),
		FraudRule: &fraudv1.FraudRule{
			Audit: &commonv1.AuditInfo{
				Etag:      req.GetPriceList().GetAudit().GetEtag(),
				UpdatedBy: req.GetPriceList().GetAudit().GetUpdatedBy(),
			},
			Owner:  &identityv1.UserRef{Email: tenancy.Value(ctx, "email")},
			Action: &fraudv1.FraudRule_Discount{Discount: &fraudv1.FraudRuleDiscountAction{Amount: req.GetPriceList().GetDiscount().GetAmount()}},
		},
	}
	quoteFraudRuleReply := new(fraudv1.QuoteFraudRuleResponse)
	if err := grpcx.Invoke(ctx, s.conn, fraudv1.FraudRuleCalculationService_QuoteFraudRule_FullMethodName, quoteFraudRuleRequest, quoteFraudRuleReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.FraudRuleCalculationService/QuoteFraudRule")
	}
	calculatePriceQuoteRequest := buildCalculatePriceQuoteRequestForExplainPriceListDecision(req)
	calculatePriceQuoteReply, err := s.priceQuoteCalculationClient.CalculatePriceQuote(ctx, calculatePriceQuoteRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceQuoteCalculationService/CalculatePriceQuote")
	}
	cloned := proto.Clone(req.GetDetails()).(*pricingv1.ExplainPriceListDecisionRequest_Details)
	scoreNotificationRequest := &notificationsv1.ScoreNotificationRequest{
		Context: req.GetContext(),
		Details: &notificationsv1.ScoreNotificationRequest_Details{
			Destination: &commonv1.PostalAddress{
				Locality: calculatePriceQuoteReply.GetPriceQuote().GetBillingAddress().GetLocality(),
			},
		},
		Notification: &notificationsv1.Notification{
			Comments: []*notificationsv1.NotificationComment{&notificationsv1.NotificationComment{
				CreatedAt: calculatePriceQuoteReply.GetPriceQuote().GetAudit().GetCreatedAt(),
			}},
			Requester: &commonv1.ContactInfo{
				Email:            req.GetPriceList().GetOwner().GetEmail(),
				PreferredChannel: cloned.GetContact().GetPreferredChannel(),
			},
		},
	}
	scoreNotificationReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*notificationsv1.ScoreNotificationResponse, error) {
		return s.notificationValidationClient.ScoreNotification(ctx, scoreNotificationRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.notifications.v1.NotificationValidationService/ScoreNotification")
	}
	return &pricingv1.ExplainPriceListDecisionResponse{
		PriceList: &pricingv1.PriceList{
			Priority: quoteFraudRuleReply.GetFraudRule().GetPriority(),
			Owner: &identityv1.UserRef{
				DisplayName: scoreNotificationReply.GetNotification().GetAssignee().GetDisplayName(),
			},
			PriceListId: req.GetPriceListId(),
			Name:        req.GetPriceList().GetName(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: quoteFraudRuleReply.GetResult().GetCorrelationId(),
			Message:       scoreNotificationReply.GetResult().GetMessage(),
			Status:        "ok",
		},
		Outcome: &pricingv1.ExplainPriceListDecisionResponse_Outcome{Message: calculatePriceQuoteReply.GetResult().GetMessage()},
	}, nil
}

// ScorePriceList scores a price list.
func (s *PriceListValidationServer) ScorePriceList(ctx context.Context, req *pricingv1.ScorePriceListRequest) (*pricingv1.ScorePriceListResponse, error) {
	if err := s.store.SavePriceList(ctx, req.GetContext().GetTenantId(), req.GetPriceList()); err != nil {
		return nil, grpcx.Wrap(err, "save price list")
	}
	s.log.InfoContext(ctx, "score price list", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPriceList().GetAudit().GetEtag())
	estimatePriceQuoteReply, err := s.priceQuoteCalculationClient.EstimatePriceQuote(ctx, &pricingv1.EstimatePriceQuoteRequest{
		Context: req.GetContext(),
		PriceQuote: &pricingv1.PriceQuote{
			BillingAddress: &commonv1.PostalAddress{Locality: req.GetDetails().GetDestination().GetLocality()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceQuoteCalculationService/EstimatePriceQuote")
	}
	cloned := proto.Clone(req.GetDetails()).(*pricingv1.ScorePriceListRequest_Details)
	comparePriceQuoteOptionsRequest := &pricingv1.ComparePriceQuoteOptionsRequest{
		Context: req.GetContext(),
		PriceQuote: &pricingv1.PriceQuote{
			Settlement: &pricingv1.PriceQuote_Card{Card: &pricingv1.PriceQuoteCardSettlement{
				Network: estimatePriceQuoteReply.GetPriceQuote().GetCard().GetNetwork(),
			}},
			BillingAddress: &commonv1.PostalAddress{
				Recipient: estimatePriceQuoteReply.GetPriceQuote().GetBillingAddress().GetRecipient(),
			},
		},
		Details: &pricingv1.ComparePriceQuoteOptionsRequest_Details{
			Destination: &commonv1.PostalAddress{Region: cloned.GetDestination().GetRegion()},
		},
	}
	comparePriceQuoteOptionsReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*pricingv1.ComparePriceQuoteOptionsResponse, error) {
		return s.priceQuoteCalculationClient.ComparePriceQuoteOptions(ctx, comparePriceQuoteOptionsRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceQuoteCalculationService/ComparePriceQuoteOptions")
	}
	return &pricingv1.ScorePriceListResponse{
		Result: &commonv1.OperationResult{
			Details: estimatePriceQuoteReply.GetResult().GetDetails(),
			Status:  comparePriceQuoteOptionsReply.GetResult().GetStatus(),
		},
		PriceList: &pricingv1.PriceList{
			Version:     comparePriceQuoteOptionsReply.GetPriceQuote().GetCounterparty().GetVersion(),
			PriceListId: req.GetPriceListId(),
			Effective:   req.GetPriceList().GetEffective(),
		},
	}, nil
}

// LintPriceLists lints a price list.
func (s *PriceListValidationServer) LintPriceLists(ctx context.Context, req *pricingv1.LintPriceListsRequest) (*pricingv1.LintPriceListsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePriceList(ctx, req.GetContext().GetTenantId(), req.GetPriceList()); err != nil {
		return nil, grpcx.Wrap(err, "save price list")
	}
	s.log.InfoContext(ctx, "lint price lists", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPriceList().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "phone", req.GetDetails().GetContact().GetPhone())
	explainPriceQuoteCalculationReply, err := s.priceQuoteCalculationClient.ExplainPriceQuoteCalculation(ctx, &pricingv1.ExplainPriceQuoteCalculationRequest{
		Context: req.GetContext(),
		Details: &pricingv1.ExplainPriceQuoteCalculationRequest_Details{
			Contact: &commonv1.ContactInfo{Phone: tenancy.Value(ctx, "phone")},
		},
		PriceQuote: &pricingv1.PriceQuote{
			Audit: &commonv1.AuditInfo{
				Etag:      req.GetPriceList().GetAudit().GetEtag(),
				CreatedAt: req.GetPriceList().GetAudit().GetCreatedAt(),
				CreatedBy: req.GetPriceList().GetAudit().GetCreatedBy(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceQuoteCalculationService/ExplainPriceQuoteCalculation")
	}
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "pricing.lint_price_lists.update_risk_register",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"risk_register.children.children.audit.created_by": explainPriceQuoteCalculationReply.GetPriceQuote().GetAudit().GetCreatedBy(),
			"risk_register.children.children.manager.user_id":  req.GetPriceList().GetOwner().GetUserId(),
			"context.tenant_id": req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	return &pricingv1.LintPriceListsResponse{
		Result: &commonv1.OperationResult{
			Status: explainPriceQuoteCalculationReply.GetResult().GetStatus(),
		},
		PriceList: &pricingv1.PriceList{
			PriceListId: req.GetPriceListId(),
			Effective:   req.GetPriceList().GetEffective(),
		},
	}, nil
}

// GetPriceListRules returns a price list.
func (s *PriceListValidationServer) GetPriceListRules(ctx context.Context, req *pricingv1.GetPriceListRulesRequest) (*pricingv1.GetPriceListRulesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetPriceListRules", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "price_list", req.GetPriceListId(), "get_price_list_rules"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get price list rules", "tenant", req.GetContext().GetTenantId())
	return &pricingv1.GetPriceListRulesResponse{
		PriceList: &pricingv1.PriceList{PriceListId: req.GetPriceListId()},
		Result:    &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// priceListValidationServerSimulatePriceQuoteSteps are the ways to run
// acme.pricing.v1.PriceQuoteCalculationService/SimulatePriceQuote, by name.
var priceListValidationServerSimulatePriceQuoteSteps = map[string]func(*PriceListValidationServer, context.Context, *pricingv1.SimulatePriceQuoteRequest) (*pricingv1.SimulatePriceQuoteResponse, error){
	"default": (*PriceListValidationServer).simulatePriceQuoteStep,
	"audited": (*PriceListValidationServer).simulatePriceQuoteAuditedStep,
}

func (s *PriceListValidationServer) simulatePriceQuoteStep(ctx context.Context, request *pricingv1.SimulatePriceQuoteRequest) (*pricingv1.SimulatePriceQuoteResponse, error) {
	return s.priceQuoteCalculationClient.SimulatePriceQuote(ctx, request)
}

func (s *PriceListValidationServer) simulatePriceQuoteAuditedStep(ctx context.Context, request *pricingv1.SimulatePriceQuoteRequest) (*pricingv1.SimulatePriceQuoteResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.pricing.v1.PriceQuoteCalculationService/SimulatePriceQuote")
	return s.priceQuoteCalculationClient.SimulatePriceQuote(ctx, request)
}
