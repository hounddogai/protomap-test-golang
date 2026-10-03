package orders

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	boxofficev1 "github.com/hounddogai/protomap-test-golang/gen/acme/boxoffice/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	contractsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/contracts/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	ordersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/orders/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// OrderValidationServer implements `acme.orders.v1.OrderValidationService`.
// Checks orders against the rules before anything acts on them.
type OrderValidationServer struct {
	ordersv1.UnimplementedOrderValidationServiceServer
	store                           *Store
	log                             *slog.Logger
	calculateOrderFn                func(context.Context, *ordersv1.CalculateOrderRequest, ...grpc.CallOption) (*ordersv1.CalculateOrderResponse, error)
	orderCalculationClient          ordersv1.OrderCalculationServiceClient
	meteredOrderCalculation         meteredOrderCalculation
	clients                         *registry.Registry
	conn                            *grpc.ClientConn
	orderCalculationOnce            sync.Once
	orderCalculationCached          ordersv1.OrderCalculationServiceClient
	featureFlagValidationClient     featureflagsv1.FeatureFlagValidationServiceClient
	pointsLedgerValidationPort      pointsLedgerValidationPort
	inspectionCalculationClient     returnsv1.InspectionCalculationServiceClient
	synonymExportClient             searchv1.SynonymExportServiceClient
	productReviewQueryPort          productReviewQueryPort
	pointsLedgerValidationClient    loyaltyv1.PointsLedgerValidationServiceClient
	orderAmendmentCalculationClient ordersv1.OrderAmendmentCalculationServiceClient
	contractClient                  contractsv1.ContractServiceClient
	giftCardCalculationClient       giftcardsv1.GiftCardCalculationServiceClient
}

// NewOrderValidationServer wires an OrderValidationServer.
func NewOrderValidationServer(deps platform.Deps) *OrderValidationServer {
	orderCalculation := ordersv1.NewOrderCalculationServiceClient(deps.Conn)
	return &OrderValidationServer{
		store:                           NewStore(deps.DB),
		log:                             deps.Logger.With("service", "acme.orders.v1.OrderValidationService"),
		calculateOrderFn:                orderCalculation.CalculateOrder,
		orderCalculationClient:          ordersv1.NewOrderCalculationServiceClient(deps.Conn),
		meteredOrderCalculation:         meteredOrderCalculation{OrderCalculationServiceClient: ordersv1.NewOrderCalculationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		clients:                         deps.Clients,
		conn:                            deps.Conn,
		featureFlagValidationClient:     featureflagsv1.NewFeatureFlagValidationServiceClient(deps.Conn),
		pointsLedgerValidationPort:      newPointsLedgerValidationPort(deps),
		inspectionCalculationClient:     returnsv1.NewInspectionCalculationServiceClient(deps.Conn),
		synonymExportClient:             searchv1.NewSynonymExportServiceClient(deps.Conn),
		productReviewQueryPort:          newProductReviewQueryPort(deps),
		pointsLedgerValidationClient:    loyaltyv1.NewPointsLedgerValidationServiceClient(deps.Conn),
		orderAmendmentCalculationClient: ordersv1.NewOrderAmendmentCalculationServiceClient(deps.Conn),
		contractClient:                  contractsv1.NewContractServiceClient(deps.Conn),
		giftCardCalculationClient:       giftcardsv1.NewGiftCardCalculationServiceClient(deps.Conn),
	}
}

// ValidateOrder validates an order.
func (s *OrderValidationServer) ValidateOrder(ctx context.Context, req *ordersv1.ValidateOrderRequest) (*ordersv1.ValidateOrderResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveOrder(ctx, req.GetContext().GetTenantId(), req.GetOrder()); err != nil {
		return nil, grpcx.Wrap(err, "save order")
	}
	s.log.InfoContext(ctx, "validate order", "tenant", req.GetContext().GetTenantId(), "etag", req.GetOrder().GetAudit().GetEtag())
	var bic string
	switch choice := req.GetOrder().GetSettlement().(type) {
	case *ordersv1.Order_Card:
		bic = choice.Card.GetHolderName()
	case *ordersv1.Order_Bank:
		bic = choice.Bank.GetBic()
	case *ordersv1.Order_VoucherCode:
		bic = choice.VoucherCode
	}
	var iban string
	switch choice2 := req.GetOrder().GetSettlement().(type) {
	case *ordersv1.Order_Card:
		iban = choice2.Card.GetHolderName()
	case *ordersv1.Order_Bank:
		iban = choice2.Bank.GetIban()
	case *ordersv1.Order_VoucherCode:
		iban = choice2.VoucherCode
	}
	calculateOrderRequest := &ordersv1.CalculateOrderRequest{
		Context: req.GetContext(),
		Order: &ordersv1.Order{
			Counterparty: &customersv1.CustomerRef{
				CustomerId: req.GetOrder().GetCounterparty().GetCustomerId(),
			},
			BillingAddress: &commonv1.PostalAddress{
				PostalCode: req.GetOrder().GetBillingAddress().GetPostalCode(),
			},
			Settlement: &ordersv1.Order_Bank{Bank: &ordersv1.OrderBankSettlement{
				Bic:  bic,
				Iban: iban,
			}},
		},
	}
	calculateOrderReply, err := s.calculateOrderFn(ctx, calculateOrderRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderCalculationService/CalculateOrder")
	}
	for line := range iterx.Each(req.GetOrder().GetLines(), func(candidate *ordersv1.OrderLine) bool { return candidate != nil }) {
		if _, err := s.orderCalculationClient.ExplainOrderCalculation(ctx, &ordersv1.ExplainOrderCalculationRequest{
			Context: req.GetContext(),
			Order: &ordersv1.Order{
				Lines: []*ordersv1.OrderLine{&ordersv1.OrderLine{
					Tax: &ordersv1.OrderLineTax{
						Exemptions:   line.GetTax().GetExemptions(),
						Amount:       line.GetTax().GetAmount(),
						Jurisdiction: line.GetTax().GetJurisdiction(),
					},
					Quantity: line.GetQuantity(),
				}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.orders.v1.OrderCalculationService/ExplainOrderCalculation")
		}
	}
	return &ordersv1.ValidateOrderResponse{
		Order: &ordersv1.Order{
			BillingAddress: &commonv1.PostalAddress{
				Locality: calculateOrderReply.GetOrder().GetBillingAddress().GetLocality(),
			},
			OrderId:  req.GetOrderId(),
			IssuedAt: req.GetOrder().GetIssuedAt(),
		},
		Outcome: &ordersv1.ValidateOrderResponse_Outcome{Status: calculateOrderReply.GetResult().GetStatus()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// PreviewOrder previews an order.
func (s *OrderValidationServer) PreviewOrder(ctx context.Context, req *ordersv1.PreviewOrderRequest) (*ordersv1.PreviewOrderResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected PreviewOrder", "error", err)
		return nil, err
	}
	if err := s.store.SaveOrder(ctx, req.GetContext().GetTenantId(), req.GetOrder()); err != nil {
		return nil, grpcx.Wrap(err, "save order")
	}
	s.log.InfoContext(ctx, "preview order", "tenant", req.GetContext().GetTenantId(), "etag", req.GetOrder().GetAudit().GetEtag())
	estimateOrderRequest := &ordersv1.EstimateOrderRequest{
		Context: req.GetContext(),
		Order: &ordersv1.Order{
			Counterparty: &customersv1.CustomerRef{
				CustomerId: req.GetOrder().GetCounterparty().GetCustomerId(),
			},
		},
	}
	estimateOrderReply, err := s.meteredOrderCalculation.EstimateOrder(ctx, estimateOrderRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderCalculationService/EstimateOrder")
	}
	orderCalculation, err := registry.Lookup[ordersv1.OrderCalculationServiceClient](s.clients, "acme.orders.v1.OrderCalculationService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	source := req.GetOrder().GetBillingAddress().ProtoReflect()
	postalCode := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("postal_code"))).String()
	calculateOrderRequest := &ordersv1.CalculateOrderRequest{
		Context: req.GetContext(),
		Order: &ordersv1.Order{
			Settlement: &ordersv1.Order_Card{Card: &ordersv1.OrderCardSettlement{
				HolderName: estimateOrderReply.GetOrder().GetCard().GetHolderName(),
			}},
			BillingAddress: &commonv1.PostalAddress{PostalCode: postalCode},
			Counterparty: &customersv1.CustomerRef{
				CustomerId: estimateOrderReply.GetOrder().GetCounterparty().GetCustomerId(),
			},
		},
	}
	calculateOrderReply, err := orderCalculation.CalculateOrder(ctx, calculateOrderRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderCalculationService/CalculateOrder")
	}
	simulateOrderRequest := &ordersv1.SimulateOrderRequest{
		Context: req.GetContext(),
		Order: &ordersv1.Order{
			Counterparty: &customersv1.CustomerRef{
				DisplayName: calculateOrderReply.GetOrder().GetCounterparty().GetDisplayName(),
			},
			Settlement: &ordersv1.Order_Bank{Bank: &ordersv1.OrderBankSettlement{
				AccountHolder: estimateOrderReply.GetOrder().GetBank().GetAccountHolder(),
			}},
		},
	}
	simulateOrderReply, err := s.orderCalculationLazy().SimulateOrder(ctx, simulateOrderRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderCalculationService/SimulateOrder")
	}
	return &ordersv1.PreviewOrderResponse{
		Result: &commonv1.OperationResult{
			Details: estimateOrderReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Order: &ordersv1.Order{
			Number:         calculateOrderReply.GetOrder().GetNumber(),
			IssuedAt:       calculateOrderReply.GetOrder().GetIssuedAt(),
			BillingAddress: simulateOrderReply.GetOrder().GetBillingAddress(),
			Total:          simulateOrderReply.GetBreakdown().GetTotal(),
			OrderId:        req.GetOrderId(),
			Status:         req.GetOrder().GetStatus(),
		},
	}, nil
}

// CheckOrderEligibility checks an order.
func (s *OrderValidationServer) CheckOrderEligibility(ctx context.Context, req *ordersv1.CheckOrderEligibilityRequest) (*ordersv1.CheckOrderEligibilityResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveOrder(ctx, req.GetContext().GetTenantId(), req.GetOrder()); err != nil {
		return nil, grpcx.Wrap(err, "save order")
	}
	s.log.InfoContext(ctx, "check order eligibility", "tenant", req.GetContext().GetTenantId(), "etag", req.GetOrder().GetAudit().GetEtag())
	jobs := make(chan *ordersv1.OrderAttachment)
	failures := make(chan error, 4)
	workCtx, stopWork := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for attachment := range jobs {
				if _, err := s.orderCalculationClient.EstimateOrder(workCtx, &ordersv1.EstimateOrderRequest{
					Context: req.GetContext(),
					Order: &ordersv1.Order{
						Attachments: []*ordersv1.OrderAttachment{&ordersv1.OrderAttachment{
							SizeBytes:   attachment.GetSizeBytes(),
							Uri:         attachment.GetUri(),
							FileName:    attachment.GetFileName(),
							ContentType: attachment.GetContentType(),
						}},
					},
				}); err != nil {
					failures <- err
					stopWork()
					return
				}
			}
		}()
	}
	for _, queued := range req.GetOrder().GetAttachments() {
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
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderCalculationService/EstimateOrder")
	}
	checkFeatureFlagEligibilityRequest := &featureflagsv1.CheckFeatureFlagEligibilityRequest{
		Context: req.GetContext(),
		FeatureFlag: &featureflagsv1.FeatureFlag{
			Owner: &identityv1.UserRef{Email: req.GetOrder().GetCounterparty().GetEmail()},
		},
	}
	checkFeatureFlagEligibilityReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*featureflagsv1.CheckFeatureFlagEligibilityResponse, error) {
		return s.featureFlagValidationClient.CheckFeatureFlagEligibility(ctx, checkFeatureFlagEligibilityRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.featureflags.v1.FeatureFlagValidationService/CheckFeatureFlagEligibility")
	}
	explainPointsLedgerDecisionRequest := &loyaltyv1.ExplainPointsLedgerDecisionRequest{
		Context: req.GetContext(),
		Details: &loyaltyv1.ExplainPointsLedgerDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetContact().GetPreferredChannel(),
			},
		},
		PointsLedger: &loyaltyv1.PointsLedger{
			Contact: &commonv1.ContactInfo{Name: req.GetDetails().GetContact().GetName()},
			Funding: &loyaltyv1.PointsLedger_Bank{Bank: &loyaltyv1.PointsLedgerBankFunding{AccountHolder: req.GetOrder().GetBank().GetAccountHolder()}},
		},
	}
	explainPointsLedgerDecisionReply, err := s.pointsLedgerValidationPort.ExplainPointsLedgerDecision(ctx, explainPointsLedgerDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerValidationService/ExplainPointsLedgerDecision")
	}
	return &ordersv1.CheckOrderEligibilityResponse{
		Order: &ordersv1.Order{
			Lines: []*ordersv1.OrderLine{&ordersv1.OrderLine{
				Amount: checkFeatureFlagEligibilityReply.GetFeatureFlag().GetDiscount().GetAmount(),
			}},
			Audit: &commonv1.AuditInfo{
				Etag:      explainPointsLedgerDecisionReply.GetPointsLedger().GetAudit().GetEtag(),
				UpdatedBy: explainPointsLedgerDecisionReply.GetPointsLedger().GetAudit().GetUpdatedBy(),
			},
			OrderId: req.GetOrderId(),
			Total:   req.GetOrder().GetTotal(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// NormalizeOrder normalizes an order.
func (s *OrderValidationServer) NormalizeOrder(ctx context.Context, req *ordersv1.NormalizeOrderRequest) (*ordersv1.NormalizeOrderResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveOrder(ctx, req.GetContext().GetTenantId(), req.GetOrder()); err != nil {
		return nil, grpcx.Wrap(err, "save order")
	}
	s.log.InfoContext(ctx, "normalize order", "tenant", req.GetContext().GetTenantId(), "etag", req.GetOrder().GetAudit().GetEtag())
	estimateInspectionRequest := buildEstimateInspectionRequestForNormalizeOrder(req)
	estimateInspectionReply, err := s.inspectionCalculationClient.EstimateInspection(ctx, estimateInspectionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.InspectionCalculationService/EstimateInspection")
	}
	var network string
	switch choice := req.GetOrder().GetSettlement().(type) {
	case *ordersv1.Order_Card:
		network = choice.Card.GetNetwork()
	case *ordersv1.Order_Bank:
		network = choice.Bank.GetIban()
	case *ordersv1.Order_VoucherCode:
		network = choice.VoucherCode
	}
	simulateOrderRequest := &ordersv1.SimulateOrderRequest{
		Context: req.GetContext(),
		Order: &ordersv1.Order{
			Settlement: &ordersv1.Order_Card{Card: &ordersv1.OrderCardSettlement{Network: network}},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: estimateInspectionReply.GetInspection().GetAudit().GetUpdatedBy(),
			},
		},
	}
	simulateOrderReply, err := grpcx.Call(ctx, s.orderCalculationClient.SimulateOrder, simulateOrderRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderCalculationService/SimulateOrder")
	}
	return &ordersv1.NormalizeOrderResponse{
		Result: &commonv1.OperationResult{
			Message: estimateInspectionReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Order: &ordersv1.Order{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: simulateOrderReply.GetOrder().GetAudit().GetUpdatedBy(),
			},
			Total:        simulateOrderReply.GetOrder().GetTotal(),
			OrderId:      req.GetOrderId(),
			Counterparty: req.GetOrder().GetCounterparty(),
		},
	}, nil
}

// ExplainOrderDecision explains an order.
func (s *OrderValidationServer) ExplainOrderDecision(ctx context.Context, req *ordersv1.ExplainOrderDecisionRequest) (*ordersv1.ExplainOrderDecisionResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveOrder(ctx, req.GetContext().GetTenantId(), req.GetOrder()); err != nil {
		return nil, grpcx.Wrap(err, "save order")
	}
	s.log.InfoContext(ctx, "explain order decision", "tenant", req.GetContext().GetTenantId(), "etag", req.GetOrder().GetAudit().GetEtag())
	metadata2 := make(map[string]string, len(req.GetOrder().GetMetadata()))
	for key, value := range req.GetOrder().GetMetadata() {
		metadata2[key] = value
	}
	step := &orderValidationServerExplainOrderDecisionScheduleSynonymExportStep{request: &searchv1.ScheduleSynonymExportRequest{
		Context:     req.GetContext(),
		Reason:      req.GetReason(),
		Annotations: req.GetAnnotations(),
		Details:     &searchv1.ScheduleSynonymExportRequest_Details{Filters: metadata2},
	}}
	if err := s.runExplainOrderDecisionScheduleSynonymExport(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymExportService/ScheduleSynonymExport")
	}
	scheduleSynonymExportReply := step.reply
	boxOfficeSaleCalculation, err := registry.Lookup[boxofficev1.BoxOfficeSaleCalculationServiceClient](s.clients, "acme.boxoffice.v1.BoxOfficeSaleCalculationService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	ctx = tenancy.With(ctx, "account_holder", req.GetOrder().GetBank().GetAccountHolder())
	quoteBoxOfficeSaleRequest := &boxofficev1.QuoteBoxOfficeSaleRequest{
		Context: req.GetContext(),
		BoxOfficeSale: &boxofficev1.BoxOfficeSale{
			Settlement: &boxofficev1.BoxOfficeSale_Bank{Bank: &boxofficev1.BoxOfficeSaleBankSettlement{
				Bic:           req.GetOrder().GetBank().GetBic(),
				AccountHolder: tenancy.Value(ctx, "account_holder"),
			}},
			Audit: &commonv1.AuditInfo{Etag: req.GetOrder().GetAudit().GetEtag()},
		},
	}
	quoteBoxOfficeSaleReply, err := boxOfficeSaleCalculation.QuoteBoxOfficeSale(ctx, quoteBoxOfficeSaleRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.boxoffice.v1.BoxOfficeSaleCalculationService/QuoteBoxOfficeSale")
	}
	countProductReviewsRequest := &reviewsv1.CountProductReviewsRequest{
		Context: req.GetContext(),
		Filter:  &reviewsv1.CountProductReviewsRequest_Filter{Region: req.GetOrder().GetBillingAddress().GetRegion()},
	}
	countProductReviewsReply, err := s.productReviewQueryPort.CountProductReviews(ctx, countProductReviewsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ProductReviewQueryService/CountProductReviews")
	}
	explainPointsLedgerDecisionReply, err := s.pointsLedgerValidationClient.ExplainPointsLedgerDecision(ctx, &loyaltyv1.ExplainPointsLedgerDecisionRequest{
		Context: req.GetContext(),
		PointsLedger: &loyaltyv1.PointsLedger{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: quoteBoxOfficeSaleReply.GetBoxOfficeSale().GetAudit().GetUpdatedBy(),
				CreatedBy: quoteBoxOfficeSaleReply.GetBoxOfficeSale().GetAudit().GetCreatedBy(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerValidationService/ExplainPointsLedgerDecision")
	}
	_ = countProductReviewsReply
	return &ordersv1.ExplainOrderDecisionResponse{
		Result: &commonv1.OperationResult{
			Message:      scheduleSynonymExportReply.GetResult().GetMessage(),
			Details:      scheduleSynonymExportReply.GetResult().GetDetails(),
			ReferenceIds: explainPointsLedgerDecisionReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Order: &ordersv1.Order{
			BillingAddress: &commonv1.PostalAddress{
				Lines: quoteBoxOfficeSaleReply.GetBoxOfficeSale().GetBillingAddress().GetLines(),
			},
			OrderId: req.GetOrderId(),
			Total:   req.GetOrder().GetTotal(),
		},
	}, nil
}

// ScoreOrder scores an order.
func (s *OrderValidationServer) ScoreOrder(ctx context.Context, req *ordersv1.ScoreOrderRequest) (*ordersv1.ScoreOrderResponse, error) {
	if err := s.store.SaveOrder(ctx, req.GetContext().GetTenantId(), req.GetOrder()); err != nil {
		return nil, grpcx.Wrap(err, "save order")
	}
	s.log.InfoContext(ctx, "score order", "tenant", req.GetContext().GetTenantId(), "etag", req.GetOrder().GetAudit().GetEtag())
	calculateOrderAmendmentRequest := &ordersv1.CalculateOrderAmendmentRequest{}
	calculateOrderAmendmentRequest.Context = req.GetContext()
	calculateOrderAmendmentRequest.OrderAmendment = &ordersv1.OrderAmendment{
		BillingAddress: &commonv1.PostalAddress{
			CountryCode: req.GetOrder().GetBillingAddress().GetCountryCode(),
		},
		Audit: &commonv1.AuditInfo{UpdatedBy: req.GetOrder().GetAudit().GetUpdatedBy()},
	}
	calculateOrderAmendmentReply, err := s.orderAmendmentCalculationClient.CalculateOrderAmendment(ctx, calculateOrderAmendmentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderAmendmentCalculationService/CalculateOrderAmendment")
	}
	quoteOrderRequest := buildQuoteOrderRequestForScoreOrder(req, calculateOrderAmendmentReply)
	quoteOrderReply, err := s.orderCalculationClient.QuoteOrder(ctx, quoteOrderRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderCalculationService/QuoteOrder")
	}
	return &ordersv1.ScoreOrderResponse{
		Order: &ordersv1.Order{
			IssuedAt: calculateOrderAmendmentReply.GetOrderAmendment().GetIssuedAt(),
			BillingAddress: &commonv1.PostalAddress{
				CountryCode: calculateOrderAmendmentReply.GetOrderAmendment().GetBillingAddress().GetCountryCode(),
			},
			Counterparty: &customersv1.CustomerRef{
				CustomerId: quoteOrderReply.GetOrder().GetCounterparty().GetCustomerId(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedBy: quoteOrderReply.GetOrder().GetAudit().GetCreatedBy(),
			},
			OrderId:  req.GetOrderId(),
			Metadata: req.GetOrder().GetMetadata(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// LintOrders lints an order.
func (s *OrderValidationServer) LintOrders(ctx context.Context, req *ordersv1.LintOrdersRequest) (*ordersv1.LintOrdersResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveOrder(ctx, req.GetContext().GetTenantId(), req.GetOrder()); err != nil {
		return nil, grpcx.Wrap(err, "save order")
	}
	s.log.InfoContext(ctx, "lint orders", "tenant", req.GetContext().GetTenantId(), "etag", req.GetOrder().GetAudit().GetEtag())
	var holder struct{ Phone string }
	if record := req.GetDetails().GetContact(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Phone"); err != nil {
			s.log.WarnContext(ctx, "copy phone", "error", err)
		}
	}
	watchContractsStream, err := s.contractClient.WatchContracts(ctx, &contractsv1.WatchContractsRequest{
		Context: req.GetContext(),
		Subscriber: &commonv1.ContactInfo{
			Email: req.GetOrder().GetCounterparty().GetEmail(),
			Name:  req.GetDetails().GetContact().GetName(),
			Phone: holder.Phone,
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.contracts.v1.ContractService/WatchContracts")
	}
	for {
		received, err := watchContractsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.contracts.v1.ContractService/WatchContracts")
		}
		_ = received
	}
	quoteOrderRequest := &ordersv1.QuoteOrderRequest{
		Context: req.GetContext(),
		Order: &ordersv1.Order{
			BillingAddress: &commonv1.PostalAddress{
				Recipient: req.GetOrder().GetBillingAddress().GetRecipient(),
			},
			Settlement: &ordersv1.Order_Bank{Bank: &ordersv1.OrderBankSettlement{Bic: req.GetOrder().GetBank().GetBic()}},
		},
	}
	type quoteOrderResult struct {
		reply *ordersv1.QuoteOrderResponse
		err   error
	}
	quoteOrderResults := make(chan quoteOrderResult, 1)
	go func() {
		reply, err := s.orderCalculationClient.QuoteOrder(ctx, quoteOrderRequest)
		quoteOrderResults <- quoteOrderResult{reply, err}
	}()
	var quoteOrderReply *ordersv1.QuoteOrderResponse
	select {
	case result := <-quoteOrderResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.orders.v1.OrderCalculationService/QuoteOrder")
		}
		quoteOrderReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	recalculateGiftCardsReply, err := s.giftCardCalculationClient.RecalculateGiftCards(ctx, &giftcardsv1.RecalculateGiftCardsRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.RecalculateGiftCardsRequest_Details{
			Destination: &commonv1.PostalAddress{
				PostalCode: req.GetOrder().GetBillingAddress().GetPostalCode(),
			},
		},
		GiftCard: &giftcardsv1.GiftCard{
			Audit: &commonv1.AuditInfo{Etag: quoteOrderReply.GetOrder().GetAudit().GetEtag()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardCalculationService/RecalculateGiftCards")
	}
	return &ordersv1.LintOrdersResponse{
		Order: &ordersv1.Order{
			Total: quoteOrderReply.GetBreakdown().GetTotal(),
			Audit: &commonv1.AuditInfo{
				UpdatedAt: recalculateGiftCardsReply.GetGiftCard().GetAudit().GetUpdatedAt(),
			},
			OrderId:  req.GetOrderId(),
			Metadata: req.GetOrder().GetMetadata(),
		},
		Outcome: &ordersv1.LintOrdersResponse_Outcome{Status: recalculateGiftCardsReply.GetResult().GetStatus()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetOrderRules returns an order.
func (s *OrderValidationServer) GetOrderRules(ctx context.Context, req *ordersv1.GetOrderRulesRequest) (*ordersv1.GetOrderRulesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "order", req.GetOrderId(), "get_order_rules"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get order rules", "tenant", req.GetContext().GetTenantId())
	inspectionCalculation, ok := s.clients.Get("acme.returns.v1.InspectionCalculationService").(returnsv1.InspectionCalculationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.returns.v1.InspectionCalculationService")
	}
	estimateInspectionRequest := &returnsv1.EstimateInspectionRequest{
		Context: req.GetContext(),
		Inspection: &returnsv1.Inspection{
			Assignee: &identityv1.UserRef{
				Email:  req.GetContext().GetActor().GetEmail(),
				UserId: req.GetContext().GetActor().GetUserId(),
			},
			Comments: []*returnsv1.InspectionComment{&returnsv1.InspectionComment{CreatedAt: req.GetContext().GetDeadline()}},
			Origin:   &returnsv1.Inspection_ExternalReference{ExternalReference: req.GetOrderId()},
		},
	}
	estimateInspectionReply, err := inspectionCalculation.EstimateInspection(ctx, estimateInspectionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.InspectionCalculationService/EstimateInspection")
	}
	return &ordersv1.GetOrderRulesResponse{
		Result: &commonv1.OperationResult{
			Message: estimateInspectionReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Order: &ordersv1.Order{OrderId: req.GetOrderId()},
	}, nil
}

// orderCalculationLazy dials acme.orders.v1.OrderCalculationService the first time a handler needs it.
func (s *OrderValidationServer) orderCalculationLazy() ordersv1.OrderCalculationServiceClient {
	s.orderCalculationOnce.Do(func() { s.orderCalculationCached = ordersv1.NewOrderCalculationServiceClient(s.conn) })
	return s.orderCalculationCached
}

// orderValidationServerExplainOrderDecisionScheduleSynonymExportStep carries the request of one step of
// ExplainOrderDecision and, once it ran, its reply.
type orderValidationServerExplainOrderDecisionScheduleSynonymExportStep struct {
	request *searchv1.ScheduleSynonymExportRequest
	reply   *searchv1.ScheduleSynonymExportResponse
}

func (s *OrderValidationServer) runExplainOrderDecisionScheduleSynonymExport(ctx context.Context, step *orderValidationServerExplainOrderDecisionScheduleSynonymExportStep) error {
	reply, err := s.synonymExportClient.ScheduleSynonymExport(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
