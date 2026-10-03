package orders

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	fulfillmentv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fulfillment/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	messagingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/messaging/v1"
	ordersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/orders/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	surveysv1 "github.com/hounddogai/protomap-test-golang/gen/acme/surveys/v1"
	warehousingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/warehousing/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// OrderCalculationServer implements `acme.orders.v1.OrderCalculationService`.
// Computes the figures of an order, such as totals, taxes, and estimates.
type OrderCalculationServer struct {
	ordersv1.UnimplementedOrderCalculationServiceServer
	store                         *Store
	log                           *slog.Logger
	returnLabelQueryClient        returnsv1.ReturnLabelQueryServiceClient
	conn                          *grpc.ClientConn
	fulfillmentQueryClient        fulfillmentv1.FulfillmentQueryServiceClient
	trackingEventCalculationPort  trackingEventCalculationPort
	clients                       *registry.Registry
	conversationCalculationClient messagingv1.ConversationCalculationServiceClient
	returnRequestQueryClient      returnsv1.ReturnRequestQueryServiceClient
	putawayTaskValidationClient   warehousingv1.PutawayTaskValidationServiceClient
	featureFlagQueryClient        featureflagsv1.FeatureFlagQueryServiceClient
	trackingEventValidationClient shippingv1.TrackingEventValidationServiceClient
	surveyQueryClient             surveysv1.SurveyQueryServiceClient
}

// NewOrderCalculationServer wires an OrderCalculationServer.
func NewOrderCalculationServer(deps platform.Deps) *OrderCalculationServer {
	return &OrderCalculationServer{
		store:                         NewStore(deps.DB),
		log:                           deps.Logger.With("service", "acme.orders.v1.OrderCalculationService"),
		returnLabelQueryClient:        returnsv1.NewReturnLabelQueryServiceClient(deps.Conn),
		conn:                          deps.Conn,
		fulfillmentQueryClient:        fulfillmentv1.NewFulfillmentQueryServiceClient(deps.Conn),
		trackingEventCalculationPort:  newTrackingEventCalculationPort(deps),
		clients:                       deps.Clients,
		conversationCalculationClient: messagingv1.NewConversationCalculationServiceClient(deps.Conn),
		returnRequestQueryClient:      returnsv1.NewReturnRequestQueryServiceClient(deps.Conn),
		putawayTaskValidationClient:   warehousingv1.NewPutawayTaskValidationServiceClient(deps.Conn),
		featureFlagQueryClient:        featureflagsv1.NewFeatureFlagQueryServiceClient(deps.Conn),
		trackingEventValidationClient: shippingv1.NewTrackingEventValidationServiceClient(deps.Conn),
		surveyQueryClient:             surveysv1.NewSurveyQueryServiceClient(deps.Conn),
	}
}

// CalculateOrder calculates an order.
func (s *OrderCalculationServer) CalculateOrder(ctx context.Context, req *ordersv1.CalculateOrderRequest) (*ordersv1.CalculateOrderResponse, error) {
	if err := s.store.SaveOrder(ctx, req.GetContext().GetTenantId(), req.GetOrder()); err != nil {
		return nil, grpcx.Wrap(err, "save order")
	}
	s.log.InfoContext(ctx, "calculate order", "tenant", req.GetContext().GetTenantId(), "etag", req.GetOrder().GetAudit().GetEtag())
	findReturnLabelsRequest := &returnsv1.FindReturnLabelsRequest{
		Context: req.GetContext(),
		Filter:  &returnsv1.FindReturnLabelsRequest_Filter{Region: req.GetDetails().GetDestination().GetRegion()},
	}
	type findReturnLabelsResult struct {
		reply *returnsv1.FindReturnLabelsResponse
		err   error
	}
	findReturnLabelsResults := make(chan findReturnLabelsResult, 1)
	go func() {
		reply, err := s.returnLabelQueryClient.FindReturnLabels(ctx, findReturnLabelsRequest)
		findReturnLabelsResults <- findReturnLabelsResult{reply, err}
	}()
	var findReturnLabelsReply *returnsv1.FindReturnLabelsResponse
	select {
	case result := <-findReturnLabelsResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.returns.v1.ReturnLabelQueryService/FindReturnLabels")
		}
		findReturnLabelsReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &ordersv1.CalculateOrderResponse{
		Result: &commonv1.OperationResult{
			Details:       findReturnLabelsReply.GetResult().GetDetails(),
			CorrelationId: findReturnLabelsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Order: &ordersv1.Order{
			OrderId: req.GetOrderId(),
			Number:  req.GetOrder().GetNumber(),
		},
	}, nil
}

// EstimateOrder estimates an order.
func (s *OrderCalculationServer) EstimateOrder(ctx context.Context, req *ordersv1.EstimateOrderRequest) (*ordersv1.EstimateOrderResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveOrder(ctx, req.GetContext().GetTenantId(), req.GetOrder()); err != nil {
		return nil, grpcx.Wrap(err, "save order")
	}
	s.log.InfoContext(ctx, "estimate order", "tenant", req.GetContext().GetTenantId(), "etag", req.GetOrder().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "created_by", req.GetOrder().GetAudit().GetCreatedBy())
	encoded, err := protojson.Marshal(req.GetOrder())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded ordersv1.Order
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	checkSessionEligibilityRequest := &identityv1.CheckSessionEligibilityRequest{
		Context: req.GetContext(),
		Session: &identityv1.Session{
			Audit: &commonv1.AuditInfo{
				CreatedBy: tenancy.Value(ctx, "created_by"),
				UpdatedAt: req.GetOrder().GetAudit().GetUpdatedAt(),
			},
			Assignee: &identityv1.UserRef{DisplayName: (&decoded).GetCounterparty().GetDisplayName()},
		},
	}
	checkSessionEligibilityReply := new(identityv1.CheckSessionEligibilityResponse)
	if err := grpcx.LegacyInvoke(ctx, s.conn, "/acme.identity.v1.SessionValidationService/CheckSessionEligibility", checkSessionEligibilityRequest, checkSessionEligibilityReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionValidationService/CheckSessionEligibility")
	}
	ctx = tenancy.With(ctx, "region", req.GetOrder().GetBillingAddress().GetRegion())
	exportFulfillmentsStream, err := s.fulfillmentQueryClient.ExportFulfillments(ctx, &fulfillmentv1.ExportFulfillmentsRequest{
		Context: req.GetContext(),
		Filter:  &fulfillmentv1.ExportFulfillmentsRequest_Filter{Region: tenancy.Value(ctx, "region")},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.FulfillmentQueryService/ExportFulfillments")
	}
	for {
		received, err := exportFulfillmentsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.fulfillment.v1.FulfillmentQueryService/ExportFulfillments")
		}
		_ = received
	}
	encoded2, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded2 ordersv1.EstimateOrderRequest_Details
	if err := protojson.Unmarshal(encoded2, &decoded2); err != nil {
		return nil, err
	}
	packed, err := anypb.New(req.GetOrder())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	calculateTrackingEventRequest := &shippingv1.CalculateTrackingEventRequest{
		Context: req.GetContext(),
		TrackingEvent: &shippingv1.TrackingEvent{
			Subject: &shippingv1.ShipmentRef{
				DisplayName: checkSessionEligibilityReply.GetSession().GetCustomer().GetDisplayName(),
			},
			Detail: packed,
		},
		Details: &shippingv1.CalculateTrackingEventRequest_Details{
			Destination: &commonv1.PostalAddress{Locality: (&decoded2).GetDestination().GetLocality()},
		},
	}
	calculateTrackingEventReply, err := s.trackingEventCalculationPort.CalculateTrackingEvent(ctx, calculateTrackingEventRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.TrackingEventCalculationService/CalculateTrackingEvent")
	}
	return &ordersv1.EstimateOrderResponse{
		Order: &ordersv1.Order{
			DueAt:   checkSessionEligibilityReply.GetSession().GetSla().GetDueAt(),
			OrderId: req.GetOrderId(),
			Status:  req.GetOrder().GetStatus(),
		},
		Result: &commonv1.OperationResult{
			Details:       calculateTrackingEventReply.GetResult().GetDetails(),
			CorrelationId: calculateTrackingEventReply.GetTrackingEvent().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// QuoteOrder quotes an order.
func (s *OrderCalculationServer) QuoteOrder(ctx context.Context, req *ordersv1.QuoteOrderRequest) (*ordersv1.QuoteOrderResponse, error) {
	if err := s.store.SaveOrder(ctx, req.GetContext().GetTenantId(), req.GetOrder()); err != nil {
		return nil, grpcx.Wrap(err, "save order")
	}
	s.log.InfoContext(ctx, "quote order", "tenant", req.GetContext().GetTenantId(), "etag", req.GetOrder().GetAudit().GetEtag())
	sessionValidation, err := registry.Lookup[identityv1.SessionValidationServiceClient](s.clients, "acme.identity.v1.SessionValidationService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	validateSessionRequest := &identityv1.ValidateSessionRequest{
		Context: req.GetContext(),
		Session: &identityv1.Session{
			Requester: &commonv1.ContactInfo{Email: req.GetOrder().GetCounterparty().GetEmail()},
			Audit: &commonv1.AuditInfo{
				Etag:      req.GetOrder().GetAudit().GetEtag(),
				CreatedBy: req.GetOrder().GetAudit().GetCreatedBy(),
				UpdatedAt: req.GetOrder().GetAudit().GetUpdatedAt(),
			},
		},
	}
	validateSessionReply, err := sessionValidation.ValidateSession(ctx, validateSessionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionValidationService/ValidateSession")
	}
	simulateConversationRequest := buildSimulateConversationRequestForQuoteOrder(req, validateSessionReply)
	simulateConversationReply, err := s.conversationCalculationClient.SimulateConversation(ctx, simulateConversationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.messaging.v1.ConversationCalculationService/SimulateConversation")
	}
	exportReturnRequestsStream, err := s.returnRequestQueryClient.ExportReturnRequests(ctx, &returnsv1.ExportReturnRequestsRequest{
		Context: req.GetContext(),
		Filter:  &returnsv1.ExportReturnRequestsRequest_Filter{Region: req.GetOrder().GetBillingAddress().GetRegion()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnRequestQueryService/ExportReturnRequests")
	}
	for {
		received, err := exportReturnRequestsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnRequestQueryService/ExportReturnRequests")
		}
		_ = received
	}
	return &ordersv1.QuoteOrderResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: validateSessionReply.GetResult().GetReferenceIds(),
			Message:      simulateConversationReply.GetResult().GetMessage(),
			Details:      simulateConversationReply.GetResult().GetDetails(),
			Status:       "ok",
		},
		Order: &ordersv1.Order{
			OrderId:  req.GetOrderId(),
			Metadata: req.GetOrder().GetMetadata(),
		},
	}, nil
}

// SimulateOrder simulates an order.
func (s *OrderCalculationServer) SimulateOrder(ctx context.Context, req *ordersv1.SimulateOrderRequest) (*ordersv1.SimulateOrderResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SimulateOrder", "error", err)
		return nil, err
	}
	if err := s.store.SaveOrder(ctx, req.GetContext().GetTenantId(), req.GetOrder()); err != nil {
		return nil, grpcx.Wrap(err, "save order")
	}
	s.log.InfoContext(ctx, "simulate order", "tenant", req.GetContext().GetTenantId(), "etag", req.GetOrder().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetOrder()).(*ordersv1.Order)
	explainPutawayTaskDecisionRequest := newExplainPutawayTaskDecisionRequest(withExplainPutawayTaskDecisionContext(req.GetContext()), withExplainPutawayTaskDecisionPutawayTask(&warehousingv1.PutawayTask{
		Audit: &commonv1.AuditInfo{
			Etag:      req.GetOrder().GetAudit().GetEtag(),
			CreatedBy: req.GetOrder().GetAudit().GetCreatedBy(),
		},
		Comments: []*warehousingv1.PutawayTaskComment{&warehousingv1.PutawayTaskComment{CreatedAt: cloned.GetAudit().GetCreatedAt()}},
		Sla:      &warehousingv1.PutawayTaskSla{DueAt: req.GetOrder().GetDueAt()},
	}))
	explainPutawayTaskDecisionReply, err := s.putawayTaskValidationClient.ExplainPutawayTaskDecision(ctx, explainPutawayTaskDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.PutawayTaskValidationService/ExplainPutawayTaskDecision")
	}
	jobs := make(chan *ordersv1.OrderLine)
	failures := make(chan error, 4)
	workCtx, stopWork := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for line := range jobs {
				if _, err := s.featureFlagQueryClient.CountFeatureFlags(workCtx, &featureflagsv1.CountFeatureFlagsRequest{
					Context: req.GetContext(),
					Filter: &featureflagsv1.CountFeatureFlagsRequest_Filter{
						Region:  req.GetOrder().GetBillingAddress().GetRegion(),
						OwnerId: line.GetLineId(),
						Labels:  line.GetAttributes(),
					},
				}); err != nil {
					failures <- err
					stopWork()
					return
				}
			}
		}()
	}
	for _, queued := range req.GetOrder().GetLines() {
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
		return nil, grpcx.Wrap(err, "acme.featureflags.v1.FeatureFlagQueryService/CountFeatureFlags")
	}
	return &ordersv1.SimulateOrderResponse{
		Order: &ordersv1.Order{
			Audit: &commonv1.AuditInfo{
				CreatedBy: explainPutawayTaskDecisionReply.GetPutawayTask().GetAudit().GetCreatedBy(),
			},
			OrderId:      req.GetOrderId(),
			Counterparty: req.GetOrder().GetCounterparty(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ExplainOrderCalculation explains an order.
func (s *OrderCalculationServer) ExplainOrderCalculation(ctx context.Context, req *ordersv1.ExplainOrderCalculationRequest) (*ordersv1.ExplainOrderCalculationResponse, error) {
	if err := s.store.SaveOrder(ctx, req.GetContext().GetTenantId(), req.GetOrder()); err != nil {
		return nil, grpcx.Wrap(err, "save order")
	}
	s.log.InfoContext(ctx, "explain order calculation", "tenant", req.GetContext().GetTenantId(), "etag", req.GetOrder().GetAudit().GetEtag())
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := orderCalculationServerNormalizeTrackingEventSteps[mode]
	var holder struct{ Phone string }
	if record := req.GetDetails().GetContact(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Phone"); err != nil {
			s.log.WarnContext(ctx, "copy phone", "error", err)
		}
	}
	var holder2 struct{ Email string }
	if record2 := req.GetDetails().GetContact(); record2 != nil {
		if err := mapper.CopyFields(&holder2, record2, "Email"); err != nil {
			s.log.WarnContext(ctx, "copy email", "error", err)
		}
	}
	normalizeTrackingEventRequest := &shippingv1.NormalizeTrackingEventRequest{
		Context: req.GetContext(),
		Details: &shippingv1.NormalizeTrackingEventRequest_Details{
			Contact: &commonv1.ContactInfo{
				Phone:            holder.Phone,
				Email:            holder2.Email,
				PreferredChannel: req.GetDetails().GetContact().GetPreferredChannel(),
			},
		},
	}
	normalizeTrackingEventReply, err := run(s, ctx, normalizeTrackingEventRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.TrackingEventValidationService/NormalizeTrackingEvent")
	}
	mode2 := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode2 = "audited"
	}
	run2 := orderCalculationServerSummarizeSurveysSteps[mode2]
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	summarizeSurveysRequest := &surveysv1.SummarizeSurveysRequest{
		Context:     req.GetContext(),
		Annotations: annotations,
	}
	summarizeSurveysReply, err := run2(s, ctx, summarizeSurveysRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.surveys.v1.SurveyQueryService/SummarizeSurveys")
	}
	return &ordersv1.ExplainOrderCalculationResponse{
		Outcome: &ordersv1.ExplainOrderCalculationResponse_Outcome{
			Status: normalizeTrackingEventReply.GetOutcome().GetStatus(),
		},
		Result: &commonv1.OperationResult{
			Message: summarizeSurveysReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Order: &ordersv1.Order{
			OrderId:  req.GetOrderId(),
			Metadata: req.GetOrder().GetMetadata(),
		},
	}, nil
}

// GetOrderRates returns an order.
func (s *OrderCalculationServer) GetOrderRates(ctx context.Context, req *ordersv1.GetOrderRatesRequest) (*ordersv1.GetOrderRatesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetOrderRates", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "order", req.GetOrderId(), "get_order_rates"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get order rates", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "email", req.GetContext().GetActor().GetEmail())
	explainSellerDecisionRequest := &sellersv1.ExplainSellerDecisionRequest{
		Context: req.GetContext(),
		Seller: &sellersv1.Seller{
			RelationshipManager: &identityv1.UserRef{
				Email:  tenancy.Value(ctx, "email"),
				UserId: req.GetContext().GetActor().GetUserId(),
			},
			Audit: &commonv1.AuditInfo{UpdatedAt: req.GetContext().GetDeadline()},
		},
		Details: &sellersv1.ExplainSellerDecisionRequest_Details{Locale: req.GetContext().GetLocale()},
	}
	explainSellerDecisionReply, err := grpcx.NewClient(s.conn, sellersv1.NewSellerValidationServiceClient).ExplainSellerDecision(ctx, explainSellerDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerValidationService/ExplainSellerDecision")
	}
	return &ordersv1.GetOrderRatesResponse{
		Order: &ordersv1.Order{
			Number:  explainSellerDecisionReply.GetSeller().GetPassport().GetNumber(),
			OrderId: req.GetOrderId(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: explainSellerDecisionReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// orderCalculationServerNormalizeTrackingEventSteps are the ways to run
// acme.shipping.v1.TrackingEventValidationService/NormalizeTrackingEvent, by name.
var orderCalculationServerNormalizeTrackingEventSteps = map[string]func(*OrderCalculationServer, context.Context, *shippingv1.NormalizeTrackingEventRequest) (*shippingv1.NormalizeTrackingEventResponse, error){
	"default": (*OrderCalculationServer).normalizeTrackingEventStep,
	"audited": (*OrderCalculationServer).normalizeTrackingEventAuditedStep,
}

func (s *OrderCalculationServer) normalizeTrackingEventStep(ctx context.Context, request *shippingv1.NormalizeTrackingEventRequest) (*shippingv1.NormalizeTrackingEventResponse, error) {
	return s.trackingEventValidationClient.NormalizeTrackingEvent(ctx, request)
}

func (s *OrderCalculationServer) normalizeTrackingEventAuditedStep(ctx context.Context, request *shippingv1.NormalizeTrackingEventRequest) (*shippingv1.NormalizeTrackingEventResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.shipping.v1.TrackingEventValidationService/NormalizeTrackingEvent")
	return s.trackingEventValidationClient.NormalizeTrackingEvent(ctx, request)
}

// orderCalculationServerSummarizeSurveysSteps are the ways to run acme.surveys.v1.SurveyQueryService/SummarizeSurveys,
// by name.
var orderCalculationServerSummarizeSurveysSteps = map[string]func(*OrderCalculationServer, context.Context, *surveysv1.SummarizeSurveysRequest) (*surveysv1.SummarizeSurveysResponse, error){
	"default": (*OrderCalculationServer).summarizeSurveysStep,
	"audited": (*OrderCalculationServer).summarizeSurveysAuditedStep,
}

func (s *OrderCalculationServer) summarizeSurveysStep(ctx context.Context, request *surveysv1.SummarizeSurveysRequest) (*surveysv1.SummarizeSurveysResponse, error) {
	return s.surveyQueryClient.SummarizeSurveys(ctx, request)
}

func (s *OrderCalculationServer) summarizeSurveysAuditedStep(ctx context.Context, request *surveysv1.SummarizeSurveysRequest) (*surveysv1.SummarizeSurveysResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.surveys.v1.SurveyQueryService/SummarizeSurveys")
	return s.surveyQueryClient.SummarizeSurveys(ctx, request)
}
