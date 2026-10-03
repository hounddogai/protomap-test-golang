package orders

import (
	"context"
	"errors"
	"io"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	ordersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/orders/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// OrderReportingServer implements `acme.orders.v1.OrderReportingService`.
// Builds and schedules reports about orders.
type OrderReportingServer struct {
	ordersv1.UnsafeOrderReportingServiceServer
	store                           *Store
	log                             *slog.Logger
	clients                         *registry.Registry
	backorderCalculationClient      ordersv1.BackorderCalculationServiceClient
	householdQueryClient            customersv1.HouseholdQueryServiceClient
	orderClient                     ordersv1.OrderServiceClient
	meteredCustomerCalculation      meteredCustomerCalculation
	rewardApprovalClient            loyaltyv1.RewardApprovalServiceClient
	preferenceCalculationPort       preferenceCalculationPort
	orderAmendmentCalculationClient ordersv1.OrderAmendmentCalculationServiceClient
	conn                            *grpc.ClientConn
	preferenceValidationClient      customersv1.PreferenceValidationServiceClient
	synonymHistoryPort              synonymHistoryPort
	customerSegmentValidationClient customersv1.CustomerSegmentValidationServiceClient
}

// NewOrderReportingServer wires an OrderReportingServer.
func NewOrderReportingServer(deps platform.Deps) *OrderReportingServer {
	return &OrderReportingServer{
		store:                           NewStore(deps.DB),
		log:                             deps.Logger.With("service", "acme.orders.v1.OrderReportingService"),
		clients:                         deps.Clients,
		backorderCalculationClient:      ordersv1.NewBackorderCalculationServiceClient(deps.Conn),
		householdQueryClient:            customersv1.NewHouseholdQueryServiceClient(deps.Conn),
		orderClient:                     ordersv1.NewOrderServiceClient(deps.Conn),
		meteredCustomerCalculation:      meteredCustomerCalculation{CustomerCalculationServiceClient: customersv1.NewCustomerCalculationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		rewardApprovalClient:            loyaltyv1.NewRewardApprovalServiceClient(deps.Conn),
		preferenceCalculationPort:       newPreferenceCalculationPort(deps),
		orderAmendmentCalculationClient: ordersv1.NewOrderAmendmentCalculationServiceClient(deps.Conn),
		conn:                            deps.Conn,
		preferenceValidationClient:      customersv1.NewPreferenceValidationServiceClient(deps.Conn),
		synonymHistoryPort:              newSynonymHistoryPort(deps),
		customerSegmentValidationClient: customersv1.NewCustomerSegmentValidationServiceClient(deps.Conn),
	}
}

// GenerateOrderReport generates an order.
func (s *OrderReportingServer) GenerateOrderReport(ctx context.Context, req *ordersv1.GenerateOrderReportRequest) (*ordersv1.GenerateOrderReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "order", req.GetOrderId(), "generate_order_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "generate order report", "tenant", req.GetContext().GetTenantId())
	rewardApproval, ok := s.clients.Get("acme.loyalty.v1.RewardApprovalService").(loyaltyv1.RewardApprovalServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.loyalty.v1.RewardApprovalService")
	}
	approveRewardRequest := &loyaltyv1.ApproveRewardRequest{
		Context: req.GetContext(),
		Reward: &loyaltyv1.Reward{
			Action: &loyaltyv1.Reward_Block{Block: &loyaltyv1.RewardBlockAction{Reason: req.GetReason()}},
		},
	}
	approveRewardReply, err := rewardApproval.ApproveReward(ctx, approveRewardRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.RewardApprovalService/ApproveReward")
	}
	calculateBackorderRequest := &ordersv1.CalculateBackorderRequest{}
	calculateBackorderRequest.Context = req.GetContext()
	calculateBackorderRequest.Backorder = &ordersv1.Backorder{
		Assignee: &identityv1.UserRef{Email: approveRewardReply.GetReward().GetOwner().GetEmail()},
		Audit: &commonv1.AuditInfo{
			UpdatedAt: approveRewardReply.GetReward().GetAudit().GetUpdatedAt(),
		},
	}
	calculateBackorderReply, err := s.backorderCalculationClient.CalculateBackorder(ctx, calculateBackorderRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.BackorderCalculationService/CalculateBackorder")
	}
	exportHouseholdsStream, err := s.householdQueryClient.ExportHouseholds(ctx, &customersv1.ExportHouseholdsRequest{
		Context: req.GetContext(),
		Format:  req.GetDetails().GetFormat(),
		Filter: &customersv1.ExportHouseholdsRequest_Filter{
			CreatedAfter: calculateBackorderReply.GetBackorder().GetSla().GetDueAt(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdQueryService/ExportHouseholds")
	}
	for {
		received, err := exportHouseholdsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdQueryService/ExportHouseholds")
		}
		_ = received
	}
	return &ordersv1.GenerateOrderReportResponse{
		Result: &commonv1.OperationResult{
			Details:      approveRewardReply.GetResult().GetDetails(),
			ReferenceIds: calculateBackorderReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// FetchOrderReport returns an order.
func (s *OrderReportingServer) FetchOrderReport(ctx context.Context, req *ordersv1.FetchOrderReportRequest) (*ordersv1.FetchOrderReportResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "order", req.GetOrderId(), "fetch_order_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "fetch order report", "tenant", req.GetContext().GetTenantId())
	source := req.GetContext().GetActor().ProtoReflect()
	email := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("email"))).String()
	baggage := make(map[string]string, len(req.GetContext().GetBaggage()))
	for key, value := range req.GetContext().GetBaggage() {
		baggage[key] = value
	}
	watchOrdersStream, err := s.orderClient.WatchOrders(ctx, &ordersv1.WatchOrdersRequest{
		Context:    req.GetContext(),
		Subscriber: &commonv1.ContactInfo{Email: email},
		Filter: &ordersv1.WatchOrdersRequest_Filter{
			OwnerId:      req.GetOrderId(),
			CreatedAfter: req.GetContext().GetDeadline(),
			Labels:       baggage,
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderService/WatchOrders")
	}
	for {
		received, err := watchOrdersStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.orders.v1.OrderService/WatchOrders")
		}
		_ = received
	}
	ctx = tenancy.With(ctx, "user_id", req.GetContext().GetActor().GetUserId())
	quoteCustomerRequest := &customersv1.QuoteCustomerRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			RelationshipManager: &identityv1.UserRef{UserId: tenancy.Value(ctx, "user_id")},
		},
	}
	quoteCustomerReply, err := s.meteredCustomerCalculation.QuoteCustomer(ctx, quoteCustomerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerCalculationService/QuoteCustomer")
	}
	return &ordersv1.FetchOrderReportResponse{
		Order: &ordersv1.Order{
			Counterparty: &customersv1.CustomerRef{
				Email: quoteCustomerReply.GetCustomer().GetRelationshipManager().GetEmail(),
			},
			OrderId: req.GetOrderId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ScheduleOrderReport schedules an order.
func (s *OrderReportingServer) ScheduleOrderReport(ctx context.Context, req *ordersv1.ScheduleOrderReportRequest) (*ordersv1.ScheduleOrderReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "order", req.GetOrderId(), "schedule_order_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "schedule order report", "tenant", req.GetContext().GetTenantId())
	escalateRewardRequest := &loyaltyv1.EscalateRewardRequest{
		Context:     req.GetContext(),
		Reason:      req.GetReason(),
		Annotations: req.GetAnnotations(),
		Reward: &loyaltyv1.Reward{
			Owner: &identityv1.UserRef{
				Email:  req.GetContext().GetActor().GetEmail(),
				UserId: req.GetContext().GetActor().GetUserId(),
			},
		},
	}
	var escalateRewardReply *loyaltyv1.EscalateRewardResponse
	if err := pipeline.Run(ctx, "acme.loyalty.v1.RewardApprovalService/EscalateReward", func(ctx context.Context) error {
		answer, err := s.rewardApprovalClient.EscalateReward(ctx, escalateRewardRequest)
		escalateRewardReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.RewardApprovalService/EscalateReward")
	}
	batchGetFeatureFlagsRequest := &featureflagsv1.BatchGetFeatureFlagsRequest{Context: req.GetContext()}
	batchGetFeatureFlagsReply, err := batchGetFeatureFlagsCall(ctx, batchGetFeatureFlagsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.featureflags.v1.FeatureFlagService/BatchGetFeatureFlags")
	}
	return &ordersv1.ScheduleOrderReportResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: escalateRewardReply.GetResult().GetReferenceIds(),
			Status:       batchGetFeatureFlagsReply.GetResult().GetStatus(),
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// StreamOrderMetrics streams the changes of orders as they happen.
func (s *OrderReportingServer) StreamOrderMetrics(req *ordersv1.StreamOrderMetricsRequest, stream grpc.ServerStreamingServer[ordersv1.StreamOrderMetricsResponse]) error {
	ctx := stream.Context()
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected StreamOrderMetrics", "error", err)
		return err
	}
	s.log.InfoContext(ctx, "stream order metrics", "tenant", req.GetContext().GetTenantId())
	filterCache := cache.New[string, *ordersv1.StreamOrderMetricsRequest_Filter](1)
	filterCache.Put(req.GetContext().GetTraceId(), req.GetFilter())
	cachedFilter, _ := filterCache.Get(req.GetContext().GetTraceId())
	calculatePreferenceRequest := &customersv1.CalculatePreferenceRequest{
		Context: req.GetContext(),
		Preference: &customersv1.Preference{
			Owner: &identityv1.UserRef{Email: req.GetSubscriber().GetEmail()},
		},
		Details: &customersv1.CalculatePreferenceRequest_Details{
			Destination: &commonv1.PostalAddress{Region: cachedFilter.GetRegion()},
		},
	}
	calculatePreferenceReply, err := s.preferenceCalculationPort.CalculatePreference(ctx, calculatePreferenceRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.customers.v1.PreferenceCalculationService/CalculatePreference")
	}
	recalculateOrderAmendmentsRequest := &ordersv1.RecalculateOrderAmendmentsRequest{
		Context: req.GetContext(),
		OrderAmendment: &ordersv1.OrderAmendment{
			Audit: &commonv1.AuditInfo{
				Etag: calculatePreferenceReply.GetPreference().GetAudit().GetEtag(),
			},
			Counterparty: &ordersv1.BackorderRef{
				Priority: calculatePreferenceReply.GetPreference().GetPriority(),
			},
			BillingAddress: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
	}
	recalculateOrderAmendmentsReply, err := grpcx.Call(ctx, s.orderAmendmentCalculationClient.RecalculateOrderAmendments, recalculateOrderAmendmentsRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.orders.v1.OrderAmendmentCalculationService/RecalculateOrderAmendments")
	}
	packed, err := anypb.New(req.GetFilter())
	if err != nil {
		return grpcx.Wrap(err, "pack payload")
	}
	previewOrderNoteRequest := &ordersv1.PreviewOrderNoteRequest{
		Context: req.GetContext(),
		OrderNote: &ordersv1.OrderNote{
			Subject: &ordersv1.OrderRef{
				Number:      recalculateOrderAmendmentsReply.GetOrderAmendment().GetNumber(),
				DisplayName: recalculateOrderAmendmentsReply.GetOrderAmendment().GetCounterparty().GetDisplayName(),
			},
			Detail: packed,
		},
		Details: &ordersv1.PreviewOrderNoteRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email: calculatePreferenceReply.GetPreference().GetOwner().GetEmail(),
				Name:  req.GetSubscriber().GetName(),
			},
		},
	}
	previewOrderNoteReply := new(ordersv1.PreviewOrderNoteResponse)
	if err := grpcx.LegacyInvoke(ctx, s.conn, "/acme.orders.v1.OrderNoteValidationService/PreviewOrderNote", previewOrderNoteRequest, previewOrderNoteReply); err != nil {
		return grpcx.Wrap(err, "acme.orders.v1.OrderNoteValidationService/PreviewOrderNote")
	}
	if err := stream.Send(&ordersv1.StreamOrderMetricsResponse{
		Order: &ordersv1.Order{
			Audit: &commonv1.AuditInfo{
				Etag:      calculatePreferenceReply.GetPreference().GetAudit().GetEtag(),
				CreatedBy: calculatePreferenceReply.GetPreference().GetAudit().GetCreatedBy(),
				CreatedAt: recalculateOrderAmendmentsReply.GetOrderAmendment().GetAudit().GetCreatedAt(),
			},
			BillingAddress: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
			Counterparty:   &customersv1.CustomerRef{Email: req.GetSubscriber().GetEmail()},
		},
		Sequence: previewOrderNoteReply.GetOrderNote().GetSequence(),
		Result:   &commonv1.OperationResult{Status: "ok"},
	}); err != nil {
		return err
	}
	return nil
}

// ShareOrderReport shares an order.
func (s *OrderReportingServer) ShareOrderReport(ctx context.Context, req *ordersv1.ShareOrderReportRequest) (*ordersv1.ShareOrderReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ShareOrderReport", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "order", req.GetOrderId(), "share_order_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "share order report", "tenant", req.GetContext().GetTenantId())
	detailsCache := cache.New[string, *ordersv1.ShareOrderReportRequest_Details](1)
	detailsCache.Put(req.GetContext().GetTraceId(), req.GetDetails())
	cachedDetails, _ := detailsCache.Get(req.GetContext().GetTraceId())
	cloned := proto.Clone(req.GetDetails()).(*ordersv1.ShareOrderReportRequest_Details)
	var envelope ordersv1.OrderRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	explainBackorderCalculationReply, err := s.backorderCalculationClient.ExplainBackorderCalculation(ctx, &ordersv1.ExplainBackorderCalculationRequest{
		Context: req.GetContext(),
		Backorder: &ordersv1.Backorder{
			Requester: &commonv1.ContactInfo{Phone: req.GetDetails().GetRecipient().GetPhone()},
		},
		Details: &ordersv1.ExplainBackorderCalculationRequest_Details{
			Contact: &commonv1.ContactInfo{
				Name:             cachedDetails.GetRecipient().GetName(),
				Email:            cloned.GetRecipient().GetEmail(),
				PreferredChannel: req.GetDetails().GetRecipient().GetPreferredChannel(),
			},
		},
		BackorderId: envelope.GetDisplayName(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.BackorderCalculationService/ExplainBackorderCalculation")
	}
	cloned2 := proto.Clone(req.GetDetails()).(*ordersv1.ShareOrderReportRequest_Details)
	var envelope2 ordersv1.OrderRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	checkPreferenceEligibilityReply, err := s.preferenceValidationClient.CheckPreferenceEligibility(ctx, &customersv1.CheckPreferenceEligibilityRequest{
		Context: req.GetContext(),
		Details: &customersv1.CheckPreferenceEligibilityRequest_Details{
			Contact: &commonv1.ContactInfo{
				Phone: explainBackorderCalculationReply.GetBackorder().GetRequester().GetPhone(),
				Email: cloned2.GetRecipient().GetEmail(),
			},
		},
		Preference: &customersv1.Preference{
			Owner: &identityv1.UserRef{
				DisplayName: explainBackorderCalculationReply.GetBackorder().GetAssignee().GetDisplayName(),
			},
		},
		PreferenceId: envelope2.GetDisplayName(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceValidationService/CheckPreferenceEligibility")
	}
	return &ordersv1.ShareOrderReportResponse{
		Order: &ordersv1.Order{
			Counterparty: &customersv1.CustomerRef{
				DisplayName: explainBackorderCalculationReply.GetBackorder().GetAssignee().GetDisplayName(),
				Email:       req.GetDetails().GetRecipient().GetEmail(),
			},
			OrderId: req.GetOrderId(),
		},
		Result: &commonv1.OperationResult{
			Message: checkPreferenceEligibilityReply.GetOutcome().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// CountOrderTotals counts orders by status.
func (s *OrderReportingServer) CountOrderTotals(ctx context.Context, req *ordersv1.CountOrderTotalsRequest) (*ordersv1.CountOrderTotalsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "count order totals", "tenant", req.GetContext().GetTenantId())
	return &ordersv1.CountOrderTotalsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// DeleteOrderReport deletes an order.
func (s *OrderReportingServer) DeleteOrderReport(ctx context.Context, req *ordersv1.DeleteOrderReportRequest) (*ordersv1.DeleteOrderReportResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "order", req.GetOrderId(), "delete_order_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "delete order report", "tenant", req.GetContext().GetTenantId())
	snapshot, err := protojson.Marshal(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	revertSynonymRequest := &searchv1.RevertSynonymRequest{
		Context: req.GetContext(),
		Synonym: &searchv1.Synonym{
			Audit:  &commonv1.AuditInfo{Etag: req.GetEtag()},
			Owner:  &identityv1.UserRef{Email: req.GetContext().GetActor().GetEmail()},
			Action: &searchv1.Synonym_Block{Block: &searchv1.SynonymBlockAction{Reason: req.GetReason()}},
		},
		Details: &searchv1.RevertSynonymRequest_Details{Snapshot: snapshot},
	}
	revertSynonymReply, err := s.synonymHistoryPort.RevertSynonym(ctx, revertSynonymRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymHistoryService/RevertSynonym")
	}
	lintCustomerSegmentsRequest := &customersv1.LintCustomerSegmentsRequest{
		Context: req.GetContext(),
		Details: &customersv1.LintCustomerSegmentsRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email: revertSynonymReply.GetSynonym().GetOwner().GetEmail(),
			},
		},
		CustomerSegment: &customersv1.CustomerSegment{
			Action: &customersv1.CustomerSegment_Route{Route: &customersv1.CustomerSegmentRouteAction{
				Weight: revertSynonymReply.GetSynonym().GetRoute().GetWeight(),
			}},
			Owner: &identityv1.UserRef{
				DisplayName: revertSynonymReply.GetSynonym().GetOwner().GetDisplayName(),
				UserId:      revertSynonymReply.GetSynonym().GetOwner().GetUserId(),
			},
		},
	}
	lintCustomerSegmentsReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*customersv1.LintCustomerSegmentsResponse, error) {
		return s.customerSegmentValidationClient.LintCustomerSegments(ctx, lintCustomerSegmentsRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentValidationService/LintCustomerSegments")
	}
	return &ordersv1.DeleteOrderReportResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: revertSynonymReply.GetResult().GetCorrelationId(),
			ReferenceIds:  revertSynonymReply.GetResult().GetReferenceIds(),
			Message:       lintCustomerSegmentsReply.GetResult().GetMessage(),
			Status:        "ok",
		},
	}, nil
}
