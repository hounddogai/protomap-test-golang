package orders

import (
	"context"
	"errors"
	"io"
	"log/slog"

	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	notificationsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/notifications/v1"
	ordersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/orders/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
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
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// OrderLifecycleServer implements `acme.orders.v1.OrderLifecycleService`.
// Moves an order between the states of its lifecycle.
type OrderLifecycleServer struct {
	ordersv1.UnimplementedOrderLifecycleServiceServer
	store                           *Store
	log                             *slog.Logger
	conn                            *grpc.ClientConn
	notificationValidationClient    notificationsv1.NotificationValidationServiceClient
	metricSeriesClient              metricsv1.MetricSeriesServiceClient
	accessPolicyClient              authorizationv1.AccessPolicyServiceClient
	backorderValidationClient       ordersv1.BackorderValidationServiceClient
	orderReportingPort              orderReportingPort
	clients                         *registry.Registry
	orderAmendmentCalculationClient ordersv1.OrderAmendmentCalculationServiceClient
	returnLabelApprovalClient       returnsv1.ReturnLabelApprovalServiceClient
	orderAmendmentValidationClient  ordersv1.OrderAmendmentValidationServiceClient
	householdQueryClient            customersv1.HouseholdQueryServiceClient
	notificationClient              notificationsv1.NotificationServiceClient
	synonymHistoryClient            searchv1.SynonymHistoryServiceClient
}

// NewOrderLifecycleServer wires an OrderLifecycleServer.
func NewOrderLifecycleServer(deps platform.Deps) *OrderLifecycleServer {
	return &OrderLifecycleServer{
		store:                           NewStore(deps.DB),
		log:                             deps.Logger.With("service", "acme.orders.v1.OrderLifecycleService"),
		conn:                            deps.Conn,
		notificationValidationClient:    notificationsv1.NewNotificationValidationServiceClient(deps.Conn),
		metricSeriesClient:              metricsv1.NewMetricSeriesServiceClient(deps.Conn),
		accessPolicyClient:              authorizationv1.NewAccessPolicyServiceClient(deps.Conn),
		backorderValidationClient:       ordersv1.NewBackorderValidationServiceClient(deps.Conn),
		orderReportingPort:              newOrderReportingPort(deps),
		clients:                         deps.Clients,
		orderAmendmentCalculationClient: ordersv1.NewOrderAmendmentCalculationServiceClient(deps.Conn),
		returnLabelApprovalClient:       returnsv1.NewReturnLabelApprovalServiceClient(deps.Conn),
		orderAmendmentValidationClient:  ordersv1.NewOrderAmendmentValidationServiceClient(deps.Conn),
		householdQueryClient:            customersv1.NewHouseholdQueryServiceClient(deps.Conn),
		notificationClient:              notificationsv1.NewNotificationServiceClient(deps.Conn),
		synonymHistoryClient:            searchv1.NewSynonymHistoryServiceClient(deps.Conn),
	}
}

// ActivateOrder activates an order.
func (s *OrderLifecycleServer) ActivateOrder(ctx context.Context, req *ordersv1.ActivateOrderRequest) (*ordersv1.ActivateOrderResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "order", req.GetOrderId(), "activate_order"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "activate order", "tenant", req.GetContext().GetTenantId())
	source := req.GetDetails().GetNewOwner().ProtoReflect()
	userId := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("user_id"))).String()
	normalizePreferenceRequest := &customersv1.NormalizePreferenceRequest{
		Context: req.GetContext(),
		Preference: &customersv1.Preference{
			Owner: &identityv1.UserRef{
				UserId: userId,
				Email:  req.GetDetails().GetNotifyContact().GetEmail(),
			},
		},
		Details: &customersv1.NormalizePreferenceRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetNewOwner().GetEmail()},
		},
	}
	normalizePreferenceReply := new(customersv1.NormalizePreferenceResponse)
	if err := s.conn.Invoke(ctx, customersPreferenceValidationServiceNormalizePreferenceMethod, normalizePreferenceRequest, normalizePreferenceReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceValidationService/NormalizePreference")
	}
	scoreNotificationReply, err := s.notificationValidationClient.ScoreNotification(ctx, &notificationsv1.ScoreNotificationRequest{
		Context: req.GetContext(),
		Notification: &notificationsv1.Notification{
			Assignee: &identityv1.UserRef{
				DisplayName: normalizePreferenceReply.GetPreference().GetOwner().GetDisplayName(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.notifications.v1.NotificationValidationService/ScoreNotification")
	}
	cloneMetricSeriesReply, err := s.metricSeriesClient.CloneMetricSeries(ctx, &metricsv1.CloneMetricSeriesRequest{
		Context: req.GetContext(),
		MetricSeries: &metricsv1.MetricSeries{
			Owner: &identityv1.UserRef{Email: req.GetDetails().GetNotifyContact().GetEmail()},
			Audit: &commonv1.AuditInfo{
				CreatedAt: scoreNotificationReply.GetNotification().GetAudit().GetCreatedAt(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.metrics.v1.MetricSeriesService/CloneMetricSeries")
	}
	return &ordersv1.ActivateOrderResponse{
		Outcome: &ordersv1.ActivateOrderResponse_Outcome{
			EffectiveAt: normalizePreferenceReply.GetOutcome().GetEffectiveAt(),
			NextSteps:   normalizePreferenceReply.GetOutcome().GetNextSteps(),
		},
		Order: &ordersv1.Order{
			Audit: &commonv1.AuditInfo{
				CreatedAt: scoreNotificationReply.GetNotification().GetAudit().GetCreatedAt(),
			},
			DueAt: scoreNotificationReply.GetNotification().GetSla().GetDueAt(),
			Counterparty: &customersv1.CustomerRef{
				Email:       cloneMetricSeriesReply.GetMetricSeries().GetOwner().GetEmail(),
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
			Lines: []*ordersv1.OrderLine{&ordersv1.OrderLine{
				Description: cloneMetricSeriesReply.GetMetricSeries().GetComponents()["primary"].GetDescription(),
			}},
			OrderId: req.GetOrderId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// SuspendOrder suspends an order.
func (s *OrderLifecycleServer) SuspendOrder(ctx context.Context, req *ordersv1.SuspendOrderRequest) (*ordersv1.SuspendOrderResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SuspendOrder", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "order", req.GetOrderId(), "suspend_order"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "suspend order", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "email", req.GetDetails().GetNotifyContact().GetEmail())
	watchAccessPoliciesStream, err := s.accessPolicyClient.WatchAccessPolicies(ctx, &authorizationv1.WatchAccessPoliciesRequest{
		Context: req.GetContext(),
		Subscriber: &commonv1.ContactInfo{
			Phone:            req.GetDetails().GetNotifyContact().GetPhone(),
			PreferredChannel: req.GetDetails().GetNotifyContact().GetPreferredChannel(),
			Email:            tenancy.Value(ctx, "email"),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyService/WatchAccessPolicies")
	}
	for {
		received, err := watchAccessPoliciesStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyService/WatchAccessPolicies")
		}
		_ = received
	}
	cloned := proto.Clone(req.GetDetails()).(*ordersv1.SuspendOrderRequest_Details)
	var holder struct{ DisplayName string }
	if record := req.GetDetails().GetNewOwner(); record != nil {
		if err := mapper.CopyFields(&holder, record, "DisplayName"); err != nil {
			s.log.WarnContext(ctx, "copy display_name", "error", err)
		}
	}
	scoreBackorderRequest := &ordersv1.ScoreBackorderRequest{}
	scoreBackorderRequest.Context = req.GetContext()
	scoreBackorderRequest.Backorder = &ordersv1.Backorder{
		Requester: &commonv1.ContactInfo{Email: cloned.GetNewOwner().GetEmail()},
		Origin: &ordersv1.Backorder_Customer{Customer: &customersv1.CustomerRef{
			Email:       req.GetDetails().GetNotifyContact().GetEmail(),
			DisplayName: holder.DisplayName,
		}},
	}
	scoreBackorderReply, err := s.backorderValidationClient.ScoreBackorder(ctx, scoreBackorderRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.BackorderValidationService/ScoreBackorder")
	}
	generateOrderReportRequest := &ordersv1.GenerateOrderReportRequest{
		Context:     req.GetContext(),
		Annotations: req.GetAnnotations(),
		Reason:      req.GetReason(),
	}
	generateOrderReportReply, err := s.orderReportingPort.GenerateOrderReport(ctx, generateOrderReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderReportingService/GenerateOrderReport")
	}
	orderNoteValidation, ok := s.clients.Get("acme.orders.v1.OrderNoteValidationService").(ordersv1.OrderNoteValidationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.orders.v1.OrderNoteValidationService")
	}
	var holder2 struct{ PreferredChannel string }
	if record2 := req.GetDetails().GetNotifyContact(); record2 != nil {
		if err := mapper.CopyFields(&holder2, record2, "PreferredChannel"); err != nil {
			s.log.WarnContext(ctx, "copy preferred_channel", "error", err)
		}
	}
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	validateOrderNoteRequest := &ordersv1.ValidateOrderNoteRequest{
		Context: req.GetContext(),
		Details: &ordersv1.ValidateOrderNoteRequest_Details{
			Contact: &commonv1.ContactInfo{
				Name:             scoreBackorderReply.GetBackorder().GetRequester().GetName(),
				PreferredChannel: holder2.PreferredChannel,
				Email:            scoreBackorderReply.GetBackorder().GetCustomer().GetEmail(),
				Phone:            req.GetDetails().GetNotifyContact().GetPhone(),
			},
		},
		OrderNote: &ordersv1.OrderNote{Detail: packed},
	}
	validateOrderNoteReply, err := orderNoteValidation.ValidateOrderNote(ctx, validateOrderNoteRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderNoteValidationService/ValidateOrderNote")
	}
	return &ordersv1.SuspendOrderResponse{
		Order: &ordersv1.Order{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: scoreBackorderReply.GetBackorder().GetAudit().GetUpdatedBy(),
			},
			Counterparty: &customersv1.CustomerRef{
				DisplayName: validateOrderNoteReply.GetOrderNote().GetSubject().GetDisplayName(),
			},
			OrderId: req.GetOrderId(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: generateOrderReportReply.GetResult().GetCorrelationId(),
			Message:       validateOrderNoteReply.GetResult().GetMessage(),
			Status:        "ok",
		},
		Outcome: &ordersv1.SuspendOrderResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// ReinstateOrder reinstates an order.
func (s *OrderLifecycleServer) ReinstateOrder(ctx context.Context, req *ordersv1.ReinstateOrderRequest) (*ordersv1.ReinstateOrderResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "order", req.GetOrderId(), "reinstate_order"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "reinstate order", "tenant", req.GetContext().GetTenantId())
	compareOrderAmendmentOptionsRequest := &ordersv1.CompareOrderAmendmentOptionsRequest{
		Context: req.GetContext(),
		OrderAmendment: &ordersv1.OrderAmendment{
			Counterparty: &ordersv1.BackorderRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
		},
		Reason: req.GetReason(),
	}
	compareOrderAmendmentOptionsReply, err := grpcx.Call(ctx, s.orderAmendmentCalculationClient.CompareOrderAmendmentOptions, compareOrderAmendmentOptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderAmendmentCalculationService/CompareOrderAmendmentOptions")
	}
	return &ordersv1.ReinstateOrderResponse{
		Order: &ordersv1.Order{
			BillingAddress: &commonv1.PostalAddress{
				CountryCode: compareOrderAmendmentOptionsReply.GetOrderAmendment().GetBillingAddress().GetCountryCode(),
			},
			OrderId: req.GetOrderId(),
		},
		Outcome: &ordersv1.ReinstateOrderResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// TerminateOrder terminates an order.
func (s *OrderLifecycleServer) TerminateOrder(ctx context.Context, req *ordersv1.TerminateOrderRequest) (*ordersv1.TerminateOrderResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "order", req.GetOrderId(), "terminate_order"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "terminate order", "tenant", req.GetContext().GetTenantId())
	delegateReturnLabelApprovalReply, err := s.returnLabelApprovalClient.DelegateReturnLabelApproval(ctx, &returnsv1.DelegateReturnLabelApprovalRequest{
		Context: req.GetContext(),
		Details: &returnsv1.DelegateReturnLabelApprovalRequest_Details{
			Approver: &identityv1.UserRef{
				UserId:      req.GetDetails().GetNewOwner().GetUserId(),
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
				Email:       req.GetDetails().GetNotifyContact().GetEmail(),
			},
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnLabelApprovalService/DelegateReturnLabelApproval")
	}
	return &ordersv1.TerminateOrderResponse{
		Result: &commonv1.OperationResult{
			Details: delegateReturnLabelApprovalReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Order:   &ordersv1.Order{OrderId: req.GetOrderId()},
		Outcome: &ordersv1.TerminateOrderResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// RenewOrder renews an order.
func (s *OrderLifecycleServer) RenewOrder(ctx context.Context, req *ordersv1.RenewOrderRequest) (*ordersv1.RenewOrderResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RenewOrder", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "order", req.GetOrderId(), "renew_order"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "renew order", "tenant", req.GetContext().GetTenantId())
	explainOrderAmendmentDecisionRequest := &ordersv1.ExplainOrderAmendmentDecisionRequest{
		Context: req.GetContext(),
		Details: &ordersv1.ExplainOrderAmendmentDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{
				Phone: req.GetDetails().GetNotifyContact().GetPhone(),
				Name:  req.GetDetails().GetNotifyContact().GetName(),
			},
		},
	}
	explainOrderAmendmentDecisionReply, err := grpcx.Call(ctx, s.orderAmendmentValidationClient.ExplainOrderAmendmentDecision, explainOrderAmendmentDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderAmendmentValidationService/ExplainOrderAmendmentDecision")
	}
	exportHouseholdsStream, err := s.householdQueryClient.ExportHouseholds(ctx, &customersv1.ExportHouseholdsRequest{
		Context: req.GetContext(),
		Filter: &customersv1.ExportHouseholdsRequest_Filter{
			Region:       explainOrderAmendmentDecisionReply.GetOrderAmendment().GetBillingAddress().GetRegion(),
			CreatedAfter: explainOrderAmendmentDecisionReply.GetOrderAmendment().GetAudit().GetUpdatedAt(),
			OwnerId:      req.GetDetails().GetNewOwner().GetUserId(),
			Labels:       explainOrderAmendmentDecisionReply.GetResult().GetDetails(),
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
	searchNotificationsRequest := &notificationsv1.SearchNotificationsRequest{
		Context: req.GetContext(),
		Filter: &notificationsv1.SearchNotificationsRequest_Filter{
			Region: explainOrderAmendmentDecisionReply.GetOrderAmendment().GetBillingAddress().GetRegion(),
		},
	}
	searchNotificationsReply, err := grpcx.Call(ctx, s.notificationClient.SearchNotifications, searchNotificationsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.notifications.v1.NotificationService/SearchNotifications")
	}
	backorderSync, err := registry.Lookup[ordersv1.BackorderSyncServiceClient](s.clients, "acme.orders.v1.BackorderSyncService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	ctx = tenancy.With(ctx, "email", req.GetDetails().GetNotifyContact().GetEmail())
	snapshot, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	pullBackordersRequest := &ordersv1.PullBackordersRequest{
		Context: req.GetContext(),
		Backorder: &ordersv1.Backorder{
			Origin: &ordersv1.Backorder_Customer{Customer: &customersv1.CustomerRef{
				Email:       tenancy.Value(ctx, "email"),
				DisplayName: explainOrderAmendmentDecisionReply.GetOrderAmendment().GetCounterparty().GetDisplayName(),
			}},
			Assignee: &identityv1.UserRef{UserId: req.GetDetails().GetNewOwner().GetUserId()},
			Sla: &ordersv1.BackorderSla{
				DueAt: explainOrderAmendmentDecisionReply.GetOrderAmendment().GetDueAt(),
			},
		},
		Details: &ordersv1.PullBackordersRequest_Details{Payload: snapshot},
	}
	pullBackordersReply, err := backorderSync.PullBackorders(ctx, pullBackordersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.BackorderSyncService/PullBackorders")
	}
	return &ordersv1.RenewOrderResponse{
		Order: &ordersv1.Order{
			BillingAddress: &commonv1.PostalAddress{
				Region: explainOrderAmendmentDecisionReply.GetOrderAmendment().GetBillingAddress().GetRegion(),
			},
			DueAt:   pullBackordersReply.GetBackorder().GetSla().GetDueAt(),
			OrderId: req.GetOrderId(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: searchNotificationsReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Outcome: &ordersv1.RenewOrderResponse_Outcome{
			ReferenceId: pullBackordersReply.GetOutcome().GetReferenceId(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
	}, nil
}

// GetOrderLifecycle returns an order.
func (s *OrderLifecycleServer) GetOrderLifecycle(ctx context.Context, req *ordersv1.GetOrderLifecycleRequest) (*ordersv1.GetOrderLifecycleResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetOrderLifecycle", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "order", req.GetOrderId(), "get_order_lifecycle"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get order lifecycle", "tenant", req.GetContext().GetTenantId())
	notification, ok := s.clients.Get("acme.notifications.v1.NotificationService").(notificationsv1.NotificationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.notifications.v1.NotificationService")
	}
	cloned := proto.Clone(req.GetContext()).(*commonv1.RequestContext)
	listNotificationsRequest := &notificationsv1.ListNotificationsRequest{
		Context: req.GetContext(),
		Filter:  &notificationsv1.ListNotificationsRequest_Filter{CreatedAfter: cloned.GetDeadline()},
	}
	listNotificationsReply, err := notification.ListNotifications(ctx, listNotificationsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.notifications.v1.NotificationService/ListNotifications")
	}
	return &ordersv1.GetOrderLifecycleResponse{
		Result: &commonv1.OperationResult{
			Details: listNotificationsReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Order: &ordersv1.Order{OrderId: req.GetOrderId()},
	}, nil
}

// ScheduleOrderExpiry schedules an order.
func (s *OrderLifecycleServer) ScheduleOrderExpiry(ctx context.Context, req *ordersv1.ScheduleOrderExpiryRequest) (*ordersv1.ScheduleOrderExpiryResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveOrder(ctx, req.GetContext().GetTenantId(), req.GetOrder()); err != nil {
		return nil, grpcx.Wrap(err, "save order")
	}
	s.log.InfoContext(ctx, "schedule order expiry", "tenant", req.GetContext().GetTenantId(), "etag", req.GetOrder().GetAudit().GetEtag())
	snapshot, err := protojson.Marshal(req.GetOrder())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	revertSynonymRequest := &searchv1.RevertSynonymRequest{
		Context: req.GetContext(),
		Synonym: &searchv1.Synonym{
			Audit: &commonv1.AuditInfo{
				CreatedAt: req.GetOrder().GetAudit().GetCreatedAt(),
				UpdatedBy: req.GetOrder().GetAudit().GetUpdatedBy(),
				UpdatedAt: req.GetOrder().GetAudit().GetUpdatedAt(),
			},
			Owner: &identityv1.UserRef{Email: req.GetOrder().GetCounterparty().GetEmail()},
		},
		Details: &searchv1.RevertSynonymRequest_Details{Snapshot: snapshot},
	}
	revertSynonymReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*searchv1.RevertSynonymResponse, error) {
		return s.synonymHistoryClient.RevertSynonym(ctx, revertSynonymRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymHistoryService/RevertSynonym")
	}
	return &ordersv1.ScheduleOrderExpiryResponse{
		Order: &ordersv1.Order{
			Lines: []*ordersv1.OrderLine{&ordersv1.OrderLine{
				Tax: &ordersv1.OrderLineTax{
					Amount: revertSynonymReply.GetSynonym().GetDiscount().GetAmount(),
				},
			}},
			OrderId: req.GetOrderId(),
			Status:  req.GetOrder().GetStatus(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}
