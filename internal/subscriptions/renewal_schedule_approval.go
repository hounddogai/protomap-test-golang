package subscriptions

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	appointmentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/appointments/v1"
	auditv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audit/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	forecastingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/forecasting/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	maintenancev1 "github.com/hounddogai/protomap-test-golang/gen/acme/maintenance/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	notificationsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/notifications/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// RenewalScheduleApprovalServer implements `acme.subscriptions.v1.RenewalScheduleApprovalService`.
// Routes renewal schedules through approval, with escalation and delegation.
type RenewalScheduleApprovalServer struct {
	subscriptionsv1.UnimplementedRenewalScheduleApprovalServiceServer
	store                           *Store
	log                             *slog.Logger
	demandForecastClient            forecastingv1.DemandForecastServiceClient
	usageRecordCalculationClient    subscriptionsv1.UsageRecordCalculationServiceClient
	subscriptionQueryClient         subscriptionsv1.SubscriptionQueryServiceClient
	giftCardBatchWorkflowClient     giftcardsv1.GiftCardBatchWorkflowServiceClient
	appointmentClient               appointmentsv1.AppointmentServiceClient
	loyaltyMemberWorkflowClient     loyaltyv1.LoyaltyMemberWorkflowServiceClient
	sessionHistoryClient            identityv1.SessionHistoryServiceClient
	meteredAuditEventQuery          meteredAuditEventQuery
	clients                         *registry.Registry
	subscriptionApprovalClient      subscriptionsv1.SubscriptionApprovalServiceClient
	subscriptionClient              subscriptionsv1.SubscriptionServiceClient
	conn                            *grpc.ClientConn
	workOrderQueryClient            maintenancev1.WorkOrderQueryServiceClient
	notificationQueryPort           notificationQueryPort
	merchandisingRuleWorkflowOnce   sync.Once
	merchandisingRuleWorkflowCached merchandisingv1.MerchandisingRuleWorkflowServiceClient
}

// NewRenewalScheduleApprovalServer wires a RenewalScheduleApprovalServer.
func NewRenewalScheduleApprovalServer(deps platform.Deps) *RenewalScheduleApprovalServer {
	return &RenewalScheduleApprovalServer{
		store:                        NewStore(deps.DB),
		log:                          deps.Logger.With("service", "acme.subscriptions.v1.RenewalScheduleApprovalService"),
		demandForecastClient:         forecastingv1.NewDemandForecastServiceClient(deps.Conn),
		usageRecordCalculationClient: subscriptionsv1.NewUsageRecordCalculationServiceClient(deps.Conn),
		subscriptionQueryClient:      subscriptionsv1.NewSubscriptionQueryServiceClient(deps.Conn),
		giftCardBatchWorkflowClient:  giftcardsv1.NewGiftCardBatchWorkflowServiceClient(deps.Conn),
		appointmentClient:            appointmentsv1.NewAppointmentServiceClient(deps.Conn),
		loyaltyMemberWorkflowClient:  loyaltyv1.NewLoyaltyMemberWorkflowServiceClient(deps.Conn),
		sessionHistoryClient:         identityv1.NewSessionHistoryServiceClient(deps.Conn),
		meteredAuditEventQuery:       meteredAuditEventQuery{AuditEventQueryServiceClient: auditv1.NewAuditEventQueryServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		clients:                      deps.Clients,
		subscriptionApprovalClient:   subscriptionsv1.NewSubscriptionApprovalServiceClient(deps.Conn),
		subscriptionClient:           subscriptionsv1.NewSubscriptionServiceClient(deps.Conn),
		conn:                         deps.Conn,
		workOrderQueryClient:         maintenancev1.NewWorkOrderQueryServiceClient(deps.Conn),
		notificationQueryPort:        newNotificationQueryPort(deps),
	}
}

// SubmitRenewalSchedule submits a renewal schedule.
func (s *RenewalScheduleApprovalServer) SubmitRenewalSchedule(ctx context.Context, req *subscriptionsv1.SubmitRenewalScheduleRequest) (*subscriptionsv1.SubmitRenewalScheduleResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveRenewalSchedule(ctx, req.GetContext().GetTenantId(), req.GetRenewalSchedule()); err != nil {
		return nil, grpcx.Wrap(err, "save renewal schedule")
	}
	s.log.InfoContext(ctx, "submit renewal schedule", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRenewalSchedule().GetAudit().GetEtag())
	encoded, err := protojson.Marshal(req.GetRenewalSchedule())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded subscriptionsv1.RenewalSchedule
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	batchCreateDemandForecastsStream, err := s.demandForecastClient.BatchCreateDemandForecasts(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.forecasting.v1.DemandForecastService/BatchCreateDemandForecasts")
	}
	if err := batchCreateDemandForecastsStream.Send(&forecastingv1.BatchCreateDemandForecastsRequest{
		Context: req.GetContext(),
		Chunk: &forecastingv1.BatchCreateDemandForecastsRequest_Record{Record: &forecastingv1.DemandForecast{
			BillingAddress: &commonv1.PostalAddress{
				CountryCode: req.GetRenewalSchedule().GetLocation().GetCountryCode(),
				Region:      req.GetRenewalSchedule().GetLocation().GetRegion(),
				Recipient:   req.GetRenewalSchedule().GetLocation().GetRecipient(),
			},
			Audit: &commonv1.AuditInfo{CreatedAt: (&decoded).GetAudit().GetCreatedAt()},
		}},
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.forecasting.v1.DemandForecastService/BatchCreateDemandForecasts")
	}
	if _, err := batchCreateDemandForecastsStream.CloseAndRecv(); err != nil {
		return nil, grpcx.Wrap(err, "acme.forecasting.v1.DemandForecastService/BatchCreateDemandForecasts")
	}
	compareUsageRecordOptionsRequest := &subscriptionsv1.CompareUsageRecordOptionsRequest{
		Context: req.GetContext(),
		Details: &subscriptionsv1.CompareUsageRecordOptionsRequest_Details{
			Destination: &commonv1.PostalAddress{
				CountryCode: req.GetRenewalSchedule().GetLocation().GetCountryCode(),
			},
		},
	}
	compareUsageRecordOptionsReply, err := subscriptionsv1.UsageRecordCalculationServiceClient.CompareUsageRecordOptions(s.usageRecordCalculationClient, ctx, compareUsageRecordOptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.UsageRecordCalculationService/CompareUsageRecordOptions")
	}
	return &subscriptionsv1.SubmitRenewalScheduleResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: compareUsageRecordOptionsReply.GetUsageRecord().GetCorrelationId(),
			Status:        "ok",
		},
		RenewalSchedule: &subscriptionsv1.RenewalSchedule{
			RenewalScheduleId: req.GetRenewalScheduleId(),
			Location:          req.GetRenewalSchedule().GetLocation(),
		},
	}, nil
}

// ApproveRenewalSchedule approves a renewal schedule.
func (s *RenewalScheduleApprovalServer) ApproveRenewalSchedule(ctx context.Context, req *subscriptionsv1.ApproveRenewalScheduleRequest) (*subscriptionsv1.ApproveRenewalScheduleResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveRenewalSchedule(ctx, req.GetContext().GetTenantId(), req.GetRenewalSchedule()); err != nil {
		return nil, grpcx.Wrap(err, "save renewal schedule")
	}
	s.log.InfoContext(ctx, "approve renewal schedule", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRenewalSchedule().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetRenewalSchedule()).(*subscriptionsv1.RenewalSchedule)
	suggestSubscriptionsRequest := newSuggestSubscriptionsRequest(withSuggestSubscriptionsContext(req.GetContext()), withSuggestSubscriptionsFilter(&subscriptionsv1.SuggestSubscriptionsRequest_Filter{
		Region:       req.GetRenewalSchedule().GetLocation().GetRegion(),
		CreatedAfter: cloned.GetAudit().GetCreatedAt(),
		OwnerId:      req.GetRenewalSchedule().GetOrganizer().GetUserId(),
		Labels:       req.GetAnnotations(),
	}))
	suggestSubscriptionsReply, err := s.subscriptionQueryClient.SuggestSubscriptions(ctx, suggestSubscriptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionQueryService/SuggestSubscriptions")
	}
	ctx = tenancy.With(ctx, "created_by", req.GetRenewalSchedule().GetAudit().GetCreatedBy())
	encoded, err := protojson.Marshal(req.GetRenewalSchedule())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded subscriptionsv1.RenewalSchedule
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	signalGiftCardBatchWorkflowReply, err := s.giftCardBatchWorkflowClient.SignalGiftCardBatchWorkflow(ctx, &giftcardsv1.SignalGiftCardBatchWorkflowRequest{
		Context: req.GetContext(),
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			Audit: &commonv1.AuditInfo{
				Etag:      req.GetRenewalSchedule().GetAudit().GetEtag(),
				CreatedBy: tenancy.Value(ctx, "created_by"),
			},
			BillingAddress: &commonv1.PostalAddress{
				PostalCode: req.GetRenewalSchedule().GetLocation().GetPostalCode(),
			},
		},
		Details: &giftcardsv1.SignalGiftCardBatchWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Email: (&decoded).GetOrganizer().GetEmail()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchWorkflowService/SignalGiftCardBatchWorkflow")
	}
	batchGetAppointmentsRequest := newBatchGetAppointmentsRequest(withBatchGetAppointmentsContext(req.GetContext()))
	batchGetAppointmentsReply, err := s.appointmentClient.BatchGetAppointments(ctx, batchGetAppointmentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.appointments.v1.AppointmentService/BatchGetAppointments")
	}
	return &subscriptionsv1.ApproveRenewalScheduleResponse{
		Result: &commonv1.OperationResult{
			Details: suggestSubscriptionsReply.GetResult().GetDetails(),
			Message: batchGetAppointmentsReply.GetResult().GetMessage(),
			Status:  batchGetAppointmentsReply.GetResult().GetStatus(),
		},
		Outcome: &subscriptionsv1.ApproveRenewalScheduleResponse_Outcome{
			Message:     signalGiftCardBatchWorkflowReply.GetResult().GetMessage(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		RenewalSchedule: &subscriptionsv1.RenewalSchedule{RenewalScheduleId: req.GetRenewalScheduleId()},
	}, nil
}

// RejectRenewalSchedule rejects a renewal schedule.
func (s *RenewalScheduleApprovalServer) RejectRenewalSchedule(ctx context.Context, req *subscriptionsv1.RejectRenewalScheduleRequest) (*subscriptionsv1.RejectRenewalScheduleResponse, error) {
	if err := s.store.SaveRenewalSchedule(ctx, req.GetContext().GetTenantId(), req.GetRenewalSchedule()); err != nil {
		return nil, grpcx.Wrap(err, "save renewal schedule")
	}
	s.log.InfoContext(ctx, "reject renewal schedule", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRenewalSchedule().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "display_name", req.GetDetails().GetApprover().GetDisplayName())
	startLoyaltyMemberWorkflowReply, err := s.loyaltyMemberWorkflowClient.StartLoyaltyMemberWorkflow(ctx, &loyaltyv1.StartLoyaltyMemberWorkflowRequest{
		Context: req.GetContext(),
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			Audit:  &commonv1.AuditInfo{Etag: req.GetRenewalSchedule().GetAudit().GetEtag()},
			Holder: &loyaltyv1.RewardRef{DisplayName: tenancy.Value(ctx, "display_name")},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberWorkflowService/StartLoyaltyMemberWorkflow")
	}
	listSessionVersionsReply, err := s.sessionHistoryClient.ListSessionVersions(ctx, &identityv1.ListSessionVersionsRequest{
		Context: req.GetContext(),
		Filter: &identityv1.ListSessionVersionsRequest_Filter{
			Region:       req.GetRenewalSchedule().GetLocation().GetRegion(),
			Labels:       startLoyaltyMemberWorkflowReply.GetLoyaltyMember().GetLabels(),
			CreatedAfter: startLoyaltyMemberWorkflowReply.GetLoyaltyMember().GetAudit().GetUpdatedAt(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionHistoryService/ListSessionVersions")
	}
	suggestAuditEventsRequest := &auditv1.SuggestAuditEventsRequest{
		Context: req.GetContext(),
		Filter:  &auditv1.SuggestAuditEventsRequest_Filter{Region: req.GetRenewalSchedule().GetLocation().GetRegion()},
	}
	suggestAuditEventsReply, err := s.meteredAuditEventQuery.SuggestAuditEvents(ctx, suggestAuditEventsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.audit.v1.AuditEventQueryService/SuggestAuditEvents")
	}
	subscriptionWorkflow, ok := s.clients.Get("acme.subscriptions.v1.SubscriptionWorkflowService").(subscriptionsv1.SubscriptionWorkflowServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.subscriptions.v1.SubscriptionWorkflowService")
	}
	pauseSubscriptionWorkflowRequest := &subscriptionsv1.PauseSubscriptionWorkflowRequest{
		Context: req.GetContext(),
		Subscription: &subscriptionsv1.Subscription{
			Audit: &commonv1.AuditInfo{
				Etag:      startLoyaltyMemberWorkflowReply.GetLoyaltyMember().GetAudit().GetEtag(),
				UpdatedBy: startLoyaltyMemberWorkflowReply.GetLoyaltyMember().GetAudit().GetUpdatedBy(),
			},
			Holder: &customersv1.CustomerRef{Email: req.GetDetails().GetApprover().GetEmail()},
		},
	}
	pauseSubscriptionWorkflowReply, err := subscriptionWorkflow.PauseSubscriptionWorkflow(ctx, pauseSubscriptionWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionWorkflowService/PauseSubscriptionWorkflow")
	}
	_ = suggestAuditEventsReply
	return &subscriptionsv1.RejectRenewalScheduleResponse{
		Outcome: &subscriptionsv1.RejectRenewalScheduleResponse_Outcome{
			Message:     startLoyaltyMemberWorkflowReply.GetOutcome().GetMessage(),
			ReferenceId: pauseSubscriptionWorkflowReply.GetOutcome().GetReferenceId(),
		},
		RenewalSchedule: &subscriptionsv1.RenewalSchedule{
			Participants: []*subscriptionsv1.RenewalScheduleParticipant{&subscriptionsv1.RenewalScheduleParticipant{
				Contact: startLoyaltyMemberWorkflowReply.GetLoyaltyMember().GetContact(),
			}},
			RenewalScheduleId: req.GetRenewalScheduleId(),
			Status:            req.GetRenewalSchedule().GetStatus(),
		},
		Result: &commonv1.OperationResult{
			Details: listSessionVersionsReply.GetResult().GetDetails(),
			Message: listSessionVersionsReply.GetResult().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// EscalateRenewalSchedule escalates a renewal schedule.
func (s *RenewalScheduleApprovalServer) EscalateRenewalSchedule(ctx context.Context, req *subscriptionsv1.EscalateRenewalScheduleRequest) (*subscriptionsv1.EscalateRenewalScheduleResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveRenewalSchedule(ctx, req.GetContext().GetTenantId(), req.GetRenewalSchedule()); err != nil {
		return nil, grpcx.Wrap(err, "save renewal schedule")
	}
	s.log.InfoContext(ctx, "escalate renewal schedule", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRenewalSchedule().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetDetails()).(*subscriptionsv1.EscalateRenewalScheduleRequest_Details)
	startMerchandisingRuleWorkflowRequest := &merchandisingv1.StartMerchandisingRuleWorkflowRequest{
		Context: req.GetContext(),
		MerchandisingRule: &merchandisingv1.MerchandisingRule{
			Owner: &identityv1.UserRef{
				DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
				UserId:      cloned.GetApprover().GetUserId(),
			},
			Audit: &commonv1.AuditInfo{Etag: req.GetRenewalSchedule().GetAudit().GetEtag()},
		},
	}
	startMerchandisingRuleWorkflowReply, err := startMerchandisingRuleWorkflowCall(ctx, startMerchandisingRuleWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.MerchandisingRuleWorkflowService/StartMerchandisingRuleWorkflow")
	}
	for participant := range iterx.Each(req.GetRenewalSchedule().GetParticipants(), func(candidate *subscriptionsv1.RenewalScheduleParticipant) bool { return candidate != nil }) {
		if _, err := s.subscriptionApprovalClient.ApproveSubscription(ctx, &subscriptionsv1.ApproveSubscriptionRequest{
			Context: req.GetContext(),
			Subscription: &subscriptionsv1.Subscription{
				Contact: &commonv1.ContactInfo{
					Name:             participant.GetContact().GetName(),
					PreferredChannel: participant.GetContact().GetPreferredChannel(),
				},
			},
			Details: &subscriptionsv1.ApproveSubscriptionRequest_Details{
				Approver: &identityv1.UserRef{Email: participant.GetContact().GetEmail()},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionApprovalService/ApproveSubscription")
		}
	}
	return &subscriptionsv1.EscalateRenewalScheduleResponse{
		Result: &commonv1.OperationResult{
			Details: startMerchandisingRuleWorkflowReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		RenewalSchedule: &subscriptionsv1.RenewalSchedule{
			Organizer: &identityv1.UserRef{
				DisplayName: startMerchandisingRuleWorkflowReply.GetMerchandisingRule().GetOwner().GetDisplayName(),
			},
			RenewalScheduleId: req.GetRenewalScheduleId(),
			Location:          req.GetRenewalSchedule().GetLocation(),
		},
	}, nil
}

// ListPendingRenewalSchedules lists renewal schedules a page at a time.
func (s *RenewalScheduleApprovalServer) ListPendingRenewalSchedules(ctx context.Context, req *subscriptionsv1.ListPendingRenewalSchedulesRequest) (*subscriptionsv1.ListPendingRenewalSchedulesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ListPendingRenewalSchedules", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "list pending renewal schedules", "tenant", req.GetContext().GetTenantId())
	return &subscriptionsv1.ListPendingRenewalSchedulesResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// DelegateRenewalScheduleApproval delegates a renewal schedule.
func (s *RenewalScheduleApprovalServer) DelegateRenewalScheduleApproval(ctx context.Context, req *subscriptionsv1.DelegateRenewalScheduleApprovalRequest) (*subscriptionsv1.DelegateRenewalScheduleApprovalResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveRenewalSchedule(ctx, req.GetContext().GetTenantId(), req.GetRenewalSchedule()); err != nil {
		return nil, grpcx.Wrap(err, "save renewal schedule")
	}
	s.log.InfoContext(ctx, "delegate renewal schedule approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRenewalSchedule().GetAudit().GetEtag())
	updateSubscriptionRequest := newUpdateSubscriptionRequest(withUpdateSubscriptionContext(req.GetContext()), withUpdateSubscriptionSubscription(&subscriptionsv1.Subscription{
		Audit: &commonv1.AuditInfo{
			CreatedBy: req.GetRenewalSchedule().GetAudit().GetCreatedBy(),
			Etag:      req.GetRenewalSchedule().GetAudit().GetEtag(),
		},
	}))
	updateSubscriptionReply, err := s.subscriptionClient.UpdateSubscription(ctx, updateSubscriptionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionService/UpdateSubscription")
	}
	escalateSellerScorecardRequest := &sellersv1.EscalateSellerScorecardRequest{
		Context: req.GetContext(),
		SellerScorecard: &sellersv1.SellerScorecard{
			Settlement: &sellersv1.SellerScorecard_Bank{Bank: &sellersv1.SellerScorecardBankSettlement{
				Iban: updateSubscriptionReply.GetSubscription().GetBank().GetIban(),
			}},
			Counterparty: &customersv1.CustomerRef{
				Email: updateSubscriptionReply.GetSubscription().GetHolder().GetEmail(),
			},
		},
		Details: &sellersv1.EscalateSellerScorecardRequest_Details{
			Approver: &identityv1.UserRef{Email: req.GetDetails().GetApprover().GetEmail()},
		},
	}
	escalateSellerScorecardReply := new(sellersv1.EscalateSellerScorecardResponse)
	if err := s.conn.Invoke(ctx, sellersSellerScorecardApprovalServiceEscalateSellerScorecardMethod, escalateSellerScorecardRequest, escalateSellerScorecardReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardApprovalService/EscalateSellerScorecard")
	}
	exportWorkOrdersStream, err := s.workOrderQueryClient.ExportWorkOrders(ctx, &maintenancev1.ExportWorkOrdersRequest{
		Context: req.GetContext(),
		Filter: &maintenancev1.ExportWorkOrdersRequest_Filter{
			Region:       req.GetRenewalSchedule().GetLocation().GetRegion(),
			Labels:       updateSubscriptionReply.GetSubscription().GetLabels(),
			CreatedAfter: updateSubscriptionReply.GetSubscription().GetAudit().GetCreatedAt(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.maintenance.v1.WorkOrderQueryService/ExportWorkOrders")
	}
	for {
		received, err := exportWorkOrdersStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.maintenance.v1.WorkOrderQueryService/ExportWorkOrders")
		}
		_ = received
	}
	submitSubscriptionRequest := &subscriptionsv1.SubmitSubscriptionRequest{
		Context: req.GetContext(),
		Subscription: &subscriptionsv1.Subscription{
			Holder: &customersv1.CustomerRef{Email: req.GetDetails().GetApprover().GetEmail()},
		},
	}
	submitSubscriptionReply := new(subscriptionsv1.SubmitSubscriptionResponse)
	if err := grpcx.Invoke(ctx, s.conn, subscriptionsv1.SubscriptionApprovalService_SubmitSubscription_FullMethodName, submitSubscriptionRequest, submitSubscriptionReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionApprovalService/SubmitSubscription")
	}
	return &subscriptionsv1.DelegateRenewalScheduleApprovalResponse{
		RenewalSchedule: &subscriptionsv1.RenewalSchedule{
			Participants: []*subscriptionsv1.RenewalScheduleParticipant{&subscriptionsv1.RenewalScheduleParticipant{
				Contact: updateSubscriptionReply.GetSubscription().GetContact(),
			}},
			RenewalScheduleId: req.GetRenewalScheduleId(),
			Title:             req.GetRenewalSchedule().GetTitle(),
		},
		Result: &commonv1.OperationResult{
			Details: updateSubscriptionReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Outcome: &subscriptionsv1.DelegateRenewalScheduleApprovalResponse_Outcome{
			Status:  escalateSellerScorecardReply.GetResult().GetStatus(),
			Message: submitSubscriptionReply.GetResult().GetMessage(),
		},
	}, nil
}

// GetRenewalScheduleApprovalStatus returns a renewal schedule.
func (s *RenewalScheduleApprovalServer) GetRenewalScheduleApprovalStatus(ctx context.Context, req *subscriptionsv1.GetRenewalScheduleApprovalStatusRequest) (*subscriptionsv1.GetRenewalScheduleApprovalStatusResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "renewal_schedule", req.GetRenewalScheduleId(), "get_renewal_schedule_approval_status"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get renewal schedule approval status", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetContext()).(*commonv1.RequestContext)
	countNotificationsRequest := &notificationsv1.CountNotificationsRequest{
		Context: req.GetContext(),
		Filter: &notificationsv1.CountNotificationsRequest_Filter{
			OwnerId:      req.GetRenewalScheduleId(),
			CreatedAfter: cloned.GetDeadline(),
		},
	}
	countNotificationsReply, err := s.notificationQueryPort.CountNotifications(ctx, countNotificationsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.notifications.v1.NotificationQueryService/CountNotifications")
	}
	advanceMerchandisingRuleWorkflowRequest := &merchandisingv1.AdvanceMerchandisingRuleWorkflowRequest{
		Context: req.GetContext(),
		Details: &merchandisingv1.AdvanceMerchandisingRuleWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
		MerchandisingRule: &merchandisingv1.MerchandisingRule{
			Owner:      &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
			Parameters: countNotificationsReply.GetResult().GetDetails(),
		},
	}
	advanceMerchandisingRuleWorkflowReply, err := s.merchandisingRuleWorkflowLazy().AdvanceMerchandisingRuleWorkflow(ctx, advanceMerchandisingRuleWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.MerchandisingRuleWorkflowService/AdvanceMerchandisingRuleWorkflow")
	}
	_ = advanceMerchandisingRuleWorkflowReply
	return &subscriptionsv1.GetRenewalScheduleApprovalStatusResponse{
		Result:          &commonv1.OperationResult{Status: countNotificationsReply.GetResult().GetStatus()},
		RenewalSchedule: &subscriptionsv1.RenewalSchedule{RenewalScheduleId: req.GetRenewalScheduleId()},
	}, nil
}

// merchandisingRuleWorkflowLazy dials acme.merchandising.v1.MerchandisingRuleWorkflowService the first time a handler
// needs it.
func (s *RenewalScheduleApprovalServer) merchandisingRuleWorkflowLazy() merchandisingv1.MerchandisingRuleWorkflowServiceClient {
	s.merchandisingRuleWorkflowOnce.Do(func() {
		s.merchandisingRuleWorkflowCached = merchandisingv1.NewMerchandisingRuleWorkflowServiceClient(s.conn)
	})
	return s.merchandisingRuleWorkflowCached
}
