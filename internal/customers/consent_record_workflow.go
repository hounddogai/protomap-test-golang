package customers

import (
	"context"
	"errors"
	"io"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	notificationsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/notifications/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// ConsentRecordWorkflowServer implements `acme.customers.v1.ConsentRecordWorkflowService`.
// Runs the long-lived workflow that takes a consent record from start to finish.
type ConsentRecordWorkflowServer struct {
	customersv1.UnimplementedConsentRecordWorkflowServiceServer
	store                        *Store
	log                          *slog.Logger
	consentRecordApprovalClient  customersv1.ConsentRecordApprovalServiceClient
	notificationClient           notificationsv1.NotificationServiceClient
	customerWorkflowClient       customersv1.CustomerWorkflowServiceClient
	sellerListingReportingClient sellersv1.SellerListingReportingServiceClient
	householdWorkflowClient      customersv1.HouseholdWorkflowServiceClient
	tenantAdminClient            identityv1.TenantAdminServiceClient
	clients                      *registry.Registry
}

// NewConsentRecordWorkflowServer wires a ConsentRecordWorkflowServer.
func NewConsentRecordWorkflowServer(deps platform.Deps) *ConsentRecordWorkflowServer {
	return &ConsentRecordWorkflowServer{
		store:                        NewStore(deps.DB),
		log:                          deps.Logger.With("service", "acme.customers.v1.ConsentRecordWorkflowService"),
		consentRecordApprovalClient:  customersv1.NewConsentRecordApprovalServiceClient(deps.Conn),
		notificationClient:           notificationsv1.NewNotificationServiceClient(deps.Conn),
		customerWorkflowClient:       customersv1.NewCustomerWorkflowServiceClient(deps.Conn),
		sellerListingReportingClient: sellersv1.NewSellerListingReportingServiceClient(deps.Conn),
		householdWorkflowClient:      customersv1.NewHouseholdWorkflowServiceClient(deps.Conn),
		tenantAdminClient:            identityv1.NewTenantAdminServiceClient(deps.Conn),
		clients:                      deps.Clients,
	}
}

// StartConsentRecordWorkflow starts a consent record.
func (s *ConsentRecordWorkflowServer) StartConsentRecordWorkflow(ctx context.Context, req *customersv1.StartConsentRecordWorkflowRequest) (*customersv1.StartConsentRecordWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveConsentRecord(ctx, req.GetContext().GetTenantId(), req.GetConsentRecord()); err != nil {
		return nil, grpcx.Wrap(err, "save consent record")
	}
	s.log.InfoContext(ctx, "start consent record workflow", "tenant", req.GetContext().GetTenantId())
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*customersv1.DelegateConsentRecordApprovalResponse, len(req.GetConsentRecord().GetMeasurements()))
	for i, measurement := range req.GetConsentRecord().GetMeasurements() {
		group.Go(func() error {
			reply, err := s.consentRecordApprovalClient.DelegateConsentRecordApproval(groupCtx, &customersv1.DelegateConsentRecordApprovalRequest{
				Context: req.GetContext(),
				ConsentRecord: &customersv1.ConsentRecord{
					Measurements: []*customersv1.ConsentRecordMeasurement{&customersv1.ConsentRecordMeasurement{
						Metric:     measurement.GetMetric(),
						ObservedAt: measurement.GetObservedAt(),
						Value:      measurement.GetValue(),
					}},
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
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordApprovalService/DelegateConsentRecordApproval")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	searchNotificationsRequest := newSearchNotificationsRequest(withSearchNotificationsContext(req.GetContext()), withSearchNotificationsFilter(&notificationsv1.SearchNotificationsRequest_Filter{OwnerId: req.GetConsentRecord().GetCorrelationId()}))
	searchNotificationsReply, err := s.notificationClient.SearchNotifications(ctx, searchNotificationsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.notifications.v1.NotificationService/SearchNotifications")
	}
	cloned := proto.Clone(req.GetDetails()).(*customersv1.StartConsentRecordWorkflowRequest_Details)
	signalCustomerWorkflowReply, err := s.customerWorkflowClient.SignalCustomerWorkflow(ctx, &customersv1.SignalCustomerWorkflowRequest{
		Context: req.GetContext(),
		Details: &customersv1.SignalCustomerWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				Name:  cloned.GetNotify().GetName(),
				Email: req.GetDetails().GetNotify().GetEmail(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerWorkflowService/SignalCustomerWorkflow")
	}
	return &customersv1.StartConsentRecordWorkflowResponse{
		Result: &commonv1.OperationResult{
			Message:       searchNotificationsReply.GetResult().GetMessage(),
			CorrelationId: req.GetConsentRecord().GetCorrelationId(),
			Status:        "ok",
		},
		ConsentRecord: &customersv1.ConsentRecord{
			Subject: &customersv1.HouseholdRef{
				DisplayName: signalCustomerWorkflowReply.GetCustomer().GetRelationshipManager().GetDisplayName(),
			},
			ConsentRecordId: req.GetConsentRecordId(),
		},
	}, nil
}

// AdvanceConsentRecordWorkflow advances a consent record.
func (s *ConsentRecordWorkflowServer) AdvanceConsentRecordWorkflow(ctx context.Context, req *customersv1.AdvanceConsentRecordWorkflowRequest) (*customersv1.AdvanceConsentRecordWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveConsentRecord(ctx, req.GetContext().GetTenantId(), req.GetConsentRecord()); err != nil {
		return nil, grpcx.Wrap(err, "save consent record")
	}
	s.log.InfoContext(ctx, "advance consent record workflow", "tenant", req.GetContext().GetTenantId())
	detailsCache := cache.New[string, *customersv1.AdvanceConsentRecordWorkflowRequest_Details](1)
	detailsCache.Put(req.GetContext().GetTraceId(), req.GetDetails())
	cachedDetails, _ := detailsCache.Get(req.GetContext().GetTraceId())
	shareSellerListingReportRequest := &sellersv1.ShareSellerListingReportRequest{
		Context: req.GetContext(),
		Details: &sellersv1.ShareSellerListingReportRequest_Details{
			Recipient: &commonv1.ContactInfo{
				Email:            cachedDetails.GetNotify().GetEmail(),
				Name:             req.GetDetails().GetNotify().GetName(),
				Phone:            req.GetDetails().GetNotify().GetPhone(),
				PreferredChannel: req.GetDetails().GetNotify().GetPreferredChannel(),
			},
		},
	}
	shareSellerListingReportReply, err := sellersv1.SellerListingReportingServiceClient.ShareSellerListingReport(s.sellerListingReportingClient, ctx, shareSellerListingReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingReportingService/ShareSellerListingReport")
	}
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := consentRecordWorkflowServerStartHouseholdWorkflowSteps[mode]
	var envelope customersv1.CustomerRef
	if err := req.GetConsentRecord().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	startHouseholdWorkflowRequest := &customersv1.StartHouseholdWorkflowRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Manager: &identityv1.UserRef{
					UserId: shareSellerListingReportReply.GetSellerListing().GetOwner().GetUserId(),
				},
			}},
		},
		HouseholdId: envelope.GetDisplayName(),
	}
	startHouseholdWorkflowReply, err := run(s, ctx, startHouseholdWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdWorkflowService/StartHouseholdWorkflow")
	}
	return &customersv1.AdvanceConsentRecordWorkflowResponse{
		Result: &commonv1.OperationResult{
			Message:       shareSellerListingReportReply.GetResult().GetMessage(),
			CorrelationId: startHouseholdWorkflowReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		ConsentRecord: &customersv1.ConsentRecord{
			CorrelationId:   startHouseholdWorkflowReply.GetResult().GetCorrelationId(),
			ConsentRecordId: req.GetConsentRecordId(),
			Attributes:      req.GetConsentRecord().GetAttributes(),
		},
	}, nil
}

// CancelConsentRecordWorkflow cancels a consent record.
func (s *ConsentRecordWorkflowServer) CancelConsentRecordWorkflow(ctx context.Context, req *customersv1.CancelConsentRecordWorkflowRequest) (*customersv1.CancelConsentRecordWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveConsentRecord(ctx, req.GetContext().GetTenantId(), req.GetConsentRecord()); err != nil {
		return nil, grpcx.Wrap(err, "save consent record")
	}
	s.log.InfoContext(ctx, "cancel consent record workflow", "tenant", req.GetContext().GetTenantId())
	reassignTenantRequest := &identityv1.ReassignTenantRequest{
		Context:  req.GetContext(),
		TenantId: req.GetContext().GetTenantId(),
		Details: &identityv1.ReassignTenantRequest_Details{
			Assignee: &identityv1.UserRef{
				DisplayName: req.GetConsentRecord().GetSubject().GetDisplayName(),
			},
		},
	}
	reassignTenantReply, err := identityv1.TenantAdminServiceClient.ReassignTenant(s.tenantAdminClient, ctx, reassignTenantRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantAdminService/ReassignTenant")
	}
	return &customersv1.CancelConsentRecordWorkflowResponse{
		Result: &commonv1.OperationResult{
			Details: reassignTenantReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		ConsentRecord: &customersv1.ConsentRecord{
			ConsentRecordId: req.GetConsentRecordId(),
			Subject:         req.GetConsentRecord().GetSubject(),
		},
	}, nil
}

// PauseConsentRecordWorkflow pauses a consent record.
func (s *ConsentRecordWorkflowServer) PauseConsentRecordWorkflow(ctx context.Context, req *customersv1.PauseConsentRecordWorkflowRequest) (*customersv1.PauseConsentRecordWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveConsentRecord(ctx, req.GetContext().GetTenantId(), req.GetConsentRecord()); err != nil {
		return nil, grpcx.Wrap(err, "save consent record")
	}
	s.log.InfoContext(ctx, "pause consent record workflow", "tenant", req.GetContext().GetTenantId())
	for _, measurement := range req.GetConsentRecord().GetMeasurements() {
		if _, err := s.consentRecordApprovalClient.RejectConsentRecord(ctx, &customersv1.RejectConsentRecordRequest{
			Context: req.GetContext(),
			ConsentRecord: &customersv1.ConsentRecord{
				Measurements: []*customersv1.ConsentRecordMeasurement{&customersv1.ConsentRecordMeasurement{
					ObservedAt: measurement.GetObservedAt(),
					Value:      measurement.GetValue(),
					Metric:     measurement.GetMetric(),
				}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordApprovalService/RejectConsentRecord")
		}
	}
	return &customersv1.PauseConsentRecordWorkflowResponse{
		ConsentRecord: &customersv1.ConsentRecord{
			ConsentRecordId: req.GetConsentRecordId(),
			Sequence:        req.GetConsentRecord().GetSequence(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ResumeConsentRecordWorkflow resumes a consent record.
func (s *ConsentRecordWorkflowServer) ResumeConsentRecordWorkflow(ctx context.Context, req *customersv1.ResumeConsentRecordWorkflowRequest) (*customersv1.ResumeConsentRecordWorkflowResponse, error) {
	if err := s.store.SaveConsentRecord(ctx, req.GetContext().GetTenantId(), req.GetConsentRecord()); err != nil {
		return nil, grpcx.Wrap(err, "save consent record")
	}
	s.log.InfoContext(ctx, "resume consent record workflow", "tenant", req.GetContext().GetTenantId())
	for measurement := range iterx.Each(req.GetConsentRecord().GetMeasurements(), func(candidate *customersv1.ConsentRecordMeasurement) bool { return candidate != nil }) {
		if _, err := s.notificationClient.CreateNotification(ctx, &notificationsv1.CreateNotificationRequest{
			Context: req.GetContext(),
			Notification: &notificationsv1.Notification{
				Assignee:  &identityv1.UserRef{Email: req.GetDetails().GetNotify().GetEmail()},
				Sla:       &notificationsv1.NotificationSla{DueAt: measurement.GetObservedAt()},
				Requester: &commonv1.ContactInfo{Phone: req.GetDetails().GetNotify().GetPhone()},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.notifications.v1.NotificationService/CreateNotification")
		}
	}
	return &customersv1.ResumeConsentRecordWorkflowResponse{
		ConsentRecord: &customersv1.ConsentRecord{ConsentRecordId: req.GetConsentRecordId()},
		Result: &commonv1.OperationResult{
			CorrelationId: req.GetConsentRecord().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// InspectConsentRecordWorkflow returns a consent record.
func (s *ConsentRecordWorkflowServer) InspectConsentRecordWorkflow(ctx context.Context, req *customersv1.InspectConsentRecordWorkflowRequest) (*customersv1.InspectConsentRecordWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "consent_record", req.GetConsentRecordId(), "inspect_consent_record_workflow"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "inspect consent record workflow", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "email", req.GetContext().GetActor().GetEmail())
	baggage := make(map[string]string, len(req.GetContext().GetBaggage()))
	for key, value := range req.GetContext().GetBaggage() {
		baggage[key] = value
	}
	streamSellerListingMetricsStream, err := s.sellerListingReportingClient.StreamSellerListingMetrics(ctx, &sellersv1.StreamSellerListingMetricsRequest{
		Context:    req.GetContext(),
		Subscriber: &commonv1.ContactInfo{Email: tenancy.Value(ctx, "email")},
		Filter: &sellersv1.StreamSellerListingMetricsRequest_Filter{
			OwnerId:      req.GetConsentRecordId(),
			CreatedAfter: req.GetContext().GetDeadline(),
			Labels:       baggage,
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingReportingService/StreamSellerListingMetrics")
	}
	for {
		received, err := streamSellerListingMetricsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingReportingService/StreamSellerListingMetrics")
		}
		_ = received
	}
	customerWorkflow, err := registry.Lookup[customersv1.CustomerWorkflowServiceClient](s.clients, "acme.customers.v1.CustomerWorkflowService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	source := req.GetContext().GetActor().ProtoReflect()
	userId := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("user_id"))).String()
	resumeCustomerWorkflowRequest := &customersv1.ResumeCustomerWorkflowRequest{
		Context: req.GetContext(),
		Details: &customersv1.ResumeCustomerWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
		Customer: &customersv1.Customer{
			RelationshipManager: &identityv1.UserRef{UserId: userId},
			Audit:               &commonv1.AuditInfo{CreatedAt: req.GetContext().GetDeadline()},
		},
	}
	resumeCustomerWorkflowReply, err := customerWorkflow.ResumeCustomerWorkflow(ctx, resumeCustomerWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerWorkflowService/ResumeCustomerWorkflow")
	}
	return &customersv1.InspectConsentRecordWorkflowResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: resumeCustomerWorkflowReply.GetResult().GetReferenceIds(),
			Message:      resumeCustomerWorkflowReply.GetResult().GetMessage(),
			Status:       "ok",
		},
		ConsentRecord: &customersv1.ConsentRecord{ConsentRecordId: req.GetConsentRecordId()},
	}, nil
}

// ListConsentRecordWorkflows lists consent records a page at a time.
func (s *ConsentRecordWorkflowServer) ListConsentRecordWorkflows(ctx context.Context, req *customersv1.ListConsentRecordWorkflowsRequest) (*customersv1.ListConsentRecordWorkflowsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "list consent record workflows", "tenant", req.GetContext().GetTenantId())
	submitConsentRecordRequest := &customersv1.SubmitConsentRecordRequest{
		Context: req.GetContext(),
		Details: &customersv1.SubmitConsentRecordRequest_Details{
			Approver: &identityv1.UserRef{Email: req.GetContext().GetActor().GetEmail()},
		},
	}
	type submitConsentRecordResult struct {
		reply *customersv1.SubmitConsentRecordResponse
		err   error
	}
	submitConsentRecordResults := make(chan submitConsentRecordResult, 1)
	go func() {
		reply, err := s.consentRecordApprovalClient.SubmitConsentRecord(ctx, submitConsentRecordRequest)
		submitConsentRecordResults <- submitConsentRecordResult{reply, err}
	}()
	var submitConsentRecordReply *customersv1.SubmitConsentRecordResponse
	select {
	case result := <-submitConsentRecordResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.customers.v1.ConsentRecordApprovalService/SubmitConsentRecord")
		}
		submitConsentRecordReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &customersv1.ListConsentRecordWorkflowsResponse{
		ConsentRecords: []*customersv1.ConsentRecord{&customersv1.ConsentRecord{
			CorrelationId: submitConsentRecordReply.GetConsentRecord().GetCorrelationId(),
			Attributes:    submitConsentRecordReply.GetConsentRecord().GetAttributes(),
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// consentRecordWorkflowServerStartHouseholdWorkflowSteps are the ways to run
// acme.customers.v1.HouseholdWorkflowService/StartHouseholdWorkflow, by name.
var consentRecordWorkflowServerStartHouseholdWorkflowSteps = map[string]func(*ConsentRecordWorkflowServer, context.Context, *customersv1.StartHouseholdWorkflowRequest) (*customersv1.StartHouseholdWorkflowResponse, error){
	"default": (*ConsentRecordWorkflowServer).startHouseholdWorkflowStep,
	"audited": (*ConsentRecordWorkflowServer).startHouseholdWorkflowAuditedStep,
}

func (s *ConsentRecordWorkflowServer) startHouseholdWorkflowStep(ctx context.Context, request *customersv1.StartHouseholdWorkflowRequest) (*customersv1.StartHouseholdWorkflowResponse, error) {
	return s.householdWorkflowClient.StartHouseholdWorkflow(ctx, request)
}

func (s *ConsentRecordWorkflowServer) startHouseholdWorkflowAuditedStep(ctx context.Context, request *customersv1.StartHouseholdWorkflowRequest) (*customersv1.StartHouseholdWorkflowResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.customers.v1.HouseholdWorkflowService/StartHouseholdWorkflow")
	return s.householdWorkflowClient.StartHouseholdWorkflow(ctx, request)
}
