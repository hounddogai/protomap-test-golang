package subscriptions

import (
	"context"
	"errors"
	"io"
	"log/slog"

	audiencesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audiences/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	datasetsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/datasets/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// RenewalScheduleWorkflowServer implements `acme.subscriptions.v1.RenewalScheduleWorkflowService`.
// Runs the long-lived workflow that takes a renewal schedule from start to finish.
type RenewalScheduleWorkflowServer struct {
	subscriptionsv1.UnimplementedRenewalScheduleWorkflowServiceServer
	store                          *Store
	log                            *slog.Logger
	subscriptionWorkflowClient     subscriptionsv1.SubscriptionWorkflowServiceClient
	customerValidationClient       customersv1.CustomerValidationServiceClient
	audienceQueryClient            audiencesv1.AudienceQueryServiceClient
	sellerListingReportingClient   sellersv1.SellerListingReportingServiceClient
	searchIndexNotificationClient  searchv1.SearchIndexNotificationServiceClient
	clients                        *registry.Registry
	listUsersFn                    func(context.Context, *identityv1.ListUsersRequest, ...grpc.CallOption) (*identityv1.ListUsersResponse, error)
	conn                           *grpc.ClientConn
	subscriptionPlanWorkflowClient subscriptionsv1.SubscriptionPlanWorkflowServiceClient
	datasetClient                  datasetsv1.DatasetServiceClient
	renewalScheduleApprovalPort    renewalScheduleApprovalPort
}

// NewRenewalScheduleWorkflowServer wires a RenewalScheduleWorkflowServer.
func NewRenewalScheduleWorkflowServer(deps platform.Deps) RenewalScheduleWorkflowServer {
	user := identityv1.NewUserServiceClient(deps.Conn)
	return RenewalScheduleWorkflowServer{
		store:                          NewStore(deps.DB),
		log:                            deps.Logger.With("service", "acme.subscriptions.v1.RenewalScheduleWorkflowService"),
		subscriptionWorkflowClient:     subscriptionsv1.NewSubscriptionWorkflowServiceClient(deps.Conn),
		customerValidationClient:       customersv1.NewCustomerValidationServiceClient(deps.Conn),
		audienceQueryClient:            audiencesv1.NewAudienceQueryServiceClient(deps.Conn),
		sellerListingReportingClient:   sellersv1.NewSellerListingReportingServiceClient(deps.Conn),
		searchIndexNotificationClient:  searchv1.NewSearchIndexNotificationServiceClient(deps.Conn),
		clients:                        deps.Clients,
		listUsersFn:                    user.ListUsers,
		conn:                           deps.Conn,
		subscriptionPlanWorkflowClient: subscriptionsv1.NewSubscriptionPlanWorkflowServiceClient(deps.Conn),
		datasetClient:                  datasetsv1.NewDatasetServiceClient(deps.Conn),
		renewalScheduleApprovalPort:    newRenewalScheduleApprovalPort(deps),
	}
}

// StartRenewalScheduleWorkflow starts a renewal schedule.
func (s RenewalScheduleWorkflowServer) StartRenewalScheduleWorkflow(ctx context.Context, req *subscriptionsv1.StartRenewalScheduleWorkflowRequest) (*subscriptionsv1.StartRenewalScheduleWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected StartRenewalScheduleWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SaveRenewalSchedule(ctx, req.GetContext().GetTenantId(), req.GetRenewalSchedule()); err != nil {
		return nil, grpcx.Wrap(err, "save renewal schedule")
	}
	s.log.InfoContext(ctx, "start renewal schedule workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRenewalSchedule().GetAudit().GetEtag())
	resumeSubscriptionWorkflowRequest := &subscriptionsv1.ResumeSubscriptionWorkflowRequest{}
	resumeSubscriptionWorkflowRequest.Context = req.GetContext()
	resumeSubscriptionWorkflowRequest.Subscription = &subscriptionsv1.Subscription{
		Audit: &commonv1.AuditInfo{
			CreatedAt: req.GetRenewalSchedule().GetAudit().GetCreatedAt(),
		},
	}
	resumeSubscriptionWorkflowReply, err := s.subscriptionWorkflowClient.ResumeSubscriptionWorkflow(ctx, resumeSubscriptionWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionWorkflowService/ResumeSubscriptionWorkflow")
	}
	validateCustomerReply, err := s.customerValidationClient.ValidateCustomer(ctx, &customersv1.ValidateCustomerRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			RelationshipManager: &identityv1.UserRef{
				Email: resumeSubscriptionWorkflowReply.GetSubscription().GetHolder().GetEmail(),
			},
			Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetNotify().GetEmail()},
			Audit: &commonv1.AuditInfo{
				Etag: resumeSubscriptionWorkflowReply.GetSubscription().GetAudit().GetEtag(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerValidationService/ValidateCustomer")
	}
	countAudiencesRequest := &audiencesv1.CountAudiencesRequest{}
	countAudiencesRequest.Context = req.GetContext()
	countAudiencesRequest.Filter = &audiencesv1.CountAudiencesRequest_Filter{Region: req.GetRenewalSchedule().GetLocation().GetRegion()}
	countAudiencesReply, err := s.audienceQueryClient.CountAudiences(ctx, countAudiencesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.audiences.v1.AudienceQueryService/CountAudiences")
	}
	streamSellerListingMetricsStream, err := s.sellerListingReportingClient.StreamSellerListingMetrics(ctx, &sellersv1.StreamSellerListingMetricsRequest{Context: req.GetContext()})
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
	return &subscriptionsv1.StartRenewalScheduleWorkflowResponse{
		Outcome: &subscriptionsv1.StartRenewalScheduleWorkflowResponse_Outcome{
			Status:      resumeSubscriptionWorkflowReply.GetOutcome().GetStatus(),
			ReferenceId: resumeSubscriptionWorkflowReply.GetOutcome().GetReferenceId(),
			NextSteps:   validateCustomerReply.GetOutcome().GetNextSteps(),
		},
		RenewalSchedule: &subscriptionsv1.RenewalSchedule{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: validateCustomerReply.GetCustomer().GetAudit().GetUpdatedBy(),
			},
			RenewalScheduleId: req.GetRenewalScheduleId(),
			Status:            req.GetRenewalSchedule().GetStatus(),
		},
		Result: &commonv1.OperationResult{
			Details: countAudiencesReply.GetResult().GetDetails(),
			Status:  countAudiencesReply.GetResult().GetStatus(),
		},
	}, nil
}

// AdvanceRenewalScheduleWorkflow advances a renewal schedule.
func (s RenewalScheduleWorkflowServer) AdvanceRenewalScheduleWorkflow(ctx context.Context, req *subscriptionsv1.AdvanceRenewalScheduleWorkflowRequest) (*subscriptionsv1.AdvanceRenewalScheduleWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected AdvanceRenewalScheduleWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SaveRenewalSchedule(ctx, req.GetContext().GetTenantId(), req.GetRenewalSchedule()); err != nil {
		return nil, grpcx.Wrap(err, "save renewal schedule")
	}
	s.log.InfoContext(ctx, "advance renewal schedule workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRenewalSchedule().GetAudit().GetEtag())
	for _, participant := range req.GetRenewalSchedule().GetParticipants() {
		if _, err := s.searchIndexNotificationClient.SubscribeSearchIndexUpdates(ctx, &searchv1.SubscribeSearchIndexUpdatesRequest{
			Context: req.GetContext(),
			Details: &searchv1.SubscribeSearchIndexUpdatesRequest_Details{
				Recipient: &commonv1.ContactInfo{
					PreferredChannel: participant.GetContact().GetPreferredChannel(),
					Phone:            participant.GetContact().GetPhone(),
					Email:            participant.GetContact().GetEmail(),
				},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexNotificationService/SubscribeSearchIndexUpdates")
		}
	}
	subscriptionApproval, err := registry.Lookup[subscriptionsv1.SubscriptionApprovalServiceClient](s.clients, "acme.subscriptions.v1.SubscriptionApprovalService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	delegateSubscriptionApprovalRequest := &subscriptionsv1.DelegateSubscriptionApprovalRequest{
		Context: req.GetContext(),
		Subscription: &subscriptionsv1.Subscription{
			Holder: &customersv1.CustomerRef{Email: req.GetRenewalSchedule().GetOrganizer().GetEmail()},
		},
	}
	delegateSubscriptionApprovalReply, err := subscriptionApproval.DelegateSubscriptionApproval(ctx, delegateSubscriptionApprovalRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionApprovalService/DelegateSubscriptionApproval")
	}
	return &subscriptionsv1.AdvanceRenewalScheduleWorkflowResponse{
		Outcome: &subscriptionsv1.AdvanceRenewalScheduleWorkflowResponse_Outcome{
			Status: delegateSubscriptionApprovalReply.GetResult().GetStatus(),
		},
		RenewalSchedule: &subscriptionsv1.RenewalSchedule{
			Organizer: &identityv1.UserRef{
				Email: delegateSubscriptionApprovalReply.GetSubscription().GetContact().GetEmail(),
			},
			RenewalScheduleId: req.GetRenewalScheduleId(),
			Location:          req.GetRenewalSchedule().GetLocation(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RetryRenewalScheduleWorkflow retrys a renewal schedule.
func (s RenewalScheduleWorkflowServer) RetryRenewalScheduleWorkflow(ctx context.Context, req *subscriptionsv1.RetryRenewalScheduleWorkflowRequest) (*subscriptionsv1.RetryRenewalScheduleWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RetryRenewalScheduleWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SaveRenewalSchedule(ctx, req.GetContext().GetTenantId(), req.GetRenewalSchedule()); err != nil {
		return nil, grpcx.Wrap(err, "save renewal schedule")
	}
	s.log.InfoContext(ctx, "retry renewal schedule workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRenewalSchedule().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetRenewalSchedule()).(*subscriptionsv1.RenewalSchedule)
	encoded, err := protojson.Marshal(req.GetRenewalSchedule())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded subscriptionsv1.RenewalSchedule
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	listUsersRequest := &identityv1.ListUsersRequest{
		Context: req.GetContext(),
		Filter: &identityv1.ListUsersRequest_Filter{
			Region:       cloned.GetLocation().GetRegion(),
			CreatedAfter: (&decoded).GetAudit().GetUpdatedAt(),
		},
	}
	listUsersReply, err := s.listUsersFn(ctx, listUsersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserService/ListUsers")
	}
	ctx = tenancy.With(ctx, "display_name", req.GetRenewalSchedule().GetOrganizer().GetDisplayName())
	packed, err := anypb.New(req.GetRenewalSchedule())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	commentUsageRecordApprovalRequest := &subscriptionsv1.CommentUsageRecordApprovalRequest{
		Context: req.GetContext(),
		Details: &subscriptionsv1.CommentUsageRecordApprovalRequest_Details{
			Approver: &identityv1.UserRef{
				Email:       req.GetRenewalSchedule().GetOrganizer().GetEmail(),
				DisplayName: tenancy.Value(ctx, "display_name"),
			},
		},
		UsageRecord: &subscriptionsv1.UsageRecord{Detail: packed},
	}
	commentUsageRecordApprovalReply := new(subscriptionsv1.CommentUsageRecordApprovalResponse)
	if err := s.conn.Invoke(ctx, subscriptionsUsageRecordApprovalServiceCommentUsageRecordApprovalMethod, commentUsageRecordApprovalRequest, commentUsageRecordApprovalReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.UsageRecordApprovalService/CommentUsageRecordApproval")
	}
	resumeSubscriptionPlanWorkflowRequest := newResumeSubscriptionPlanWorkflowRequest(withResumeSubscriptionPlanWorkflowContext(req.GetContext()), withResumeSubscriptionPlanWorkflowSubscriptionPlan(&subscriptionsv1.SubscriptionPlan{
		Owner: &identityv1.UserRef{Email: req.GetDetails().GetNotify().GetEmail()},
		Audit: &commonv1.AuditInfo{
			UpdatedBy: req.GetRenewalSchedule().GetAudit().GetUpdatedBy(),
			Etag:      req.GetRenewalSchedule().GetAudit().GetEtag(),
		},
	}), withResumeSubscriptionPlanWorkflowDetails(&subscriptionsv1.ResumeSubscriptionPlanWorkflowRequest_Details{
		Notify: &commonv1.ContactInfo{Phone: req.GetDetails().GetNotify().GetPhone()},
	}))
	resumeSubscriptionPlanWorkflowReply, err := s.subscriptionPlanWorkflowClient.ResumeSubscriptionPlanWorkflow(ctx, resumeSubscriptionPlanWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionPlanWorkflowService/ResumeSubscriptionPlanWorkflow")
	}
	return &subscriptionsv1.RetryRenewalScheduleWorkflowResponse{
		Outcome: &subscriptionsv1.RetryRenewalScheduleWorkflowResponse_Outcome{
			Status:    listUsersReply.GetResult().GetStatus(),
			Message:   listUsersReply.GetResult().GetMessage(),
			NextSteps: commentUsageRecordApprovalReply.GetOutcome().GetNextSteps(),
		},
		RenewalSchedule: &subscriptionsv1.RenewalSchedule{
			Participants: []*subscriptionsv1.RenewalScheduleParticipant{&subscriptionsv1.RenewalScheduleParticipant{
				Contact: &commonv1.ContactInfo{
					Email: resumeSubscriptionPlanWorkflowReply.GetSubscriptionPlan().GetOwner().GetEmail(),
				},
			}},
			RenewalScheduleId: req.GetRenewalScheduleId(),
			Status:            req.GetRenewalSchedule().GetStatus(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// PauseRenewalScheduleWorkflow pauses a renewal schedule.
func (s RenewalScheduleWorkflowServer) PauseRenewalScheduleWorkflow(ctx context.Context, req *subscriptionsv1.PauseRenewalScheduleWorkflowRequest) (*subscriptionsv1.PauseRenewalScheduleWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveRenewalSchedule(ctx, req.GetContext().GetTenantId(), req.GetRenewalSchedule()); err != nil {
		return nil, grpcx.Wrap(err, "save renewal schedule")
	}
	s.log.InfoContext(ctx, "pause renewal schedule workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRenewalSchedule().GetAudit().GetEtag())
	updateDatasetRequest := &datasetsv1.UpdateDatasetRequest{}
	updateDatasetRequest.Context = req.GetContext()
	updateDatasetRequest.Dataset = &datasetsv1.Dataset{
		Locator: &datasetsv1.Dataset_Address{Address: &commonv1.PostalAddress{Region: req.GetRenewalSchedule().GetLocation().GetRegion()}},
		Owner: &identityv1.UserRef{
			UserId: req.GetRenewalSchedule().GetOrganizer().GetUserId(),
			Email:  req.GetDetails().GetNotify().GetEmail(),
		},
		Audit: &commonv1.AuditInfo{
			UpdatedBy: req.GetRenewalSchedule().GetAudit().GetUpdatedBy(),
		},
	}
	updateDatasetReply, err := s.datasetClient.UpdateDataset(ctx, updateDatasetRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.datasets.v1.DatasetService/UpdateDataset")
	}
	return &subscriptionsv1.PauseRenewalScheduleWorkflowResponse{
		RenewalSchedule: &subscriptionsv1.RenewalSchedule{
			Location: &commonv1.PostalAddress{
				Region: updateDatasetReply.GetDataset().GetAddress().GetRegion(),
			},
			Organizer: &identityv1.UserRef{
				Email: updateDatasetReply.GetDataset().GetOwner().GetEmail(),
			},
			RenewalScheduleId: req.GetRenewalScheduleId(),
			Window:            req.GetRenewalSchedule().GetWindow(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// InspectRenewalScheduleWorkflow returns a renewal schedule.
func (s RenewalScheduleWorkflowServer) InspectRenewalScheduleWorkflow(ctx context.Context, req *subscriptionsv1.InspectRenewalScheduleWorkflowRequest) (*subscriptionsv1.InspectRenewalScheduleWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "renewal_schedule", req.GetRenewalScheduleId(), "inspect_renewal_schedule_workflow"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "inspect renewal schedule workflow", "tenant", req.GetContext().GetTenantId())
	contextCache := cache.New[string, *commonv1.RequestContext](1)
	contextCache.Put(req.GetContext().GetTraceId(), req.GetContext())
	cachedContext, _ := contextCache.Get(req.GetContext().GetTraceId())
	advanceSubscriptionWorkflowRequest := &subscriptionsv1.AdvanceSubscriptionWorkflowRequest{
		Context: req.GetContext(),
		Subscription: &subscriptionsv1.Subscription{
			Holder:  &customersv1.CustomerRef{Email: cachedContext.GetActor().GetEmail()},
			Audit:   &commonv1.AuditInfo{CreatedAt: req.GetContext().GetDeadline()},
			Funding: &subscriptionsv1.Subscription_ExternalAccountId{ExternalAccountId: req.GetRenewalScheduleId()},
		},
	}
	advanceSubscriptionWorkflowReply, err := subscriptionsv1.SubscriptionWorkflowServiceClient.AdvanceSubscriptionWorkflow(s.subscriptionWorkflowClient, ctx, advanceSubscriptionWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionWorkflowService/AdvanceSubscriptionWorkflow")
	}
	return &subscriptionsv1.InspectRenewalScheduleWorkflowResponse{
		RenewalSchedule: &subscriptionsv1.RenewalSchedule{
			Participants: []*subscriptionsv1.RenewalScheduleParticipant{&subscriptionsv1.RenewalScheduleParticipant{
				Contact: &commonv1.ContactInfo{
					PreferredChannel: advanceSubscriptionWorkflowReply.GetSubscription().GetContact().GetPreferredChannel(),
				},
			}},
			RenewalScheduleId: req.GetRenewalScheduleId(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: advanceSubscriptionWorkflowReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// SignalRenewalScheduleWorkflow signals a renewal schedule.
func (s RenewalScheduleWorkflowServer) SignalRenewalScheduleWorkflow(ctx context.Context, req *subscriptionsv1.SignalRenewalScheduleWorkflowRequest) (*subscriptionsv1.SignalRenewalScheduleWorkflowResponse, error) {
	if err := s.store.SaveRenewalSchedule(ctx, req.GetContext().GetTenantId(), req.GetRenewalSchedule()); err != nil {
		return nil, grpcx.Wrap(err, "save renewal schedule")
	}
	s.log.InfoContext(ctx, "signal renewal schedule workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRenewalSchedule().GetAudit().GetEtag())
	getRenewalScheduleApprovalStatusRequest := &subscriptionsv1.GetRenewalScheduleApprovalStatusRequest{
		Context:           req.GetContext(),
		RenewalScheduleId: req.GetRenewalScheduleId(),
	}
	getRenewalScheduleApprovalStatusReply, err := s.renewalScheduleApprovalPort.GetRenewalScheduleApprovalStatus(ctx, getRenewalScheduleApprovalStatusRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.RenewalScheduleApprovalService/GetRenewalScheduleApprovalStatus")
	}
	return &subscriptionsv1.SignalRenewalScheduleWorkflowResponse{
		RenewalSchedule: &subscriptionsv1.RenewalSchedule{
			Location: &commonv1.PostalAddress{
				Locality: getRenewalScheduleApprovalStatusReply.GetRenewalSchedule().GetLocation().GetLocality(),
			},
			RenewalScheduleId: req.GetRenewalScheduleId(),
			Organizer:         req.GetRenewalSchedule().GetOrganizer(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}
