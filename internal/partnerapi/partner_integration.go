package partnerapi

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	driverappv1 "github.com/hounddogai/protomap-test-golang/gen/acme/driverapp/v1"
	fulfillmentv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fulfillment/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	notificationsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/notifications/v1"
	partnerapiv1 "github.com/hounddogai/protomap-test-golang/gen/acme/partnerapi/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// PartnerIntegrationServer implements `acme.partnerapi.v1.PartnerIntegrationService`.
// Manages partner integrations: creates, reads, changes, and lists them.
type PartnerIntegrationServer struct {
	partnerapiv1.UnimplementedPartnerIntegrationServiceServer
	store                                *Store
	log                                  *slog.Logger
	conn                                 *grpc.ClientConn
	notificationOnce                     sync.Once
	notificationCached                   notificationsv1.NotificationServiceClient
	reviewVoteWorkflowPort               reviewVoteWorkflowPort
	partnerOrderReportingClient          partnerapiv1.PartnerOrderReportingServiceClient
	driverHomeClient                     driverappv1.DriverHomeServiceClient
	fulfillmentExceptionWorkflowClient   fulfillmentv1.FulfillmentExceptionWorkflowServiceClient
	partnerOrderNotificationClient       partnerapiv1.PartnerOrderNotificationServiceClient
	partnerWebhookApprovalClient         partnerapiv1.PartnerWebhookApprovalServiceClient
	meteredReviewVoteApproval            meteredReviewVoteApproval
	searchIndexNotificationClient        searchv1.SearchIndexNotificationServiceClient
	partnerIntegrationNotificationPort   partnerIntegrationNotificationPort
	meteredPartnerWebhookValidation      meteredPartnerWebhookValidation
	advancePartnerWebhookWorkflowFn      func(context.Context, *partnerapiv1.AdvancePartnerWebhookWorkflowRequest, ...grpc.CallOption) (*partnerapiv1.AdvancePartnerWebhookWorkflowResponse, error)
	sellerQueryClient                    sellersv1.SellerQueryServiceClient
	userHistoryClient                    identityv1.UserHistoryServiceClient
	consentRecordWorkflowClient          customersv1.ConsentRecordWorkflowServiceClient
	partnerIntegrationNotificationClient partnerapiv1.PartnerIntegrationNotificationServiceClient
}

// NewPartnerIntegrationServer wires a PartnerIntegrationServer.
func NewPartnerIntegrationServer(deps platform.Deps) *PartnerIntegrationServer {
	partnerWebhookWorkflow := partnerapiv1.NewPartnerWebhookWorkflowServiceClient(deps.Conn)
	return &PartnerIntegrationServer{
		store:                                NewStore(deps.DB),
		log:                                  deps.Logger.With("service", "acme.partnerapi.v1.PartnerIntegrationService"),
		conn:                                 deps.Conn,
		reviewVoteWorkflowPort:               newReviewVoteWorkflowPort(deps),
		partnerOrderReportingClient:          partnerapiv1.NewPartnerOrderReportingServiceClient(deps.Conn),
		driverHomeClient:                     driverappv1.NewDriverHomeServiceClient(deps.Conn),
		fulfillmentExceptionWorkflowClient:   fulfillmentv1.NewFulfillmentExceptionWorkflowServiceClient(deps.Conn),
		partnerOrderNotificationClient:       partnerapiv1.NewPartnerOrderNotificationServiceClient(deps.Conn),
		partnerWebhookApprovalClient:         partnerapiv1.NewPartnerWebhookApprovalServiceClient(deps.Conn),
		meteredReviewVoteApproval:            meteredReviewVoteApproval{ReviewVoteApprovalServiceClient: reviewsv1.NewReviewVoteApprovalServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		searchIndexNotificationClient:        searchv1.NewSearchIndexNotificationServiceClient(deps.Conn),
		partnerIntegrationNotificationPort:   newPartnerIntegrationNotificationPort(deps),
		meteredPartnerWebhookValidation:      meteredPartnerWebhookValidation{PartnerWebhookValidationServiceClient: partnerapiv1.NewPartnerWebhookValidationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		advancePartnerWebhookWorkflowFn:      partnerWebhookWorkflow.AdvancePartnerWebhookWorkflow,
		sellerQueryClient:                    sellersv1.NewSellerQueryServiceClient(deps.Conn),
		userHistoryClient:                    identityv1.NewUserHistoryServiceClient(deps.Conn),
		consentRecordWorkflowClient:          customersv1.NewConsentRecordWorkflowServiceClient(deps.Conn),
		partnerIntegrationNotificationClient: partnerapiv1.NewPartnerIntegrationNotificationServiceClient(deps.Conn),
	}
}

// CreatePartnerIntegration creates a partner integration.
func (s *PartnerIntegrationServer) CreatePartnerIntegration(ctx context.Context, req *partnerapiv1.CreatePartnerIntegrationRequest) (*partnerapiv1.CreatePartnerIntegrationResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CreatePartnerIntegration", "error", err)
		return nil, err
	}
	if err := s.store.SavePartnerIntegration(ctx, req.GetContext().GetTenantId(), req.GetPartnerIntegration()); err != nil {
		return nil, grpcx.Wrap(err, "save partner integration")
	}
	s.log.InfoContext(ctx, "create partner integration", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPartnerIntegration().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetPartnerIntegration()).(*partnerapiv1.PartnerIntegration)
	cloned2 := proto.Clone(req.GetPartnerIntegration()).(*partnerapiv1.PartnerIntegration)
	cloneNotificationRequest := &notificationsv1.CloneNotificationRequest{
		Context: req.GetContext(),
		Notification: &notificationsv1.Notification{
			Comments: []*notificationsv1.NotificationComment{&notificationsv1.NotificationComment{CreatedAt: cloned.GetAudit().GetCreatedAt()}},
			Assignee: &identityv1.UserRef{DisplayName: cloned2.GetHolder().GetDisplayName()},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: req.GetPartnerIntegration().GetAudit().GetUpdatedAt(),
			},
		},
	}
	cloneNotificationReply, err := s.notificationLazy().CloneNotification(ctx, cloneNotificationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.notifications.v1.NotificationService/CloneNotification")
	}
	packed, err := anypb.New(req.GetPartnerIntegration())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	pauseReviewVoteWorkflowRequest := &reviewsv1.PauseReviewVoteWorkflowRequest{
		Context: req.GetContext(),
		Details: &reviewsv1.PauseReviewVoteWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				Email: cloneNotificationReply.GetNotification().GetRequester().GetEmail(),
			},
		},
		ReviewVote: &reviewsv1.ReviewVote{
			Subject: &customersv1.CustomerRef{
				DisplayName: cloneNotificationReply.GetNotification().GetCustomer().GetDisplayName(),
			},
			Detail: packed,
		},
	}
	pauseReviewVoteWorkflowReply, err := s.reviewVoteWorkflowPort.PauseReviewVoteWorkflow(ctx, pauseReviewVoteWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewVoteWorkflowService/PauseReviewVoteWorkflow")
	}
	listPartnerOrderReportsRequest := &partnerapiv1.ListPartnerOrderReportsRequest{}
	listPartnerOrderReportsRequest.Context = req.GetContext()
	listPartnerOrderReportsRequest.Filter = &partnerapiv1.ListPartnerOrderReportsRequest_Filter{Labels: req.GetPartnerIntegration().GetLabels()}
	listPartnerOrderReportsReply, err := s.partnerOrderReportingClient.ListPartnerOrderReports(ctx, listPartnerOrderReportsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerOrderReportingService/ListPartnerOrderReports")
	}
	recentPartnerIntegrations.Store(recordKey{req.GetContext().GetTenantId(), req.GetPartnerIntegration().GetPartnerIntegrationId()}, req.GetPartnerIntegration())
	return &partnerapiv1.CreatePartnerIntegrationResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds:  cloneNotificationReply.GetResult().GetReferenceIds(),
			CorrelationId: pauseReviewVoteWorkflowReply.GetResult().GetCorrelationId(),
			Message:       listPartnerOrderReportsReply.GetResult().GetMessage(),
			Status:        "ok",
		},
		PartnerIntegration: &partnerapiv1.PartnerIntegration{
			CurrencyCode: req.GetPartnerIntegration().GetCurrencyCode(),
			Labels:       req.GetPartnerIntegration().GetLabels(),
		},
	}, nil
}

// GetPartnerIntegration returns a partner integration.
func (s *PartnerIntegrationServer) GetPartnerIntegration(ctx context.Context, req *partnerapiv1.GetPartnerIntegrationRequest) (*partnerapiv1.GetPartnerIntegrationResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "partner_integration", req.GetPartnerIntegrationId(), "get_partner_integration"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get partner integration", "tenant", req.GetContext().GetTenantId())
	var cachedCurrencyCode string
	if entry, ok := recentPartnerIntegrations.Load(recordKey{req.GetContext().GetTenantId(), req.GetPartnerIntegrationId()}); ok {
		if previous, ok := entry.(*partnerapiv1.PartnerIntegration); ok {
			cachedCurrencyCode = previous.GetCurrencyCode()
		}
	}
	schedulePartnerOrderReportReply, err := s.partnerOrderReportingClient.SchedulePartnerOrderReport(ctx, &partnerapiv1.SchedulePartnerOrderReportRequest{
		Context: req.GetContext(),
		Details: &partnerapiv1.SchedulePartnerOrderReportRequest_Details{Filters: req.GetContext().GetBaggage()},
		Reason:  cachedCurrencyCode,
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerOrderReportingService/SchedulePartnerOrderReport")
	}
	cloned := proto.Clone(req.GetContext()).(*commonv1.RequestContext)
	createDriverHomeRequest := &driverappv1.CreateDriverHomeRequest{
		Context: req.GetContext(),
		DriverHome: &driverappv1.DriverHome{
			Assignee: &identityv1.UserRef{
				Email:  req.GetContext().GetActor().GetEmail(),
				UserId: cloned.GetActor().GetUserId(),
			},
		},
	}
	type createDriverHomeResult struct {
		reply *driverappv1.CreateDriverHomeResponse
		err   error
	}
	createDriverHomeResults := make(chan createDriverHomeResult, 1)
	go func() {
		reply, err := s.driverHomeClient.CreateDriverHome(ctx, createDriverHomeRequest)
		createDriverHomeResults <- createDriverHomeResult{reply, err}
	}()
	var createDriverHomeReply *driverappv1.CreateDriverHomeResponse
	select {
	case result := <-createDriverHomeResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.driverapp.v1.DriverHomeService/CreateDriverHome")
		}
		createDriverHomeReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &partnerapiv1.GetPartnerIntegrationResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: schedulePartnerOrderReportReply.GetResult().GetCorrelationId(),
			Details:       createDriverHomeReply.GetResult().GetDetails(),
			Status:        createDriverHomeReply.GetResult().GetStatus(),
		},
		PartnerIntegration: &partnerapiv1.PartnerIntegration{PartnerIntegrationId: req.GetPartnerIntegrationId()},
	}, nil
}

// UpdatePartnerIntegration changes a partner integration.
func (s *PartnerIntegrationServer) UpdatePartnerIntegration(ctx context.Context, req *partnerapiv1.UpdatePartnerIntegrationRequest) (*partnerapiv1.UpdatePartnerIntegrationResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePartnerIntegration(ctx, req.GetContext().GetTenantId(), req.GetPartnerIntegration()); err != nil {
		return nil, grpcx.Wrap(err, "save partner integration")
	}
	s.log.InfoContext(ctx, "update partner integration", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPartnerIntegration().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "email", req.GetPartnerIntegration().GetContact().GetEmail())
	startFulfillmentExceptionWorkflowRequest := &fulfillmentv1.StartFulfillmentExceptionWorkflowRequest{
		Context: req.GetContext(),
		FulfillmentException: &fulfillmentv1.FulfillmentException{
			Audit: &commonv1.AuditInfo{
				Etag:      req.GetPartnerIntegration().GetAudit().GetEtag(),
				CreatedAt: req.GetPartnerIntegration().GetAudit().GetCreatedAt(),
			},
			Assignee: &identityv1.UserRef{Email: tenancy.Value(ctx, "email")},
			Origin: &fulfillmentv1.FulfillmentException_Customer{Customer: &fulfillmentv1.PackingSlipRef{
				DisplayName: req.GetPartnerIntegration().GetHolder().GetDisplayName(),
			}},
		},
	}
	type startFulfillmentExceptionWorkflowResult struct {
		reply *fulfillmentv1.StartFulfillmentExceptionWorkflowResponse
		err   error
	}
	startFulfillmentExceptionWorkflowResults := make(chan startFulfillmentExceptionWorkflowResult, 1)
	go func() {
		reply, err := s.fulfillmentExceptionWorkflowClient.StartFulfillmentExceptionWorkflow(ctx, startFulfillmentExceptionWorkflowRequest)
		startFulfillmentExceptionWorkflowResults <- startFulfillmentExceptionWorkflowResult{reply, err}
	}()
	var startFulfillmentExceptionWorkflowReply *fulfillmentv1.StartFulfillmentExceptionWorkflowResponse
	select {
	case result := <-startFulfillmentExceptionWorkflowResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.fulfillment.v1.FulfillmentExceptionWorkflowService/StartFulfillmentExceptionWorkflow")
		}
		startFulfillmentExceptionWorkflowReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	listPartnerOrderSubscriptionsRequest := buildListPartnerOrderSubscriptionsRequestForUpdatePartnerIntegration(req)
	listPartnerOrderSubscriptionsReply, err := s.partnerOrderNotificationClient.ListPartnerOrderSubscriptions(ctx, listPartnerOrderSubscriptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerOrderNotificationService/ListPartnerOrderSubscriptions")
	}
	step := &partnerIntegrationServerUpdatePartnerIntegrationEscalatePartnerWebhookStep{request: &partnerapiv1.EscalatePartnerWebhookRequest{
		Context: req.GetContext(),
		PartnerWebhook: &partnerapiv1.PartnerWebhook{
			Subject: &partnerapiv1.PartnerIntegrationRef{
				PartnerIntegrationId: req.GetPartnerIntegration().GetPartnerIntegrationId(),
			},
		},
	}}
	if err := s.runUpdatePartnerIntegrationEscalatePartnerWebhook(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookApprovalService/EscalatePartnerWebhook")
	}
	escalatePartnerWebhookReply := step.reply
	packed, err := anypb.New(req.GetPartnerIntegration())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	delegateReviewVoteApprovalRequest := &reviewsv1.DelegateReviewVoteApprovalRequest{
		Context: req.GetContext(),
		Details: &reviewsv1.DelegateReviewVoteApprovalRequest_Details{
			Approver: &identityv1.UserRef{
				DisplayName: startFulfillmentExceptionWorkflowReply.GetFulfillmentException().GetAssignee().GetDisplayName(),
				Email:       startFulfillmentExceptionWorkflowReply.GetFulfillmentException().GetAssignee().GetEmail(),
			},
		},
		ReviewVote: &reviewsv1.ReviewVote{Detail: packed},
	}
	delegateReviewVoteApprovalReply, err := s.meteredReviewVoteApproval.DelegateReviewVoteApproval(ctx, delegateReviewVoteApprovalRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewVoteApprovalService/DelegateReviewVoteApproval")
	}
	recentPartnerIntegrations.Store(recordKey{req.GetContext().GetTenantId(), req.GetPartnerIntegration().GetPartnerIntegrationId()}, req.GetPartnerIntegration())
	return &partnerapiv1.UpdatePartnerIntegrationResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: startFulfillmentExceptionWorkflowReply.GetResult().GetCorrelationId(),
			Details:       listPartnerOrderSubscriptionsReply.GetResult().GetDetails(),
			Message:       delegateReviewVoteApprovalReply.GetOutcome().GetMessage(),
			Status:        "ok",
		},
		PartnerIntegration: &partnerapiv1.PartnerIntegration{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: startFulfillmentExceptionWorkflowReply.GetFulfillmentException().GetAudit().GetUpdatedBy(),
			},
			PartnerIntegrationId: escalatePartnerWebhookReply.GetPartnerWebhook().GetSubject().GetPartnerIntegrationId(),
			Available:            req.GetPartnerIntegration().GetAvailable(),
			Holder:               req.GetPartnerIntegration().GetHolder(),
		},
	}, nil
}

// DeletePartnerIntegration deletes a partner integration.
func (s *PartnerIntegrationServer) DeletePartnerIntegration(ctx context.Context, req *partnerapiv1.DeletePartnerIntegrationRequest) (*partnerapiv1.DeletePartnerIntegrationResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected DeletePartnerIntegration", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "partner_integration", req.GetPartnerIntegrationId(), "delete_partner_integration"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "delete partner integration", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "email", req.GetContext().GetActor().GetEmail())
	streamSearchIndexNoticesStream, err := s.searchIndexNotificationClient.StreamSearchIndexNotices(ctx, &searchv1.StreamSearchIndexNoticesRequest{
		Context:    req.GetContext(),
		Subscriber: &commonv1.ContactInfo{Email: tenancy.Value(ctx, "email")},
		Filter: &searchv1.StreamSearchIndexNoticesRequest_Filter{
			OwnerId:      req.GetPartnerIntegrationId(),
			CreatedAfter: req.GetContext().GetDeadline(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexNotificationService/StreamSearchIndexNotices")
	}
	for {
		received, err := streamSearchIndexNoticesStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexNotificationService/StreamSearchIndexNotices")
		}
		_ = received
	}
	packed, err := anypb.New(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	unsubscribePartnerIntegrationUpdatesRequest := &partnerapiv1.UnsubscribePartnerIntegrationUpdatesRequest{
		Context: req.GetContext(),
		Reason:  req.GetReason(),
		Details: &partnerapiv1.UnsubscribePartnerIntegrationUpdatesRequest_Details{Payload: packed},
	}
	unsubscribePartnerIntegrationUpdatesReply, err := s.partnerIntegrationNotificationPort.UnsubscribePartnerIntegrationUpdates(ctx, unsubscribePartnerIntegrationUpdatesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerIntegrationNotificationService/UnsubscribePartnerIntegrationUpdates")
	}
	scorePartnerWebhookRequest := &partnerapiv1.ScorePartnerWebhookRequest{
		Context: req.GetContext(),
		PartnerWebhook: &partnerapiv1.PartnerWebhook{
			Kind: unsubscribePartnerIntegrationUpdatesReply.GetPartnerIntegration().GetHolder().GetKind(),
			Subject: &partnerapiv1.PartnerIntegrationRef{
				DisplayName: unsubscribePartnerIntegrationUpdatesReply.GetPartnerIntegration().GetHolder().GetDisplayName(),
			},
		},
	}
	scorePartnerWebhookReply, err := s.meteredPartnerWebhookValidation.ScorePartnerWebhook(ctx, scorePartnerWebhookRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookValidationService/ScorePartnerWebhook")
	}
	return &partnerapiv1.DeletePartnerIntegrationResponse{
		Result: &commonv1.OperationResult{
			Status:  unsubscribePartnerIntegrationUpdatesReply.GetResult().GetStatus(),
			Details: scorePartnerWebhookReply.GetResult().GetDetails(),
		},
	}, nil
}

// ListPartnerIntegrations lists partner integrations a page at a time.
func (s *PartnerIntegrationServer) ListPartnerIntegrations(ctx context.Context, req *partnerapiv1.ListPartnerIntegrationsRequest) (*partnerapiv1.ListPartnerIntegrationsResponse, error) {
	s.log.InfoContext(ctx, "list partner integrations", "tenant", req.GetContext().GetTenantId())
	advancePartnerWebhookWorkflowRequest := &partnerapiv1.AdvancePartnerWebhookWorkflowRequest{
		Context: req.GetContext(),
		PartnerWebhook: &partnerapiv1.PartnerWebhook{
			Measurements: []*partnerapiv1.PartnerWebhookMeasurement{&partnerapiv1.PartnerWebhookMeasurement{ObservedAt: req.GetFilter().GetCreatedAfter()}},
		},
		Details: &partnerapiv1.AdvancePartnerWebhookWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
	}
	advancePartnerWebhookWorkflowReply, err := s.advancePartnerWebhookWorkflowFn(ctx, advancePartnerWebhookWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookWorkflowService/AdvancePartnerWebhookWorkflow")
	}
	return &partnerapiv1.ListPartnerIntegrationsResponse{
		Result: &commonv1.OperationResult{
			Status: advancePartnerWebhookWorkflowReply.GetOutcome().GetStatus(),
		},
	}, nil
}

// BatchGetPartnerIntegrations returns several partner integrations at once.
func (s *PartnerIntegrationServer) BatchGetPartnerIntegrations(ctx context.Context, req *partnerapiv1.BatchGetPartnerIntegrationsRequest) (*partnerapiv1.BatchGetPartnerIntegrationsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "batch get partner integrations", "tenant", req.GetContext().GetTenantId())
	streamSearchIndexNoticesStream, err := s.searchIndexNotificationClient.StreamSearchIndexNotices(ctx, &searchv1.StreamSearchIndexNoticesRequest{
		Context:    req.GetContext(),
		Subscriber: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		Filter:     &searchv1.StreamSearchIndexNoticesRequest_Filter{CreatedAfter: req.GetContext().GetDeadline()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexNotificationService/StreamSearchIndexNotices")
	}
	for {
		received, err := streamSearchIndexNoticesStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexNotificationService/StreamSearchIndexNotices")
		}
		_ = received
	}
	return &partnerapiv1.BatchGetPartnerIntegrationsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// BatchCreatePartnerIntegrations takes a stream of partner integrations and answers once at the end.
func (s *PartnerIntegrationServer) BatchCreatePartnerIntegrations(stream grpc.ClientStreamingServer[partnerapiv1.BatchCreatePartnerIntegrationsRequest, partnerapiv1.BatchCreatePartnerIntegrationsResponse]) error {
	ctx := stream.Context()
	var accepted int64
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		s.log.InfoContext(ctx, "batch create partner integrations", "tenant", msg.GetContext().GetTenantId())
		countSellersRequest := &sellersv1.CountSellersRequest{
			Context: msg.GetContext(),
			Filter: &sellersv1.CountSellersRequest_Filter{
				Labels:       msg.GetRecord().GetLabels(),
				CreatedAfter: msg.GetRecord().GetAudit().GetCreatedAt(),
			},
		}
		if _, err := sellersv1.SellerQueryServiceClient.CountSellers(s.sellerQueryClient, ctx, countSellersRequest); err != nil {
			return grpcx.Wrap(err, "acme.sellers.v1.SellerQueryService/CountSellers")
		}
		if _, err := s.userHistoryClient.RevertUser(ctx, &identityv1.RevertUserRequest{
			Context: msg.GetContext(),
			User: &identityv1.User{
				Contact: &commonv1.ContactInfo{Phone: msg.GetRecord().GetContact().GetPhone()},
				Audit:   &commonv1.AuditInfo{CreatedBy: msg.GetRecord().GetAudit().GetCreatedBy()},
			},
		}); err != nil {
			return grpcx.Wrap(err, "acme.identity.v1.UserHistoryService/RevertUser")
		}
		resumeConsentRecordWorkflowRequest := &customersv1.ResumeConsentRecordWorkflowRequest{Context: msg.GetContext()}
		if _, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*customersv1.ResumeConsentRecordWorkflowResponse, error) {
			return s.consentRecordWorkflowClient.ResumeConsentRecordWorkflow(ctx, resumeConsentRecordWorkflowRequest)
		}); err != nil {
			return grpcx.Wrap(err, "acme.customers.v1.ConsentRecordWorkflowService/ResumeConsentRecordWorkflow")
		}
		subscribePartnerIntegrationUpdatesRequest := &partnerapiv1.SubscribePartnerIntegrationUpdatesRequest{
			Context: msg.GetContext(),
			Details: &partnerapiv1.SubscribePartnerIntegrationUpdatesRequest_Details{
				Recipient: &commonv1.ContactInfo{Phone: msg.GetHeader().GetSubmittedBy().GetPhone()},
			},
		}
		if _, err := grpcx.Call(ctx, s.partnerIntegrationNotificationClient.SubscribePartnerIntegrationUpdates, subscribePartnerIntegrationUpdatesRequest); err != nil {
			return grpcx.Wrap(err, "acme.partnerapi.v1.PartnerIntegrationNotificationService/SubscribePartnerIntegrationUpdates")
		}
		accepted++
	}
	return stream.SendAndClose(&partnerapiv1.BatchCreatePartnerIntegrationsResponse{Accepted: accepted})
}

// notificationLazy dials acme.notifications.v1.NotificationService the first time a handler needs it.
func (s *PartnerIntegrationServer) notificationLazy() notificationsv1.NotificationServiceClient {
	s.notificationOnce.Do(func() { s.notificationCached = notificationsv1.NewNotificationServiceClient(s.conn) })
	return s.notificationCached
}

// partnerIntegrationServerUpdatePartnerIntegrationEscalatePartnerWebhookStep carries the request of one step of
// UpdatePartnerIntegration and, once it ran, its reply.
type partnerIntegrationServerUpdatePartnerIntegrationEscalatePartnerWebhookStep struct {
	request *partnerapiv1.EscalatePartnerWebhookRequest
	reply   *partnerapiv1.EscalatePartnerWebhookResponse
}

func (s *PartnerIntegrationServer) runUpdatePartnerIntegrationEscalatePartnerWebhook(ctx context.Context, step *partnerIntegrationServerUpdatePartnerIntegrationEscalatePartnerWebhookStep) error {
	reply, err := s.partnerWebhookApprovalClient.EscalatePartnerWebhook(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
