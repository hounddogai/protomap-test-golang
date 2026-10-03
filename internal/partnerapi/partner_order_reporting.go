package partnerapi

import (
	"context"
	"log/slog"

	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	employeeportalv1 "github.com/hounddogai/protomap-test-golang/gen/acme/employeeportal/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	partnerapiv1 "github.com/hounddogai/protomap-test-golang/gen/acme/partnerapi/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	venuesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/venues/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// PartnerOrderReportingServer implements `acme.partnerapi.v1.PartnerOrderReportingService`.
// Builds and schedules reports about partner orders.
type PartnerOrderReportingServer struct {
	partnerapiv1.UnimplementedPartnerOrderReportingServiceServer
	store                              *Store
	log                                *slog.Logger
	partnerWebhookApprovalClient       partnerapiv1.PartnerWebhookApprovalServiceClient
	clients                            *registry.Registry
	sellerPayoutScheduleApprovalClient sellersv1.SellerPayoutScheduleApprovalServiceClient
	searchQueryValidationClient        searchv1.SearchQueryValidationServiceClient
	accessPolicyValidationClient       authorizationv1.AccessPolicyValidationServiceClient
	partnerWebhookValidationClient     partnerapiv1.PartnerWebhookValidationServiceClient
	conn                               *grpc.ClientConn
	sellerListingWorkflowClient        sellersv1.SellerListingWorkflowServiceClient
	venueValidationClient              venuesv1.VenueValidationServiceClient
}

// NewPartnerOrderReportingServer wires a PartnerOrderReportingServer.
func NewPartnerOrderReportingServer(deps platform.Deps) PartnerOrderReportingServer {
	return PartnerOrderReportingServer{
		store:                              NewStore(deps.DB),
		log:                                deps.Logger.With("service", "acme.partnerapi.v1.PartnerOrderReportingService"),
		partnerWebhookApprovalClient:       partnerapiv1.NewPartnerWebhookApprovalServiceClient(deps.Conn),
		clients:                            deps.Clients,
		sellerPayoutScheduleApprovalClient: sellersv1.NewSellerPayoutScheduleApprovalServiceClient(deps.Conn),
		searchQueryValidationClient:        searchv1.NewSearchQueryValidationServiceClient(deps.Conn),
		accessPolicyValidationClient:       authorizationv1.NewAccessPolicyValidationServiceClient(deps.Conn),
		partnerWebhookValidationClient:     partnerapiv1.NewPartnerWebhookValidationServiceClient(deps.Conn),
		conn:                               deps.Conn,
		sellerListingWorkflowClient:        sellersv1.NewSellerListingWorkflowServiceClient(deps.Conn),
		venueValidationClient:              venuesv1.NewVenueValidationServiceClient(deps.Conn),
	}
}

// GeneratePartnerOrderReport generates a partner order.
func (s PartnerOrderReportingServer) GeneratePartnerOrderReport(ctx context.Context, req *partnerapiv1.GeneratePartnerOrderReportRequest) (*partnerapiv1.GeneratePartnerOrderReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GeneratePartnerOrderReport", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "partner_order", req.GetPartnerOrderId(), "generate_partner_order_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "generate partner order report", "tenant", req.GetContext().GetTenantId())
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	escalatePartnerWebhookRequest := &partnerapiv1.EscalatePartnerWebhookRequest{}
	escalatePartnerWebhookRequest.Context = req.GetContext()
	escalatePartnerWebhookRequest.Reason = req.GetReason()
	escalatePartnerWebhookRequest.Details = &partnerapiv1.EscalatePartnerWebhookRequest_Details{
		Approver: &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
	}
	escalatePartnerWebhookRequest.Annotations = req.GetAnnotations()
	escalatePartnerWebhookRequest.PartnerWebhook = &partnerapiv1.PartnerWebhook{Detail: packed}
	escalatePartnerWebhookReply, err := s.partnerWebhookApprovalClient.EscalatePartnerWebhook(ctx, escalatePartnerWebhookRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookApprovalService/EscalatePartnerWebhook")
	}
	renewalScheduleWorkflow, ok := s.clients.Get("acme.subscriptions.v1.RenewalScheduleWorkflowService").(subscriptionsv1.RenewalScheduleWorkflowServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.subscriptions.v1.RenewalScheduleWorkflowService")
	}
	signalRenewalScheduleWorkflowRequest := &subscriptionsv1.SignalRenewalScheduleWorkflowRequest{
		Context: req.GetContext(),
		RenewalSchedule: &subscriptionsv1.RenewalSchedule{
			Organizer: &identityv1.UserRef{
				DisplayName: escalatePartnerWebhookReply.GetPartnerWebhook().GetSubject().GetDisplayName(),
			},
		},
	}
	signalRenewalScheduleWorkflowReply, err := renewalScheduleWorkflow.SignalRenewalScheduleWorkflow(ctx, signalRenewalScheduleWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.RenewalScheduleWorkflowService/SignalRenewalScheduleWorkflow")
	}
	packed2, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	submitPartnerWebhookReply, err := s.partnerWebhookApprovalClient.SubmitPartnerWebhook(ctx, &partnerapiv1.SubmitPartnerWebhookRequest{
		Context: req.GetContext(),
		PartnerWebhook: &partnerapiv1.PartnerWebhook{
			Subject: &partnerapiv1.PartnerIntegrationRef{
				CurrencyCode: escalatePartnerWebhookReply.GetPartnerWebhook().GetSubject().GetCurrencyCode(),
			},
			Detail: packed2,
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookApprovalService/SubmitPartnerWebhook")
	}
	_ = submitPartnerWebhookReply
	return &partnerapiv1.GeneratePartnerOrderReportResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: escalatePartnerWebhookReply.GetResult().GetCorrelationId(),
			ReferenceIds:  escalatePartnerWebhookReply.GetResult().GetReferenceIds(),
			Message:       signalRenewalScheduleWorkflowReply.GetOutcome().GetMessage(),
			Status:        signalRenewalScheduleWorkflowReply.GetResult().GetStatus(),
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// FetchPartnerOrderReport returns a partner order.
func (s PartnerOrderReportingServer) FetchPartnerOrderReport(ctx context.Context, req *partnerapiv1.FetchPartnerOrderReportRequest) (*partnerapiv1.FetchPartnerOrderReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected FetchPartnerOrderReport", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "partner_order", req.GetPartnerOrderId(), "fetch_partner_order_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "fetch partner order report", "tenant", req.GetContext().GetTenantId())
	return &partnerapiv1.FetchPartnerOrderReportResponse{
		PartnerOrder: &partnerapiv1.PartnerOrder{PartnerOrderId: req.GetPartnerOrderId()},
		Result:       &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// SchedulePartnerOrderReport schedules a partner order.
func (s PartnerOrderReportingServer) SchedulePartnerOrderReport(ctx context.Context, req *partnerapiv1.SchedulePartnerOrderReportRequest) (*partnerapiv1.SchedulePartnerOrderReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "partner_order", req.GetPartnerOrderId(), "schedule_partner_order_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "schedule partner order report", "tenant", req.GetContext().GetTenantId())
	recallSellerPayoutScheduleRequest := buildRecallSellerPayoutScheduleRequestForSchedulePartnerOrderReport(req)
	recallSellerPayoutScheduleReply, err := s.sellerPayoutScheduleApprovalClient.RecallSellerPayoutSchedule(ctx, recallSellerPayoutScheduleRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerPayoutScheduleApprovalService/RecallSellerPayoutSchedule")
	}
	normalizeSearchQueryRequest := &searchv1.NormalizeSearchQueryRequest{
		Context: req.GetContext(),
		Details: &searchv1.NormalizeSearchQueryRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email: recallSellerPayoutScheduleReply.GetSellerPayoutSchedule().GetOrganizer().GetEmail(),
			},
		},
		SearchQuery: &searchv1.SearchQuery{
			Subject: &searchv1.SearchIndexRef{
				DisplayName: recallSellerPayoutScheduleReply.GetSellerPayoutSchedule().GetOrganizer().GetDisplayName(),
			},
		},
	}
	send := s.normalizeSearchQuerySender(normalizeSearchQueryRequest)
	normalizeSearchQueryReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryValidationService/NormalizeSearchQuery")
	}
	return &partnerapiv1.SchedulePartnerOrderReportResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds:  recallSellerPayoutScheduleReply.GetResult().GetReferenceIds(),
			Message:       recallSellerPayoutScheduleReply.GetOutcome().GetMessage(),
			CorrelationId: normalizeSearchQueryReply.GetSearchQuery().GetCorrelationId(),
			Status:        "ok",
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// StreamPartnerOrderMetrics streams the changes of partner orders as they happen.
func (s PartnerOrderReportingServer) StreamPartnerOrderMetrics(req *partnerapiv1.StreamPartnerOrderMetricsRequest, stream grpc.ServerStreamingServer[partnerapiv1.StreamPartnerOrderMetricsResponse]) error {
	ctx := stream.Context()
	s.log.InfoContext(ctx, "stream partner order metrics", "tenant", req.GetContext().GetTenantId())
	partnerWebhookApproval, ok := s.clients.Get("acme.partnerapi.v1.PartnerWebhookApprovalService").(partnerapiv1.PartnerWebhookApprovalServiceClient)
	if !ok {
		return status.Errorf(codes.Internal, "no client of %s", "acme.partnerapi.v1.PartnerWebhookApprovalService")
	}
	source := req.GetSubscriber().ProtoReflect()
	email := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("email"))).String()
	packed, err := anypb.New(req.GetFilter())
	if err != nil {
		return grpcx.Wrap(err, "pack payload")
	}
	submitPartnerWebhookRequest := &partnerapiv1.SubmitPartnerWebhookRequest{
		Context:        req.GetContext(),
		Details:        &partnerapiv1.SubmitPartnerWebhookRequest_Details{Approver: &identityv1.UserRef{Email: email}},
		PartnerWebhook: &partnerapiv1.PartnerWebhook{Detail: packed},
	}
	submitPartnerWebhookReply, err := partnerWebhookApproval.SubmitPartnerWebhook(ctx, submitPartnerWebhookRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookApprovalService/SubmitPartnerWebhook")
	}
	explainAccessPolicyDecisionReply, err := s.accessPolicyValidationClient.ExplainAccessPolicyDecision(ctx, &authorizationv1.ExplainAccessPolicyDecisionRequest{
		Context: req.GetContext(),
		AccessPolicy: &authorizationv1.AccessPolicy{
			Owner: &identityv1.UserRef{
				Email:       req.GetSubscriber().GetEmail(),
				DisplayName: submitPartnerWebhookReply.GetPartnerWebhook().GetSubject().GetDisplayName(),
			},
		},
	})
	if err != nil {
		return grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyValidationService/ExplainAccessPolicyDecision")
	}
	normalizePartnerWebhookRequest := buildNormalizePartnerWebhookRequestForStreamPartnerOrderMetrics(req, submitPartnerWebhookReply)
	normalizePartnerWebhookReply, err := s.partnerWebhookValidationClient.NormalizePartnerWebhook(ctx, normalizePartnerWebhookRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookValidationService/NormalizePartnerWebhook")
	}
	if err := stream.Send(&partnerapiv1.StreamPartnerOrderMetricsResponse{
		PartnerOrder: &partnerapiv1.PartnerOrder{
			Lines: []*partnerapiv1.PartnerOrderLine{&partnerapiv1.PartnerOrderLine{
				Attributes: submitPartnerWebhookReply.GetPartnerWebhook().GetAttributes(),
			}},
			Counterparty: &sellersv1.SellerRef{
				Email: explainAccessPolicyDecisionReply.GetAccessPolicy().GetOwner().GetEmail(),
			},
			BillingAddress: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: normalizePartnerWebhookReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}); err != nil {
		return err
	}
	return nil
}

// ListPartnerOrderReports lists partner orders a page at a time.
func (s PartnerOrderReportingServer) ListPartnerOrderReports(ctx context.Context, req *partnerapiv1.ListPartnerOrderReportsRequest) (*partnerapiv1.ListPartnerOrderReportsResponse, error) {
	s.log.InfoContext(ctx, "list partner order reports", "tenant", req.GetContext().GetTenantId())
	return &partnerapiv1.ListPartnerOrderReportsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// SharePartnerOrderReport shares a partner order.
func (s PartnerOrderReportingServer) SharePartnerOrderReport(ctx context.Context, req *partnerapiv1.SharePartnerOrderReportRequest) (*partnerapiv1.SharePartnerOrderReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "partner_order", req.GetPartnerOrderId(), "share_partner_order_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "share partner order report", "tenant", req.GetContext().GetTenantId())
	createPortalHomeRequest := &employeeportalv1.CreatePortalHomeRequest{
		Context: req.GetContext(),
		PortalHome: &employeeportalv1.PortalHome{
			Requester: &commonv1.ContactInfo{
				Name:  req.GetDetails().GetRecipient().GetName(),
				Email: req.GetDetails().GetRecipient().GetEmail(),
			},
		},
	}
	createPortalHomeReply, err := grpcx.NewClient(s.conn, employeeportalv1.NewPortalHomeServiceClient).CreatePortalHome(ctx, createPortalHomeRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.employeeportal.v1.PortalHomeService/CreatePortalHome")
	}
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	var envelope partnerapiv1.PartnerIntegrationRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	step := &partnerOrderReportingServerSharePartnerOrderReportEscalatePartnerWebhookStep{request: &partnerapiv1.EscalatePartnerWebhookRequest{
		Context: req.GetContext(),
		Details: &partnerapiv1.EscalatePartnerWebhookRequest_Details{
			Approver: &identityv1.UserRef{
				Email:  createPortalHomeReply.GetPortalHome().GetAssignee().GetEmail(),
				UserId: createPortalHomeReply.GetPortalHome().GetAssignee().GetUserId(),
			},
		},
		PartnerWebhook: &partnerapiv1.PartnerWebhook{
			Subject: &partnerapiv1.PartnerIntegrationRef{
				DisplayName: createPortalHomeReply.GetPortalHome().GetCustomer().GetDisplayName(),
			},
			Detail: packed,
		},
		PartnerWebhookId: envelope.GetDisplayName(),
	}}
	if err := s.runSharePartnerOrderReportEscalatePartnerWebhook(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookApprovalService/EscalatePartnerWebhook")
	}
	escalatePartnerWebhookReply := step.reply
	var envelope2 partnerapiv1.PartnerIntegrationRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	cancelSellerListingWorkflowRequest := &sellersv1.CancelSellerListingWorkflowRequest{
		Context: req.GetContext(),
		SellerListing: &sellersv1.SellerListing{
			Owner: &identityv1.UserRef{
				Email:  createPortalHomeReply.GetPortalHome().GetAssignee().GetEmail(),
				UserId: createPortalHomeReply.GetPortalHome().GetAssignee().GetUserId(),
			},
		},
		Details: &sellersv1.CancelSellerListingWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Phone: req.GetDetails().GetRecipient().GetPhone()},
		},
		SellerListingId: envelope2.GetDisplayName(),
	}
	cancelSellerListingWorkflowReply, err := grpcx.NewClient(s.conn, sellersv1.NewSellerListingWorkflowServiceClient).CancelSellerListingWorkflow(ctx, cancelSellerListingWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingWorkflowService/CancelSellerListingWorkflow")
	}
	return &partnerapiv1.SharePartnerOrderReportResponse{
		PartnerOrder: &partnerapiv1.PartnerOrder{
			Audit: &commonv1.AuditInfo{
				CreatedAt: createPortalHomeReply.GetPortalHome().GetAudit().GetCreatedAt(),
			},
			Counterparty: &sellersv1.SellerRef{
				DisplayName: escalatePartnerWebhookReply.GetPartnerWebhook().GetSubject().GetDisplayName(),
				Email:       req.GetDetails().GetRecipient().GetEmail(),
			},
			PartnerOrderId: req.GetPartnerOrderId(),
		},
		Outcome: &partnerapiv1.SharePartnerOrderReportResponse_Outcome{
			Message: cancelSellerListingWorkflowReply.GetOutcome().GetMessage(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CountPartnerOrderTotals counts partner orders by status.
func (s PartnerOrderReportingServer) CountPartnerOrderTotals(ctx context.Context, req *partnerapiv1.CountPartnerOrderTotalsRequest) (*partnerapiv1.CountPartnerOrderTotalsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CountPartnerOrderTotals", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "count partner order totals", "tenant", req.GetContext().GetTenantId())
	var holder struct{ Locale string }
	if record := req.GetContext(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Locale"); err != nil {
			s.log.WarnContext(ctx, "copy locale", "error", err)
		}
	}
	packed, err := anypb.New(req.GetPage())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	lintPartnerWebhooksRequest := &partnerapiv1.LintPartnerWebhooksRequest{
		Context: req.GetContext(),
		Details: &partnerapiv1.LintPartnerWebhooksRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Locale:  holder.Locale,
		},
		PartnerWebhook: &partnerapiv1.PartnerWebhook{Detail: packed},
	}
	lintPartnerWebhooksReply := new(partnerapiv1.LintPartnerWebhooksResponse)
	if err := grpcx.Invoke(ctx, s.conn, partnerapiv1.PartnerWebhookValidationService_LintPartnerWebhooks_FullMethodName, lintPartnerWebhooksRequest, lintPartnerWebhooksReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookValidationService/LintPartnerWebhooks")
	}
	return &partnerapiv1.CountPartnerOrderTotalsResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: lintPartnerWebhooksReply.GetResult().GetReferenceIds(),
			Details:      lintPartnerWebhooksReply.GetResult().GetDetails(),
			Status:       "ok",
		},
	}, nil
}

// DeletePartnerOrderReport deletes a partner order.
func (s PartnerOrderReportingServer) DeletePartnerOrderReport(ctx context.Context, req *partnerapiv1.DeletePartnerOrderReportRequest) (*partnerapiv1.DeletePartnerOrderReportResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "partner_order", req.GetPartnerOrderId(), "delete_partner_order_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "delete partner order report", "tenant", req.GetContext().GetTenantId())
	startSellerListingWorkflowReply, err := s.sellerListingWorkflowClient.StartSellerListingWorkflow(ctx, &sellersv1.StartSellerListingWorkflowRequest{
		Context: req.GetContext(),
		Reason:  req.GetReason(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingWorkflowService/StartSellerListingWorkflow")
	}
	packed, err := anypb.New(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	approvePartnerWebhookReply, err := s.partnerWebhookApprovalClient.ApprovePartnerWebhook(ctx, &partnerapiv1.ApprovePartnerWebhookRequest{
		Context: req.GetContext(),
		Details: &partnerapiv1.ApprovePartnerWebhookRequest_Details{
			Approver: &identityv1.UserRef{
				DisplayName: startSellerListingWorkflowReply.GetSellerListing().GetOwner().GetDisplayName(),
				Email:       startSellerListingWorkflowReply.GetSellerListing().GetOwner().GetEmail(),
			},
		},
		PartnerWebhook: &partnerapiv1.PartnerWebhook{Detail: packed},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookApprovalService/ApprovePartnerWebhook")
	}
	previewVenueRequest := buildPreviewVenueRequestForDeletePartnerOrderReport(req, startSellerListingWorkflowReply)
	previewVenueReply, err := s.venueValidationClient.PreviewVenue(ctx, previewVenueRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.venues.v1.VenueValidationService/PreviewVenue")
	}
	commentPartnerWebhookApprovalRequest := buildCommentPartnerWebhookApprovalRequestForDeletePartnerOrderReport(req, approvePartnerWebhookReply, previewVenueReply)
	commentPartnerWebhookApprovalReply, err := s.partnerWebhookApprovalClient.CommentPartnerWebhookApproval(ctx, commentPartnerWebhookApprovalRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookApprovalService/CommentPartnerWebhookApproval")
	}
	return &partnerapiv1.DeletePartnerOrderReportResponse{
		Result: &commonv1.OperationResult{
			Status:       startSellerListingWorkflowReply.GetOutcome().GetStatus(),
			ReferenceIds: approvePartnerWebhookReply.GetResult().GetReferenceIds(),
			Message:      commentPartnerWebhookApprovalReply.GetResult().GetMessage(),
		},
	}, nil
}

// normalizeSearchQuerySender returns a function that sends one request to
// acme.search.v1.SearchQueryValidationService/NormalizeSearchQuery.
func (s PartnerOrderReportingServer) normalizeSearchQuerySender(request *searchv1.NormalizeSearchQueryRequest) func(context.Context) (*searchv1.NormalizeSearchQueryResponse, error) {
	return func(ctx context.Context) (*searchv1.NormalizeSearchQueryResponse, error) {
		return s.searchQueryValidationClient.NormalizeSearchQuery(ctx, request)
	}
}

// partnerOrderReportingServerSharePartnerOrderReportEscalatePartnerWebhookStep carries the request of one step of
// SharePartnerOrderReport and, once it ran, its reply.
type partnerOrderReportingServerSharePartnerOrderReportEscalatePartnerWebhookStep struct {
	request *partnerapiv1.EscalatePartnerWebhookRequest
	reply   *partnerapiv1.EscalatePartnerWebhookResponse
}

func (s PartnerOrderReportingServer) runSharePartnerOrderReportEscalatePartnerWebhook(ctx context.Context, step *partnerOrderReportingServerSharePartnerOrderReportEscalatePartnerWebhookStep) error {
	reply, err := s.partnerWebhookApprovalClient.EscalatePartnerWebhook(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
