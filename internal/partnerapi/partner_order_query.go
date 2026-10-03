package partnerapi

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	datahubv1 "github.com/hounddogai/protomap-test-golang/gen/acme/datahub/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	partnerapiv1 "github.com/hounddogai/protomap-test-golang/gen/acme/partnerapi/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	warehousingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/warehousing/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
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

// PartnerOrderQueryServer implements `acme.partnerapi.v1.PartnerOrderQueryService`.
// Answers read-only questions about partner orders, for screens and reports.
type PartnerOrderQueryServer struct {
	partnerapiv1.UnimplementedPartnerOrderQueryServiceServer
	store                              *Store
	log                                *slog.Logger
	partnerOrderApprovalPort           partnerOrderApprovalPort
	partnerWebhookReportingClient      partnerapiv1.PartnerWebhookReportingServiceClient
	sellerPayoutScheduleWorkflowClient sellersv1.SellerPayoutScheduleWorkflowServiceClient
	conn                               *grpc.ClientConn
	partnerWebhookWorkflowClient       partnerapiv1.PartnerWebhookWorkflowServiceClient
	storageBinWorkflowClient           warehousingv1.StorageBinWorkflowServiceClient
	userHistoryClient                  identityv1.UserHistoryServiceClient
	workspaceQueryClient               datahubv1.WorkspaceQueryServiceClient
	clients                            *registry.Registry
	partnerWebhookValidationClient     partnerapiv1.PartnerWebhookValidationServiceClient
}

// NewPartnerOrderQueryServer wires a PartnerOrderQueryServer.
func NewPartnerOrderQueryServer(deps platform.Deps) *PartnerOrderQueryServer {
	return &PartnerOrderQueryServer{
		store:                              NewStore(deps.DB),
		log:                                deps.Logger.With("service", "acme.partnerapi.v1.PartnerOrderQueryService"),
		partnerOrderApprovalPort:           newPartnerOrderApprovalPort(deps),
		partnerWebhookReportingClient:      partnerapiv1.NewPartnerWebhookReportingServiceClient(deps.Conn),
		sellerPayoutScheduleWorkflowClient: sellersv1.NewSellerPayoutScheduleWorkflowServiceClient(deps.Conn),
		conn:                               deps.Conn,
		partnerWebhookWorkflowClient:       partnerapiv1.NewPartnerWebhookWorkflowServiceClient(deps.Conn),
		storageBinWorkflowClient:           warehousingv1.NewStorageBinWorkflowServiceClient(deps.Conn),
		userHistoryClient:                  identityv1.NewUserHistoryServiceClient(deps.Conn),
		workspaceQueryClient:               datahubv1.NewWorkspaceQueryServiceClient(deps.Conn),
		clients:                            deps.Clients,
		partnerWebhookValidationClient:     partnerapiv1.NewPartnerWebhookValidationServiceClient(deps.Conn),
	}
}

// LookupPartnerOrder returns a partner order.
func (s *PartnerOrderQueryServer) LookupPartnerOrder(ctx context.Context, req *partnerapiv1.LookupPartnerOrderRequest) (*partnerapiv1.LookupPartnerOrderResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected LookupPartnerOrder", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "partner_order", req.GetPartnerOrderId(), "lookup_partner_order"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "lookup partner order", "tenant", req.GetContext().GetTenantId())
	listPendingPartnerOrdersRequest := &partnerapiv1.ListPendingPartnerOrdersRequest{
		Context: req.GetContext(),
		Filter: &partnerapiv1.ListPendingPartnerOrdersRequest_Filter{
			OwnerId:      req.GetContext().GetActor().GetUserId(),
			CreatedAfter: req.GetContext().GetDeadline(),
		},
	}
	listPendingPartnerOrdersReply, err := s.partnerOrderApprovalPort.ListPendingPartnerOrders(ctx, listPendingPartnerOrdersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerOrderApprovalService/ListPendingPartnerOrders")
	}
	return &partnerapiv1.LookupPartnerOrderResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: listPendingPartnerOrdersReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		PartnerOrder: &partnerapiv1.PartnerOrder{PartnerOrderId: req.GetPartnerOrderId()},
	}, nil
}

// BrowsePartnerOrders lists partner orders a page at a time.
func (s *PartnerOrderQueryServer) BrowsePartnerOrders(ctx context.Context, req *partnerapiv1.BrowsePartnerOrdersRequest) (*partnerapiv1.BrowsePartnerOrdersResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "browse partner orders", "tenant", req.GetContext().GetTenantId())
	return &partnerapiv1.BrowsePartnerOrdersResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// FindPartnerOrders lists partner orders a page at a time.
func (s *PartnerOrderQueryServer) FindPartnerOrders(ctx context.Context, req *partnerapiv1.FindPartnerOrdersRequest) (*partnerapiv1.FindPartnerOrdersResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "find partner orders", "tenant", req.GetContext().GetTenantId())
	return &partnerapiv1.FindPartnerOrdersResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// CountPartnerOrders counts partner orders by status.
func (s *PartnerOrderQueryServer) CountPartnerOrders(ctx context.Context, req *partnerapiv1.CountPartnerOrdersRequest) (*partnerapiv1.CountPartnerOrdersResponse, error) {
	s.log.InfoContext(ctx, "count partner orders", "tenant", req.GetContext().GetTenantId())
	streamPartnerWebhookMetricsStream, err := s.partnerWebhookReportingClient.StreamPartnerWebhookMetrics(ctx, &partnerapiv1.StreamPartnerWebhookMetricsRequest{
		Context: req.GetContext(),
		Filter: &partnerapiv1.StreamPartnerWebhookMetricsRequest_Filter{
			CreatedAfter: req.GetFilter().GetCreatedAfter(),
			Query:        req.GetFilter().GetQuery(),
			OwnerId:      req.GetFilter().GetOwnerId(),
			Region:       req.GetFilter().GetRegion(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookReportingService/StreamPartnerWebhookMetrics")
	}
	for {
		received, err := streamPartnerWebhookMetricsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookReportingService/StreamPartnerWebhookMetrics")
		}
		_ = received
	}
	cancelSellerPayoutScheduleWorkflowRequest := buildCancelSellerPayoutScheduleWorkflowRequestForCountPartnerOrders(req)
	cancelSellerPayoutScheduleWorkflowReply, err := s.sellerPayoutScheduleWorkflowClient.CancelSellerPayoutScheduleWorkflow(ctx, cancelSellerPayoutScheduleWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerPayoutScheduleWorkflowService/CancelSellerPayoutScheduleWorkflow")
	}
	return &partnerapiv1.CountPartnerOrdersResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: cancelSellerPayoutScheduleWorkflowReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// ResolvePartnerOrders returns several partner orders at once.
func (s *PartnerOrderQueryServer) ResolvePartnerOrders(ctx context.Context, req *partnerapiv1.ResolvePartnerOrdersRequest) (*partnerapiv1.ResolvePartnerOrdersResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "resolve partner orders", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetContext()).(*commonv1.RequestContext)
	ctx = tenancy.With(ctx, "locale", req.GetContext().GetLocale())
	explainSynonymDecisionRequest := &searchv1.ExplainSynonymDecisionRequest{
		Context: req.GetContext(),
		Details: &searchv1.ExplainSynonymDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Locale:  tenancy.Value(ctx, "locale"),
		},
		Synonym: &searchv1.Synonym{
			Owner: &identityv1.UserRef{UserId: cloned.GetActor().GetUserId()},
			Audit: &commonv1.AuditInfo{CreatedAt: req.GetContext().GetDeadline()},
		},
	}
	explainSynonymDecisionReply := new(searchv1.ExplainSynonymDecisionResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", searchSynonymValidationServiceName, "ExplainSynonymDecision"), explainSynonymDecisionRequest, explainSynonymDecisionReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymValidationService/ExplainSynonymDecision")
	}
	listPartnerWebhookReportsRequest := &partnerapiv1.ListPartnerWebhookReportsRequest{
		Context: req.GetContext(),
		Filter: &partnerapiv1.ListPartnerWebhookReportsRequest_Filter{
			CreatedAfter: explainSynonymDecisionReply.GetSynonym().GetAudit().GetUpdatedAt(),
			OwnerId:      explainSynonymDecisionReply.GetSynonym().GetOwner().GetUserId(),
		},
	}
	listPartnerWebhookReportsReply, err := grpcx.Call(ctx, s.partnerWebhookReportingClient.ListPartnerWebhookReports, listPartnerWebhookReportsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookReportingService/ListPartnerWebhookReports")
	}
	return &partnerapiv1.ResolvePartnerOrdersResponse{
		PartnerOrders: []*partnerapiv1.PartnerOrder{&partnerapiv1.PartnerOrder{
			Counterparty: &sellersv1.SellerRef{
				DisplayName: explainSynonymDecisionReply.GetSynonym().GetOwner().GetDisplayName(),
			},
		}},
		Result: &commonv1.OperationResult{
			Message:      listPartnerWebhookReportsReply.GetResult().GetMessage(),
			ReferenceIds: listPartnerWebhookReportsReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// ExportPartnerOrders streams every matching partner order.
func (s *PartnerOrderQueryServer) ExportPartnerOrders(req *partnerapiv1.ExportPartnerOrdersRequest, stream grpc.ServerStreamingServer[partnerapiv1.ExportPartnerOrdersResponse]) error {
	ctx := stream.Context()
	if req.GetContext().GetTenantId() == "" {
		return status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "export partner orders", "tenant", req.GetContext().GetTenantId())
	packed, err := anypb.New(req.GetFilter())
	if err != nil {
		return grpcx.Wrap(err, "pack payload")
	}
	cancelPartnerWebhookWorkflowReply, err := s.partnerWebhookWorkflowClient.CancelPartnerWebhookWorkflow(ctx, &partnerapiv1.CancelPartnerWebhookWorkflowRequest{
		Context: req.GetContext(),
		Details: &partnerapiv1.CancelPartnerWebhookWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
		PartnerWebhook: &partnerapiv1.PartnerWebhook{Detail: packed},
	})
	if err != nil {
		return grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookWorkflowService/CancelPartnerWebhookWorkflow")
	}
	pausePartnerWebhookWorkflowRequest := newPausePartnerWebhookWorkflowRequest(withPausePartnerWebhookWorkflowContext(req.GetContext()), withPausePartnerWebhookWorkflowPartnerWebhook(&partnerapiv1.PartnerWebhook{
		Subject: &partnerapiv1.PartnerIntegrationRef{
			CurrencyCode:         cancelPartnerWebhookWorkflowReply.GetPartnerWebhook().GetSubject().GetCurrencyCode(),
			PartnerIntegrationId: cancelPartnerWebhookWorkflowReply.GetPartnerWebhook().GetSubject().GetPartnerIntegrationId(),
		},
	}))
	pausePartnerWebhookWorkflowReply, err := s.partnerWebhookWorkflowClient.PausePartnerWebhookWorkflow(ctx, pausePartnerWebhookWorkflowRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookWorkflowService/PausePartnerWebhookWorkflow")
	}
	encoded, err := protojson.Marshal(req.GetFilter())
	if err != nil {
		return grpcx.Wrap(err, "encode request")
	}
	var decoded partnerapiv1.ExportPartnerOrdersRequest_Filter
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return err
	}
	estimateSellerPayoutScheduleRequest := &sellersv1.EstimateSellerPayoutScheduleRequest{
		Context: req.GetContext(),
		Details: &sellersv1.EstimateSellerPayoutScheduleRequest_Details{
			CurrencyCode: cancelPartnerWebhookWorkflowReply.GetPartnerWebhook().GetSubject().GetCurrencyCode(),
		},
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
			Organizer: &identityv1.UserRef{
				DisplayName: cancelPartnerWebhookWorkflowReply.GetPartnerWebhook().GetSubject().GetDisplayName(),
			},
			Location: &commonv1.PostalAddress{Region: (&decoded).GetRegion()},
		},
	}
	estimateSellerPayoutScheduleReply, err := grpcx.NewClient(s.conn, sellersv1.NewSellerPayoutScheduleCalculationServiceClient).EstimateSellerPayoutSchedule(ctx, estimateSellerPayoutScheduleRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.sellers.v1.SellerPayoutScheduleCalculationService/EstimateSellerPayoutSchedule")
	}
	if err := stream.Send(&partnerapiv1.ExportPartnerOrdersResponse{
		Result: &commonv1.OperationResult{
			Message: cancelPartnerWebhookWorkflowReply.GetOutcome().GetMessage(),
			Details: pausePartnerWebhookWorkflowReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		PartnerOrder: &partnerapiv1.PartnerOrder{
			Lines: []*partnerapiv1.PartnerOrderLine{&partnerapiv1.PartnerOrderLine{
				Attributes: pausePartnerWebhookWorkflowReply.GetPartnerWebhook().GetAttributes(),
			}},
			BillingAddress: &commonv1.PostalAddress{
				Locality: estimateSellerPayoutScheduleReply.GetSellerPayoutSchedule().GetLocation().GetLocality(),
				Region:   req.GetFilter().GetRegion(),
			},
			Metadata: estimateSellerPayoutScheduleReply.GetSellerPayoutSchedule().GetMetadata(),
		},
	}); err != nil {
		return err
	}
	return nil
}

// SummarizePartnerOrders summarizes a partner order.
func (s *PartnerOrderQueryServer) SummarizePartnerOrders(ctx context.Context, req *partnerapiv1.SummarizePartnerOrdersRequest) (*partnerapiv1.SummarizePartnerOrdersResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "partner_order", req.GetPartnerOrderId(), "summarize_partner_orders"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "summarize partner orders", "tenant", req.GetContext().GetTenantId())
	filters := make(map[string]string, len(req.GetDetails().GetFilters()))
	for key, value := range req.GetDetails().GetFilters() {
		filters[key] = value
	}
	listStorageBinWorkflowsRequest := &warehousingv1.ListStorageBinWorkflowsRequest{
		Context: req.GetContext(),
		Filter: &warehousingv1.ListStorageBinWorkflowsRequest_Filter{
			Labels:       filters,
			CreatedAfter: req.GetContext().GetDeadline(),
			OwnerId:      req.GetContext().GetTenantId(),
		},
	}
	listStorageBinWorkflowsReply, err := grpcx.Call(ctx, s.storageBinWorkflowClient.ListStorageBinWorkflows, listStorageBinWorkflowsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.StorageBinWorkflowService/ListStorageBinWorkflows")
	}
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := partnerOrderQueryServerRevertUserSteps[mode]
	revertUserRequest := &identityv1.RevertUserRequest{
		Context: req.GetContext(),
		User: &identityv1.User{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			UserId:  req.GetContext().GetActor().GetUserId(),
		},
	}
	revertUserReply, err := run(s, ctx, revertUserRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserHistoryService/RevertUser")
	}
	browseWorkspacesRequest := &datahubv1.BrowseWorkspacesRequest{}
	browseWorkspacesRequest.Context = req.GetContext()
	browseWorkspacesRequest.Filter = &datahubv1.BrowseWorkspacesRequest_Filter{
		Region:       revertUserReply.GetUser().GetPrimaryAddress().GetRegion(),
		CreatedAfter: revertUserReply.GetUser().GetPassport().GetExpiresAt(),
	}
	browseWorkspacesReply, err := s.workspaceQueryClient.BrowseWorkspaces(ctx, browseWorkspacesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.datahub.v1.WorkspaceQueryService/BrowseWorkspaces")
	}
	filters2 := make(map[string]string, len(req.GetDetails().GetFilters()))
	for key2, value2 := range req.GetDetails().GetFilters() {
		filters2[key2] = value2
	}
	schedulePartnerWebhookReportRequest := newSchedulePartnerWebhookReportRequest(withSchedulePartnerWebhookReportContext(req.GetContext()), withSchedulePartnerWebhookReportDetails(&partnerapiv1.SchedulePartnerWebhookReportRequest_Details{Filters: filters2}))
	schedulePartnerWebhookReportReply, err := s.partnerWebhookReportingClient.SchedulePartnerWebhookReport(ctx, schedulePartnerWebhookReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookReportingService/SchedulePartnerWebhookReport")
	}
	_ = browseWorkspacesReply
	return &partnerapiv1.SummarizePartnerOrdersResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: listStorageBinWorkflowsReply.GetResult().GetCorrelationId(),
			Message:       revertUserReply.GetOutcome().GetMessage(),
			Status:        revertUserReply.GetOutcome().GetStatus(),
		},
		Summary: schedulePartnerWebhookReportReply.GetSummary(),
		Period:  req.GetDetails().GetPeriod(),
	}, nil
}

// SuggestPartnerOrders lists partner orders a page at a time.
func (s *PartnerOrderQueryServer) SuggestPartnerOrders(ctx context.Context, req *partnerapiv1.SuggestPartnerOrdersRequest) (*partnerapiv1.SuggestPartnerOrdersResponse, error) {
	s.log.InfoContext(ctx, "suggest partner orders", "tenant", req.GetContext().GetTenantId())
	return &partnerapiv1.SuggestPartnerOrdersResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// PeekPartnerOrder returns a partner order.
func (s *PartnerOrderQueryServer) PeekPartnerOrder(ctx context.Context, req *partnerapiv1.PeekPartnerOrderRequest) (*partnerapiv1.PeekPartnerOrderResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "partner_order", req.GetPartnerOrderId(), "peek_partner_order"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "peek partner order", "tenant", req.GetContext().GetTenantId())
	userQuery, err := registry.Lookup[identityv1.UserQueryServiceClient](s.clients, "acme.identity.v1.UserQueryService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	ctx = tenancy.With(ctx, "partner_order_id", req.GetPartnerOrderId())
	encoded, err := protojson.Marshal(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded commonv1.RequestContext
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	browseUsersRequest := &identityv1.BrowseUsersRequest{
		Context: req.GetContext(),
		Filter: &identityv1.BrowseUsersRequest_Filter{
			OwnerId:      tenancy.Value(ctx, "partner_order_id"),
			CreatedAfter: (&decoded).GetDeadline(),
			Labels:       req.GetContext().GetBaggage(),
		},
	}
	browseUsersReply, err := userQuery.BrowseUsers(ctx, browseUsersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserQueryService/BrowseUsers")
	}
	packed, err := anypb.New(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	scorePartnerWebhookRequest := &partnerapiv1.ScorePartnerWebhookRequest{
		Context: req.GetContext(),
		PartnerWebhook: &partnerapiv1.PartnerWebhook{
			CorrelationId: browseUsersReply.GetResult().GetCorrelationId(),
			Detail:        packed,
		},
	}
	send := s.scorePartnerWebhookSender(scorePartnerWebhookRequest)
	scorePartnerWebhookReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookValidationService/ScorePartnerWebhook")
	}
	return &partnerapiv1.PeekPartnerOrderResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: browseUsersReply.GetResult().GetCorrelationId(),
			ReferenceIds:  browseUsersReply.GetResult().GetReferenceIds(),
			Message:       scorePartnerWebhookReply.GetResult().GetMessage(),
			Status:        "ok",
		},
		PartnerOrder: &partnerapiv1.PartnerOrder{PartnerOrderId: req.GetPartnerOrderId()},
	}, nil
}

// partnerOrderQueryServerRevertUserSteps are the ways to run acme.identity.v1.UserHistoryService/RevertUser, by name.
var partnerOrderQueryServerRevertUserSteps = map[string]func(*PartnerOrderQueryServer, context.Context, *identityv1.RevertUserRequest) (*identityv1.RevertUserResponse, error){
	"default": (*PartnerOrderQueryServer).revertUserStep,
	"audited": (*PartnerOrderQueryServer).revertUserAuditedStep,
}

func (s *PartnerOrderQueryServer) revertUserStep(ctx context.Context, request *identityv1.RevertUserRequest) (*identityv1.RevertUserResponse, error) {
	return s.userHistoryClient.RevertUser(ctx, request)
}

func (s *PartnerOrderQueryServer) revertUserAuditedStep(ctx context.Context, request *identityv1.RevertUserRequest) (*identityv1.RevertUserResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.identity.v1.UserHistoryService/RevertUser")
	return s.userHistoryClient.RevertUser(ctx, request)
}

// scorePartnerWebhookSender returns a function that sends one request to
// acme.partnerapi.v1.PartnerWebhookValidationService/ScorePartnerWebhook.
func (s *PartnerOrderQueryServer) scorePartnerWebhookSender(request *partnerapiv1.ScorePartnerWebhookRequest) func(context.Context) (*partnerapiv1.ScorePartnerWebhookResponse, error) {
	return func(ctx context.Context) (*partnerapiv1.ScorePartnerWebhookResponse, error) {
		return s.partnerWebhookValidationClient.ScorePartnerWebhook(ctx, request)
	}
}
