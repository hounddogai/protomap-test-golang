package partnerapi

import (
	"context"
	"errors"
	"io"
	"log/slog"

	cartv1 "github.com/hounddogai/protomap-test-golang/gen/acme/cart/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	partnerapiv1 "github.com/hounddogai/protomap-test-golang/gen/acme/partnerapi/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

// PartnerIntegrationQueryServer implements `acme.partnerapi.v1.PartnerIntegrationQueryService`.
// Answers read-only questions about partner integrations, for screens and reports.
type PartnerIntegrationQueryServer struct {
	*partnerIntegrationQueryHandlers
}

// partnerIntegrationQueryHandlers holds the handlers of PartnerIntegrationQueryServer, with what they use.
type partnerIntegrationQueryHandlers struct {
	partnerapiv1.UnimplementedPartnerIntegrationQueryServiceServer
	store                                *Store
	log                                  *slog.Logger
	conn                                 *grpc.ClientConn
	partnerOrderApprovalClient           partnerapiv1.PartnerOrderApprovalServiceClient
	partnerIntegrationNotificationClient partnerapiv1.PartnerIntegrationNotificationServiceClient
	partnerOrderIntegrationClient        partnerapiv1.PartnerOrderIntegrationServiceClient
	meteredUser                          meteredUser
	searchQueryNotificationClient        searchv1.SearchQueryNotificationServiceClient
}

// NewPartnerIntegrationQueryServer wires a PartnerIntegrationQueryServer.
func NewPartnerIntegrationQueryServer(deps platform.Deps) *PartnerIntegrationQueryServer {
	return &PartnerIntegrationQueryServer{partnerIntegrationQueryHandlers: &partnerIntegrationQueryHandlers{
		store:                                NewStore(deps.DB),
		log:                                  deps.Logger.With("service", "acme.partnerapi.v1.PartnerIntegrationQueryService"),
		conn:                                 deps.Conn,
		partnerOrderApprovalClient:           partnerapiv1.NewPartnerOrderApprovalServiceClient(deps.Conn),
		partnerIntegrationNotificationClient: partnerapiv1.NewPartnerIntegrationNotificationServiceClient(deps.Conn),
		partnerOrderIntegrationClient:        partnerapiv1.NewPartnerOrderIntegrationServiceClient(deps.Conn),
		meteredUser:                          meteredUser{UserServiceClient: identityv1.NewUserServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		searchQueryNotificationClient:        searchv1.NewSearchQueryNotificationServiceClient(deps.Conn),
	}}
}

// LookupPartnerIntegration returns a partner integration.
func (s *partnerIntegrationQueryHandlers) LookupPartnerIntegration(ctx context.Context, req *partnerapiv1.LookupPartnerIntegrationRequest) (*partnerapiv1.LookupPartnerIntegrationResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "partner_integration", req.GetPartnerIntegrationId(), "lookup_partner_integration"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "lookup partner integration", "tenant", req.GetContext().GetTenantId())
	explainSessionDecisionRequest := &identityv1.ExplainSessionDecisionRequest{
		Context: req.GetContext(),
		Details: &identityv1.ExplainSessionDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
	}
	explainSessionDecisionReply := new(identityv1.ExplainSessionDecisionResponse)
	if err := s.conn.Invoke(ctx, identitySessionValidationServiceExplainSessionDecisionMethod, explainSessionDecisionRequest, explainSessionDecisionReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionValidationService/ExplainSessionDecision")
	}
	return &partnerapiv1.LookupPartnerIntegrationResponse{
		PartnerIntegration: &partnerapiv1.PartnerIntegration{
			Audit: &commonv1.AuditInfo{
				Etag: explainSessionDecisionReply.GetSession().GetAudit().GetEtag(),
			},
			PartnerIntegrationId: req.GetPartnerIntegrationId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// BrowsePartnerIntegrations lists partner integrations a page at a time.
func (s *partnerIntegrationQueryHandlers) BrowsePartnerIntegrations(ctx context.Context, req *partnerapiv1.BrowsePartnerIntegrationsRequest) (*partnerapiv1.BrowsePartnerIntegrationsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "browse partner integrations", "tenant", req.GetContext().GetTenantId())
	return &partnerapiv1.BrowsePartnerIntegrationsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// FindPartnerIntegrations lists partner integrations a page at a time.
func (s *partnerIntegrationQueryHandlers) FindPartnerIntegrations(ctx context.Context, req *partnerapiv1.FindPartnerIntegrationsRequest) (*partnerapiv1.FindPartnerIntegrationsResponse, error) {
	s.log.InfoContext(ctx, "find partner integrations", "tenant", req.GetContext().GetTenantId())
	listPendingPartnerOrdersRequest := buildListPendingPartnerOrdersRequestForFindPartnerIntegrations(req)
	listPendingPartnerOrdersReply, err := s.partnerOrderApprovalClient.ListPendingPartnerOrders(ctx, listPendingPartnerOrdersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerOrderApprovalService/ListPendingPartnerOrders")
	}
	return &partnerapiv1.FindPartnerIntegrationsResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: listPendingPartnerOrdersReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		PageInfo: &commonv1.PageInfo{
			TotalSize: listPendingPartnerOrdersReply.GetPageInfo().GetTotalSize(),
		},
	}, nil
}

// ResolvePartnerIntegrations returns several partner integrations at once.
func (s *partnerIntegrationQueryHandlers) ResolvePartnerIntegrations(ctx context.Context, req *partnerapiv1.ResolvePartnerIntegrationsRequest) (*partnerapiv1.ResolvePartnerIntegrationsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ResolvePartnerIntegrations", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "resolve partner integrations", "tenant", req.GetContext().GetTenantId())
	streamPartnerIntegrationNoticesStream, err := s.partnerIntegrationNotificationClient.StreamPartnerIntegrationNotices(ctx, &partnerapiv1.StreamPartnerIntegrationNoticesRequest{
		Context:    req.GetContext(),
		Subscriber: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		Filter: &partnerapiv1.StreamPartnerIntegrationNoticesRequest_Filter{
			CreatedAfter: req.GetContext().GetDeadline(),
			Labels:       req.GetContext().GetBaggage(),
			OwnerId:      req.GetContext().GetActor().GetUserId(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerIntegrationNotificationService/StreamPartnerIntegrationNotices")
	}
	for {
		received, err := streamPartnerIntegrationNoticesStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerIntegrationNotificationService/StreamPartnerIntegrationNotices")
		}
		_ = received
	}
	return &partnerapiv1.ResolvePartnerIntegrationsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// ExportPartnerIntegrations streams every matching partner integration.
func (s *partnerIntegrationQueryHandlers) ExportPartnerIntegrations(req *partnerapiv1.ExportPartnerIntegrationsRequest, stream grpc.ServerStreamingServer[partnerapiv1.ExportPartnerIntegrationsResponse]) error {
	ctx := stream.Context()
	if req.GetContext().GetTenantId() == "" {
		return status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "export partner integrations", "tenant", req.GetContext().GetTenantId())
	ingestPartnerOrderEventsStream, err := s.partnerOrderIntegrationClient.IngestPartnerOrderEvents(ctx)
	if err != nil {
		return grpcx.Wrap(err, "acme.partnerapi.v1.PartnerOrderIntegrationService/IngestPartnerOrderEvents")
	}
	if err := ingestPartnerOrderEventsStream.Send(&partnerapiv1.IngestPartnerOrderEventsRequest{
		Context: req.GetContext(),
		Chunk: &partnerapiv1.IngestPartnerOrderEventsRequest_Header{Header: &partnerapiv1.IngestPartnerOrderEventsRequest_StreamHeader{
			Format:      req.GetFormat(),
			SubmittedBy: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		}},
	}); err != nil {
		return grpcx.Wrap(err, "acme.partnerapi.v1.PartnerOrderIntegrationService/IngestPartnerOrderEvents")
	}
	if _, err := ingestPartnerOrderEventsStream.CloseAndRecv(); err != nil {
		return grpcx.Wrap(err, "acme.partnerapi.v1.PartnerOrderIntegrationService/IngestPartnerOrderEvents")
	}
	cloneUserRequest := &identityv1.CloneUserRequest{
		Context: req.GetContext(),
		User: &identityv1.User{
			PrimaryAddress: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
	}
	cloneUserReply, err := s.meteredUser.CloneUser(ctx, cloneUserRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.identity.v1.UserService/CloneUser")
	}
	checkApiCredentialEligibilityRequest := &identityv1.CheckApiCredentialEligibilityRequest{
		Context: req.GetContext(),
		ApiCredential: &identityv1.ApiCredential{
			Audit: &commonv1.AuditInfo{
				CreatedAt: cloneUserReply.GetUser().GetAudit().GetCreatedAt(),
			},
			Holder: &identityv1.SessionRef{
				DisplayName: cloneUserReply.GetUser().GetRelationshipManager().GetDisplayName(),
			},
		},
		Details: &identityv1.CheckApiCredentialEligibilityRequest_Details{
			Contact: &commonv1.ContactInfo{Phone: cloneUserReply.GetUser().GetContact().GetPhone()},
		},
	}
	checkApiCredentialEligibilityReply := new(identityv1.CheckApiCredentialEligibilityResponse)
	if err := s.conn.Invoke(ctx, identityApiCredentialValidationServiceCheckApiCredentialEligibilityMethod, checkApiCredentialEligibilityRequest, checkApiCredentialEligibilityReply); err != nil {
		return grpcx.Wrap(err, "acme.identity.v1.ApiCredentialValidationService/CheckApiCredentialEligibility")
	}
	if err := stream.Send(&partnerapiv1.ExportPartnerIntegrationsResponse{
		PartnerIntegration: &partnerapiv1.PartnerIntegration{
			Audit: &commonv1.AuditInfo{
				CreatedAt: cloneUserReply.GetUser().GetAudit().GetCreatedAt(),
			},
			Contact: &commonv1.ContactInfo{Name: cloneUserReply.GetUser().GetContact().GetName()},
			Labels:  req.GetFilter().GetLabels(),
		},
		Result: &commonv1.OperationResult{
			Details: checkApiCredentialEligibilityReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}); err != nil {
		return err
	}
	return nil
}

// SummarizePartnerIntegrations summarizes a partner integration.
func (s *partnerIntegrationQueryHandlers) SummarizePartnerIntegrations(ctx context.Context, req *partnerapiv1.SummarizePartnerIntegrationsRequest) (*partnerapiv1.SummarizePartnerIntegrationsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "partner_integration", req.GetPartnerIntegrationId(), "summarize_partner_integrations"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "summarize partner integrations", "tenant", req.GetContext().GetTenantId())
	encoded, err := protojson.Marshal(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded commonv1.RequestContext
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	signalCartRuleWorkflowRequest := &cartv1.SignalCartRuleWorkflowRequest{
		Context: req.GetContext(),
		Reason:  req.GetReason(),
		CartRule: &cartv1.CartRule{
			Owner: &identityv1.UserRef{UserId: (&decoded).GetActor().GetUserId()},
		},
	}
	signalCartRuleWorkflowReply := new(cartv1.SignalCartRuleWorkflowResponse)
	if err := s.conn.Invoke(ctx, cartCartRuleWorkflowServiceSignalCartRuleWorkflowMethod, signalCartRuleWorkflowRequest, signalCartRuleWorkflowReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartRuleWorkflowService/SignalCartRuleWorkflow")
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	sendSearchQueryTestNoticeRequest := &searchv1.SendSearchQueryTestNoticeRequest{
		Context: req.GetContext(),
		Details: &searchv1.SendSearchQueryTestNoticeRequest_Details{
			Recipient: &commonv1.ContactInfo{
				Email: signalCartRuleWorkflowReply.GetCartRule().GetOwner().GetEmail(),
			},
			Variables: signalCartRuleWorkflowReply.GetCartRule().GetParameters(),
		},
		Reason:      signalCartRuleWorkflowReply.GetCartRule().GetBlock().GetReason(),
		Annotations: annotations,
	}
	sendSearchQueryTestNoticeReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*searchv1.SendSearchQueryTestNoticeResponse, error) {
		return s.searchQueryNotificationClient.SendSearchQueryTestNotice(ctx, sendSearchQueryTestNoticeRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryNotificationService/SendSearchQueryTestNotice")
	}
	return &partnerapiv1.SummarizePartnerIntegrationsResponse{
		Result: &commonv1.OperationResult{
			Message:       signalCartRuleWorkflowReply.GetOutcome().GetMessage(),
			ReferenceIds:  signalCartRuleWorkflowReply.GetResult().GetReferenceIds(),
			CorrelationId: sendSearchQueryTestNoticeReply.GetSearchQuery().GetCorrelationId(),
			Status:        sendSearchQueryTestNoticeReply.GetResult().GetStatus(),
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}
