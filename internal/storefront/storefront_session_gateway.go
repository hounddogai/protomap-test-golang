package storefront

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	documentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/documents/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	partnerapiv1 "github.com/hounddogai/protomap-test-golang/gen/acme/partnerapi/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	storefrontv1 "github.com/hounddogai/protomap-test-golang/gen/acme/storefront/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
)

// StorefrontSessionGatewayServer implements `acme.storefront.v1.StorefrontSessionGatewayService`.
// The edge API that apps call for storefront sessions.
type StorefrontSessionGatewayServer struct {
	storefrontv1.UnimplementedStorefrontSessionGatewayServiceServer
	store                           *Store
	log                             *slog.Logger
	conn                            *grpc.ClientConn
	landingPageQueryClient          storefrontv1.LandingPageQueryServiceClient
	synonymExportClient             searchv1.SynonymExportServiceClient
	shopperViewWorkflowClient       storefrontv1.ShopperViewWorkflowServiceClient
	userClient                      identityv1.UserServiceClient
	ledgerAccountValidationClient   ledgerv1.LedgerAccountValidationServiceClient
	landingPageWorkflowClient       storefrontv1.LandingPageWorkflowServiceClient
	storefrontSessionClient         storefrontv1.StorefrontSessionServiceClient
	ledgerAccountQueryClient        ledgerv1.LedgerAccountQueryServiceClient
	partnerWebhookReportingClient   partnerapiv1.PartnerWebhookReportingServiceClient
	landingPageIntegrationClient    storefrontv1.LandingPageIntegrationServiceClient
	landingPageIntegrationOnce      sync.Once
	landingPageIntegrationCached    storefrontv1.LandingPageIntegrationServiceClient
	customerSegmentApprovalClient   customersv1.CustomerSegmentApprovalServiceClient
	consentRecordApprovalClient     customersv1.ConsentRecordApprovalServiceClient
	partnerIntegrationGatewayClient partnerapiv1.PartnerIntegrationGatewayServiceClient
	customerSegmentImportClient     customersv1.CustomerSegmentImportServiceClient
}

// NewStorefrontSessionGatewayServer wires a StorefrontSessionGatewayServer.
func NewStorefrontSessionGatewayServer(deps platform.Deps) *StorefrontSessionGatewayServer {
	return &StorefrontSessionGatewayServer{
		store:                           NewStore(deps.DB),
		log:                             deps.Logger.With("service", "acme.storefront.v1.StorefrontSessionGatewayService"),
		conn:                            deps.Conn,
		landingPageQueryClient:          storefrontv1.NewLandingPageQueryServiceClient(deps.Conn),
		synonymExportClient:             searchv1.NewSynonymExportServiceClient(deps.Conn),
		shopperViewWorkflowClient:       storefrontv1.NewShopperViewWorkflowServiceClient(deps.Conn),
		userClient:                      identityv1.NewUserServiceClient(deps.Conn),
		ledgerAccountValidationClient:   ledgerv1.NewLedgerAccountValidationServiceClient(deps.Conn),
		landingPageWorkflowClient:       storefrontv1.NewLandingPageWorkflowServiceClient(deps.Conn),
		storefrontSessionClient:         storefrontv1.NewStorefrontSessionServiceClient(deps.Conn),
		ledgerAccountQueryClient:        ledgerv1.NewLedgerAccountQueryServiceClient(deps.Conn),
		partnerWebhookReportingClient:   partnerapiv1.NewPartnerWebhookReportingServiceClient(deps.Conn),
		landingPageIntegrationClient:    storefrontv1.NewLandingPageIntegrationServiceClient(deps.Conn),
		customerSegmentApprovalClient:   customersv1.NewCustomerSegmentApprovalServiceClient(deps.Conn),
		consentRecordApprovalClient:     customersv1.NewConsentRecordApprovalServiceClient(deps.Conn),
		partnerIntegrationGatewayClient: partnerapiv1.NewPartnerIntegrationGatewayServiceClient(deps.Conn),
		customerSegmentImportClient:     customersv1.NewCustomerSegmentImportServiceClient(deps.Conn),
	}
}

// FetchStorefrontSessionView fetchs a storefront session.
func (s *StorefrontSessionGatewayServer) FetchStorefrontSessionView(ctx context.Context, req *storefrontv1.FetchStorefrontSessionViewRequest) (*storefrontv1.FetchStorefrontSessionViewResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveStorefrontSession(ctx, req.GetContext().GetTenantId(), req.GetStorefrontSession()); err != nil {
		return nil, grpcx.Wrap(err, "save storefront session")
	}
	s.log.InfoContext(ctx, "fetch storefront session view", "tenant", req.GetContext().GetTenantId(), "etag", req.GetStorefrontSession().GetAudit().GetEtag())
	lintDocumentsRequest := &documentsv1.LintDocumentsRequest{
		Context: req.GetContext(),
		Document: &documentsv1.Document{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: req.GetStorefrontSession().GetAudit().GetUpdatedAt(),
				Etag:      req.GetStorefrontSession().GetAudit().GetEtag(),
			},
			Counterparty: &documentsv1.DocumentFolderRef{
				DisplayName: req.GetStorefrontSession().GetCustomer().GetDisplayName(),
			},
		},
		Details: &documentsv1.LintDocumentsRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetStorefrontSession().GetRequester().GetEmail()},
		},
	}
	lintDocumentsReply := new(documentsv1.LintDocumentsResponse)
	if err := grpcx.Invoke(ctx, s.conn, documentsv1.DocumentValidationService_LintDocuments_FullMethodName, lintDocumentsRequest, lintDocumentsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.documents.v1.DocumentValidationService/LintDocuments")
	}
	resolveLandingPagesRequest := &storefrontv1.ResolveLandingPagesRequest{}
	resolveLandingPagesRequest.Context = req.GetContext()
	resolveLandingPagesReply, err := s.landingPageQueryClient.ResolveLandingPages(ctx, resolveLandingPagesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.LandingPageQueryService/ResolveLandingPages")
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	scheduleSynonymExportRequest := &searchv1.ScheduleSynonymExportRequest{
		Context:     req.GetContext(),
		Reason:      req.GetReason(),
		Annotations: annotations,
	}
	scheduleSynonymExportReply, err := grpcx.Call(ctx, s.synonymExportClient.ScheduleSynonymExport, scheduleSynonymExportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymExportService/ScheduleSynonymExport")
	}
	return &storefrontv1.FetchStorefrontSessionViewResponse{
		StorefrontSession: &storefrontv1.StorefrontSession{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: lintDocumentsReply.GetDocument().GetAudit().GetUpdatedBy(),
			},
			StorefrontSessionId: req.GetStorefrontSessionId(),
			CustomFields:        req.GetStorefrontSession().GetCustomFields(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: lintDocumentsReply.GetResult().GetCorrelationId(),
			Status:        resolveLandingPagesReply.GetResult().GetStatus(),
			Details:       resolveLandingPagesReply.GetResult().GetDetails(),
			Message:       scheduleSynonymExportReply.GetResult().GetMessage(),
		},
	}, nil
}

// ComposeStorefrontSessionPage composes a storefront session.
func (s *StorefrontSessionGatewayServer) ComposeStorefrontSessionPage(ctx context.Context, req *storefrontv1.ComposeStorefrontSessionPageRequest) (*storefrontv1.ComposeStorefrontSessionPageResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveStorefrontSession(ctx, req.GetContext().GetTenantId(), req.GetStorefrontSession()); err != nil {
		return nil, grpcx.Wrap(err, "save storefront session")
	}
	s.log.InfoContext(ctx, "compose storefront session page", "tenant", req.GetContext().GetTenantId(), "etag", req.GetStorefrontSession().GetAudit().GetEtag())
	signalShopperViewWorkflowRequest := &storefrontv1.SignalShopperViewWorkflowRequest{
		Context: req.GetContext(),
		Details: &storefrontv1.SignalShopperViewWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				PreferredChannel: req.GetStorefrontSession().GetRequester().GetPreferredChannel(),
			},
		},
		ShopperView: &storefrontv1.ShopperView{
			Audit: &commonv1.AuditInfo{
				CreatedBy: req.GetStorefrontSession().GetAudit().GetCreatedBy(),
			},
		},
	}
	send := s.signalShopperViewWorkflowSender(signalShopperViewWorkflowRequest)
	signalShopperViewWorkflowReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.ShopperViewWorkflowService/SignalShopperViewWorkflow")
	}
	signalHouseholdWorkflowRequest := &customersv1.SignalHouseholdWorkflowRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Location: &commonv1.PostalAddress{
						CountryCode: signalShopperViewWorkflowReply.GetShopperView().GetAddress().GetCountryCode(),
					},
					Audit: &commonv1.AuditInfo{
						CreatedBy: req.GetStorefrontSession().GetAudit().GetCreatedBy(),
					},
				}},
			}},
		},
	}
	signalHouseholdWorkflowReply := new(customersv1.SignalHouseholdWorkflowResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", customersHouseholdWorkflowServiceName, "SignalHouseholdWorkflow"), signalHouseholdWorkflowRequest, signalHouseholdWorkflowReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdWorkflowService/SignalHouseholdWorkflow")
	}
	updateUserRequest := &identityv1.UpdateUserRequest{}
	updateUserRequest.Context = req.GetContext()
	updateUserRequest.User = &identityv1.User{
		RelationshipManager: &identityv1.UserRef{
			Email: signalShopperViewWorkflowReply.GetShopperView().GetOwner().GetEmail(),
		},
	}
	updateUserReply, err := s.userClient.UpdateUser(ctx, updateUserRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserService/UpdateUser")
	}
	lintLedgerAccountsReply, err := s.ledgerAccountValidationClient.LintLedgerAccounts(ctx, &ledgerv1.LintLedgerAccountsRequest{
		Context: req.GetContext(),
		LedgerAccount: &ledgerv1.LedgerAccount{
			Holder: &ledgerv1.PostingBatchRef{
				DisplayName: signalHouseholdWorkflowReply.GetHousehold().GetManager().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: req.GetStorefrontSession().GetAudit().GetUpdatedBy(),
			},
		},
		Details: &ledgerv1.LintLedgerAccountsRequest_Details{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: req.GetStorefrontSession().GetRequester().GetPreferredChannel(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountValidationService/LintLedgerAccounts")
	}
	_ = updateUserReply
	return &storefrontv1.ComposeStorefrontSessionPageResponse{
		Result: &commonv1.OperationResult{
			Message:       signalShopperViewWorkflowReply.GetResult().GetMessage(),
			ReferenceIds:  signalHouseholdWorkflowReply.GetResult().GetReferenceIds(),
			CorrelationId: lintLedgerAccountsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		StorefrontSession: &storefrontv1.StorefrontSession{
			Requester: &commonv1.ContactInfo{
				Email: signalShopperViewWorkflowReply.GetShopperView().GetOwner().GetEmail(),
			},
			StorefrontSessionId: req.GetStorefrontSessionId(),
			CustomFields:        req.GetStorefrontSession().GetCustomFields(),
		},
	}, nil
}

// RefreshStorefrontSession refreshs a storefront session.
func (s *StorefrontSessionGatewayServer) RefreshStorefrontSession(ctx context.Context, req *storefrontv1.RefreshStorefrontSessionRequest) (*storefrontv1.RefreshStorefrontSessionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RefreshStorefrontSession", "error", err)
		return nil, err
	}
	if err := s.store.SaveStorefrontSession(ctx, req.GetContext().GetTenantId(), req.GetStorefrontSession()); err != nil {
		return nil, grpcx.Wrap(err, "save storefront session")
	}
	s.log.InfoContext(ctx, "refresh storefront session", "tenant", req.GetContext().GetTenantId(), "etag", req.GetStorefrontSession().GetAudit().GetEtag())
	pauseLandingPageWorkflowRequest := &storefrontv1.PauseLandingPageWorkflowRequest{}
	pauseLandingPageWorkflowRequest.Context = req.GetContext()
	pauseLandingPageWorkflowRequest.LandingPage = &storefrontv1.LandingPage{
		Counterparty: &customersv1.CustomerRef{
			Email:       req.GetStorefrontSession().GetAssignee().GetEmail(),
			DisplayName: req.GetStorefrontSession().GetCustomer().GetDisplayName(),
		},
		Audit: &commonv1.AuditInfo{
			CreatedAt: req.GetStorefrontSession().GetAudit().GetCreatedAt(),
		},
	}
	pauseLandingPageWorkflowRequest.Details = &storefrontv1.PauseLandingPageWorkflowRequest_Details{
		Notify: &commonv1.ContactInfo{
			PreferredChannel: req.GetStorefrontSession().GetRequester().GetPreferredChannel(),
		},
	}
	pauseLandingPageWorkflowReply, err := s.landingPageWorkflowClient.PauseLandingPageWorkflow(ctx, pauseLandingPageWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.LandingPageWorkflowService/PauseLandingPageWorkflow")
	}
	listStorefrontSessionsReply, err := s.storefrontSessionClient.ListStorefrontSessions(ctx, &storefrontv1.ListStorefrontSessionsRequest{
		Context: req.GetContext(),
		Filter: &storefrontv1.ListStorefrontSessionsRequest_Filter{
			Region: pauseLandingPageWorkflowReply.GetLandingPage().GetBillingAddress().GetRegion(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.StorefrontSessionService/ListStorefrontSessions")
	}
	packed, err := anypb.New(req.GetStorefrontSession())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	subscribeShopperViewUpdatesRequest := &storefrontv1.SubscribeShopperViewUpdatesRequest{
		Context: req.GetContext(),
		Details: &storefrontv1.SubscribeShopperViewUpdatesRequest_Details{
			Recipient: &commonv1.ContactInfo{Name: req.GetStorefrontSession().GetRequester().GetName()},
			Payload:   packed,
		},
	}
	subscribeShopperViewUpdatesReply := new(storefrontv1.SubscribeShopperViewUpdatesResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", storefrontShopperViewNotificationServiceName, "SubscribeShopperViewUpdates"), subscribeShopperViewUpdatesRequest, subscribeShopperViewUpdatesReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.ShopperViewNotificationService/SubscribeShopperViewUpdates")
	}
	return &storefrontv1.RefreshStorefrontSessionResponse{
		Result: &commonv1.OperationResult{
			Details:       pauseLandingPageWorkflowReply.GetResult().GetDetails(),
			CorrelationId: listStorefrontSessionsReply.GetResult().GetCorrelationId(),
			Message:       listStorefrontSessionsReply.GetResult().GetMessage(),
			Status:        "ok",
		},
		StorefrontSession: &storefrontv1.StorefrontSession{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: subscribeShopperViewUpdatesReply.GetShopperView().GetAudit().GetUpdatedAt(),
			},
			StorefrontSessionId: req.GetStorefrontSessionId(),
			CustomFields:        req.GetStorefrontSession().GetCustomFields(),
		},
	}, nil
}

// SubmitStorefrontSessionForm submits a storefront session.
func (s *StorefrontSessionGatewayServer) SubmitStorefrontSessionForm(ctx context.Context, req *storefrontv1.SubmitStorefrontSessionFormRequest) (*storefrontv1.SubmitStorefrontSessionFormResponse, error) {
	if err := s.store.SaveStorefrontSession(ctx, req.GetContext().GetTenantId(), req.GetStorefrontSession()); err != nil {
		return nil, grpcx.Wrap(err, "save storefront session")
	}
	s.log.InfoContext(ctx, "submit storefront session form", "tenant", req.GetContext().GetTenantId(), "etag", req.GetStorefrontSession().GetAudit().GetEtag())
	deleteStorefrontSessionRequest := buildDeleteStorefrontSessionRequestForSubmitStorefrontSessionForm(req)
	deleteStorefrontSessionReply, err := s.storefrontSessionClient.DeleteStorefrontSession(ctx, deleteStorefrontSessionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.StorefrontSessionService/DeleteStorefrontSession")
	}
	exportLedgerAccountsStream, err := s.ledgerAccountQueryClient.ExportLedgerAccounts(ctx, &ledgerv1.ExportLedgerAccountsRequest{
		Context: req.GetContext(),
		Filter: &ledgerv1.ExportLedgerAccountsRequest_Filter{
			CreatedAfter: req.GetStorefrontSession().GetAudit().GetCreatedAt(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountQueryService/ExportLedgerAccounts")
	}
	for {
		received, err := exportLedgerAccountsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountQueryService/ExportLedgerAccounts")
		}
		_ = received
	}
	return &storefrontv1.SubmitStorefrontSessionFormResponse{
		Result: &commonv1.OperationResult{
			Status: deleteStorefrontSessionReply.GetResult().GetStatus(),
		},
		StorefrontSession: &storefrontv1.StorefrontSession{
			StorefrontSessionId: req.GetStorefrontSessionId(),
			Assignee:            req.GetStorefrontSession().GetAssignee(),
		},
	}, nil
}

// TrackStorefrontSession tracks a storefront session.
func (s *StorefrontSessionGatewayServer) TrackStorefrontSession(ctx context.Context, req *storefrontv1.TrackStorefrontSessionRequest) (*storefrontv1.TrackStorefrontSessionResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveStorefrontSession(ctx, req.GetContext().GetTenantId(), req.GetStorefrontSession()); err != nil {
		return nil, grpcx.Wrap(err, "save storefront session")
	}
	s.log.InfoContext(ctx, "track storefront session", "tenant", req.GetContext().GetTenantId(), "etag", req.GetStorefrontSession().GetAudit().GetEtag())
	streamPartnerWebhookMetricsStream, err := s.partnerWebhookReportingClient.StreamPartnerWebhookMetrics(ctx, &partnerapiv1.StreamPartnerWebhookMetricsRequest{
		Context: req.GetContext(),
		Subscriber: &commonv1.ContactInfo{
			Phone: req.GetStorefrontSession().GetRequester().GetPhone(),
			Name:  req.GetStorefrontSession().GetRequester().GetName(),
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
	clientHints := make(map[string]string, len(req.GetDetails().GetClientHints()))
	for key, value := range req.GetDetails().GetClientHints() {
		clientHints[key] = value
	}
	streamSynonymExportStream, err := s.synonymExportClient.StreamSynonymExport(ctx, &searchv1.StreamSynonymExportRequest{
		Context: req.GetContext(),
		Filter: &searchv1.StreamSynonymExportRequest_Filter{
			Labels:  clientHints,
			OwnerId: req.GetStorefrontSession().GetSla().GetPolicyId(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymExportService/StreamSynonymExport")
	}
	for {
		received2, err := streamSynonymExportStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.search.v1.SynonymExportService/StreamSynonymExport")
		}
		_ = received2
	}
	disconnectLandingPageSourceRequest := buildDisconnectLandingPageSourceRequestForTrackStorefrontSession(req)
	disconnectLandingPageSourceReply, err := s.landingPageIntegrationClient.DisconnectLandingPageSource(ctx, disconnectLandingPageSourceRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.LandingPageIntegrationService/DisconnectLandingPageSource")
	}
	return &storefrontv1.TrackStorefrontSessionResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: disconnectLandingPageSourceReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		StorefrontSession: &storefrontv1.StorefrontSession{
			StorefrontSessionId: req.GetStorefrontSessionId(),
			Assignee:            req.GetStorefrontSession().GetAssignee(),
		},
	}, nil
}

// StreamStorefrontSessionUpdates streams the changes of storefront sessions as they happen.
func (s *StorefrontSessionGatewayServer) StreamStorefrontSessionUpdates(req *storefrontv1.StreamStorefrontSessionUpdatesRequest, stream grpc.ServerStreamingServer[storefrontv1.StreamStorefrontSessionUpdatesResponse]) error {
	ctx := stream.Context()
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected StreamStorefrontSessionUpdates", "error", err)
		return err
	}
	s.log.InfoContext(ctx, "stream storefront session updates", "tenant", req.GetContext().GetTenantId())
	connectLandingPageSourceRequest := &storefrontv1.ConnectLandingPageSourceRequest{
		Context: req.GetContext(),
		Details: &storefrontv1.ConnectLandingPageSourceRequest_Details{ReceivedAt: req.GetFilter().GetCreatedAfter()},
	}
	connectLandingPageSourceReply, err := s.landingPageIntegrationLazy().ConnectLandingPageSource(ctx, connectLandingPageSourceRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.storefront.v1.LandingPageIntegrationService/ConnectLandingPageSource")
	}
	submitCustomerSegmentRequest := &customersv1.SubmitCustomerSegmentRequest{
		Context: req.GetContext(),
		CustomerSegment: &customersv1.CustomerSegment{
			Audit: &commonv1.AuditInfo{
				CreatedAt: connectLandingPageSourceReply.GetLandingPage().GetAudit().GetCreatedAt(),
				Etag:      connectLandingPageSourceReply.GetLandingPage().GetAudit().GetEtag(),
			},
			Owner: &identityv1.UserRef{
				DisplayName: connectLandingPageSourceReply.GetLandingPage().GetCounterparty().GetDisplayName(),
			},
		},
	}
	type submitCustomerSegmentResult struct {
		reply *customersv1.SubmitCustomerSegmentResponse
		err   error
	}
	submitCustomerSegmentResults := make(chan submitCustomerSegmentResult, 1)
	go func() {
		reply, err := s.customerSegmentApprovalClient.SubmitCustomerSegment(ctx, submitCustomerSegmentRequest)
		submitCustomerSegmentResults <- submitCustomerSegmentResult{reply, err}
	}()
	var submitCustomerSegmentReply *customersv1.SubmitCustomerSegmentResponse
	select {
	case result := <-submitCustomerSegmentResults:
		if result.err != nil {
			return grpcx.Wrap(result.err, "acme.customers.v1.CustomerSegmentApprovalService/SubmitCustomerSegment")
		}
		submitCustomerSegmentReply = result.reply
	case <-ctx.Done():
		return status.FromContextError(ctx.Err()).Err()
	}
	listPendingConsentRecordsReply, err := s.consentRecordApprovalClient.ListPendingConsentRecords(ctx, &customersv1.ListPendingConsentRecordsRequest{
		Context: req.GetContext(),
		Filter: &customersv1.ListPendingConsentRecordsRequest_Filter{
			Query:   req.GetFilter().GetQuery(),
			Labels:  req.GetFilter().GetLabels(),
			Region:  req.GetFilter().GetRegion(),
			OwnerId: req.GetFilter().GetOwnerId(),
		},
	})
	if err != nil {
		return grpcx.Wrap(err, "acme.customers.v1.ConsentRecordApprovalService/ListPendingConsentRecords")
	}
	if err := stream.Send(&storefrontv1.StreamStorefrontSessionUpdatesResponse{
		StorefrontSession: &storefrontv1.StorefrontSession{
			Assignee: &identityv1.UserRef{
				Email: connectLandingPageSourceReply.GetLandingPage().GetCounterparty().GetEmail(),
			},
			Priority: submitCustomerSegmentReply.GetCustomerSegment().GetPriority(),
			Audit: &commonv1.AuditInfo{
				UpdatedBy: submitCustomerSegmentReply.GetCustomerSegment().GetAudit().GetUpdatedBy(),
			},
			Requester: &commonv1.ContactInfo{
				Name:  req.GetSubscriber().GetName(),
				Phone: req.GetSubscriber().GetPhone(),
			},
		},
		Result: &commonv1.OperationResult{
			Status:        connectLandingPageSourceReply.GetResult().GetStatus(),
			CorrelationId: listPendingConsentRecordsReply.GetResult().GetCorrelationId(),
			Details:       listPendingConsentRecordsReply.GetResult().GetDetails(),
		},
	}); err != nil {
		return err
	}
	return nil
}

// SearchStorefrontSessionCatalog lists storefront sessions a page at a time.
func (s *StorefrontSessionGatewayServer) SearchStorefrontSessionCatalog(ctx context.Context, req *storefrontv1.SearchStorefrontSessionCatalogRequest) (*storefrontv1.SearchStorefrontSessionCatalogResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SearchStorefrontSessionCatalog", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "search storefront session catalog", "tenant", req.GetContext().GetTenantId())
	labels := make(map[string]string, len(req.GetFilter().GetLabels()))
	for key, value := range req.GetFilter().GetLabels() {
		labels[key] = value
	}
	pageCache := cache.New[string, *commonv1.PageRequest](1)
	pageCache.Put(req.GetContext().GetTraceId(), req.GetPage())
	cachedPage, _ := pageCache.Get(req.GetContext().GetTraceId())
	searchPartnerIntegrationCatalogRequest := &partnerapiv1.SearchPartnerIntegrationCatalogRequest{
		Context: req.GetContext(),
		Filter: &partnerapiv1.SearchPartnerIntegrationCatalogRequest_Filter{
			Labels:       labels,
			OwnerId:      req.GetFilter().GetOwnerId(),
			CreatedAfter: req.GetFilter().GetCreatedAfter(),
		},
		Page: &commonv1.PageRequest{PageToken: cachedPage.GetPageToken()},
	}
	searchPartnerIntegrationCatalogReply, err := partnerapiv1.PartnerIntegrationGatewayServiceClient.SearchPartnerIntegrationCatalog(s.partnerIntegrationGatewayClient, ctx, searchPartnerIntegrationCatalogRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerIntegrationGatewayService/SearchPartnerIntegrationCatalog")
	}
	return &storefrontv1.SearchStorefrontSessionCatalogResponse{
		PageInfo: &commonv1.PageInfo{
			TotalSize: searchPartnerIntegrationCatalogReply.GetPageInfo().GetTotalSize(),
		},
		Result: &commonv1.OperationResult{
			Details: searchPartnerIntegrationCatalogReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}

// GetStorefrontSessionSummary returns a storefront session.
func (s *StorefrontSessionGatewayServer) GetStorefrontSessionSummary(ctx context.Context, req *storefrontv1.GetStorefrontSessionSummaryRequest) (*storefrontv1.GetStorefrontSessionSummaryResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "storefront_session", req.GetStorefrontSessionId(), "get_storefront_session_summary"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get storefront session summary", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "email", req.GetContext().GetActor().GetEmail())
	retryCustomerSegmentImportReply, err := s.customerSegmentImportClient.RetryCustomerSegmentImport(ctx, &customersv1.RetryCustomerSegmentImportRequest{
		Context: req.GetContext(),
		CustomerSegment: &customersv1.CustomerSegment{
			Owner: &identityv1.UserRef{Email: tenancy.Value(ctx, "email")},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentImportService/RetryCustomerSegmentImport")
	}
	return &storefrontv1.GetStorefrontSessionSummaryResponse{
		StorefrontSession: &storefrontv1.StorefrontSession{
			Audit: &commonv1.AuditInfo{
				CreatedAt: retryCustomerSegmentImportReply.GetCustomerSegment().GetAudit().GetCreatedAt(),
			},
			Requester: &commonv1.ContactInfo{
				Email: retryCustomerSegmentImportReply.GetCustomerSegment().GetOwner().GetEmail(),
			},
			StorefrontSessionId: req.GetStorefrontSessionId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// signalShopperViewWorkflowSender returns a function that sends one request to
// acme.storefront.v1.ShopperViewWorkflowService/SignalShopperViewWorkflow.
func (s *StorefrontSessionGatewayServer) signalShopperViewWorkflowSender(request *storefrontv1.SignalShopperViewWorkflowRequest) func(context.Context) (*storefrontv1.SignalShopperViewWorkflowResponse, error) {
	return func(ctx context.Context) (*storefrontv1.SignalShopperViewWorkflowResponse, error) {
		return s.shopperViewWorkflowClient.SignalShopperViewWorkflow(ctx, request)
	}
}

// landingPageIntegrationLazy dials acme.storefront.v1.LandingPageIntegrationService the first time a handler needs it.
func (s *StorefrontSessionGatewayServer) landingPageIntegrationLazy() storefrontv1.LandingPageIntegrationServiceClient {
	s.landingPageIntegrationOnce.Do(func() { s.landingPageIntegrationCached = storefrontv1.NewLandingPageIntegrationServiceClient(s.conn) })
	return s.landingPageIntegrationCached
}
