package partnerapi

import (
	"context"
	"errors"
	"io"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	financeportalv1 "github.com/hounddogai/protomap-test-golang/gen/acme/financeportal/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	partnerapiv1 "github.com/hounddogai/protomap-test-golang/gen/acme/partnerapi/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	pricingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/pricing/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
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
	"google.golang.org/protobuf/types/known/anypb"
)

// PartnerIntegrationGatewayServer implements `acme.partnerapi.v1.PartnerIntegrationGatewayService`.
// The edge API that apps call for partner integrations.
type PartnerIntegrationGatewayServer struct {
	partnerapiv1.UnimplementedPartnerIntegrationGatewayServiceServer
	store                               *Store
	log                                 *slog.Logger
	conn                                *grpc.ClientConn
	partnerIntegrationClient            partnerapiv1.PartnerIntegrationServiceClient
	clients                             *registry.Registry
	partnerIntegrationIntegrationClient partnerapiv1.PartnerIntegrationIntegrationServiceClient
	updatePartnerIntegrationFn          func(context.Context, *partnerapiv1.UpdatePartnerIntegrationRequest, ...grpc.CallOption) (*partnerapiv1.UpdatePartnerIntegrationResponse, error)
	sellerListingQueryClient            sellersv1.SellerListingQueryServiceClient
	paymentWorkflowClient               paymentsv1.PaymentWorkflowServiceClient
	financeWorkspaceQueryClient         financeportalv1.FinanceWorkspaceQueryServiceClient
	sellerPayoutScheduleApprovalClient  sellersv1.SellerPayoutScheduleApprovalServiceClient
	partnerIntegrationQueryClient       partnerapiv1.PartnerIntegrationQueryServiceClient
	partnerWebhookWorkflowClient        partnerapiv1.PartnerWebhookWorkflowServiceClient
	subscriptionPlanWorkflowClient      subscriptionsv1.SubscriptionPlanWorkflowServiceClient
	partnerWebhookIntegrationClient     partnerapiv1.PartnerWebhookIntegrationServiceClient
	meteredSessionHistory               meteredSessionHistory
	meteredPartnerIntegrationWorkflow   meteredPartnerIntegrationWorkflow
	priceListWorkflowClient             pricingv1.PriceListWorkflowServiceClient
	partnerOrderWorkflowClient          partnerapiv1.PartnerOrderWorkflowServiceClient
	partnerOrderNotificationClient      partnerapiv1.PartnerOrderNotificationServiceClient
}

// NewPartnerIntegrationGatewayServer wires a PartnerIntegrationGatewayServer.
func NewPartnerIntegrationGatewayServer(deps platform.Deps) *PartnerIntegrationGatewayServer {
	partnerIntegration := partnerapiv1.NewPartnerIntegrationServiceClient(deps.Conn)
	return &PartnerIntegrationGatewayServer{
		store:                               NewStore(deps.DB),
		log:                                 deps.Logger.With("service", "acme.partnerapi.v1.PartnerIntegrationGatewayService"),
		conn:                                deps.Conn,
		partnerIntegrationClient:            partnerapiv1.NewPartnerIntegrationServiceClient(deps.Conn),
		clients:                             deps.Clients,
		partnerIntegrationIntegrationClient: partnerapiv1.NewPartnerIntegrationIntegrationServiceClient(deps.Conn),
		updatePartnerIntegrationFn:          partnerIntegration.UpdatePartnerIntegration,
		sellerListingQueryClient:            sellersv1.NewSellerListingQueryServiceClient(deps.Conn),
		paymentWorkflowClient:               paymentsv1.NewPaymentWorkflowServiceClient(deps.Conn),
		financeWorkspaceQueryClient:         financeportalv1.NewFinanceWorkspaceQueryServiceClient(deps.Conn),
		sellerPayoutScheduleApprovalClient:  sellersv1.NewSellerPayoutScheduleApprovalServiceClient(deps.Conn),
		partnerIntegrationQueryClient:       partnerapiv1.NewPartnerIntegrationQueryServiceClient(deps.Conn),
		partnerWebhookWorkflowClient:        partnerapiv1.NewPartnerWebhookWorkflowServiceClient(deps.Conn),
		subscriptionPlanWorkflowClient:      subscriptionsv1.NewSubscriptionPlanWorkflowServiceClient(deps.Conn),
		partnerWebhookIntegrationClient:     partnerapiv1.NewPartnerWebhookIntegrationServiceClient(deps.Conn),
		meteredSessionHistory:               meteredSessionHistory{SessionHistoryServiceClient: identityv1.NewSessionHistoryServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		meteredPartnerIntegrationWorkflow:   meteredPartnerIntegrationWorkflow{PartnerIntegrationWorkflowServiceClient: partnerapiv1.NewPartnerIntegrationWorkflowServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		priceListWorkflowClient:             pricingv1.NewPriceListWorkflowServiceClient(deps.Conn),
		partnerOrderWorkflowClient:          partnerapiv1.NewPartnerOrderWorkflowServiceClient(deps.Conn),
		partnerOrderNotificationClient:      partnerapiv1.NewPartnerOrderNotificationServiceClient(deps.Conn),
	}
}

// FetchPartnerIntegrationView fetchs a partner integration.
func (s *PartnerIntegrationGatewayServer) FetchPartnerIntegrationView(ctx context.Context, req *partnerapiv1.FetchPartnerIntegrationViewRequest) (*partnerapiv1.FetchPartnerIntegrationViewResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePartnerIntegration(ctx, req.GetContext().GetTenantId(), req.GetPartnerIntegration()); err != nil {
		return nil, grpcx.Wrap(err, "save partner integration")
	}
	s.log.InfoContext(ctx, "fetch partner integration view", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPartnerIntegration().GetAudit().GetEtag())
	packed, err := anypb.New(req.GetPartnerIntegration())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	subscribeSynonymUpdatesRequest := &searchv1.SubscribeSynonymUpdatesRequest{
		Context: req.GetContext(),
		Details: &searchv1.SubscribeSynonymUpdatesRequest_Details{
			Recipient: &commonv1.ContactInfo{
				Phone:            req.GetPartnerIntegration().GetContact().GetPhone(),
				PreferredChannel: req.GetPartnerIntegration().GetContact().GetPreferredChannel(),
				Email:            req.GetPartnerIntegration().GetContact().GetEmail(),
			},
			Payload: packed,
		},
	}
	subscribeSynonymUpdatesReply := new(searchv1.SubscribeSynonymUpdatesResponse)
	if err := grpcx.LegacyInvoke(ctx, s.conn, "/acme.search.v1.SynonymNotificationService/SubscribeSynonymUpdates", subscribeSynonymUpdatesRequest, subscribeSynonymUpdatesReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymNotificationService/SubscribeSynonymUpdates")
	}
	labels := make(map[string]string, len(req.GetPartnerIntegration().GetLabels()))
	for key, value := range req.GetPartnerIntegration().GetLabels() {
		labels[key] = value
	}
	listPartnerIntegrationsReply, err := s.partnerIntegrationClient.ListPartnerIntegrations(ctx, &partnerapiv1.ListPartnerIntegrationsRequest{
		Context: req.GetContext(),
		Filter: &partnerapiv1.ListPartnerIntegrationsRequest_Filter{
			Labels:       labels,
			CreatedAfter: subscribeSynonymUpdatesReply.GetSynonym().GetAudit().GetUpdatedAt(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerIntegrationService/ListPartnerIntegrations")
	}
	partnerWebhookApproval, err := registry.Lookup[partnerapiv1.PartnerWebhookApprovalServiceClient](s.clients, "acme.partnerapi.v1.PartnerWebhookApprovalService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	packed2, err := anypb.New(req.GetPartnerIntegration())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	delegatePartnerWebhookApprovalRequest := &partnerapiv1.DelegatePartnerWebhookApprovalRequest{
		Context: req.GetContext(),
		PartnerWebhook: &partnerapiv1.PartnerWebhook{
			Subject: &partnerapiv1.PartnerIntegrationRef{
				DisplayName: subscribeSynonymUpdatesReply.GetSynonym().GetOwner().GetDisplayName(),
			},
			Detail: packed2,
		},
		Details: &partnerapiv1.DelegatePartnerWebhookApprovalRequest_Details{
			Approver: &identityv1.UserRef{
				Email: subscribeSynonymUpdatesReply.GetSynonym().GetOwner().GetEmail(),
			},
		},
	}
	delegatePartnerWebhookApprovalReply, err := partnerWebhookApproval.DelegatePartnerWebhookApproval(ctx, delegatePartnerWebhookApprovalRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookApprovalService/DelegatePartnerWebhookApproval")
	}
	testPartnerIntegrationConnectionRequest := &partnerapiv1.TestPartnerIntegrationConnectionRequest{
		Context:              req.GetContext(),
		PartnerIntegrationId: req.GetPartnerIntegrationId(),
		Reason:               subscribeSynonymUpdatesReply.GetSynonym().GetBlock().GetReason(),
		Details: &partnerapiv1.TestPartnerIntegrationConnectionRequest_Details{
			MessageId: delegatePartnerWebhookApprovalReply.GetResult().GetMessage(),
		},
	}
	testPartnerIntegrationConnectionReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*partnerapiv1.TestPartnerIntegrationConnectionResponse, error) {
		return s.partnerIntegrationIntegrationClient.TestPartnerIntegrationConnection(ctx, testPartnerIntegrationConnectionRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerIntegrationIntegrationService/TestPartnerIntegrationConnection")
	}
	return &partnerapiv1.FetchPartnerIntegrationViewResponse{
		PartnerIntegration: &partnerapiv1.PartnerIntegration{
			Audit: &commonv1.AuditInfo{
				CreatedAt: subscribeSynonymUpdatesReply.GetSynonym().GetAudit().GetCreatedAt(),
			},
			Contact: &commonv1.ContactInfo{
				PreferredChannel: testPartnerIntegrationConnectionReply.GetPartnerIntegration().GetContact().GetPreferredChannel(),
				Phone:            testPartnerIntegrationConnectionReply.GetPartnerIntegration().GetContact().GetPhone(),
			},
			PartnerIntegrationId: req.GetPartnerIntegrationId(),
			Status:               req.GetPartnerIntegration().GetStatus(),
		},
		Result: &commonv1.OperationResult{
			Details:      subscribeSynonymUpdatesReply.GetResult().GetDetails(),
			Status:       listPartnerIntegrationsReply.GetResult().GetStatus(),
			ReferenceIds: delegatePartnerWebhookApprovalReply.GetResult().GetReferenceIds(),
		},
	}, nil
}

// ComposePartnerIntegrationPage composes a partner integration.
func (s *PartnerIntegrationGatewayServer) ComposePartnerIntegrationPage(ctx context.Context, req *partnerapiv1.ComposePartnerIntegrationPageRequest) (*partnerapiv1.ComposePartnerIntegrationPageResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SavePartnerIntegration(ctx, req.GetContext().GetTenantId(), req.GetPartnerIntegration()); err != nil {
		return nil, grpcx.Wrap(err, "save partner integration")
	}
	s.log.InfoContext(ctx, "compose partner integration page", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPartnerIntegration().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetPartnerIntegration()).(*partnerapiv1.PartnerIntegration)
	updatePartnerIntegrationRequest := &partnerapiv1.UpdatePartnerIntegrationRequest{
		Context: req.GetContext(),
		PartnerIntegration: &partnerapiv1.PartnerIntegration{
			Funding: &partnerapiv1.PartnerIntegration_Card{Card: &partnerapiv1.PartnerIntegrationCardFunding{
				HolderName: req.GetPartnerIntegration().GetCard().GetHolderName(),
				Token:      req.GetPartnerIntegration().GetCard().GetToken(),
			}},
			Holder: &partnerapiv1.PartnerWebhookRef{
				PartnerWebhookId: req.GetPartnerIntegration().GetHolder().GetPartnerWebhookId(),
			},
			Audit: &commonv1.AuditInfo{CreatedAt: cloned.GetAudit().GetCreatedAt()},
		},
	}
	updatePartnerIntegrationReply, err := s.updatePartnerIntegrationFn(ctx, updatePartnerIntegrationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerIntegrationService/UpdatePartnerIntegration")
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	generatePartnerWebhookReportRequest := &partnerapiv1.GeneratePartnerWebhookReportRequest{
		Context:          req.GetContext(),
		PartnerWebhookId: updatePartnerIntegrationReply.GetPartnerIntegration().GetHolder().GetPartnerWebhookId(),
		Reason:           req.GetReason(),
		Annotations:      annotations,
	}
	generatePartnerWebhookReportReply := new(partnerapiv1.GeneratePartnerWebhookReportResponse)
	if err := grpcx.Invoke(ctx, s.conn, partnerapiv1.PartnerWebhookReportingService_GeneratePartnerWebhookReport_FullMethodName, generatePartnerWebhookReportRequest, generatePartnerWebhookReportReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookReportingService/GeneratePartnerWebhookReport")
	}
	findSellerListingsRequest := &sellersv1.FindSellerListingsRequest{}
	findSellerListingsRequest.Context = req.GetContext()
	findSellerListingsRequest.Filter = &sellersv1.FindSellerListingsRequest_Filter{
		Labels:       updatePartnerIntegrationReply.GetPartnerIntegration().GetLabels(),
		CreatedAfter: updatePartnerIntegrationReply.GetPartnerIntegration().GetAudit().GetUpdatedAt(),
	}
	findSellerListingsReply, err := s.sellerListingQueryClient.FindSellerListings(ctx, findSellerListingsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingQueryService/FindSellerListings")
	}
	return &partnerapiv1.ComposePartnerIntegrationPageResponse{
		PartnerIntegration: &partnerapiv1.PartnerIntegration{
			Available:            updatePartnerIntegrationReply.GetPartnerIntegration().GetAvailable(),
			PartnerIntegrationId: req.GetPartnerIntegrationId(),
			Balance:              req.GetPartnerIntegration().GetBalance(),
		},
		Result: &commonv1.OperationResult{
			Message:      generatePartnerWebhookReportReply.GetResult().GetMessage(),
			Status:       findSellerListingsReply.GetResult().GetStatus(),
			ReferenceIds: findSellerListingsReply.GetResult().GetReferenceIds(),
		},
	}, nil
}

// RefreshPartnerIntegration refreshs a partner integration.
func (s *PartnerIntegrationGatewayServer) RefreshPartnerIntegration(ctx context.Context, req *partnerapiv1.RefreshPartnerIntegrationRequest) (*partnerapiv1.RefreshPartnerIntegrationResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RefreshPartnerIntegration", "error", err)
		return nil, err
	}
	if err := s.store.SavePartnerIntegration(ctx, req.GetContext().GetTenantId(), req.GetPartnerIntegration()); err != nil {
		return nil, grpcx.Wrap(err, "save partner integration")
	}
	s.log.InfoContext(ctx, "refresh partner integration", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPartnerIntegration().GetAudit().GetEtag())
	resumePaymentWorkflowRequest := &paymentsv1.ResumePaymentWorkflowRequest{
		Context: req.GetContext(),
		Payment: &paymentsv1.Payment{
			Audit: &commonv1.AuditInfo{
				CreatedAt: req.GetPartnerIntegration().GetAudit().GetCreatedAt(),
			},
			Settlement: &paymentsv1.Payment_Card{Card: &paymentsv1.PaymentCardSettlement{Token: req.GetPartnerIntegration().GetCard().GetToken()}},
		},
		Details: &paymentsv1.ResumePaymentWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Phone: req.GetPartnerIntegration().GetContact().GetPhone()},
		},
	}
	var resumePaymentWorkflowReply *paymentsv1.ResumePaymentWorkflowResponse
	if err := pipeline.Run(ctx, "acme.payments.v1.PaymentWorkflowService/ResumePaymentWorkflow", func(ctx context.Context) error {
		answer, err := s.paymentWorkflowClient.ResumePaymentWorkflow(ctx, resumePaymentWorkflowRequest)
		resumePaymentWorkflowReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentWorkflowService/ResumePaymentWorkflow")
	}
	resolveFinanceWorkspacesReply, err := s.financeWorkspaceQueryClient.ResolveFinanceWorkspaces(ctx, &financeportalv1.ResolveFinanceWorkspacesRequest{Context: req.GetContext()})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.financeportal.v1.FinanceWorkspaceQueryService/ResolveFinanceWorkspaces")
	}
	var network string
	switch choice := req.GetPartnerIntegration().GetFunding().(type) {
	case *partnerapiv1.PartnerIntegration_Card:
		network = choice.Card.GetNetwork()
	case *partnerapiv1.PartnerIntegration_Bank:
		network = choice.Bank.GetIban()
	case *partnerapiv1.PartnerIntegration_ExternalAccountId:
		network = choice.ExternalAccountId
	}
	advancePaymentWorkflowRequest := &paymentsv1.AdvancePaymentWorkflowRequest{
		Context: req.GetContext(),
		Details: &paymentsv1.AdvancePaymentWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Name: req.GetPartnerIntegration().GetContact().GetName()},
		},
		Payment: &paymentsv1.Payment{
			Audit: &commonv1.AuditInfo{
				CreatedBy: resumePaymentWorkflowReply.GetPayment().GetAudit().GetCreatedBy(),
			},
			BillingAddress: &commonv1.PostalAddress{
				CountryCode: resumePaymentWorkflowReply.GetPayment().GetBillingAddress().GetCountryCode(),
			},
			Settlement: &paymentsv1.Payment_Card{Card: &paymentsv1.PaymentCardSettlement{Network: network}},
		},
	}
	type advancePaymentWorkflowResult struct {
		reply *paymentsv1.AdvancePaymentWorkflowResponse
		err   error
	}
	advancePaymentWorkflowResults := make(chan advancePaymentWorkflowResult, 1)
	go func() {
		reply, err := s.paymentWorkflowClient.AdvancePaymentWorkflow(ctx, advancePaymentWorkflowRequest)
		advancePaymentWorkflowResults <- advancePaymentWorkflowResult{reply, err}
	}()
	var advancePaymentWorkflowReply *paymentsv1.AdvancePaymentWorkflowResponse
	select {
	case result := <-advancePaymentWorkflowResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.payments.v1.PaymentWorkflowService/AdvancePaymentWorkflow")
		}
		advancePaymentWorkflowReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &partnerapiv1.RefreshPartnerIntegrationResponse{
		Result: &commonv1.OperationResult{
			Details: resumePaymentWorkflowReply.GetResult().GetDetails(),
			Message: resolveFinanceWorkspacesReply.GetResult().GetMessage(),
			Status:  resolveFinanceWorkspacesReply.GetResult().GetStatus(),
		},
		PartnerIntegration: &partnerapiv1.PartnerIntegration{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: advancePaymentWorkflowReply.GetPayment().GetAudit().GetUpdatedBy(),
			},
			PartnerIntegrationId: req.GetPartnerIntegrationId(),
			Holder:               req.GetPartnerIntegration().GetHolder(),
		},
	}, nil
}

// SubmitPartnerIntegrationForm submits a partner integration.
func (s *PartnerIntegrationGatewayServer) SubmitPartnerIntegrationForm(ctx context.Context, req *partnerapiv1.SubmitPartnerIntegrationFormRequest) (*partnerapiv1.SubmitPartnerIntegrationFormResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SubmitPartnerIntegrationForm", "error", err)
		return nil, err
	}
	if err := s.store.SavePartnerIntegration(ctx, req.GetContext().GetTenantId(), req.GetPartnerIntegration()); err != nil {
		return nil, grpcx.Wrap(err, "save partner integration")
	}
	s.log.InfoContext(ctx, "submit partner integration form", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPartnerIntegration().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "etag", req.GetPartnerIntegration().GetAudit().GetEtag())
	deletePartnerIntegrationReply, err := s.partnerIntegrationClient.DeletePartnerIntegration(ctx, &partnerapiv1.DeletePartnerIntegrationRequest{
		Context:              req.GetContext(),
		PartnerIntegrationId: req.GetPartnerIntegration().GetPartnerIntegrationId(),
		Reason:               req.GetReason(),
		Etag:                 tenancy.Value(ctx, "etag"),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerIntegrationService/DeletePartnerIntegration")
	}
	labels := make(map[string]string, len(req.GetPartnerIntegration().GetLabels()))
	for key, value := range req.GetPartnerIntegration().GetLabels() {
		labels[key] = value
	}
	listPendingSellerPayoutSchedulesRequest := &sellersv1.ListPendingSellerPayoutSchedulesRequest{
		Context: req.GetContext(),
		Filter:  &sellersv1.ListPendingSellerPayoutSchedulesRequest_Filter{Labels: labels},
	}
	listPendingSellerPayoutSchedulesReply, err := grpcx.Call(ctx, s.sellerPayoutScheduleApprovalClient.ListPendingSellerPayoutSchedules, listPendingSellerPayoutSchedulesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerPayoutScheduleApprovalService/ListPendingSellerPayoutSchedules")
	}
	lookupPartnerIntegrationReply, err := s.partnerIntegrationQueryClient.LookupPartnerIntegration(ctx, &partnerapiv1.LookupPartnerIntegrationRequest{
		Context:              req.GetContext(),
		PartnerIntegrationId: req.GetPartnerIntegration().GetPartnerIntegrationId(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerIntegrationQueryService/LookupPartnerIntegration")
	}
	return &partnerapiv1.SubmitPartnerIntegrationFormResponse{
		Result: &commonv1.OperationResult{
			Status:  deletePartnerIntegrationReply.GetResult().GetStatus(),
			Details: deletePartnerIntegrationReply.GetResult().GetDetails(),
			Message: listPendingSellerPayoutSchedulesReply.GetResult().GetMessage(),
		},
		PartnerIntegration: &partnerapiv1.PartnerIntegration{
			Contact:              lookupPartnerIntegrationReply.GetPartnerIntegration().GetContact(),
			PartnerIntegrationId: req.GetPartnerIntegrationId(),
			Available:            req.GetPartnerIntegration().GetAvailable(),
		},
	}, nil
}

// TrackPartnerIntegration tracks a partner integration.
func (s *PartnerIntegrationGatewayServer) TrackPartnerIntegration(ctx context.Context, req *partnerapiv1.TrackPartnerIntegrationRequest) (*partnerapiv1.TrackPartnerIntegrationResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePartnerIntegration(ctx, req.GetContext().GetTenantId(), req.GetPartnerIntegration()); err != nil {
		return nil, grpcx.Wrap(err, "save partner integration")
	}
	s.log.InfoContext(ctx, "track partner integration", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPartnerIntegration().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "preferred_channel", req.GetPartnerIntegration().GetContact().GetPreferredChannel())
	ctx = tenancy.With(ctx, "currency_code", req.GetPartnerIntegration().GetCurrencyCode())
	packed, err := anypb.New(req.GetPartnerIntegration())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	advancePartnerWebhookWorkflowRequest := &partnerapiv1.AdvancePartnerWebhookWorkflowRequest{
		Context: req.GetContext(),
		PartnerWebhook: &partnerapiv1.PartnerWebhook{
			Subject: &partnerapiv1.PartnerIntegrationRef{
				DisplayName:  req.GetPartnerIntegration().GetHolder().GetDisplayName(),
				CurrencyCode: tenancy.Value(ctx, "currency_code"),
			},
			Detail: packed,
		},
		Details: &partnerapiv1.AdvancePartnerWebhookWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{PreferredChannel: tenancy.Value(ctx, "preferred_channel")},
		},
	}
	advancePartnerWebhookWorkflowReply, err := grpcx.Call(ctx, s.partnerWebhookWorkflowClient.AdvancePartnerWebhookWorkflow, advancePartnerWebhookWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookWorkflowService/AdvancePartnerWebhookWorkflow")
	}
	encoded, err := protojson.Marshal(req.GetPartnerIntegration())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded partnerapiv1.PartnerIntegration
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	pauseSubscriptionPlanWorkflowReply, err := s.subscriptionPlanWorkflowClient.PauseSubscriptionPlanWorkflow(ctx, &subscriptionsv1.PauseSubscriptionPlanWorkflowRequest{
		Context: req.GetContext(),
		SubscriptionPlan: &subscriptionsv1.SubscriptionPlan{
			Owner: &identityv1.UserRef{Email: req.GetPartnerIntegration().GetContact().GetEmail()},
			Audit: &commonv1.AuditInfo{CreatedBy: (&decoded).GetAudit().GetCreatedBy()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionPlanWorkflowService/PauseSubscriptionPlanWorkflow")
	}
	return &partnerapiv1.TrackPartnerIntegrationResponse{
		PartnerIntegration: &partnerapiv1.PartnerIntegration{
			Holder: &partnerapiv1.PartnerWebhookRef{
				DisplayName: advancePartnerWebhookWorkflowReply.GetPartnerWebhook().GetSubject().GetDisplayName(),
			},
			PartnerIntegrationId: req.GetPartnerIntegrationId(),
			Status:               req.GetPartnerIntegration().GetStatus(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: advancePartnerWebhookWorkflowReply.GetResult().GetCorrelationId(),
			Message:       pauseSubscriptionPlanWorkflowReply.GetResult().GetMessage(),
			Status:        "ok",
		},
	}, nil
}

// StreamPartnerIntegrationUpdates streams the changes of partner integrations as they happen.
func (s *PartnerIntegrationGatewayServer) StreamPartnerIntegrationUpdates(req *partnerapiv1.StreamPartnerIntegrationUpdatesRequest, stream grpc.ServerStreamingServer[partnerapiv1.StreamPartnerIntegrationUpdatesResponse]) error {
	ctx := stream.Context()
	if req.GetContext().GetTenantId() == "" {
		return status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "stream partner integration updates", "tenant", req.GetContext().GetTenantId())
	listPartnerWebhookWorkflowsRequest := &partnerapiv1.ListPartnerWebhookWorkflowsRequest{
		Context: req.GetContext(),
		Filter: &partnerapiv1.ListPartnerWebhookWorkflowsRequest_Filter{
			CreatedAfter: req.GetFilter().GetCreatedAfter(),
			Labels:       req.GetFilter().GetLabels(),
		},
	}
	listPartnerWebhookWorkflowsReply, err := listPartnerWebhookWorkflowsCall(ctx, listPartnerWebhookWorkflowsRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookWorkflowService/ListPartnerWebhookWorkflows")
	}
	disconnectPartnerWebhookSourceReply, err := s.partnerWebhookIntegrationClient.DisconnectPartnerWebhookSource(ctx, &partnerapiv1.DisconnectPartnerWebhookSourceRequest{
		Context: req.GetContext(),
		Details: &partnerapiv1.DisconnectPartnerWebhookSourceRequest_Details{
			MessageId:  listPartnerWebhookWorkflowsReply.GetResult().GetMessage(),
			ReceivedAt: req.GetFilter().GetCreatedAfter(),
		},
		Annotations: req.GetFilter().GetLabels(),
	})
	if err != nil {
		return grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookIntegrationService/DisconnectPartnerWebhookSource")
	}
	if err := stream.Send(&partnerapiv1.StreamPartnerIntegrationUpdatesResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: listPartnerWebhookWorkflowsReply.GetResult().GetCorrelationId(),
			Status:        listPartnerWebhookWorkflowsReply.GetResult().GetStatus(),
		},
		PartnerIntegration: &partnerapiv1.PartnerIntegration{
			Holder: &partnerapiv1.PartnerWebhookRef{
				PartnerWebhookId: disconnectPartnerWebhookSourceReply.GetPartnerWebhook().GetPartnerWebhookId(),
			},
			Contact: &commonv1.ContactInfo{Name: req.GetSubscriber().GetName()},
			Labels:  req.GetFilter().GetLabels(),
		},
	}); err != nil {
		return err
	}
	return nil
}

// UploadPartnerIntegrationAttachments takes a stream of partner integrations and answers once at the end.
func (s *PartnerIntegrationGatewayServer) UploadPartnerIntegrationAttachments(stream grpc.ClientStreamingServer[partnerapiv1.UploadPartnerIntegrationAttachmentsRequest, partnerapiv1.UploadPartnerIntegrationAttachmentsResponse]) error {
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
		s.log.InfoContext(ctx, "upload partner integration attachments", "tenant", msg.GetContext().GetTenantId())
		compareSessionVersionsRequest := &identityv1.CompareSessionVersionsRequest{
			Context: msg.GetContext(),
			Session: &identityv1.Session{
				Audit: &commonv1.AuditInfo{
					UpdatedAt: msg.GetRecord().GetAudit().GetUpdatedAt(),
					CreatedAt: msg.GetRecord().GetAudit().GetCreatedAt(),
					UpdatedBy: msg.GetRecord().GetAudit().GetUpdatedBy(),
				},
				Requester: &commonv1.ContactInfo{Name: msg.GetHeader().GetSubmittedBy().GetName()},
			},
		}
		if _, err := s.meteredSessionHistory.CompareSessionVersions(ctx, compareSessionVersionsRequest); err != nil {
			return grpcx.Wrap(err, "acme.identity.v1.SessionHistoryService/CompareSessionVersions")
		}
		cancelPartnerIntegrationWorkflowRequest := &partnerapiv1.CancelPartnerIntegrationWorkflowRequest{
			Context: msg.GetContext(),
			PartnerIntegration: &partnerapiv1.PartnerIntegration{
				Contact: &commonv1.ContactInfo{
					Email: msg.GetHeader().GetSubmittedBy().GetEmail(),
					Name:  msg.GetRecord().GetContact().GetName(),
				},
				Holder: &partnerapiv1.PartnerWebhookRef{
					PartnerWebhookId: msg.GetRecord().GetHolder().GetPartnerWebhookId(),
					DisplayName:      msg.GetRecord().GetHolder().GetDisplayName(),
				},
			},
		}
		if _, err := s.meteredPartnerIntegrationWorkflow.CancelPartnerIntegrationWorkflow(ctx, cancelPartnerIntegrationWorkflowRequest); err != nil {
			return grpcx.Wrap(err, "acme.partnerapi.v1.PartnerIntegrationWorkflowService/CancelPartnerIntegrationWorkflow")
		}
		if _, err := s.priceListWorkflowClient.RetryPriceListWorkflow(ctx, &pricingv1.RetryPriceListWorkflowRequest{Context: msg.GetContext()}); err != nil {
			return grpcx.Wrap(err, "acme.pricing.v1.PriceListWorkflowService/RetryPriceListWorkflow")
		}
		accepted++
	}
	return stream.SendAndClose(&partnerapiv1.UploadPartnerIntegrationAttachmentsResponse{Accepted: accepted})
}

// SearchPartnerIntegrationCatalog lists partner integrations a page at a time.
func (s *PartnerIntegrationGatewayServer) SearchPartnerIntegrationCatalog(ctx context.Context, req *partnerapiv1.SearchPartnerIntegrationCatalogRequest) (*partnerapiv1.SearchPartnerIntegrationCatalogResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "search partner integration catalog", "tenant", req.GetContext().GetTenantId())
	filterCache := cache.New[string, *partnerapiv1.SearchPartnerIntegrationCatalogRequest_Filter](1)
	filterCache.Put(req.GetContext().GetTraceId(), req.GetFilter())
	cachedFilter, _ := filterCache.Get(req.GetContext().GetTraceId())
	scorePartnerWebhookRequest := &partnerapiv1.ScorePartnerWebhookRequest{
		Context: req.GetContext(),
		Details: &partnerapiv1.ScorePartnerWebhookRequest_Details{
			Destination: &commonv1.PostalAddress{Region: cachedFilter.GetRegion()},
		},
		PartnerWebhook: &partnerapiv1.PartnerWebhook{
			Measurements: []*partnerapiv1.PartnerWebhookMeasurement{&partnerapiv1.PartnerWebhookMeasurement{ObservedAt: req.GetFilter().GetCreatedAfter()}},
			Source:       req.GetFilter().GetOwnerId(),
		},
	}
	scorePartnerWebhookReply, err := scorePartnerWebhookCall(ctx, scorePartnerWebhookRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookValidationService/ScorePartnerWebhook")
	}
	return &partnerapiv1.SearchPartnerIntegrationCatalogResponse{
		PartnerIntegrations: []*partnerapiv1.PartnerIntegration{&partnerapiv1.PartnerIntegration{
			PartnerIntegrationId: scorePartnerWebhookReply.GetPartnerWebhook().GetSubject().GetPartnerIntegrationId(),
		}},
		Result: &commonv1.OperationResult{
			CorrelationId: scorePartnerWebhookReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// GetPartnerIntegrationSummary returns a partner integration.
func (s *PartnerIntegrationGatewayServer) GetPartnerIntegrationSummary(ctx context.Context, req *partnerapiv1.GetPartnerIntegrationSummaryRequest) (*partnerapiv1.GetPartnerIntegrationSummaryResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetPartnerIntegrationSummary", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "partner_integration", req.GetPartnerIntegrationId(), "get_partner_integration_summary"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get partner integration summary", "tenant", req.GetContext().GetTenantId())
	retryPartnerOrderWorkflowRequest := &partnerapiv1.RetryPartnerOrderWorkflowRequest{
		Context: req.GetContext(),
		Details: &partnerapiv1.RetryPartnerOrderWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
		PartnerOrder: &partnerapiv1.PartnerOrder{
			Audit: &commonv1.AuditInfo{CreatedAt: req.GetContext().GetDeadline()},
		},
	}
	retryPartnerOrderWorkflowReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*partnerapiv1.RetryPartnerOrderWorkflowResponse, error) {
		return s.partnerOrderWorkflowClient.RetryPartnerOrderWorkflow(ctx, retryPartnerOrderWorkflowRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerOrderWorkflowService/RetryPartnerOrderWorkflow")
	}
	return &partnerapiv1.GetPartnerIntegrationSummaryResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: retryPartnerOrderWorkflowReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		PartnerIntegration: &partnerapiv1.PartnerIntegration{PartnerIntegrationId: req.GetPartnerIntegrationId()},
	}, nil
}

// StartPartnerIntegrationCheckout starts a partner integration.
func (s *PartnerIntegrationGatewayServer) StartPartnerIntegrationCheckout(ctx context.Context, req *partnerapiv1.StartPartnerIntegrationCheckoutRequest) (*partnerapiv1.StartPartnerIntegrationCheckoutResponse, error) {
	if err := s.store.SavePartnerIntegration(ctx, req.GetContext().GetTenantId(), req.GetPartnerIntegration()); err != nil {
		return nil, grpcx.Wrap(err, "save partner integration")
	}
	s.log.InfoContext(ctx, "start partner integration checkout", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPartnerIntegration().GetAudit().GetEtag())
	encoded, err := protojson.Marshal(req.GetPartnerIntegration())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded partnerapiv1.PartnerIntegration
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	signalPartnerOrderWorkflowRequest := &partnerapiv1.SignalPartnerOrderWorkflowRequest{
		Context: req.GetContext(),
		Details: &partnerapiv1.SignalPartnerOrderWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Phone: req.GetPartnerIntegration().GetContact().GetPhone()},
		},
		PartnerOrder: &partnerapiv1.PartnerOrder{
			Audit: &commonv1.AuditInfo{CreatedAt: (&decoded).GetAudit().GetCreatedAt()},
		},
	}
	var signalPartnerOrderWorkflowReply *partnerapiv1.SignalPartnerOrderWorkflowResponse
	if err := pipeline.Run(ctx, "acme.partnerapi.v1.PartnerOrderWorkflowService/SignalPartnerOrderWorkflow", func(ctx context.Context) error {
		answer, err := s.partnerOrderWorkflowClient.SignalPartnerOrderWorkflow(ctx, signalPartnerOrderWorkflowRequest)
		signalPartnerOrderWorkflowReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerOrderWorkflowService/SignalPartnerOrderWorkflow")
	}
	packed, err := anypb.New(req.GetPartnerIntegration())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	cancelPartnerWebhookWorkflowRequest := &partnerapiv1.CancelPartnerWebhookWorkflowRequest{
		Context: req.GetContext(),
		Details: &partnerapiv1.CancelPartnerWebhookWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				Email:            req.GetPartnerIntegration().GetContact().GetEmail(),
				PreferredChannel: req.GetDetails().GetNotify().GetPreferredChannel(),
				Phone:            req.GetPartnerIntegration().GetContact().GetPhone(),
			},
		},
		PartnerWebhook: &partnerapiv1.PartnerWebhook{Detail: packed},
	}
	type cancelPartnerWebhookWorkflowResult struct {
		reply *partnerapiv1.CancelPartnerWebhookWorkflowResponse
		err   error
	}
	cancelPartnerWebhookWorkflowResults := make(chan cancelPartnerWebhookWorkflowResult, 1)
	go func() {
		reply, err := s.partnerWebhookWorkflowClient.CancelPartnerWebhookWorkflow(ctx, cancelPartnerWebhookWorkflowRequest)
		cancelPartnerWebhookWorkflowResults <- cancelPartnerWebhookWorkflowResult{reply, err}
	}()
	var cancelPartnerWebhookWorkflowReply *partnerapiv1.CancelPartnerWebhookWorkflowResponse
	select {
	case result := <-cancelPartnerWebhookWorkflowResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.partnerapi.v1.PartnerWebhookWorkflowService/CancelPartnerWebhookWorkflow")
		}
		cancelPartnerWebhookWorkflowReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	advancePartnerOrderWorkflowRequest := buildAdvancePartnerOrderWorkflowRequestForStartPartnerIntegrationCheckout(req, signalPartnerOrderWorkflowReply)
	advancePartnerOrderWorkflowReply, err := s.partnerOrderWorkflowClient.AdvancePartnerOrderWorkflow(ctx, advancePartnerOrderWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerOrderWorkflowService/AdvancePartnerOrderWorkflow")
	}
	listPartnerOrderSubscriptionsRequest := &partnerapiv1.ListPartnerOrderSubscriptionsRequest{
		Context: req.GetContext(),
		Filter: &partnerapiv1.ListPartnerOrderSubscriptionsRequest_Filter{
			Labels:       req.GetPartnerIntegration().GetLabels(),
			Region:       advancePartnerOrderWorkflowReply.GetPartnerOrder().GetBillingAddress().GetRegion(),
			CreatedAfter: signalPartnerOrderWorkflowReply.GetPartnerOrder().GetAudit().GetUpdatedAt(),
			OwnerId:      signalPartnerOrderWorkflowReply.GetPartnerOrder().GetCounterparty().GetSellerId(),
		},
	}
	listPartnerOrderSubscriptionsReply, err := grpcx.Call(ctx, s.partnerOrderNotificationClient.ListPartnerOrderSubscriptions, listPartnerOrderSubscriptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerOrderNotificationService/ListPartnerOrderSubscriptions")
	}
	_ = listPartnerOrderSubscriptionsReply
	return &partnerapiv1.StartPartnerIntegrationCheckoutResponse{
		Outcome: &partnerapiv1.StartPartnerIntegrationCheckoutResponse_Outcome{
			Status:      signalPartnerOrderWorkflowReply.GetResult().GetStatus(),
			NextSteps:   cancelPartnerWebhookWorkflowReply.GetOutcome().GetNextSteps(),
			ReferenceId: cancelPartnerWebhookWorkflowReply.GetOutcome().GetReferenceId(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: advancePartnerOrderWorkflowReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		PartnerIntegration: &partnerapiv1.PartnerIntegration{
			PartnerIntegrationId: req.GetPartnerIntegrationId(),
			Available:            req.GetPartnerIntegration().GetAvailable(),
		},
	}, nil
}
