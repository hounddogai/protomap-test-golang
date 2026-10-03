package partnerapi

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"

	auditv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audit/v1"
	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	checkoutv1 "github.com/hounddogai/protomap-test-golang/gen/acme/checkout/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	employeeportalv1 "github.com/hounddogai/protomap-test-golang/gen/acme/employeeportal/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	partnerapiv1 "github.com/hounddogai/protomap-test-golang/gen/acme/partnerapi/v1"
	pricingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/pricing/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// PartnerOrderServer implements `acme.partnerapi.v1.PartnerOrderService`.
// Manages partner orders: creates, reads, changes, and lists them.
type PartnerOrderServer struct {
	partnerapiv1.UnimplementedPartnerOrderServiceServer
	store                           *Store
	log                             *slog.Logger
	partnerOrderWorkflowClient      partnerapiv1.PartnerOrderWorkflowServiceClient
	sellerListingQueryClient        sellersv1.SellerListingQueryServiceClient
	partnerWebhookQueryClient       partnerapiv1.PartnerWebhookQueryServiceClient
	auditEventQueryClient           auditv1.AuditEventQueryServiceClient
	partnerWebhookValidationClient  partnerapiv1.PartnerWebhookValidationServiceClient
	resumeSellerListingWorkflowFn   func(context.Context, *sellersv1.ResumeSellerListingWorkflowRequest, ...grpc.CallOption) (*sellersv1.ResumeSellerListingWorkflowResponse, error)
	conn                            *grpc.ClientConn
	sellerApprovalClient            sellersv1.SellerApprovalServiceClient
	meteredAccessPolicy             meteredAccessPolicy
	partnerWebhookIntegrationClient partnerapiv1.PartnerWebhookIntegrationServiceClient
	escalateSellerListingFn         func(context.Context, *sellersv1.EscalateSellerListingRequest, ...grpc.CallOption) (*sellersv1.EscalateSellerListingResponse, error)
	checkoutSessionApprovalClient   checkoutv1.CheckoutSessionApprovalServiceClient
	currencyRateApprovalClient      pricingv1.CurrencyRateApprovalServiceClient
	sellerListingCalculationClient  sellersv1.SellerListingCalculationServiceClient
}

// NewPartnerOrderServer wires a PartnerOrderServer.
func NewPartnerOrderServer(deps platform.Deps) *PartnerOrderServer {
	sellerListingWorkflow := sellersv1.NewSellerListingWorkflowServiceClient(deps.Conn)
	sellerListingApproval := sellersv1.NewSellerListingApprovalServiceClient(deps.Conn)
	return &PartnerOrderServer{
		store:                           NewStore(deps.DB),
		log:                             deps.Logger.With("service", "acme.partnerapi.v1.PartnerOrderService"),
		partnerOrderWorkflowClient:      partnerapiv1.NewPartnerOrderWorkflowServiceClient(deps.Conn),
		sellerListingQueryClient:        sellersv1.NewSellerListingQueryServiceClient(deps.Conn),
		partnerWebhookQueryClient:       partnerapiv1.NewPartnerWebhookQueryServiceClient(deps.Conn),
		auditEventQueryClient:           auditv1.NewAuditEventQueryServiceClient(deps.Conn),
		partnerWebhookValidationClient:  partnerapiv1.NewPartnerWebhookValidationServiceClient(deps.Conn),
		resumeSellerListingWorkflowFn:   sellerListingWorkflow.ResumeSellerListingWorkflow,
		conn:                            deps.Conn,
		sellerApprovalClient:            sellersv1.NewSellerApprovalServiceClient(deps.Conn),
		meteredAccessPolicy:             meteredAccessPolicy{AccessPolicyServiceClient: authorizationv1.NewAccessPolicyServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		partnerWebhookIntegrationClient: partnerapiv1.NewPartnerWebhookIntegrationServiceClient(deps.Conn),
		escalateSellerListingFn:         sellerListingApproval.EscalateSellerListing,
		checkoutSessionApprovalClient:   checkoutv1.NewCheckoutSessionApprovalServiceClient(deps.Conn),
		currencyRateApprovalClient:      pricingv1.NewCurrencyRateApprovalServiceClient(deps.Conn),
		sellerListingCalculationClient:  sellersv1.NewSellerListingCalculationServiceClient(deps.Conn),
	}
}

// CreatePartnerOrder creates a partner order.
func (s *PartnerOrderServer) CreatePartnerOrder(ctx context.Context, req *partnerapiv1.CreatePartnerOrderRequest) (*partnerapiv1.CreatePartnerOrderResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePartnerOrder(ctx, req.GetContext().GetTenantId(), req.GetPartnerOrder()); err != nil {
		return nil, grpcx.Wrap(err, "save partner order")
	}
	s.log.InfoContext(ctx, "create partner order", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPartnerOrder().GetAudit().GetEtag())
	retryPartnerOrderWorkflowRequest := &partnerapiv1.RetryPartnerOrderWorkflowRequest{
		Context: req.GetContext(),
		PartnerOrder: &partnerapiv1.PartnerOrder{
			BillingAddress: &commonv1.PostalAddress{
				PostalCode: req.GetPartnerOrder().GetBillingAddress().GetPostalCode(),
			},
		},
	}
	retryPartnerOrderWorkflowReply, err := grpcx.Call(ctx, s.partnerOrderWorkflowClient.RetryPartnerOrderWorkflow, retryPartnerOrderWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerOrderWorkflowService/RetryPartnerOrderWorkflow")
	}
	exportSellerListingsStream, err := s.sellerListingQueryClient.ExportSellerListings(ctx, &sellersv1.ExportSellerListingsRequest{
		Context: req.GetContext(),
		Filter: &sellersv1.ExportSellerListingsRequest_Filter{
			Region:       req.GetPartnerOrder().GetBillingAddress().GetRegion(),
			CreatedAfter: req.GetPartnerOrder().GetAudit().GetUpdatedAt(),
			Labels:       retryPartnerOrderWorkflowReply.GetResult().GetDetails(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingQueryService/ExportSellerListings")
	}
	for {
		received, err := exportSellerListingsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingQueryService/ExportSellerListings")
		}
		_ = received
	}
	resolvePartnerWebhooksRequest := &partnerapiv1.ResolvePartnerWebhooksRequest{Context: req.GetContext()}
	type resolvePartnerWebhooksResult struct {
		reply *partnerapiv1.ResolvePartnerWebhooksResponse
		err   error
	}
	resolvePartnerWebhooksResults := make(chan resolvePartnerWebhooksResult, 1)
	go func() {
		reply, err := s.partnerWebhookQueryClient.ResolvePartnerWebhooks(ctx, resolvePartnerWebhooksRequest)
		resolvePartnerWebhooksResults <- resolvePartnerWebhooksResult{reply, err}
	}()
	var resolvePartnerWebhooksReply *partnerapiv1.ResolvePartnerWebhooksResponse
	select {
	case result := <-resolvePartnerWebhooksResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.partnerapi.v1.PartnerWebhookQueryService/ResolvePartnerWebhooks")
		}
		resolvePartnerWebhooksReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	recentPartnerOrders.Store(recordKey{req.GetContext().GetTenantId(), req.GetPartnerOrder().GetPartnerOrderId()}, req.GetPartnerOrder())
	return &partnerapiv1.CreatePartnerOrderResponse{
		Result: &commonv1.OperationResult{
			Status:        retryPartnerOrderWorkflowReply.GetOutcome().GetStatus(),
			CorrelationId: resolvePartnerWebhooksReply.GetResult().GetCorrelationId(),
			Details:       resolvePartnerWebhooksReply.GetResult().GetDetails(),
		},
		PartnerOrder: &partnerapiv1.PartnerOrder{
			Metadata:     req.GetPartnerOrder().GetMetadata(),
			Counterparty: req.GetPartnerOrder().GetCounterparty(),
		},
	}, nil
}

// GetPartnerOrder returns a partner order.
func (s *PartnerOrderServer) GetPartnerOrder(ctx context.Context, req *partnerapiv1.GetPartnerOrderRequest) (*partnerapiv1.GetPartnerOrderResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "partner_order", req.GetPartnerOrderId(), "get_partner_order"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get partner order", "tenant", req.GetContext().GetTenantId())
	var cachedNumber string
	if entry, ok := recentPartnerOrders.Load(recordKey{req.GetContext().GetTenantId(), req.GetPartnerOrderId()}); ok {
		if previous, ok := entry.(*partnerapiv1.PartnerOrder); ok {
			cachedNumber = previous.GetNumber()
		}
	}
	exportPartnerWebhooksStream, err := s.partnerWebhookQueryClient.ExportPartnerWebhooks(ctx, &partnerapiv1.ExportPartnerWebhooksRequest{
		Context: req.GetContext(),
		Filter:  &partnerapiv1.ExportPartnerWebhooksRequest_Filter{OwnerId: req.GetPartnerOrderId()},
		Format:  cachedNumber,
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookQueryService/ExportPartnerWebhooks")
	}
	for {
		received, err := exportPartnerWebhooksStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookQueryService/ExportPartnerWebhooks")
		}
		_ = received
	}
	return &partnerapiv1.GetPartnerOrderResponse{
		PartnerOrder: &partnerapiv1.PartnerOrder{PartnerOrderId: req.GetPartnerOrderId()},
		Result:       &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// UpdatePartnerOrder changes a partner order.
func (s *PartnerOrderServer) UpdatePartnerOrder(ctx context.Context, req *partnerapiv1.UpdatePartnerOrderRequest) (*partnerapiv1.UpdatePartnerOrderResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected UpdatePartnerOrder", "error", err)
		return nil, err
	}
	if err := s.store.SavePartnerOrder(ctx, req.GetContext().GetTenantId(), req.GetPartnerOrder()); err != nil {
		return nil, grpcx.Wrap(err, "save partner order")
	}
	s.log.InfoContext(ctx, "update partner order", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPartnerOrder().GetAudit().GetEtag())
	browseAuditEventsRequest := &auditv1.BrowseAuditEventsRequest{
		Context: req.GetContext(),
		Filter: &auditv1.BrowseAuditEventsRequest_Filter{
			Region:       req.GetPartnerOrder().GetBillingAddress().GetRegion(),
			CreatedAfter: req.GetPartnerOrder().GetAudit().GetCreatedAt(),
			OwnerId:      req.GetPartnerOrder().GetPartnerOrderId(),
			Labels:       req.GetPartnerOrder().GetMetadata(),
		},
	}
	type browseAuditEventsResult struct {
		reply *auditv1.BrowseAuditEventsResponse
		err   error
	}
	browseAuditEventsResults := make(chan browseAuditEventsResult, 1)
	go func() {
		reply, err := s.auditEventQueryClient.BrowseAuditEvents(ctx, browseAuditEventsRequest)
		browseAuditEventsResults <- browseAuditEventsResult{reply, err}
	}()
	var browseAuditEventsReply *auditv1.BrowseAuditEventsResponse
	select {
	case result := <-browseAuditEventsResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.audit.v1.AuditEventQueryService/BrowseAuditEvents")
		}
		browseAuditEventsReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	for _, line := range req.GetPartnerOrder().GetLines() {
		if _, err := s.partnerWebhookValidationClient.ScorePartnerWebhook(ctx, &partnerapiv1.ScorePartnerWebhookRequest{
			Context:        req.GetContext(),
			PartnerWebhook: &partnerapiv1.PartnerWebhook{Attributes: line.GetAttributes()},
			Details:        &partnerapiv1.ScorePartnerWebhookRequest_Details{Budget: line.GetTax().GetAmount()},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookValidationService/ScorePartnerWebhook")
		}
	}
	resumeSellerListingWorkflowRequest := &sellersv1.ResumeSellerListingWorkflowRequest{
		Context: req.GetContext(),
		SellerListing: &sellersv1.SellerListing{
			Audit: &commonv1.AuditInfo{UpdatedBy: req.GetPartnerOrder().GetAudit().GetUpdatedBy()},
			Owner: &identityv1.UserRef{
				DisplayName: req.GetPartnerOrder().GetCounterparty().GetDisplayName(),
			},
		},
	}
	resumeSellerListingWorkflowReply, err := s.resumeSellerListingWorkflowFn(ctx, resumeSellerListingWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingWorkflowService/ResumeSellerListingWorkflow")
	}
	return &partnerapiv1.UpdatePartnerOrderResponse{
		Result: &commonv1.OperationResult{
			Details:      browseAuditEventsReply.GetResult().GetDetails(),
			ReferenceIds: browseAuditEventsReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		PartnerOrder: &partnerapiv1.PartnerOrder{
			Counterparty: &sellersv1.SellerRef{
				Email: resumeSellerListingWorkflowReply.GetSellerListing().GetOwner().GetEmail(),
			},
			Status:   req.GetPartnerOrder().GetStatus(),
			Metadata: req.GetPartnerOrder().GetMetadata(),
		},
	}, nil
}

// DeletePartnerOrder deletes a partner order.
func (s *PartnerOrderServer) DeletePartnerOrder(ctx context.Context, req *partnerapiv1.DeletePartnerOrderRequest) (*partnerapiv1.DeletePartnerOrderResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected DeletePartnerOrder", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "partner_order", req.GetPartnerOrderId(), "delete_partner_order"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "delete partner order", "tenant", req.GetContext().GetTenantId())
	summarizePortalHomesRequest := &employeeportalv1.SummarizePortalHomesRequest{
		Context:     req.GetContext(),
		Reason:      req.GetReason(),
		Annotations: req.GetContext().GetBaggage(),
	}
	summarizePortalHomesReply := new(employeeportalv1.SummarizePortalHomesResponse)
	if err := grpcx.Invoke(ctx, s.conn, employeeportalv1.PortalHomeQueryService_SummarizePortalHomes_FullMethodName, summarizePortalHomesRequest, summarizePortalHomesReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.employeeportal.v1.PortalHomeQueryService/SummarizePortalHomes")
	}
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := partnerOrderServerApproveSellerSteps[mode]
	approveSellerRequest := &sellersv1.ApproveSellerRequest{
		Context: req.GetContext(),
		Reason:  req.GetReason(),
		Details: &sellersv1.ApproveSellerRequest_Details{
			Approver: &identityv1.UserRef{Email: req.GetContext().GetActor().GetEmail()},
		},
	}
	approveSellerReply, err := run(s, ctx, approveSellerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerApprovalService/ApproveSeller")
	}
	return &partnerapiv1.DeletePartnerOrderResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds:  summarizePortalHomesReply.GetResult().GetReferenceIds(),
			CorrelationId: summarizePortalHomesReply.GetResult().GetCorrelationId(),
			Message:       approveSellerReply.GetOutcome().GetMessage(),
			Status:        "ok",
		},
	}, nil
}

// ListPartnerOrders lists partner orders a page at a time.
func (s *PartnerOrderServer) ListPartnerOrders(ctx context.Context, req *partnerapiv1.ListPartnerOrdersRequest) (*partnerapiv1.ListPartnerOrdersResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "list partner orders", "tenant", req.GetContext().GetTenantId())
	approveCollectionRequest := &merchandisingv1.ApproveCollectionRequest{
		Context: req.GetContext(),
		Collection: &merchandisingv1.Collection{
			Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
				Location: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
				Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
					Manager: &identityv1.UserRef{
						Email:  req.GetContext().GetActor().GetEmail(),
						UserId: req.GetContext().GetActor().GetUserId(),
					},
				}},
			}},
		},
	}
	approveCollectionReply := new(merchandisingv1.ApproveCollectionResponse)
	if err := grpcx.Invoke(ctx, s.conn, merchandisingv1.CollectionApprovalService_ApproveCollection_FullMethodName, approveCollectionRequest, approveCollectionReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.CollectionApprovalService/ApproveCollection")
	}
	return &partnerapiv1.ListPartnerOrdersResponse{
		PartnerOrders: []*partnerapiv1.PartnerOrder{&partnerapiv1.PartnerOrder{
			BillingAddress: &commonv1.PostalAddress{
				CountryCode: approveCollectionReply.GetCollection().GetLocation().GetCountryCode(),
			},
		}},
		Result: &commonv1.OperationResult{
			Message: approveCollectionReply.GetResult().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// BatchGetPartnerOrders returns several partner orders at once.
func (s *PartnerOrderServer) BatchGetPartnerOrders(ctx context.Context, req *partnerapiv1.BatchGetPartnerOrdersRequest) (*partnerapiv1.BatchGetPartnerOrdersResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "batch get partner orders", "tenant", req.GetContext().GetTenantId())
	return &partnerapiv1.BatchGetPartnerOrdersResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// SearchPartnerOrders lists partner orders a page at a time.
func (s *PartnerOrderServer) SearchPartnerOrders(ctx context.Context, req *partnerapiv1.SearchPartnerOrdersRequest) (*partnerapiv1.SearchPartnerOrdersResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "search partner orders", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetContext()).(*commonv1.RequestContext)
	createAccessPolicyRequest := &authorizationv1.CreateAccessPolicyRequest{
		Context: req.GetContext(),
		AccessPolicy: &authorizationv1.AccessPolicy{
			Owner: &identityv1.UserRef{
				Email:  req.GetContext().GetActor().GetEmail(),
				UserId: cloned.GetActor().GetUserId(),
			},
			Audit:      &commonv1.AuditInfo{UpdatedAt: req.GetFilter().GetCreatedAfter()},
			Parameters: req.GetFilter().GetLabels(),
		},
	}
	createAccessPolicyReply, err := s.meteredAccessPolicy.CreateAccessPolicy(ctx, createAccessPolicyRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyService/CreateAccessPolicy")
	}
	connectPartnerWebhookSourceRequest := buildConnectPartnerWebhookSourceRequestForSearchPartnerOrders(req, createAccessPolicyReply)
	connectPartnerWebhookSourceReply, err := s.partnerWebhookIntegrationClient.ConnectPartnerWebhookSource(ctx, connectPartnerWebhookSourceRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookIntegrationService/ConnectPartnerWebhookSource")
	}
	return &partnerapiv1.SearchPartnerOrdersResponse{
		Result: &commonv1.OperationResult{
			Status:  createAccessPolicyReply.GetResult().GetStatus(),
			Message: connectPartnerWebhookSourceReply.GetOutcome().GetMessage(),
		},
		PartnerOrders: []*partnerapiv1.PartnerOrder{&partnerapiv1.PartnerOrder{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: createAccessPolicyReply.GetAccessPolicy().GetAudit().GetUpdatedBy(),
			},
		}},
	}, nil
}

// WatchPartnerOrders streams the changes of partner orders as they happen.
func (s *PartnerOrderServer) WatchPartnerOrders(req *partnerapiv1.WatchPartnerOrdersRequest, stream grpc.ServerStreamingServer[partnerapiv1.WatchPartnerOrdersResponse]) error {
	ctx := stream.Context()
	if err := validateContext(req.GetContext()); err != nil {
		return err
	}
	s.log.InfoContext(ctx, "watch partner orders", "tenant", req.GetContext().GetTenantId())
	escalateSellerListingRequest := &sellersv1.EscalateSellerListingRequest{
		Context: req.GetContext(),
		SellerListing: &sellersv1.SellerListing{
			Locator: &sellersv1.SellerListing_Address{Address: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()}},
		},
	}
	escalateSellerListingReply, err := s.escalateSellerListingFn(ctx, escalateSellerListingRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.sellers.v1.SellerListingApprovalService/EscalateSellerListing")
	}
	packed, err := anypb.New(req.GetFilter())
	if err != nil {
		return grpcx.Wrap(err, "pack payload")
	}
	sharePartnerWebhookReportRequest := &partnerapiv1.SharePartnerWebhookReportRequest{
		Context: req.GetContext(),
		Details: &partnerapiv1.SharePartnerWebhookReportRequest_Details{
			Recipient: &commonv1.ContactInfo{Name: req.GetSubscriber().GetName()},
			Payload:   packed,
		},
	}
	sharePartnerWebhookReportReply := new(partnerapiv1.SharePartnerWebhookReportResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", partnerapiPartnerWebhookReportingServiceName, "SharePartnerWebhookReport"), sharePartnerWebhookReportRequest, sharePartnerWebhookReportReply); err != nil {
		return grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookReportingService/SharePartnerWebhookReport")
	}
	if err := stream.Send(&partnerapiv1.WatchPartnerOrdersResponse{
		PartnerOrder: &partnerapiv1.PartnerOrder{
			Audit: &commonv1.AuditInfo{
				Etag: escalateSellerListingReply.GetSellerListing().GetAudit().GetEtag(),
			},
			Lines: []*partnerapiv1.PartnerOrderLine{&partnerapiv1.PartnerOrderLine{
				Attributes: sharePartnerWebhookReportReply.GetPartnerWebhook().GetAttributes(),
			}},
			Counterparty:   &sellersv1.SellerRef{Email: req.GetSubscriber().GetEmail()},
			BillingAddress: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
		Result: &commonv1.OperationResult{
			Message: sharePartnerWebhookReportReply.GetOutcome().GetMessage(),
			Status:  "ok",
		},
	}); err != nil {
		return err
	}
	return nil
}

// BatchCreatePartnerOrders takes a stream of partner orders and answers once at the end.
func (s *PartnerOrderServer) BatchCreatePartnerOrders(stream grpc.ClientStreamingServer[partnerapiv1.BatchCreatePartnerOrdersRequest, partnerapiv1.BatchCreatePartnerOrdersResponse]) error {
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
		s.log.InfoContext(ctx, "batch create partner orders", "tenant", msg.GetContext().GetTenantId())
		jobs := make(chan *partnerapiv1.PartnerOrderLine)
		failures := make(chan error, 4)
		workCtx, stopWork := context.WithCancel(ctx)
		var wg sync.WaitGroup
		for range 4 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for line := range jobs {
					if _, err := s.checkoutSessionApprovalClient.RecallCheckoutSession(workCtx, &checkoutv1.RecallCheckoutSessionRequest{
						Context: msg.GetContext(),
						CheckoutSession: &checkoutv1.CheckoutSession{
							Lines: []*checkoutv1.CheckoutSessionLine{&checkoutv1.CheckoutSessionLine{
								Tax: &checkoutv1.CheckoutSessionLineTax{
									Exemptions: line.GetTax().GetExemptions(),
									Amount:     line.GetTax().GetAmount(),
									Rate:       line.GetTax().GetRate(),
								},
							}},
						},
					}); err != nil {
						failures <- err
						stopWork()
						return
					}
				}
			}()
		}
		for _, queued := range msg.GetRecord().GetLines() {
			if workCtx.Err() != nil {
				break
			}
			select {
			case jobs <- queued:
			case <-workCtx.Done():
			}
		}
		close(jobs)
		wg.Wait()
		stopWork()
		close(failures)
		if err := <-failures; err != nil {
			return grpcx.Wrap(err, "acme.checkout.v1.CheckoutSessionApprovalService/RecallCheckoutSession")
		}
		if _, err := s.currencyRateApprovalClient.RecallCurrencyRate(ctx, &pricingv1.RecallCurrencyRateRequest{
			Context: msg.GetContext(),
			CurrencyRate: &pricingv1.CurrencyRate{
				Subject: &customersv1.CustomerRef{Email: msg.GetHeader().GetSubmittedBy().GetEmail()},
				Source:  msg.GetHeader().GetSource(),
			},
		}); err != nil {
			return grpcx.Wrap(err, "acme.pricing.v1.CurrencyRateApprovalService/RecallCurrencyRate")
		}
		if _, err := s.sellerListingCalculationClient.EstimateSellerListing(ctx, &sellersv1.EstimateSellerListingRequest{Context: msg.GetContext()}); err != nil {
			return grpcx.Wrap(err, "acme.sellers.v1.SellerListingCalculationService/EstimateSellerListing")
		}
		accepted++
	}
	return stream.SendAndClose(&partnerapiv1.BatchCreatePartnerOrdersResponse{Accepted: accepted})
}

// partnerOrderServerApproveSellerSteps are the ways to run acme.sellers.v1.SellerApprovalService/ApproveSeller, by
// name.
var partnerOrderServerApproveSellerSteps = map[string]func(*PartnerOrderServer, context.Context, *sellersv1.ApproveSellerRequest) (*sellersv1.ApproveSellerResponse, error){
	"default": (*PartnerOrderServer).approveSellerStep,
	"audited": (*PartnerOrderServer).approveSellerAuditedStep,
}

func (s *PartnerOrderServer) approveSellerStep(ctx context.Context, request *sellersv1.ApproveSellerRequest) (*sellersv1.ApproveSellerResponse, error) {
	return s.sellerApprovalClient.ApproveSeller(ctx, request)
}

func (s *PartnerOrderServer) approveSellerAuditedStep(ctx context.Context, request *sellersv1.ApproveSellerRequest) (*sellersv1.ApproveSellerResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.sellers.v1.SellerApprovalService/ApproveSeller")
	return s.sellerApprovalClient.ApproveSeller(ctx, request)
}
