package partnerapi

import (
	"context"
	"log/slog"
	"sync"

	cartv1 "github.com/hounddogai/protomap-test-golang/gen/acme/cart/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	datahubv1 "github.com/hounddogai/protomap-test-golang/gen/acme/datahub/v1"
	fulfillmentv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fulfillment/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	inventoryv1 "github.com/hounddogai/protomap-test-golang/gen/acme/inventory/v1"
	partnerapiv1 "github.com/hounddogai/protomap-test-golang/gen/acme/partnerapi/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	promotionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/promotions/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// PartnerOrderWorkflowServer implements `acme.partnerapi.v1.PartnerOrderWorkflowService`.
// Runs the long-lived workflow that takes a partner order from start to finish.
type PartnerOrderWorkflowServer struct {
	partnerapiv1.UnimplementedPartnerOrderWorkflowServiceServer
	store                               *Store
	log                                 *slog.Logger
	searchIndexNotificationClient       searchv1.SearchIndexNotificationServiceClient
	promotionApprovalClient             promotionsv1.PromotionApprovalServiceClient
	disputeSyncClient                   paymentsv1.DisputeSyncServiceClient
	stockItemWorkflowClient             inventoryv1.StockItemWorkflowServiceClient
	meteredPartnerIntegrationWorkflow   meteredPartnerIntegrationWorkflow
	conn                                *grpc.ClientConn
	partnerWebhookReportingClient       partnerapiv1.PartnerWebhookReportingServiceClient
	meteredSellerApproval               meteredSellerApproval
	partnerWebhookValidationClient      partnerapiv1.PartnerWebhookValidationServiceClient
	partnerIntegrationWorkflowClient    partnerapiv1.PartnerIntegrationWorkflowServiceClient
	packingSlipWorkflowOnce             sync.Once
	packingSlipWorkflowCached           fulfillmentv1.PackingSlipWorkflowServiceClient
	sellerLifecycleClient               sellersv1.SellerLifecycleServiceClient
	partnerOrderApprovalPort            partnerOrderApprovalPort
	sellerListingReportingClient        sellersv1.SellerListingReportingServiceClient
	promotionWorkflowClient             promotionsv1.PromotionWorkflowServiceClient
	clients                             *registry.Registry
	meteredSellerPayoutScheduleWorkflow meteredSellerPayoutScheduleWorkflow
	partnerOrderReportingClient         partnerapiv1.PartnerOrderReportingServiceClient
}

// NewPartnerOrderWorkflowServer wires a PartnerOrderWorkflowServer.
func NewPartnerOrderWorkflowServer(deps platform.Deps) *PartnerOrderWorkflowServer {
	return &PartnerOrderWorkflowServer{
		store:                               NewStore(deps.DB),
		log:                                 deps.Logger.With("service", "acme.partnerapi.v1.PartnerOrderWorkflowService"),
		searchIndexNotificationClient:       searchv1.NewSearchIndexNotificationServiceClient(deps.Conn),
		promotionApprovalClient:             promotionsv1.NewPromotionApprovalServiceClient(deps.Conn),
		disputeSyncClient:                   paymentsv1.NewDisputeSyncServiceClient(deps.Conn),
		stockItemWorkflowClient:             inventoryv1.NewStockItemWorkflowServiceClient(deps.Conn),
		meteredPartnerIntegrationWorkflow:   meteredPartnerIntegrationWorkflow{PartnerIntegrationWorkflowServiceClient: partnerapiv1.NewPartnerIntegrationWorkflowServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		conn:                                deps.Conn,
		partnerWebhookReportingClient:       partnerapiv1.NewPartnerWebhookReportingServiceClient(deps.Conn),
		meteredSellerApproval:               meteredSellerApproval{SellerApprovalServiceClient: sellersv1.NewSellerApprovalServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		partnerWebhookValidationClient:      partnerapiv1.NewPartnerWebhookValidationServiceClient(deps.Conn),
		partnerIntegrationWorkflowClient:    partnerapiv1.NewPartnerIntegrationWorkflowServiceClient(deps.Conn),
		sellerLifecycleClient:               sellersv1.NewSellerLifecycleServiceClient(deps.Conn),
		partnerOrderApprovalPort:            newPartnerOrderApprovalPort(deps),
		sellerListingReportingClient:        sellersv1.NewSellerListingReportingServiceClient(deps.Conn),
		promotionWorkflowClient:             promotionsv1.NewPromotionWorkflowServiceClient(deps.Conn),
		clients:                             deps.Clients,
		meteredSellerPayoutScheduleWorkflow: meteredSellerPayoutScheduleWorkflow{SellerPayoutScheduleWorkflowServiceClient: sellersv1.NewSellerPayoutScheduleWorkflowServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		partnerOrderReportingClient:         partnerapiv1.NewPartnerOrderReportingServiceClient(deps.Conn),
	}
}

// StartPartnerOrderWorkflow starts a partner order.
func (s *PartnerOrderWorkflowServer) StartPartnerOrderWorkflow(ctx context.Context, req *partnerapiv1.StartPartnerOrderWorkflowRequest) (*partnerapiv1.StartPartnerOrderWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected StartPartnerOrderWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SavePartnerOrder(ctx, req.GetContext().GetTenantId(), req.GetPartnerOrder()); err != nil {
		return nil, grpcx.Wrap(err, "save partner order")
	}
	s.log.InfoContext(ctx, "start partner order workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPartnerOrder().GetAudit().GetEtag())
	packed, err := anypb.New(req.GetPartnerOrder())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	notifySearchIndexWatchersReply, err := s.searchIndexNotificationClient.NotifySearchIndexWatchers(ctx, &searchv1.NotifySearchIndexWatchersRequest{
		Context: req.GetContext(),
		Details: &searchv1.NotifySearchIndexWatchersRequest_Details{
			Recipient: &commonv1.ContactInfo{
				Name:  req.GetDetails().GetNotify().GetName(),
				Email: req.GetPartnerOrder().GetCounterparty().GetEmail(),
				Phone: req.GetDetails().GetNotify().GetPhone(),
			},
			Payload: packed,
		},
		Reason: req.GetReason(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexNotificationService/NotifySearchIndexWatchers")
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*promotionsv1.DelegatePromotionApprovalResponse, len(req.GetPartnerOrder().GetLines()))
	for i, line := range req.GetPartnerOrder().GetLines() {
		group.Go(func() error {
			reply, err := s.promotionApprovalClient.DelegatePromotionApproval(groupCtx, &promotionsv1.DelegatePromotionApprovalRequest{
				Context: req.GetContext(),
				Promotion: &promotionsv1.Promotion{
					Action:      &promotionsv1.Promotion_Discount{Discount: &promotionsv1.PromotionDiscountAction{Amount: line.GetAmount()}},
					Description: line.GetDescription(),
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
		return nil, grpcx.Wrap(err, "acme.promotions.v1.PromotionApprovalService/DelegatePromotionApproval")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	snapshot, err := protojson.Marshal(req.GetPartnerOrder())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	pullDisputesRequest := &paymentsv1.PullDisputesRequest{}
	pullDisputesRequest.Context = req.GetContext()
	pullDisputesRequest.Dispute = &paymentsv1.Dispute{
		Assignee: &identityv1.UserRef{
			UserId: notifySearchIndexWatchersReply.GetSearchIndex().GetOwner().GetUserId(),
			Email:  req.GetDetails().GetNotify().GetEmail(),
		},
	}
	pullDisputesRequest.Details = &paymentsv1.PullDisputesRequest_Details{Payload: snapshot}
	pullDisputesReply, err := s.disputeSyncClient.PullDisputes(ctx, pullDisputesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.DisputeSyncService/PullDisputes")
	}
	return &partnerapiv1.StartPartnerOrderWorkflowResponse{
		PartnerOrder: &partnerapiv1.PartnerOrder{
			Audit: &commonv1.AuditInfo{
				CreatedBy: notifySearchIndexWatchersReply.GetSearchIndex().GetAudit().GetCreatedBy(),
			},
			BillingAddress: &commonv1.PostalAddress{
				Lines: notifySearchIndexWatchersReply.GetSearchIndex().GetAddress().GetLines(),
			},
			DueAt:          pullDisputesReply.GetDispute().GetSla().GetDueAt(),
			PartnerOrderId: req.GetPartnerOrderId(),
			Notes:          req.GetPartnerOrder().GetNotes(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// AdvancePartnerOrderWorkflow advances a partner order.
func (s *PartnerOrderWorkflowServer) AdvancePartnerOrderWorkflow(ctx context.Context, req *partnerapiv1.AdvancePartnerOrderWorkflowRequest) (*partnerapiv1.AdvancePartnerOrderWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected AdvancePartnerOrderWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SavePartnerOrder(ctx, req.GetContext().GetTenantId(), req.GetPartnerOrder()); err != nil {
		return nil, grpcx.Wrap(err, "save partner order")
	}
	s.log.InfoContext(ctx, "advance partner order workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPartnerOrder().GetAudit().GetEtag())
	startStockItemWorkflowReply, err := s.stockItemWorkflowClient.StartStockItemWorkflow(ctx, &inventoryv1.StartStockItemWorkflowRequest{
		Context: req.GetContext(),
		StockItem: &inventoryv1.StockItem{
			Audit: &commonv1.AuditInfo{UpdatedAt: req.GetPartnerOrder().GetAudit().GetUpdatedAt()},
			Locator: &inventoryv1.StockItem_Address{Address: &commonv1.PostalAddress{
				Recipient: req.GetPartnerOrder().GetBillingAddress().GetRecipient(),
			}},
			Owner: &identityv1.UserRef{Email: req.GetPartnerOrder().GetCounterparty().GetEmail()},
		},
		Details: &inventoryv1.StartStockItemWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetNotify().GetPreferredChannel(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.StockItemWorkflowService/StartStockItemWorkflow")
	}
	listStockItemWorkflowsRequest := buildListStockItemWorkflowsRequestForAdvancePartnerOrderWorkflow(req, startStockItemWorkflowReply)
	listStockItemWorkflowsReply, err := s.stockItemWorkflowClient.ListStockItemWorkflows(ctx, listStockItemWorkflowsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.StockItemWorkflowService/ListStockItemWorkflows")
	}
	pausePartnerIntegrationWorkflowRequest := &partnerapiv1.PausePartnerIntegrationWorkflowRequest{
		Context: req.GetContext(),
		PartnerIntegration: &partnerapiv1.PartnerIntegration{
			Funding: &partnerapiv1.PartnerIntegration_Card{Card: &partnerapiv1.PartnerIntegrationCardFunding{Token: req.GetPartnerOrder().GetCard().GetToken()}},
		},
	}
	pausePartnerIntegrationWorkflowReply, err := s.meteredPartnerIntegrationWorkflow.PausePartnerIntegrationWorkflow(ctx, pausePartnerIntegrationWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerIntegrationWorkflowService/PausePartnerIntegrationWorkflow")
	}
	return &partnerapiv1.AdvancePartnerOrderWorkflowResponse{
		PartnerOrder: &partnerapiv1.PartnerOrder{
			Audit: &commonv1.AuditInfo{
				Etag: startStockItemWorkflowReply.GetStockItem().GetAudit().GetEtag(),
			},
			PartnerOrderId: req.GetPartnerOrderId(),
			Counterparty:   req.GetPartnerOrder().GetCounterparty(),
		},
		Result: &commonv1.OperationResult{
			Status:        listStockItemWorkflowsReply.GetResult().GetStatus(),
			CorrelationId: listStockItemWorkflowsReply.GetResult().GetCorrelationId(),
		},
		Outcome: &partnerapiv1.AdvancePartnerOrderWorkflowResponse_Outcome{
			Status: pausePartnerIntegrationWorkflowReply.GetResult().GetStatus(),
		},
	}, nil
}

// CancelPartnerOrderWorkflow cancels a partner order.
func (s *PartnerOrderWorkflowServer) CancelPartnerOrderWorkflow(ctx context.Context, req *partnerapiv1.CancelPartnerOrderWorkflowRequest) (*partnerapiv1.CancelPartnerOrderWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePartnerOrder(ctx, req.GetContext().GetTenantId(), req.GetPartnerOrder()); err != nil {
		return nil, grpcx.Wrap(err, "save partner order")
	}
	s.log.InfoContext(ctx, "cancel partner order workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPartnerOrder().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "region", req.GetPartnerOrder().GetBillingAddress().GetRegion())
	ctx = tenancy.With(ctx, "seller_id", req.GetPartnerOrder().GetCounterparty().GetSellerId())
	findWorkspacesRequest := &datahubv1.FindWorkspacesRequest{
		Context: req.GetContext(),
		Filter: &datahubv1.FindWorkspacesRequest_Filter{
			Region:       tenancy.Value(ctx, "region"),
			CreatedAfter: req.GetPartnerOrder().GetAudit().GetUpdatedAt(),
			OwnerId:      tenancy.Value(ctx, "seller_id"),
		},
	}
	findWorkspacesReply, err := grpcx.NewClient(s.conn, datahubv1.NewWorkspaceQueryServiceClient).FindWorkspaces(ctx, findWorkspacesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.datahub.v1.WorkspaceQueryService/FindWorkspaces")
	}
	partnerOrderCache := cache.New[string, *partnerapiv1.PartnerOrder](1)
	partnerOrderCache.Put(req.GetContext().GetTraceId(), req.GetPartnerOrder())
	cachedPartnerOrder, _ := partnerOrderCache.Get(req.GetContext().GetTraceId())
	sharePartnerWebhookReportRequest := &partnerapiv1.SharePartnerWebhookReportRequest{
		Context: req.GetContext(),
		Details: &partnerapiv1.SharePartnerWebhookReportRequest_Details{
			Recipient: &commonv1.ContactInfo{Email: cachedPartnerOrder.GetCounterparty().GetEmail()},
		},
	}
	var sharePartnerWebhookReportReply *partnerapiv1.SharePartnerWebhookReportResponse
	if err := pipeline.Run(ctx, "acme.partnerapi.v1.PartnerWebhookReportingService/SharePartnerWebhookReport", func(ctx context.Context) error {
		answer, err := s.partnerWebhookReportingClient.SharePartnerWebhookReport(ctx, sharePartnerWebhookReportRequest)
		sharePartnerWebhookReportReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookReportingService/SharePartnerWebhookReport")
	}
	return &partnerapiv1.CancelPartnerOrderWorkflowResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: findWorkspacesReply.GetResult().GetReferenceIds(),
			Status:       findWorkspacesReply.GetResult().GetStatus(),
		},
		PartnerOrder: &partnerapiv1.PartnerOrder{
			Counterparty: &sellersv1.SellerRef{
				DisplayName: sharePartnerWebhookReportReply.GetPartnerWebhook().GetSubject().GetDisplayName(),
			},
			PartnerOrderId: req.GetPartnerOrderId(),
			IssuedAt:       req.GetPartnerOrder().GetIssuedAt(),
		},
	}, nil
}

// RetryPartnerOrderWorkflow retrys a partner order.
func (s *PartnerOrderWorkflowServer) RetryPartnerOrderWorkflow(ctx context.Context, req *partnerapiv1.RetryPartnerOrderWorkflowRequest) (*partnerapiv1.RetryPartnerOrderWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePartnerOrder(ctx, req.GetContext().GetTenantId(), req.GetPartnerOrder()); err != nil {
		return nil, grpcx.Wrap(err, "save partner order")
	}
	s.log.InfoContext(ctx, "retry partner order workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPartnerOrder().GetAudit().GetEtag())
	commentSellerApprovalRequest := &sellersv1.CommentSellerApprovalRequest{
		Context: req.GetContext(),
		Details: &sellersv1.CommentSellerApprovalRequest_Details{
			Approver: &identityv1.UserRef{Email: req.GetPartnerOrder().GetCounterparty().GetEmail()},
		},
		Seller: &sellersv1.Seller{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetNotify().GetPreferredChannel(),
			},
		},
	}
	commentSellerApprovalReply, err := s.meteredSellerApproval.CommentSellerApproval(ctx, commentSellerApprovalRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerApprovalService/CommentSellerApproval")
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*partnerapiv1.PreviewPartnerWebhookResponse, len(req.GetPartnerOrder().GetLines()))
	for i, line := range req.GetPartnerOrder().GetLines() {
		group.Go(func() error {
			reply, err := s.partnerWebhookValidationClient.PreviewPartnerWebhook(groupCtx, &partnerapiv1.PreviewPartnerWebhookRequest{
				Context:        req.GetContext(),
				PartnerWebhook: &partnerapiv1.PartnerWebhook{Attributes: line.GetAttributes()},
				Details: &partnerapiv1.PreviewPartnerWebhookRequest_Details{
					Contact: &commonv1.ContactInfo{
						Email: commentSellerApprovalReply.GetSeller().GetContact().GetEmail(),
						Name:  req.GetDetails().GetNotify().GetName(),
					},
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
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookValidationService/PreviewPartnerWebhook")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := partnerOrderWorkflowServerSignalPartnerIntegrationWorkflowSteps[mode]
	signalPartnerIntegrationWorkflowRequest := &partnerapiv1.SignalPartnerIntegrationWorkflowRequest{
		Context: req.GetContext(),
		PartnerIntegration: &partnerapiv1.PartnerIntegration{
			Funding: &partnerapiv1.PartnerIntegration_Bank{Bank: &partnerapiv1.PartnerIntegrationBankFunding{
				AccountHolder: req.GetPartnerOrder().GetBank().GetAccountHolder(),
			}},
			Audit: &commonv1.AuditInfo{
				Etag:      commentSellerApprovalReply.GetSeller().GetAudit().GetEtag(),
				CreatedAt: commentSellerApprovalReply.GetSeller().GetAudit().GetCreatedAt(),
			},
		},
	}
	signalPartnerIntegrationWorkflowReply, err := run(s, ctx, signalPartnerIntegrationWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerIntegrationWorkflowService/SignalPartnerIntegrationWorkflow")
	}
	return &partnerapiv1.RetryPartnerOrderWorkflowResponse{
		PartnerOrder: &partnerapiv1.PartnerOrder{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: commentSellerApprovalReply.GetSeller().GetAudit().GetUpdatedBy(),
			},
			Number:         commentSellerApprovalReply.GetSeller().GetPassport().GetNumber(),
			PartnerOrderId: req.GetPartnerOrderId(),
			BillingAddress: req.GetPartnerOrder().GetBillingAddress(),
		},
		Result: &commonv1.OperationResult{
			Message: signalPartnerIntegrationWorkflowReply.GetOutcome().GetMessage(),
			Details: signalPartnerIntegrationWorkflowReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}

// PausePartnerOrderWorkflow pauses a partner order.
func (s *PartnerOrderWorkflowServer) PausePartnerOrderWorkflow(ctx context.Context, req *partnerapiv1.PausePartnerOrderWorkflowRequest) (*partnerapiv1.PausePartnerOrderWorkflowResponse, error) {
	if err := s.store.SavePartnerOrder(ctx, req.GetContext().GetTenantId(), req.GetPartnerOrder()); err != nil {
		return nil, grpcx.Wrap(err, "save partner order")
	}
	s.log.InfoContext(ctx, "pause partner order workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPartnerOrder().GetAudit().GetEtag())
	source := req.GetPartnerOrder().GetBillingAddress().ProtoReflect()
	region := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("region"))).String()
	listPackingSlipWorkflowsRequest := &fulfillmentv1.ListPackingSlipWorkflowsRequest{
		Context: req.GetContext(),
		Filter:  &fulfillmentv1.ListPackingSlipWorkflowsRequest_Filter{Region: region},
	}
	listPackingSlipWorkflowsReply, err := s.packingSlipWorkflowLazy().ListPackingSlipWorkflows(ctx, listPackingSlipWorkflowsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.PackingSlipWorkflowService/ListPackingSlipWorkflows")
	}
	expireSellerRequest := &sellersv1.ExpireSellerRequest{
		Context:  req.GetContext(),
		SellerId: req.GetPartnerOrder().GetCounterparty().GetSellerId(),
		Details: &sellersv1.ExpireSellerRequest_Details{
			NewOwner: &identityv1.UserRef{
				DisplayName: req.GetPartnerOrder().GetCounterparty().GetDisplayName(),
			},
			NotifyContact: &commonv1.ContactInfo{
				Email: req.GetPartnerOrder().GetCounterparty().GetEmail(),
				Name:  req.GetDetails().GetNotify().GetName(),
			},
		},
	}
	expireSellerReply, err := grpcx.Call(ctx, s.sellerLifecycleClient.ExpireSeller, expireSellerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerLifecycleService/ExpireSeller")
	}
	return &partnerapiv1.PausePartnerOrderWorkflowResponse{
		Result: &commonv1.OperationResult{
			Message: listPackingSlipWorkflowsReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Outcome: &partnerapiv1.PausePartnerOrderWorkflowResponse_Outcome{Status: expireSellerReply.GetOutcome().GetStatus()},
		PartnerOrder: &partnerapiv1.PartnerOrder{
			PartnerOrderId: req.GetPartnerOrderId(),
			Total:          req.GetPartnerOrder().GetTotal(),
		},
	}, nil
}

// ResumePartnerOrderWorkflow resumes a partner order.
func (s *PartnerOrderWorkflowServer) ResumePartnerOrderWorkflow(ctx context.Context, req *partnerapiv1.ResumePartnerOrderWorkflowRequest) (*partnerapiv1.ResumePartnerOrderWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SavePartnerOrder(ctx, req.GetContext().GetTenantId(), req.GetPartnerOrder()); err != nil {
		return nil, grpcx.Wrap(err, "save partner order")
	}
	s.log.InfoContext(ctx, "resume partner order workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPartnerOrder().GetAudit().GetEtag())
	var holder struct{ UpdatedBy string }
	if record := req.GetPartnerOrder().GetAudit(); record != nil {
		if err := mapper.CopyFields(&holder, record, "UpdatedBy"); err != nil {
			s.log.WarnContext(ctx, "copy updated_by", "error", err)
		}
	}
	recallPartnerOrderRequest := &partnerapiv1.RecallPartnerOrderRequest{
		Context: req.GetContext(),
		PartnerOrder: &partnerapiv1.PartnerOrder{
			BillingAddress: &commonv1.PostalAddress{
				Region:    req.GetPartnerOrder().GetBillingAddress().GetRegion(),
				Recipient: req.GetPartnerOrder().GetBillingAddress().GetRecipient(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: holder.UpdatedBy,
				UpdatedAt: req.GetPartnerOrder().GetAudit().GetUpdatedAt(),
			},
		},
	}
	recallPartnerOrderReply, err := s.partnerOrderApprovalPort.RecallPartnerOrder(ctx, recallPartnerOrderRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerOrderApprovalService/RecallPartnerOrder")
	}
	scheduleSellerListingReportRequest := &sellersv1.ScheduleSellerListingReportRequest{
		Context: req.GetContext(),
		Reason:  req.GetReason(),
	}
	scheduleSellerListingReportReply, err := grpcx.Call(ctx, s.sellerListingReportingClient.ScheduleSellerListingReport, scheduleSellerListingReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingReportingService/ScheduleSellerListingReport")
	}
	encoded, err := protojson.Marshal(req.GetPartnerOrder())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded partnerapiv1.PartnerOrder
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	startPromotionWorkflowRequest := &promotionsv1.StartPromotionWorkflowRequest{
		Context: req.GetContext(),
		Promotion: &promotionsv1.Promotion{
			Owner: &identityv1.UserRef{
				DisplayName: recallPartnerOrderReply.GetPartnerOrder().GetCounterparty().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				Etag:      recallPartnerOrderReply.GetPartnerOrder().GetAudit().GetEtag(),
				CreatedBy: (&decoded).GetAudit().GetCreatedBy(),
			},
		},
	}
	startPromotionWorkflowReply, err := grpcx.Call(ctx, s.promotionWorkflowClient.StartPromotionWorkflow, startPromotionWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.PromotionWorkflowService/StartPromotionWorkflow")
	}
	return &partnerapiv1.ResumePartnerOrderWorkflowResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: recallPartnerOrderReply.GetResult().GetReferenceIds(),
			Details:      scheduleSellerListingReportReply.GetResult().GetDetails(),
			Status:       startPromotionWorkflowReply.GetOutcome().GetStatus(),
		},
		PartnerOrder: &partnerapiv1.PartnerOrder{
			PartnerOrderId: req.GetPartnerOrderId(),
			Total:          req.GetPartnerOrder().GetTotal(),
		},
	}, nil
}

// InspectPartnerOrderWorkflow returns a partner order.
func (s *PartnerOrderWorkflowServer) InspectPartnerOrderWorkflow(ctx context.Context, req *partnerapiv1.InspectPartnerOrderWorkflowRequest) (*partnerapiv1.InspectPartnerOrderWorkflowResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "partner_order", req.GetPartnerOrderId(), "inspect_partner_order_workflow"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "inspect partner order workflow", "tenant", req.GetContext().GetTenantId())
	return &partnerapiv1.InspectPartnerOrderWorkflowResponse{
		PartnerOrder: &partnerapiv1.PartnerOrder{PartnerOrderId: req.GetPartnerOrderId()},
		Result:       &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ListPartnerOrderWorkflows lists partner orders a page at a time.
func (s *PartnerOrderWorkflowServer) ListPartnerOrderWorkflows(ctx context.Context, req *partnerapiv1.ListPartnerOrderWorkflowsRequest) (*partnerapiv1.ListPartnerOrderWorkflowsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "list partner order workflows", "tenant", req.GetContext().GetTenantId())
	savedListWorkflow, ok := s.clients.Get("acme.cart.v1.SavedListWorkflowService").(cartv1.SavedListWorkflowServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.cart.v1.SavedListWorkflowService")
	}
	pauseSavedListWorkflowRequest := &cartv1.PauseSavedListWorkflowRequest{
		Context: req.GetContext(),
		SavedList: &cartv1.SavedList{
			BillingAddress: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
			DueAt:          req.GetFilter().GetCreatedAfter(),
		},
		Details: &cartv1.PauseSavedListWorkflowRequest_Details{
			Notify:     &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Parameters: req.GetFilter().GetLabels(),
		},
	}
	pauseSavedListWorkflowReply, err := savedListWorkflow.PauseSavedListWorkflow(ctx, pauseSavedListWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.SavedListWorkflowService/PauseSavedListWorkflow")
	}
	return &partnerapiv1.ListPartnerOrderWorkflowsResponse{
		Result: &commonv1.OperationResult{
			Message: pauseSavedListWorkflowReply.GetResult().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// SignalPartnerOrderWorkflow signals a partner order.
func (s *PartnerOrderWorkflowServer) SignalPartnerOrderWorkflow(ctx context.Context, req *partnerapiv1.SignalPartnerOrderWorkflowRequest) (*partnerapiv1.SignalPartnerOrderWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePartnerOrder(ctx, req.GetContext().GetTenantId(), req.GetPartnerOrder()); err != nil {
		return nil, grpcx.Wrap(err, "save partner order")
	}
	s.log.InfoContext(ctx, "signal partner order workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPartnerOrder().GetAudit().GetEtag())
	expireSellerRequest := &sellersv1.ExpireSellerRequest{
		Context:  req.GetContext(),
		SellerId: req.GetPartnerOrder().GetCounterparty().GetSellerId(),
		Details: &sellersv1.ExpireSellerRequest_Details{
			NotifyContact: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetNotify().GetPreferredChannel(),
				Phone:            req.GetDetails().GetNotify().GetPhone(),
			},
		},
	}
	send := s.expireSellerSender(expireSellerRequest)
	expireSellerReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerLifecycleService/ExpireSeller")
	}
	ctx = tenancy.With(ctx, "region", req.GetPartnerOrder().GetBillingAddress().GetRegion())
	listSellerPayoutScheduleWorkflowsRequest := &sellersv1.ListSellerPayoutScheduleWorkflowsRequest{
		Context: req.GetContext(),
		Filter: &sellersv1.ListSellerPayoutScheduleWorkflowsRequest_Filter{
			Region:       tenancy.Value(ctx, "region"),
			CreatedAfter: expireSellerReply.GetSeller().GetAudit().GetUpdatedAt(),
		},
	}
	listSellerPayoutScheduleWorkflowsReply, err := s.meteredSellerPayoutScheduleWorkflow.ListSellerPayoutScheduleWorkflows(ctx, listSellerPayoutScheduleWorkflowsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerPayoutScheduleWorkflowService/ListSellerPayoutScheduleWorkflows")
	}
	countPartnerOrderTotalsRequest := &partnerapiv1.CountPartnerOrderTotalsRequest{}
	countPartnerOrderTotalsRequest.Context = req.GetContext()
	countPartnerOrderTotalsRequest.Filter = &partnerapiv1.CountPartnerOrderTotalsRequest_Filter{
		Region:       req.GetPartnerOrder().GetBillingAddress().GetRegion(),
		CreatedAfter: expireSellerReply.GetSeller().GetPassport().GetExpiresAt(),
	}
	countPartnerOrderTotalsReply, err := s.partnerOrderReportingClient.CountPartnerOrderTotals(ctx, countPartnerOrderTotalsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerOrderReportingService/CountPartnerOrderTotals")
	}
	return &partnerapiv1.SignalPartnerOrderWorkflowResponse{
		Outcome: &partnerapiv1.SignalPartnerOrderWorkflowResponse_Outcome{
			Status:  expireSellerReply.GetOutcome().GetStatus(),
			Message: countPartnerOrderTotalsReply.GetResult().GetMessage(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: expireSellerReply.GetResult().GetCorrelationId(),
			Details:       listSellerPayoutScheduleWorkflowsReply.GetResult().GetDetails(),
			Status:        "ok",
		},
		PartnerOrder: &partnerapiv1.PartnerOrder{
			PartnerOrderId: req.GetPartnerOrderId(),
			Status:         req.GetPartnerOrder().GetStatus(),
		},
	}, nil
}

// partnerOrderWorkflowServerSignalPartnerIntegrationWorkflowSteps are the ways to run
// acme.partnerapi.v1.PartnerIntegrationWorkflowService/SignalPartnerIntegrationWorkflow, by name.
var partnerOrderWorkflowServerSignalPartnerIntegrationWorkflowSteps = map[string]func(*PartnerOrderWorkflowServer, context.Context, *partnerapiv1.SignalPartnerIntegrationWorkflowRequest) (*partnerapiv1.SignalPartnerIntegrationWorkflowResponse, error){
	"default": (*PartnerOrderWorkflowServer).signalPartnerIntegrationWorkflowStep,
	"audited": (*PartnerOrderWorkflowServer).signalPartnerIntegrationWorkflowAuditedStep,
}

func (s *PartnerOrderWorkflowServer) signalPartnerIntegrationWorkflowStep(ctx context.Context, request *partnerapiv1.SignalPartnerIntegrationWorkflowRequest) (*partnerapiv1.SignalPartnerIntegrationWorkflowResponse, error) {
	return s.partnerIntegrationWorkflowClient.SignalPartnerIntegrationWorkflow(ctx, request)
}

func (s *PartnerOrderWorkflowServer) signalPartnerIntegrationWorkflowAuditedStep(ctx context.Context, request *partnerapiv1.SignalPartnerIntegrationWorkflowRequest) (*partnerapiv1.SignalPartnerIntegrationWorkflowResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.partnerapi.v1.PartnerIntegrationWorkflowService/SignalPartnerIntegrationWorkflow")
	return s.partnerIntegrationWorkflowClient.SignalPartnerIntegrationWorkflow(ctx, request)
}

// packingSlipWorkflowLazy dials acme.fulfillment.v1.PackingSlipWorkflowService the first time a handler needs it.
func (s *PartnerOrderWorkflowServer) packingSlipWorkflowLazy() fulfillmentv1.PackingSlipWorkflowServiceClient {
	s.packingSlipWorkflowOnce.Do(func() { s.packingSlipWorkflowCached = fulfillmentv1.NewPackingSlipWorkflowServiceClient(s.conn) })
	return s.packingSlipWorkflowCached
}

// expireSellerSender returns a function that sends one request to acme.sellers.v1.SellerLifecycleService/ExpireSeller.
func (s *PartnerOrderWorkflowServer) expireSellerSender(request *sellersv1.ExpireSellerRequest) func(context.Context) (*sellersv1.ExpireSellerResponse, error) {
	return func(ctx context.Context) (*sellersv1.ExpireSellerResponse, error) {
		return s.sellerLifecycleClient.ExpireSeller(ctx, request)
	}
}
