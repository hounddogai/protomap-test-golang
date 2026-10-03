package partnerapi

import (
	"context"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	fanportalv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fanportal/v1"
	financeportalv1 "github.com/hounddogai/protomap-test-golang/gen/acme/financeportal/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	inventoryv1 "github.com/hounddogai/protomap-test-golang/gen/acme/inventory/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	partnerapiv1 "github.com/hounddogai/protomap-test-golang/gen/acme/partnerapi/v1"
	pricingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/pricing/v1"
	promotionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/promotions/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	webbffv1 "github.com/hounddogai/protomap-test-golang/gen/acme/webbff/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// PartnerWebhookApprovalServer implements `acme.partnerapi.v1.PartnerWebhookApprovalService`.
// Routes partner webhooks through approval, with escalation and delegation.
type PartnerWebhookApprovalServer struct {
	partnerapiv1.UnsafePartnerWebhookApprovalServiceServer
	store                              *Store
	log                                *slog.Logger
	partnerOrderApprovalClient         partnerapiv1.PartnerOrderApprovalServiceClient
	clients                            *registry.Registry
	sellerWorkflowClient               sellersv1.SellerWorkflowServiceClient
	meteredPartnerIntegrationReporting meteredPartnerIntegrationReporting
	sellerPayoutScheduleApprovalClient sellersv1.SellerPayoutScheduleApprovalServiceClient
	conn                               *grpc.ClientConn
	partnerOrderApprovalOnce           sync.Once
	partnerOrderApprovalCached         partnerapiv1.PartnerOrderApprovalServiceClient
	partnerWebhookValidationClient     partnerapiv1.PartnerWebhookValidationServiceClient
	redemptionImportClient             promotionsv1.RedemptionImportServiceClient
	ledgerAccountValidationClient      ledgerv1.LedgerAccountValidationServiceClient
	partnerIntegrationReportingClient  partnerapiv1.PartnerIntegrationReportingServiceClient
	meteredPartnerOrderApproval        meteredPartnerOrderApproval
	pageCompositionQueryClient         webbffv1.PageCompositionQueryServiceClient
	currencyRateApprovalClient         pricingv1.CurrencyRateApprovalServiceClient
	apiCredentialHistoryClient         identityv1.ApiCredentialHistoryServiceClient
	financeWorkspaceQueryClient        financeportalv1.FinanceWorkspaceQueryServiceClient
	customerSegmentSyncClient          customersv1.CustomerSegmentSyncServiceClient
	customerSegmentSyncClientFallback  customersv1.CustomerSegmentSyncServiceClient
}

// NewPartnerWebhookApprovalServer wires a PartnerWebhookApprovalServer.
func NewPartnerWebhookApprovalServer(deps platform.Deps) *PartnerWebhookApprovalServer {
	return &PartnerWebhookApprovalServer{
		store:                              NewStore(deps.DB),
		log:                                deps.Logger.With("service", "acme.partnerapi.v1.PartnerWebhookApprovalService"),
		partnerOrderApprovalClient:         partnerapiv1.NewPartnerOrderApprovalServiceClient(deps.Conn),
		clients:                            deps.Clients,
		sellerWorkflowClient:               sellersv1.NewSellerWorkflowServiceClient(deps.Conn),
		meteredPartnerIntegrationReporting: meteredPartnerIntegrationReporting{PartnerIntegrationReportingServiceClient: partnerapiv1.NewPartnerIntegrationReportingServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		sellerPayoutScheduleApprovalClient: sellersv1.NewSellerPayoutScheduleApprovalServiceClient(deps.Conn),
		conn:                               deps.Conn,
		partnerWebhookValidationClient:     partnerapiv1.NewPartnerWebhookValidationServiceClient(deps.Conn),
		redemptionImportClient:             promotionsv1.NewRedemptionImportServiceClient(deps.Conn),
		ledgerAccountValidationClient:      ledgerv1.NewLedgerAccountValidationServiceClient(deps.Conn),
		partnerIntegrationReportingClient:  partnerapiv1.NewPartnerIntegrationReportingServiceClient(deps.Conn),
		meteredPartnerOrderApproval:        meteredPartnerOrderApproval{PartnerOrderApprovalServiceClient: partnerapiv1.NewPartnerOrderApprovalServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		pageCompositionQueryClient:         webbffv1.NewPageCompositionQueryServiceClient(deps.Conn),
		currencyRateApprovalClient:         pricingv1.NewCurrencyRateApprovalServiceClient(deps.Conn),
		apiCredentialHistoryClient:         identityv1.NewApiCredentialHistoryServiceClient(deps.Conn),
		financeWorkspaceQueryClient:        financeportalv1.NewFinanceWorkspaceQueryServiceClient(deps.Conn),
		customerSegmentSyncClient:          customersv1.NewCustomerSegmentSyncServiceClient(deps.Conn),
		customerSegmentSyncClientFallback:  customersv1.NewCustomerSegmentSyncServiceClient(deps.Conn),
	}
}

// SubmitPartnerWebhook submits a partner webhook.
func (s *PartnerWebhookApprovalServer) SubmitPartnerWebhook(ctx context.Context, req *partnerapiv1.SubmitPartnerWebhookRequest) (*partnerapiv1.SubmitPartnerWebhookResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePartnerWebhook(ctx, req.GetContext().GetTenantId(), req.GetPartnerWebhook()); err != nil {
		return nil, grpcx.Wrap(err, "save partner webhook")
	}
	s.log.InfoContext(ctx, "submit partner webhook", "tenant", req.GetContext().GetTenantId())
	var envelope partnerapiv1.PartnerIntegrationRef
	if err := req.GetPartnerWebhook().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	commentPartnerOrderApprovalReply, err := s.partnerOrderApprovalClient.CommentPartnerOrderApproval(ctx, &partnerapiv1.CommentPartnerOrderApprovalRequest{
		Context: req.GetContext(),
		Details: &partnerapiv1.CommentPartnerOrderApprovalRequest_Details{
			Approver: &identityv1.UserRef{
				Email:       req.GetDetails().GetApprover().GetEmail(),
				DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
				UserId:      req.GetDetails().GetApprover().GetUserId(),
			},
		},
		PartnerOrder: &partnerapiv1.PartnerOrder{
			Counterparty: &sellersv1.SellerRef{
				DisplayName: req.GetPartnerWebhook().GetSubject().GetDisplayName(),
			},
		},
		PartnerOrderId: envelope.GetDisplayName(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerOrderApprovalService/CommentPartnerOrderApproval")
	}
	partnerOrderApproval, err := registry.Lookup[partnerapiv1.PartnerOrderApprovalServiceClient](s.clients, "acme.partnerapi.v1.PartnerOrderApprovalService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	rejectPartnerOrderRequest := &partnerapiv1.RejectPartnerOrderRequest{
		Context: req.GetContext(),
		PartnerOrder: &partnerapiv1.PartnerOrder{
			Counterparty: &sellersv1.SellerRef{Email: req.GetDetails().GetApprover().GetEmail()},
			BillingAddress: &commonv1.PostalAddress{
				Lines: commentPartnerOrderApprovalReply.GetPartnerOrder().GetBillingAddress().GetLines(),
			},
		},
	}
	rejectPartnerOrderReply, err := partnerOrderApproval.RejectPartnerOrder(ctx, rejectPartnerOrderRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerOrderApprovalService/RejectPartnerOrder")
	}
	return &partnerapiv1.SubmitPartnerWebhookResponse{
		PartnerWebhook: &partnerapiv1.PartnerWebhook{
			Subject: &partnerapiv1.PartnerIntegrationRef{
				DisplayName: commentPartnerOrderApprovalReply.GetPartnerOrder().GetCounterparty().GetDisplayName(),
			},
			PartnerWebhookId: req.GetPartnerWebhookId(),
			Source:           req.GetPartnerWebhook().GetSource(),
		},
		Outcome: &partnerapiv1.SubmitPartnerWebhookResponse_Outcome{
			Message: commentPartnerOrderApprovalReply.GetOutcome().GetMessage(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: rejectPartnerOrderReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// ApprovePartnerWebhook approves a partner webhook.
func (s *PartnerWebhookApprovalServer) ApprovePartnerWebhook(ctx context.Context, req *partnerapiv1.ApprovePartnerWebhookRequest) (*partnerapiv1.ApprovePartnerWebhookResponse, error) {
	if err := s.store.SavePartnerWebhook(ctx, req.GetContext().GetTenantId(), req.GetPartnerWebhook()); err != nil {
		return nil, grpcx.Wrap(err, "save partner webhook")
	}
	s.log.InfoContext(ctx, "approve partner webhook", "tenant", req.GetContext().GetTenantId())
	for _, measurement := range req.GetPartnerWebhook().GetMeasurements() {
		if _, err := s.sellerWorkflowClient.ResumeSellerWorkflow(ctx, &sellersv1.ResumeSellerWorkflowRequest{
			Context: req.GetContext(),
			Seller: &sellersv1.Seller{
				Audit: &commonv1.AuditInfo{CreatedAt: measurement.GetObservedAt()},
				RelationshipManager: &identityv1.UserRef{
					DisplayName: req.GetPartnerWebhook().GetSubject().GetDisplayName(),
				},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerWorkflowService/ResumeSellerWorkflow")
		}
	}
	resolveFanSessionsRequest := &fanportalv1.ResolveFanSessionsRequest{Context: req.GetContext()}
	resolveFanSessionsReply, err := resolveFanSessionsCall(ctx, resolveFanSessionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fanportal.v1.FanSessionQueryService/ResolveFanSessions")
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*partnerapiv1.ApprovePartnerOrderResponse, len(req.GetPartnerWebhook().GetMeasurements()))
	for i, measurement2 := range req.GetPartnerWebhook().GetMeasurements() {
		group.Go(func() error {
			reply, err := s.partnerOrderApprovalClient.ApprovePartnerOrder(groupCtx, &partnerapiv1.ApprovePartnerOrderRequest{
				Context: req.GetContext(),
				PartnerOrder: &partnerapiv1.PartnerOrder{
					Audit: &commonv1.AuditInfo{UpdatedAt: measurement2.GetObservedAt()},
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
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerOrderApprovalService/ApprovePartnerOrder")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	var envelope partnerapiv1.PartnerIntegrationRef
	if err := req.GetPartnerWebhook().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	sharePartnerIntegrationReportRequest := &partnerapiv1.SharePartnerIntegrationReportRequest{
		Context:              req.GetContext(),
		PartnerIntegrationId: envelope.GetDisplayName(),
	}
	sharePartnerIntegrationReportReply, err := s.meteredPartnerIntegrationReporting.SharePartnerIntegrationReport(ctx, sharePartnerIntegrationReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerIntegrationReportingService/SharePartnerIntegrationReport")
	}
	return &partnerapiv1.ApprovePartnerWebhookResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: resolveFanSessionsReply.GetResult().GetCorrelationId(),
			ReferenceIds:  resolveFanSessionsReply.GetResult().GetReferenceIds(),
			Status:        "ok",
		},
		PartnerWebhook: &partnerapiv1.PartnerWebhook{
			PartnerWebhookId: sharePartnerIntegrationReportReply.GetPartnerIntegration().GetHolder().GetPartnerWebhookId(),
			Kind:             req.GetPartnerWebhook().GetKind(),
			Subject:          req.GetPartnerWebhook().GetSubject(),
		},
	}, nil
}

// RejectPartnerWebhook rejects a partner webhook.
func (s *PartnerWebhookApprovalServer) RejectPartnerWebhook(ctx context.Context, req *partnerapiv1.RejectPartnerWebhookRequest) (*partnerapiv1.RejectPartnerWebhookResponse, error) {
	if err := s.store.SavePartnerWebhook(ctx, req.GetContext().GetTenantId(), req.GetPartnerWebhook()); err != nil {
		return nil, grpcx.Wrap(err, "save partner webhook")
	}
	s.log.InfoContext(ctx, "reject partner webhook", "tenant", req.GetContext().GetTenantId())
	for measurement := range iterx.Each(req.GetPartnerWebhook().GetMeasurements(), func(candidate *partnerapiv1.PartnerWebhookMeasurement) bool { return candidate != nil }) {
		if _, err := s.partnerOrderApprovalClient.SubmitPartnerOrder(ctx, &partnerapiv1.SubmitPartnerOrderRequest{
			Context: req.GetContext(),
			PartnerOrder: &partnerapiv1.PartnerOrder{
				Counterparty: &sellersv1.SellerRef{
					DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
					Email:       req.GetDetails().GetApprover().GetEmail(),
				},
				IssuedAt: measurement.GetObservedAt(),
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerOrderApprovalService/SubmitPartnerOrder")
		}
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*sellersv1.ListPendingSellerPayoutSchedulesResponse, len(req.GetPartnerWebhook().GetMeasurements()))
	for i, measurement2 := range req.GetPartnerWebhook().GetMeasurements() {
		group.Go(func() error {
			reply, err := s.sellerPayoutScheduleApprovalClient.ListPendingSellerPayoutSchedules(groupCtx, &sellersv1.ListPendingSellerPayoutSchedulesRequest{
				Context: req.GetContext(),
				Filter:  &sellersv1.ListPendingSellerPayoutSchedulesRequest_Filter{CreatedAfter: measurement2.GetObservedAt()},
			})
			if err != nil {
				return err
			}
			replies[i] = reply
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerPayoutScheduleApprovalService/ListPendingSellerPayoutSchedules")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	listPendingPartnerOrdersRequest := &partnerapiv1.ListPendingPartnerOrdersRequest{Context: req.GetContext()}
	listPendingPartnerOrdersReply, err := s.partnerOrderApprovalLazy().ListPendingPartnerOrders(ctx, listPendingPartnerOrdersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerOrderApprovalService/ListPendingPartnerOrders")
	}
	return &partnerapiv1.RejectPartnerWebhookResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: listPendingPartnerOrdersReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		PartnerWebhook: &partnerapiv1.PartnerWebhook{
			PartnerWebhookId: req.GetPartnerWebhookId(),
			Attributes:       req.GetPartnerWebhook().GetAttributes(),
		},
	}, nil
}

// EscalatePartnerWebhook escalates a partner webhook.
func (s *PartnerWebhookApprovalServer) EscalatePartnerWebhook(ctx context.Context, req *partnerapiv1.EscalatePartnerWebhookRequest) (*partnerapiv1.EscalatePartnerWebhookResponse, error) {
	if err := s.store.SavePartnerWebhook(ctx, req.GetContext().GetTenantId(), req.GetPartnerWebhook()); err != nil {
		return nil, grpcx.Wrap(err, "save partner webhook")
	}
	s.log.InfoContext(ctx, "escalate partner webhook", "tenant", req.GetContext().GetTenantId())
	jobs := make(chan *partnerapiv1.PartnerWebhookMeasurement)
	failures := make(chan error, 4)
	workCtx, stopWork := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for measurement := range jobs {
				if _, err := s.partnerOrderApprovalClient.ApprovePartnerOrder(workCtx, &partnerapiv1.ApprovePartnerOrderRequest{
					Context: req.GetContext(),
					PartnerOrder: &partnerapiv1.PartnerOrder{
						Counterparty: &sellersv1.SellerRef{Email: req.GetDetails().GetApprover().GetEmail()},
						Audit:        &commonv1.AuditInfo{UpdatedAt: measurement.GetObservedAt()},
					},
				}); err != nil {
					failures <- err
					stopWork()
					return
				}
			}
		}()
	}
	for _, queued := range req.GetPartnerWebhook().GetMeasurements() {
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
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerOrderApprovalService/ApprovePartnerOrder")
	}
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := partnerWebhookApprovalServerPreviewPartnerWebhookSteps[mode]
	encoded, err := protojson.Marshal(req.GetPartnerWebhook())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded partnerapiv1.PartnerWebhook
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	previewPartnerWebhookRequest := &partnerapiv1.PreviewPartnerWebhookRequest{
		Context: req.GetContext(),
		PartnerWebhook: &partnerapiv1.PartnerWebhook{
			Subject: &partnerapiv1.PartnerIntegrationRef{CurrencyCode: (&decoded).GetSubject().GetCurrencyCode()},
		},
	}
	previewPartnerWebhookReply, err := run(s, ctx, previewPartnerWebhookRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookValidationService/PreviewPartnerWebhook")
	}
	for _, measurement2 := range req.GetPartnerWebhook().GetMeasurements() {
		if _, err := s.partnerWebhookValidationClient.LintPartnerWebhooks(ctx, &partnerapiv1.LintPartnerWebhooksRequest{
			Context: req.GetContext(),
			PartnerWebhook: &partnerapiv1.PartnerWebhook{
				Measurements: []*partnerapiv1.PartnerWebhookMeasurement{&partnerapiv1.PartnerWebhookMeasurement{
					Unit:       measurement2.GetUnit(),
					Value:      measurement2.GetValue(),
					ObservedAt: measurement2.GetObservedAt(),
					Metric:     measurement2.GetMetric(),
				}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookValidationService/LintPartnerWebhooks")
		}
	}
	for _, measurement3 := range req.GetPartnerWebhook().GetMeasurements() {
		if _, err := s.redemptionImportClient.ValidateRedemptionImport(ctx, &promotionsv1.ValidateRedemptionImportRequest{
			Context: req.GetContext(),
			Redemption: &promotionsv1.Redemption{
				Measurements: []*promotionsv1.RedemptionMeasurement{&promotionsv1.RedemptionMeasurement{
					Metric: measurement3.GetMetric(),
					Value:  measurement3.GetValue(),
				}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.promotions.v1.RedemptionImportService/ValidateRedemptionImport")
		}
	}
	return &partnerapiv1.EscalatePartnerWebhookResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: previewPartnerWebhookReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		PartnerWebhook: &partnerapiv1.PartnerWebhook{
			PartnerWebhookId: req.GetPartnerWebhookId(),
			Sequence:         req.GetPartnerWebhook().GetSequence(),
		},
	}, nil
}

// ListPendingPartnerWebhooks lists partner webhooks a page at a time.
func (s *PartnerWebhookApprovalServer) ListPendingPartnerWebhooks(ctx context.Context, req *partnerapiv1.ListPendingPartnerWebhooksRequest) (*partnerapiv1.ListPendingPartnerWebhooksResponse, error) {
	s.log.InfoContext(ctx, "list pending partner webhooks", "tenant", req.GetContext().GetTenantId())
	normalizeLedgerAccountRequest := buildNormalizeLedgerAccountRequestForListPendingPartnerWebhooks(req)
	normalizeLedgerAccountReply, err := s.ledgerAccountValidationClient.NormalizeLedgerAccount(ctx, normalizeLedgerAccountRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountValidationService/NormalizeLedgerAccount")
	}
	return &partnerapiv1.ListPendingPartnerWebhooksResponse{
		Result: &commonv1.OperationResult{
			Details:      normalizeLedgerAccountReply.GetResult().GetDetails(),
			ReferenceIds: normalizeLedgerAccountReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// DelegatePartnerWebhookApproval delegates a partner webhook.
func (s *PartnerWebhookApprovalServer) DelegatePartnerWebhookApproval(ctx context.Context, req *partnerapiv1.DelegatePartnerWebhookApprovalRequest) (*partnerapiv1.DelegatePartnerWebhookApprovalResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePartnerWebhook(ctx, req.GetContext().GetTenantId(), req.GetPartnerWebhook()); err != nil {
		return nil, grpcx.Wrap(err, "save partner webhook")
	}
	s.log.InfoContext(ctx, "delegate partner webhook approval", "tenant", req.GetContext().GetTenantId())
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := partnerWebhookApprovalServerCountPartnerIntegrationTotalsSteps[mode]
	countPartnerIntegrationTotalsRequest := &partnerapiv1.CountPartnerIntegrationTotalsRequest{
		Context: req.GetContext(),
		Filter:  &partnerapiv1.CountPartnerIntegrationTotalsRequest_Filter{CreatedAfter: req.GetPartnerWebhook().GetOccurredAt()},
	}
	countPartnerIntegrationTotalsReply, err := run(s, ctx, countPartnerIntegrationTotalsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerIntegrationReportingService/CountPartnerIntegrationTotals")
	}
	cloned := proto.Clone(req.GetDetails()).(*partnerapiv1.DelegatePartnerWebhookApprovalRequest_Details)
	cloned2 := proto.Clone(req.GetDetails()).(*partnerapiv1.DelegatePartnerWebhookApprovalRequest_Details)
	var envelope partnerapiv1.PartnerIntegrationRef
	if err := req.GetPartnerWebhook().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	delegatePartnerOrderApprovalRequest := &partnerapiv1.DelegatePartnerOrderApprovalRequest{
		Context: req.GetContext(),
		Details: &partnerapiv1.DelegatePartnerOrderApprovalRequest_Details{
			Approver: &identityv1.UserRef{
				DisplayName: req.GetPartnerWebhook().GetSubject().GetDisplayName(),
				Email:       cloned.GetApprover().GetEmail(),
			},
			EffectiveAt: cloned2.GetEffectiveAt(),
		},
		PartnerOrderId: envelope.GetDisplayName(),
	}
	delegatePartnerOrderApprovalReply, err := s.meteredPartnerOrderApproval.DelegatePartnerOrderApproval(ctx, delegatePartnerOrderApprovalRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerOrderApprovalService/DelegatePartnerOrderApproval")
	}
	var envelope2 partnerapiv1.PartnerIntegrationRef
	if err := req.GetPartnerWebhook().GetDetail().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	suggestPageCompositionsRequest := &webbffv1.SuggestPageCompositionsRequest{
		Context: req.GetContext(),
		Filter: &webbffv1.SuggestPageCompositionsRequest_Filter{
			Region:       delegatePartnerOrderApprovalReply.GetPartnerOrder().GetBillingAddress().GetRegion(),
			CreatedAfter: delegatePartnerOrderApprovalReply.GetPartnerOrder().GetAudit().GetCreatedAt(),
			OwnerId:      delegatePartnerOrderApprovalReply.GetResult().GetCorrelationId(),
		},
		Page: &commonv1.PageRequest{PageToken: envelope2.GetDisplayName()},
	}
	suggestPageCompositionsReply, err := grpcx.Call(ctx, s.pageCompositionQueryClient.SuggestPageCompositions, suggestPageCompositionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.webbff.v1.PageCompositionQueryService/SuggestPageCompositions")
	}
	return &partnerapiv1.DelegatePartnerWebhookApprovalResponse{
		PartnerWebhook: &partnerapiv1.PartnerWebhook{
			CorrelationId: countPartnerIntegrationTotalsReply.GetResult().GetCorrelationId(),
			Subject: &partnerapiv1.PartnerIntegrationRef{
				DisplayName: delegatePartnerOrderApprovalReply.GetPartnerOrder().GetCounterparty().GetDisplayName(),
			},
			PartnerWebhookId: req.GetPartnerWebhookId(),
			Attributes:       req.GetPartnerWebhook().GetAttributes(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: countPartnerIntegrationTotalsReply.GetResult().GetCorrelationId(),
			Message:       delegatePartnerOrderApprovalReply.GetOutcome().GetMessage(),
			Details:       suggestPageCompositionsReply.GetResult().GetDetails(),
			Status:        "ok",
		},
	}, nil
}

// RecallPartnerWebhook recalls a partner webhook.
func (s *PartnerWebhookApprovalServer) RecallPartnerWebhook(ctx context.Context, req *partnerapiv1.RecallPartnerWebhookRequest) (*partnerapiv1.RecallPartnerWebhookResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SavePartnerWebhook(ctx, req.GetContext().GetTenantId(), req.GetPartnerWebhook()); err != nil {
		return nil, grpcx.Wrap(err, "save partner webhook")
	}
	s.log.InfoContext(ctx, "recall partner webhook", "tenant", req.GetContext().GetTenantId())
	for _, measurement := range req.GetPartnerWebhook().GetMeasurements() {
		if _, err := s.currencyRateApprovalClient.RejectCurrencyRate(ctx, &pricingv1.RejectCurrencyRateRequest{
			Context: req.GetContext(),
			CurrencyRate: &pricingv1.CurrencyRate{
				Measurements: []*pricingv1.CurrencyRateMeasurement{&pricingv1.CurrencyRateMeasurement{
					ObservedAt: measurement.GetObservedAt(),
					Metric:     measurement.GetMetric(),
					Value:      measurement.GetValue(),
					Unit:       measurement.GetUnit(),
				}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.pricing.v1.CurrencyRateApprovalService/RejectCurrencyRate")
		}
	}
	submitStockItemRequest := &inventoryv1.SubmitStockItemRequest{
		Context: req.GetContext(),
		StockItem: &inventoryv1.StockItem{
			Owner: &identityv1.UserRef{Email: req.GetDetails().GetApprover().GetEmail()},
		},
	}
	submitStockItemReply := new(inventoryv1.SubmitStockItemResponse)
	if err := grpcx.LegacyInvoke(ctx, s.conn, "/acme.inventory.v1.StockItemApprovalService/SubmitStockItem", submitStockItemRequest, submitStockItemReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.StockItemApprovalService/SubmitStockItem")
	}
	return &partnerapiv1.RecallPartnerWebhookResponse{
		Result: &commonv1.OperationResult{Status: submitStockItemReply.GetOutcome().GetStatus()},
		PartnerWebhook: &partnerapiv1.PartnerWebhook{
			PartnerWebhookId: req.GetPartnerWebhookId(),
			CorrelationId:    req.GetPartnerWebhook().GetCorrelationId(),
		},
	}, nil
}

// GetPartnerWebhookApprovalStatus returns a partner webhook.
func (s *PartnerWebhookApprovalServer) GetPartnerWebhookApprovalStatus(ctx context.Context, req *partnerapiv1.GetPartnerWebhookApprovalStatusRequest) (*partnerapiv1.GetPartnerWebhookApprovalStatusResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "partner_webhook", req.GetPartnerWebhookId(), "get_partner_webhook_approval_status"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get partner webhook approval status", "tenant", req.GetContext().GetTenantId())
	snapshot, err := protojson.Marshal(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	annotateApiCredentialVersionReply, err := s.apiCredentialHistoryClient.AnnotateApiCredentialVersion(ctx, &identityv1.AnnotateApiCredentialVersionRequest{
		Context: req.GetContext(),
		ApiCredential: &identityv1.ApiCredential{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Audit:   &commonv1.AuditInfo{UpdatedAt: req.GetContext().GetDeadline()},
		},
		Details: &identityv1.AnnotateApiCredentialVersionRequest_Details{Snapshot: snapshot},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.ApiCredentialHistoryService/AnnotateApiCredentialVersion")
	}
	return &partnerapiv1.GetPartnerWebhookApprovalStatusResponse{
		PartnerWebhook: &partnerapiv1.PartnerWebhook{
			CorrelationId:    annotateApiCredentialVersionReply.GetResult().GetCorrelationId(),
			PartnerWebhookId: req.GetPartnerWebhookId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CommentPartnerWebhookApproval comments a partner webhook.
func (s *PartnerWebhookApprovalServer) CommentPartnerWebhookApproval(ctx context.Context, req *partnerapiv1.CommentPartnerWebhookApprovalRequest) (*partnerapiv1.CommentPartnerWebhookApprovalResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SavePartnerWebhook(ctx, req.GetContext().GetTenantId(), req.GetPartnerWebhook()); err != nil {
		return nil, grpcx.Wrap(err, "save partner webhook")
	}
	s.log.InfoContext(ctx, "comment partner webhook approval", "tenant", req.GetContext().GetTenantId())
	var envelope partnerapiv1.PartnerIntegrationRef
	if err := req.GetPartnerWebhook().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	delegatePartnerOrderApprovalReply, err := s.partnerOrderApprovalClient.DelegatePartnerOrderApproval(ctx, &partnerapiv1.DelegatePartnerOrderApprovalRequest{
		Context: req.GetContext(),
		PartnerOrder: &partnerapiv1.PartnerOrder{
			Counterparty: &sellersv1.SellerRef{
				DisplayName: req.GetPartnerWebhook().GetSubject().GetDisplayName(),
			},
		},
		Details: &partnerapiv1.DelegatePartnerOrderApprovalRequest_Details{
			Approver: &identityv1.UserRef{
				DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
			},
		},
		PartnerOrderId: envelope.GetDisplayName(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerOrderApprovalService/DelegatePartnerOrderApproval")
	}
	var envelope2 partnerapiv1.PartnerIntegrationRef
	if err := req.GetPartnerWebhook().GetDetail().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	summarizeFinanceWorkspacesReply, err := s.financeWorkspaceQueryClient.SummarizeFinanceWorkspaces(ctx, &financeportalv1.SummarizeFinanceWorkspacesRequest{
		Context:            req.GetContext(),
		Annotations:        req.GetAnnotations(),
		FinanceWorkspaceId: envelope2.GetDisplayName(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.financeportal.v1.FinanceWorkspaceQueryService/SummarizeFinanceWorkspaces")
	}
	routes := map[string]func(context.Context, *customersv1.PushCustomerSegmentRequest, ...grpc.CallOption) (*customersv1.PushCustomerSegmentResponse, error){
		"primary":  s.customerSegmentSyncClient.PushCustomerSegment,
		"fallback": s.customerSegmentSyncClientFallback.PushCustomerSegment,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	snapshot, err := protojson.Marshal(req.GetPartnerWebhook())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	pushCustomerSegmentRequest := &customersv1.PushCustomerSegmentRequest{
		Context: req.GetContext(),
		CustomerSegment: &customersv1.CustomerSegment{
			Audit: &commonv1.AuditInfo{
				Etag:      delegatePartnerOrderApprovalReply.GetPartnerOrder().GetAudit().GetEtag(),
				CreatedAt: delegatePartnerOrderApprovalReply.GetPartnerOrder().GetAudit().GetCreatedAt(),
			},
			Owner: &identityv1.UserRef{
				DisplayName: req.GetPartnerWebhook().GetSubject().GetDisplayName(),
			},
		},
		Details: &customersv1.PushCustomerSegmentRequest_Details{Payload: snapshot},
	}
	pushCustomerSegmentReply, err := route(ctx, pushCustomerSegmentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentSyncService/PushCustomerSegment")
	}
	return &partnerapiv1.CommentPartnerWebhookApprovalResponse{
		Outcome: &partnerapiv1.CommentPartnerWebhookApprovalResponse_Outcome{
			ReferenceId: delegatePartnerOrderApprovalReply.GetOutcome().GetReferenceId(),
			Status:      delegatePartnerOrderApprovalReply.GetOutcome().GetStatus(),
			Message:     summarizeFinanceWorkspacesReply.GetResult().GetMessage(),
			NextSteps:   pushCustomerSegmentReply.GetOutcome().GetNextSteps(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: summarizeFinanceWorkspacesReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		PartnerWebhook: &partnerapiv1.PartnerWebhook{
			PartnerWebhookId: req.GetPartnerWebhookId(),
			CorrelationId:    req.GetPartnerWebhook().GetCorrelationId(),
		},
	}, nil
}

// partnerOrderApprovalLazy dials acme.partnerapi.v1.PartnerOrderApprovalService the first time a handler needs it.
func (s *PartnerWebhookApprovalServer) partnerOrderApprovalLazy() partnerapiv1.PartnerOrderApprovalServiceClient {
	s.partnerOrderApprovalOnce.Do(func() { s.partnerOrderApprovalCached = partnerapiv1.NewPartnerOrderApprovalServiceClient(s.conn) })
	return s.partnerOrderApprovalCached
}

// partnerWebhookApprovalServerPreviewPartnerWebhookSteps are the ways to run
// acme.partnerapi.v1.PartnerWebhookValidationService/PreviewPartnerWebhook, by name.
var partnerWebhookApprovalServerPreviewPartnerWebhookSteps = map[string]func(*PartnerWebhookApprovalServer, context.Context, *partnerapiv1.PreviewPartnerWebhookRequest) (*partnerapiv1.PreviewPartnerWebhookResponse, error){
	"default": (*PartnerWebhookApprovalServer).previewPartnerWebhookStep,
	"audited": (*PartnerWebhookApprovalServer).previewPartnerWebhookAuditedStep,
}

func (s *PartnerWebhookApprovalServer) previewPartnerWebhookStep(ctx context.Context, request *partnerapiv1.PreviewPartnerWebhookRequest) (*partnerapiv1.PreviewPartnerWebhookResponse, error) {
	return s.partnerWebhookValidationClient.PreviewPartnerWebhook(ctx, request)
}

func (s *PartnerWebhookApprovalServer) previewPartnerWebhookAuditedStep(ctx context.Context, request *partnerapiv1.PreviewPartnerWebhookRequest) (*partnerapiv1.PreviewPartnerWebhookResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.partnerapi.v1.PartnerWebhookValidationService/PreviewPartnerWebhook")
	return s.partnerWebhookValidationClient.PreviewPartnerWebhook(ctx, request)
}

// partnerWebhookApprovalServerCountPartnerIntegrationTotalsSteps are the ways to run
// acme.partnerapi.v1.PartnerIntegrationReportingService/CountPartnerIntegrationTotals, by name.
var partnerWebhookApprovalServerCountPartnerIntegrationTotalsSteps = map[string]func(*PartnerWebhookApprovalServer, context.Context, *partnerapiv1.CountPartnerIntegrationTotalsRequest) (*partnerapiv1.CountPartnerIntegrationTotalsResponse, error){
	"default": (*PartnerWebhookApprovalServer).countPartnerIntegrationTotalsStep,
	"audited": (*PartnerWebhookApprovalServer).countPartnerIntegrationTotalsAuditedStep,
}

func (s *PartnerWebhookApprovalServer) countPartnerIntegrationTotalsStep(ctx context.Context, request *partnerapiv1.CountPartnerIntegrationTotalsRequest) (*partnerapiv1.CountPartnerIntegrationTotalsResponse, error) {
	return s.partnerIntegrationReportingClient.CountPartnerIntegrationTotals(ctx, request)
}

func (s *PartnerWebhookApprovalServer) countPartnerIntegrationTotalsAuditedStep(ctx context.Context, request *partnerapiv1.CountPartnerIntegrationTotalsRequest) (*partnerapiv1.CountPartnerIntegrationTotalsResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.partnerapi.v1.PartnerIntegrationReportingService/CountPartnerIntegrationTotals")
	return s.partnerIntegrationReportingClient.CountPartnerIntegrationTotals(ctx, request)
}
