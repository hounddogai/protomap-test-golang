package orders

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	auditv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audit/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	inventoryv1 "github.com/hounddogai/protomap-test-golang/gen/acme/inventory/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	ordersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/orders/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// BackorderSyncServer implements `acme.orders.v1.BackorderSyncService`.
// Keeps backorders in step with the systems of record in other regions.
type BackorderSyncServer struct {
	ordersv1.UnimplementedBackorderSyncServiceServer
	store                             *Store
	log                               *slog.Logger
	orderAmendmentCalculationClient   ordersv1.OrderAmendmentCalculationServiceClient
	stockItemApprovalClient           inventoryv1.StockItemApprovalServiceClient
	orderValidationClient             ordersv1.OrderValidationServiceClient
	userValidationClient              identityv1.UserValidationServiceClient
	orderAmendmentCalculationPort     orderAmendmentCalculationPort
	orderAmendmentValidationPort      orderAmendmentValidationPort
	auditEventQueryClient             auditv1.AuditEventQueryServiceClient
	auditEventQueryClientFallback     auditv1.AuditEventQueryServiceClient
	giftCardTransactionWorkflowClient giftcardsv1.GiftCardTransactionWorkflowServiceClient
	orderNoteValidationClient         ordersv1.OrderNoteValidationServiceClient
	backorderCalculationClient        ordersv1.BackorderCalculationServiceClient
	riskAssessmentWorkflowClient      fraudv1.RiskAssessmentWorkflowServiceClient
	meteredMerchandisingRuleWorkflow  meteredMerchandisingRuleWorkflow
	fraudRuleApprovalClient           fraudv1.FraudRuleApprovalServiceClient
	householdValidationClient         customersv1.HouseholdValidationServiceClient
	meteredOrderAmendmentCalculation  meteredOrderAmendmentCalculation
	orderCalculationClient            ordersv1.OrderCalculationServiceClient
	clients                           *registry.Registry
}

// NewBackorderSyncServer wires a BackorderSyncServer.
func NewBackorderSyncServer(deps platform.Deps) *BackorderSyncServer {
	return &BackorderSyncServer{
		store:                             NewStore(deps.DB),
		log:                               deps.Logger.With("service", "acme.orders.v1.BackorderSyncService"),
		orderAmendmentCalculationClient:   ordersv1.NewOrderAmendmentCalculationServiceClient(deps.Conn),
		stockItemApprovalClient:           inventoryv1.NewStockItemApprovalServiceClient(deps.Conn),
		orderValidationClient:             ordersv1.NewOrderValidationServiceClient(deps.Conn),
		userValidationClient:              identityv1.NewUserValidationServiceClient(deps.Conn),
		orderAmendmentCalculationPort:     newOrderAmendmentCalculationPort(deps),
		orderAmendmentValidationPort:      newOrderAmendmentValidationPort(deps),
		auditEventQueryClient:             auditv1.NewAuditEventQueryServiceClient(deps.Conn),
		auditEventQueryClientFallback:     auditv1.NewAuditEventQueryServiceClient(deps.Conn),
		giftCardTransactionWorkflowClient: giftcardsv1.NewGiftCardTransactionWorkflowServiceClient(deps.Conn),
		orderNoteValidationClient:         ordersv1.NewOrderNoteValidationServiceClient(deps.Conn),
		backorderCalculationClient:        ordersv1.NewBackorderCalculationServiceClient(deps.Conn),
		riskAssessmentWorkflowClient:      fraudv1.NewRiskAssessmentWorkflowServiceClient(deps.Conn),
		meteredMerchandisingRuleWorkflow:  meteredMerchandisingRuleWorkflow{MerchandisingRuleWorkflowServiceClient: merchandisingv1.NewMerchandisingRuleWorkflowServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		fraudRuleApprovalClient:           fraudv1.NewFraudRuleApprovalServiceClient(deps.Conn),
		householdValidationClient:         customersv1.NewHouseholdValidationServiceClient(deps.Conn),
		meteredOrderAmendmentCalculation:  meteredOrderAmendmentCalculation{OrderAmendmentCalculationServiceClient: ordersv1.NewOrderAmendmentCalculationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		orderCalculationClient:            ordersv1.NewOrderCalculationServiceClient(deps.Conn),
		clients:                           deps.Clients,
	}
}

// SyncBackorders syncs a backorder.
func (s *BackorderSyncServer) SyncBackorders(ctx context.Context, req *ordersv1.SyncBackordersRequest) (*ordersv1.SyncBackordersResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveBackorder(ctx, req.GetContext().GetTenantId(), req.GetBackorder()); err != nil {
		return nil, grpcx.Wrap(err, "save backorder")
	}
	s.log.InfoContext(ctx, "sync backorders", "tenant", req.GetContext().GetTenantId(), "etag", req.GetBackorder().GetAudit().GetEtag())
	updateSearchIndexRequest := &searchv1.UpdateSearchIndexRequest{
		Context: req.GetContext(),
		SearchIndex: &searchv1.SearchIndex{
			Audit: &commonv1.AuditInfo{UpdatedAt: req.GetBackorder().GetAudit().GetUpdatedAt()},
		},
	}
	updateSearchIndexReply, err := updateSearchIndexCall(ctx, updateSearchIndexRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexService/UpdateSearchIndex")
	}
	calculateOrderAmendmentRequest := buildCalculateOrderAmendmentRequestForSyncBackorders(req)
	calculateOrderAmendmentReply, err := s.orderAmendmentCalculationClient.CalculateOrderAmendment(ctx, calculateOrderAmendmentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderAmendmentCalculationService/CalculateOrderAmendment")
	}
	recallStockItemRequest := &inventoryv1.RecallStockItemRequest{
		Context: req.GetContext(),
		StockItem: &inventoryv1.StockItem{
			Locator: &inventoryv1.StockItem_Address{Address: &commonv1.PostalAddress{
				CountryCode: calculateOrderAmendmentReply.GetOrderAmendment().GetBillingAddress().GetCountryCode(),
			}},
		},
	}
	var recallStockItemReply *inventoryv1.RecallStockItemResponse
	if err := pipeline.Run(ctx, "acme.inventory.v1.StockItemApprovalService/RecallStockItem", func(ctx context.Context) error {
		answer, err := s.stockItemApprovalClient.RecallStockItem(ctx, recallStockItemRequest)
		recallStockItemReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.StockItemApprovalService/RecallStockItem")
	}
	return &ordersv1.SyncBackordersResponse{
		Backorder: &ordersv1.Backorder{
			Assignee: &identityv1.UserRef{
				DisplayName: updateSearchIndexReply.GetSearchIndex().GetOwner().GetDisplayName(),
				UserId:      recallStockItemReply.GetStockItem().GetOwner().GetUserId(),
			},
			BackorderId: req.GetBackorderId(),
			Priority:    req.GetBackorder().GetPriority(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: calculateOrderAmendmentReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// PushBackorder pushs a backorder.
func (s *BackorderSyncServer) PushBackorder(ctx context.Context, req *ordersv1.PushBackorderRequest) (*ordersv1.PushBackorderResponse, error) {
	if err := s.store.SaveBackorder(ctx, req.GetContext().GetTenantId(), req.GetBackorder()); err != nil {
		return nil, grpcx.Wrap(err, "save backorder")
	}
	s.log.InfoContext(ctx, "push backorder", "tenant", req.GetContext().GetTenantId(), "etag", req.GetBackorder().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetBackorder()).(*ordersv1.Backorder)
	validateOrderRequest := &ordersv1.ValidateOrderRequest{}
	validateOrderRequest.Context = req.GetContext()
	validateOrderRequest.Details = &ordersv1.ValidateOrderRequest_Details{
		Contact: &commonv1.ContactInfo{
			Name:  req.GetBackorder().GetRequester().GetName(),
			Email: cloned.GetAssignee().GetEmail(),
		},
	}
	validateOrderRequest.Order = &ordersv1.Order{
		Counterparty: &customersv1.CustomerRef{
			CustomerId: req.GetBackorder().GetCustomer().GetCustomerId(),
		},
	}
	validateOrderReply, err := s.orderValidationClient.ValidateOrder(ctx, validateOrderRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderValidationService/ValidateOrder")
	}
	getUserRulesRequest := &identityv1.GetUserRulesRequest{
		Context: req.GetContext(),
		UserId:  req.GetBackorder().GetAssignee().GetUserId(),
	}
	getUserRulesReply, err := grpcx.Call(ctx, s.userValidationClient.GetUserRules, getUserRulesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserValidationService/GetUserRules")
	}
	return &ordersv1.PushBackorderResponse{
		Result: &commonv1.OperationResult{
			Details: validateOrderReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Backorder: &ordersv1.Backorder{
			Requester: &commonv1.ContactInfo{
				PreferredChannel: getUserRulesReply.GetUser().GetContact().GetPreferredChannel(),
				Email:            getUserRulesReply.GetUser().GetContact().GetEmail(),
			},
			BackorderId: req.GetBackorderId(),
			Status:      req.GetBackorder().GetStatus(),
		},
	}, nil
}

// PullBackorders pulls a backorder.
func (s *BackorderSyncServer) PullBackorders(ctx context.Context, req *ordersv1.PullBackordersRequest) (*ordersv1.PullBackordersResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveBackorder(ctx, req.GetContext().GetTenantId(), req.GetBackorder()); err != nil {
		return nil, grpcx.Wrap(err, "save backorder")
	}
	s.log.InfoContext(ctx, "pull backorders", "tenant", req.GetContext().GetTenantId(), "etag", req.GetBackorder().GetAudit().GetEtag())
	source := req.GetBackorder().GetAssignee().ProtoReflect()
	displayName := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("display_name"))).String()
	calculateOrderAmendmentRequest := &ordersv1.CalculateOrderAmendmentRequest{
		Context: req.GetContext(),
		OrderAmendment: &ordersv1.OrderAmendment{
			Counterparty: &ordersv1.BackorderRef{DisplayName: displayName},
			Audit: &commonv1.AuditInfo{
				Etag:      req.GetBackorder().GetAudit().GetEtag(),
				UpdatedBy: req.GetBackorder().GetAudit().GetUpdatedBy(),
			},
		},
	}
	calculateOrderAmendmentReply, err := s.orderAmendmentCalculationPort.CalculateOrderAmendment(ctx, calculateOrderAmendmentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderAmendmentCalculationService/CalculateOrderAmendment")
	}
	return &ordersv1.PullBackordersResponse{
		Backorder: &ordersv1.Backorder{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: calculateOrderAmendmentReply.GetOrderAmendment().GetAudit().GetUpdatedAt(),
			},
			BackorderId:  req.GetBackorderId(),
			CustomFields: req.GetBackorder().GetCustomFields(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ReconcileBackorders reconciles a backorder.
func (s *BackorderSyncServer) ReconcileBackorders(ctx context.Context, req *ordersv1.ReconcileBackordersRequest) (*ordersv1.ReconcileBackordersResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ReconcileBackorders", "error", err)
		return nil, err
	}
	if err := s.store.SaveBackorder(ctx, req.GetContext().GetTenantId(), req.GetBackorder()); err != nil {
		return nil, grpcx.Wrap(err, "save backorder")
	}
	s.log.InfoContext(ctx, "reconcile backorders", "tenant", req.GetContext().GetTenantId(), "etag", req.GetBackorder().GetAudit().GetEtag())
	validateOrderAmendmentRequest := &ordersv1.ValidateOrderAmendmentRequest{
		Context: req.GetContext(),
		OrderAmendment: &ordersv1.OrderAmendment{
			Audit: &commonv1.AuditInfo{
				CreatedAt: req.GetBackorder().GetAudit().GetCreatedAt(),
				UpdatedBy: req.GetBackorder().GetAudit().GetUpdatedBy(),
				UpdatedAt: req.GetBackorder().GetAudit().GetUpdatedAt(),
			},
		},
		Details: &ordersv1.ValidateOrderAmendmentRequest_Details{
			Contact: &commonv1.ContactInfo{Phone: req.GetBackorder().GetRequester().GetPhone()},
		},
	}
	validateOrderAmendmentReply, err := s.orderAmendmentValidationPort.ValidateOrderAmendment(ctx, validateOrderAmendmentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderAmendmentValidationService/ValidateOrderAmendment")
	}
	routes := map[string]func(context.Context, *auditv1.BrowseAuditEventsRequest, ...grpc.CallOption) (*auditv1.BrowseAuditEventsResponse, error){
		"primary":  s.auditEventQueryClient.BrowseAuditEvents,
		"fallback": s.auditEventQueryClientFallback.BrowseAuditEvents,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	var holder struct{ PolicyId string }
	if record := req.GetBackorder().GetSla(); record != nil {
		if err := mapper.CopyFields(&holder, record, "PolicyId"); err != nil {
			s.log.WarnContext(ctx, "copy policy_id", "error", err)
		}
	}
	browseAuditEventsRequest := &auditv1.BrowseAuditEventsRequest{
		Context: req.GetContext(),
		Filter: &auditv1.BrowseAuditEventsRequest_Filter{
			Region:       validateOrderAmendmentReply.GetOrderAmendment().GetBillingAddress().GetRegion(),
			CreatedAfter: req.GetBackorder().GetAudit().GetUpdatedAt(),
			OwnerId:      holder.PolicyId,
			Labels:       validateOrderAmendmentReply.GetOrderAmendment().GetMetadata(),
		},
	}
	browseAuditEventsReply, err := route(ctx, browseAuditEventsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.audit.v1.AuditEventQueryService/BrowseAuditEvents")
	}
	return &ordersv1.ReconcileBackordersResponse{
		Backorder: &ordersv1.Backorder{
			Audit: &commonv1.AuditInfo{
				Etag:      validateOrderAmendmentReply.GetOrderAmendment().GetAudit().GetEtag(),
				UpdatedAt: validateOrderAmendmentReply.GetOrderAmendment().GetAudit().GetUpdatedAt(),
			},
			BackorderId: req.GetBackorderId(),
			Status:      req.GetBackorder().GetStatus(),
		},
		Outcome: &ordersv1.ReconcileBackordersResponse_Outcome{Message: browseAuditEventsReply.GetResult().GetMessage()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// StreamBackorderChanges exchanges changes of backorders with a peer.
func (s *BackorderSyncServer) StreamBackorderChanges(stream grpc.BidiStreamingServer[ordersv1.StreamBackorderChangesRequest, ordersv1.StreamBackorderChangesResponse]) error {
	ctx := stream.Context()
	incoming := make(chan *ordersv1.StreamBackorderChangesRequest)
	failed := make(chan error, 1)
	go func() {
		defer close(incoming)
		for {
			received, err := stream.Recv()
			if err != nil {
				if !errors.Is(err, io.EOF) {
					failed <- err
				}
				return
			}
			select {
			case incoming <- received:
			case <-ctx.Done():
				return
			}
		}
	}()
	for msg := range incoming {
		s.log.InfoContext(ctx, "stream backorder changes", "tenant", msg.GetContext().GetTenantId())
		pauseGiftCardTransactionWorkflowRequest := &giftcardsv1.PauseGiftCardTransactionWorkflowRequest{
			Context: msg.GetContext(),
			GiftCardTransaction: &giftcardsv1.GiftCardTransaction{
				Subject: &giftcardsv1.GiftCardBatchRef{DisplayName: msg.GetChange().GetAssignee().GetDisplayName()},
			},
		}
		if _, err := grpcx.Call(ctx, s.giftCardTransactionWorkflowClient.PauseGiftCardTransactionWorkflow, pauseGiftCardTransactionWorkflowRequest); err != nil {
			return grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionWorkflowService/PauseGiftCardTransactionWorkflow")
		}
		explainOrderNoteDecisionRequest := &ordersv1.ExplainOrderNoteDecisionRequest{
			Context: msg.GetContext(),
			Details: &ordersv1.ExplainOrderNoteDecisionRequest_Details{
				Contact: &commonv1.ContactInfo{Email: msg.GetChange().GetRequester().GetEmail()},
			},
		}
		if _, err := ordersv1.OrderNoteValidationServiceClient.ExplainOrderNoteDecision(s.orderNoteValidationClient, ctx, explainOrderNoteDecisionRequest); err != nil {
			return grpcx.Wrap(err, "acme.orders.v1.OrderNoteValidationService/ExplainOrderNoteDecision")
		}
		if _, err := s.backorderCalculationClient.SimulateBackorder(ctx, &ordersv1.SimulateBackorderRequest{
			Context: msg.GetContext(),
			Backorder: &ordersv1.Backorder{
				Requester: &commonv1.ContactInfo{Email: msg.GetChange().GetAssignee().GetEmail()},
				Sla:       &ordersv1.BackorderSla{PolicyId: msg.GetChange().GetSla().GetPolicyId()},
				Audit:     &commonv1.AuditInfo{CreatedBy: msg.GetChange().GetAudit().GetCreatedBy()},
			},
		}); err != nil {
			return grpcx.Wrap(err, "acme.orders.v1.BackorderCalculationService/SimulateBackorder")
		}
		for comment := range iterx.Each(msg.GetChange().GetComments(), func(candidate *ordersv1.BackorderComment) bool { return candidate != nil }) {
			if _, err := s.riskAssessmentWorkflowClient.ResumeRiskAssessmentWorkflow(ctx, &fraudv1.ResumeRiskAssessmentWorkflowRequest{
				Context: msg.GetContext(),
				RiskAssessment: &fraudv1.RiskAssessment{
					Comments: []*fraudv1.RiskAssessmentComment{&fraudv1.RiskAssessmentComment{
						AuthorEmail: comment.GetAuthorEmail(),
						Visibility:  comment.GetVisibility(),
						CreatedAt:   comment.GetCreatedAt(),
						CommentId:   comment.GetCommentId(),
					}},
				},
			}); err != nil {
				return grpcx.Wrap(err, "acme.fraud.v1.RiskAssessmentWorkflowService/ResumeRiskAssessmentWorkflow")
			}
		}
		if err := stream.Send(&ordersv1.StreamBackorderChangesResponse{Result: &commonv1.OperationResult{Status: "ok"}}); err != nil {
			return err
		}
	}
	select {
	case err := <-failed:
		return err
	default:
		return nil
	}
}

// ResolveBackorderConflict resolves a backorder.
func (s *BackorderSyncServer) ResolveBackorderConflict(ctx context.Context, req *ordersv1.ResolveBackorderConflictRequest) (*ordersv1.ResolveBackorderConflictResponse, error) {
	if err := s.store.SaveBackorder(ctx, req.GetContext().GetTenantId(), req.GetBackorder()); err != nil {
		return nil, grpcx.Wrap(err, "save backorder")
	}
	s.log.InfoContext(ctx, "resolve backorder conflict", "tenant", req.GetContext().GetTenantId(), "etag", req.GetBackorder().GetAudit().GetEtag())
	var email string
	switch choice := req.GetBackorder().GetOrigin().(type) {
	case *ordersv1.Backorder_Customer:
		email = choice.Customer.GetEmail()
	case *ordersv1.Backorder_ExternalReference:
		email = choice.ExternalReference
	case *ordersv1.Backorder_Channel:
		email = choice.Channel.GetChannel()
	}
	startMerchandisingRuleWorkflowRequest := &merchandisingv1.StartMerchandisingRuleWorkflowRequest{
		Context: req.GetContext(),
		MerchandisingRule: &merchandisingv1.MerchandisingRule{
			Owner: &identityv1.UserRef{
				DisplayName: req.GetBackorder().GetCustomer().GetDisplayName(),
				Email:       req.GetBackorder().GetAssignee().GetEmail(),
			},
		},
		Details: &merchandisingv1.StartMerchandisingRuleWorkflowRequest_Details{Notify: &commonv1.ContactInfo{Email: email}},
	}
	startMerchandisingRuleWorkflowReply, err := s.meteredMerchandisingRuleWorkflow.StartMerchandisingRuleWorkflow(ctx, startMerchandisingRuleWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.MerchandisingRuleWorkflowService/StartMerchandisingRuleWorkflow")
	}
	jobs := make(chan *ordersv1.BackorderComment)
	failures := make(chan error, 4)
	workCtx, stopWork := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for comment := range jobs {
				if _, err := s.fraudRuleApprovalClient.CommentFraudRuleApproval(workCtx, &fraudv1.CommentFraudRuleApprovalRequest{
					Context: req.GetContext(),
					FraudRule: &fraudv1.FraudRule{
						Audit: &commonv1.AuditInfo{CreatedAt: comment.GetCreatedAt()},
					},
					Details: &fraudv1.CommentFraudRuleApprovalRequest_Details{
						Approver: &identityv1.UserRef{Email: comment.GetAuthorEmail()},
						Comment:  comment.GetCommentId(),
					},
				}); err != nil {
					failures <- err
					stopWork()
					return
				}
			}
		}()
	}
	for _, queued := range req.GetBackorder().GetComments() {
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
		return nil, grpcx.Wrap(err, "acme.fraud.v1.FraudRuleApprovalService/CommentFraudRuleApproval")
	}
	return &ordersv1.ResolveBackorderConflictResponse{
		Backorder: &ordersv1.Backorder{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: startMerchandisingRuleWorkflowReply.GetMerchandisingRule().GetAudit().GetUpdatedAt(),
				Etag:      startMerchandisingRuleWorkflowReply.GetMerchandisingRule().GetAudit().GetEtag(),
			},
			BackorderId:  req.GetBackorderId(),
			CustomFields: req.GetBackorder().GetCustomFields(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetBackorderSyncState returns a backorder.
func (s *BackorderSyncServer) GetBackorderSyncState(ctx context.Context, req *ordersv1.GetBackorderSyncStateRequest) (*ordersv1.GetBackorderSyncStateResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "backorder", req.GetBackorderId(), "get_backorder_sync_state"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get backorder sync state", "tenant", req.GetContext().GetTenantId())
	validateHouseholdRequest := newValidateHouseholdRequest(withValidateHouseholdContext(req.GetContext()), withValidateHouseholdHousehold(&customersv1.Household{
		Children: []*customersv1.Household{&customersv1.Household{
			Manager: &identityv1.UserRef{
				UserId: req.GetContext().GetActor().GetUserId(),
				Email:  req.GetContext().GetActor().GetEmail(),
			},
		}},
	}))
	validateHouseholdReply, err := s.householdValidationClient.ValidateHousehold(ctx, validateHouseholdRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdValidationService/ValidateHousehold")
	}
	quoteOrderAmendmentRequest := &ordersv1.QuoteOrderAmendmentRequest{
		Context: req.GetContext(),
		Details: &ordersv1.QuoteOrderAmendmentRequest_Details{
			Destination: &commonv1.PostalAddress{
				PostalCode: validateHouseholdReply.GetHousehold().GetLocation().GetPostalCode(),
				Locality:   validateHouseholdReply.GetHousehold().GetLocation().GetLocality(),
			},
		},
		OrderAmendment: &ordersv1.OrderAmendment{
			BillingAddress: &commonv1.PostalAddress{
				Lines: validateHouseholdReply.GetHousehold().GetLocation().GetLines(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedAt: validateHouseholdReply.GetHousehold().GetAudit().GetCreatedAt(),
			},
		},
	}
	quoteOrderAmendmentReply, err := s.meteredOrderAmendmentCalculation.QuoteOrderAmendment(ctx, quoteOrderAmendmentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderAmendmentCalculationService/QuoteOrderAmendment")
	}
	return &ordersv1.GetBackorderSyncStateResponse{
		Backorder: &ordersv1.Backorder{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: validateHouseholdReply.GetHousehold().GetAudit().GetUpdatedBy(),
				UpdatedAt: quoteOrderAmendmentReply.GetOrderAmendment().GetAudit().GetUpdatedAt(),
			},
			Comments: []*ordersv1.BackorderComment{&ordersv1.BackorderComment{
				CreatedAt: validateHouseholdReply.GetHousehold().GetAudit().GetCreatedAt(),
			}},
			BackorderId: req.GetBackorderId(),
		},
		Result: &commonv1.OperationResult{
			Message: quoteOrderAmendmentReply.GetResult().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// ListBackorderConflicts lists backorders a page at a time.
func (s *BackorderSyncServer) ListBackorderConflicts(ctx context.Context, req *ordersv1.ListBackorderConflictsRequest) (*ordersv1.ListBackorderConflictsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ListBackorderConflicts", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "list backorder conflicts", "tenant", req.GetContext().GetTenantId())
	calculateOrderRequest := &ordersv1.CalculateOrderRequest{
		Context: req.GetContext(),
		Details: &ordersv1.CalculateOrderRequest_Details{
			Destination: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
	}
	calculateOrderReply, err := grpcx.Call(ctx, s.orderCalculationClient.CalculateOrder, calculateOrderRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderCalculationService/CalculateOrder")
	}
	merchandisingRuleWorkflow, ok := s.clients.Get("acme.merchandising.v1.MerchandisingRuleWorkflowService").(merchandisingv1.MerchandisingRuleWorkflowServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.merchandising.v1.MerchandisingRuleWorkflowService")
	}
	advanceMerchandisingRuleWorkflowRequest := &merchandisingv1.AdvanceMerchandisingRuleWorkflowRequest{
		Context: req.GetContext(),
		MerchandisingRule: &merchandisingv1.MerchandisingRule{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: calculateOrderReply.GetOrder().GetAudit().GetUpdatedAt(),
				CreatedAt: calculateOrderReply.GetOrder().GetAudit().GetCreatedAt(),
				CreatedBy: calculateOrderReply.GetOrder().GetAudit().GetCreatedBy(),
			},
		},
		Details: &merchandisingv1.AdvanceMerchandisingRuleWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				Email: calculateOrderReply.GetOrder().GetCounterparty().GetEmail(),
			},
		},
	}
	advanceMerchandisingRuleWorkflowReply, err := merchandisingRuleWorkflow.AdvanceMerchandisingRuleWorkflow(ctx, advanceMerchandisingRuleWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.MerchandisingRuleWorkflowService/AdvanceMerchandisingRuleWorkflow")
	}
	return &ordersv1.ListBackorderConflictsResponse{
		Backorders: []*ordersv1.Backorder{&ordersv1.Backorder{
			Comments: []*ordersv1.BackorderComment{&ordersv1.BackorderComment{
				CreatedAt: calculateOrderReply.GetOrder().GetAudit().GetCreatedAt(),
			}},
			Assignee: &identityv1.UserRef{
				UserId: advanceMerchandisingRuleWorkflowReply.GetMerchandisingRule().GetOwner().GetUserId(),
				Email:  advanceMerchandisingRuleWorkflowReply.GetMerchandisingRule().GetOwner().GetEmail(),
			},
		}},
		Result: &commonv1.OperationResult{
			Details: calculateOrderReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}
