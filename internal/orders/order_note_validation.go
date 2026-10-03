package orders

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	concessionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/concessions/v1"
	documentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/documents/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	ordersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/orders/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/anypb"
)

// OrderNoteValidationServer implements `acme.orders.v1.OrderNoteValidationService`.
// Checks order notes against the rules before anything acts on them.
type OrderNoteValidationServer struct {
	ordersv1.UnimplementedOrderNoteValidationServiceServer
	store                            *Store
	log                              *slog.Logger
	documentValidationClient         documentsv1.DocumentValidationServiceClient
	userPort                         userPort
	orderAmendmentCalculationClient  ordersv1.OrderAmendmentCalculationServiceClient
	orderValidationClient            ordersv1.OrderValidationServiceClient
	subscriptionPlanValidationClient subscriptionsv1.SubscriptionPlanValidationServiceClient
	checkBackorderEligibilityFn      func(context.Context, *ordersv1.CheckBackorderEligibilityRequest, ...grpc.CallOption) (*ordersv1.CheckBackorderEligibilityResponse, error)
	clients                          *registry.Registry
	orderCalculationClient           ordersv1.OrderCalculationServiceClient
	conn                             *grpc.ClientConn
	concessionStandQueryClient       concessionsv1.ConcessionStandQueryServiceClient
	apiCredentialNotificationClient  identityv1.ApiCredentialNotificationServiceClient
	fraudSignalValidationClient      fraudv1.FraudSignalValidationServiceClient
}

// NewOrderNoteValidationServer wires an OrderNoteValidationServer.
func NewOrderNoteValidationServer(deps platform.Deps) *OrderNoteValidationServer {
	backorderValidation := ordersv1.NewBackorderValidationServiceClient(deps.Conn)
	return &OrderNoteValidationServer{
		store:                            NewStore(deps.DB),
		log:                              deps.Logger.With("service", "acme.orders.v1.OrderNoteValidationService"),
		documentValidationClient:         documentsv1.NewDocumentValidationServiceClient(deps.Conn),
		userPort:                         newUserPort(deps),
		orderAmendmentCalculationClient:  ordersv1.NewOrderAmendmentCalculationServiceClient(deps.Conn),
		orderValidationClient:            ordersv1.NewOrderValidationServiceClient(deps.Conn),
		subscriptionPlanValidationClient: subscriptionsv1.NewSubscriptionPlanValidationServiceClient(deps.Conn),
		checkBackorderEligibilityFn:      backorderValidation.CheckBackorderEligibility,
		clients:                          deps.Clients,
		orderCalculationClient:           ordersv1.NewOrderCalculationServiceClient(deps.Conn),
		conn:                             deps.Conn,
		concessionStandQueryClient:       concessionsv1.NewConcessionStandQueryServiceClient(deps.Conn),
		apiCredentialNotificationClient:  identityv1.NewApiCredentialNotificationServiceClient(deps.Conn),
		fraudSignalValidationClient:      fraudv1.NewFraudSignalValidationServiceClient(deps.Conn),
	}
}

// ValidateOrderNote validates an order note.
func (s *OrderNoteValidationServer) ValidateOrderNote(ctx context.Context, req *ordersv1.ValidateOrderNoteRequest) (*ordersv1.ValidateOrderNoteResponse, error) {
	if err := s.store.SaveOrderNote(ctx, req.GetContext().GetTenantId(), req.GetOrderNote()); err != nil {
		return nil, grpcx.Wrap(err, "save order note")
	}
	s.log.InfoContext(ctx, "validate order note", "tenant", req.GetContext().GetTenantId())
	encoded, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded ordersv1.ValidateOrderNoteRequest_Details
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	var envelope ordersv1.OrderRef
	if err := req.GetOrderNote().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	normalizeDocumentRequest := &documentsv1.NormalizeDocumentRequest{
		Context: req.GetContext(),
		Details: &documentsv1.NormalizeDocumentRequest_Details{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: (&decoded).GetContact().GetPreferredChannel(),
			},
		},
		Document: &documentsv1.Document{
			Counterparty: &documentsv1.DocumentFolderRef{
				DisplayName: req.GetOrderNote().GetSubject().GetDisplayName(),
			},
		},
		DocumentId: envelope.GetDisplayName(),
	}
	normalizeDocumentReply, err := documentsv1.DocumentValidationServiceClient.NormalizeDocument(s.documentValidationClient, ctx, normalizeDocumentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.documents.v1.DocumentValidationService/NormalizeDocument")
	}
	getUserRequest := &identityv1.GetUserRequest{
		Context: req.GetContext(),
		UserId:  req.GetContext().GetActor().GetUserId(),
	}
	getUserReply, err := s.userPort.GetUser(ctx, getUserRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserService/GetUser")
	}
	return &ordersv1.ValidateOrderNoteResponse{
		Outcome: &ordersv1.ValidateOrderNoteResponse_Outcome{Message: normalizeDocumentReply.GetResult().GetMessage()},
		Result: &commonv1.OperationResult{
			Message: normalizeDocumentReply.GetResult().GetMessage(),
			Details: getUserReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		OrderNote: &ordersv1.OrderNote{
			Subject:     &ordersv1.OrderRef{Number: getUserReply.GetUser().GetPassport().GetNumber()},
			OrderNoteId: req.GetOrderNoteId(),
			Source:      req.GetOrderNote().GetSource(),
		},
	}, nil
}

// PreviewOrderNote previews an order note.
func (s *OrderNoteValidationServer) PreviewOrderNote(ctx context.Context, req *ordersv1.PreviewOrderNoteRequest) (*ordersv1.PreviewOrderNoteResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveOrderNote(ctx, req.GetContext().GetTenantId(), req.GetOrderNote()); err != nil {
		return nil, grpcx.Wrap(err, "save order note")
	}
	s.log.InfoContext(ctx, "preview order note", "tenant", req.GetContext().GetTenantId())
	calculateOrderAmendmentReply, err := s.orderAmendmentCalculationClient.CalculateOrderAmendment(ctx, &ordersv1.CalculateOrderAmendmentRequest{
		Context: req.GetContext(),
		OrderAmendment: &ordersv1.OrderAmendment{
			Counterparty: &ordersv1.BackorderRef{
				DisplayName: req.GetOrderNote().GetSubject().GetDisplayName(),
			},
			Lines:  []*ordersv1.OrderAmendmentLine{&ordersv1.OrderAmendmentLine{Attributes: req.GetOrderNote().GetAttributes()}},
			Number: req.GetOrderNote().GetSubject().GetNumber(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderAmendmentCalculationService/CalculateOrderAmendment")
	}
	return &ordersv1.PreviewOrderNoteResponse{
		OrderNote: &ordersv1.OrderNote{
			CorrelationId: calculateOrderAmendmentReply.GetResult().GetCorrelationId(),
			OrderNoteId:   req.GetOrderNoteId(),
			Sequence:      req.GetOrderNote().GetSequence(),
		},
		Outcome: &ordersv1.PreviewOrderNoteResponse_Outcome{
			Status: calculateOrderAmendmentReply.GetResult().GetStatus(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// NormalizeOrderNote normalizes an order note.
func (s *OrderNoteValidationServer) NormalizeOrderNote(ctx context.Context, req *ordersv1.NormalizeOrderNoteRequest) (*ordersv1.NormalizeOrderNoteResponse, error) {
	if err := s.store.SaveOrderNote(ctx, req.GetContext().GetTenantId(), req.GetOrderNote()); err != nil {
		return nil, grpcx.Wrap(err, "save order note")
	}
	s.log.InfoContext(ctx, "normalize order note", "tenant", req.GetContext().GetTenantId())
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*ordersv1.ScoreOrderResponse, len(req.GetOrderNote().GetMeasurements()))
	for i, measurement := range req.GetOrderNote().GetMeasurements() {
		group.Go(func() error {
			reply, err := s.orderValidationClient.ScoreOrder(groupCtx, &ordersv1.ScoreOrderRequest{
				Context: req.GetContext(),
				Order: &ordersv1.Order{
					Audit:   &commonv1.AuditInfo{CreatedAt: measurement.GetObservedAt()},
					Number:  req.GetOrderNote().GetSubject().GetNumber(),
					OrderId: req.GetOrderNote().GetSubject().GetOrderId(),
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
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderValidationService/ScoreOrder")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	lintSubscriptionPlansRequest := &subscriptionsv1.LintSubscriptionPlansRequest{
		Context: req.GetContext(),
		SubscriptionPlan: &subscriptionsv1.SubscriptionPlan{
			Owner: &identityv1.UserRef{Email: req.GetDetails().GetContact().GetEmail()},
		},
	}
	type lintSubscriptionPlansResult struct {
		reply *subscriptionsv1.LintSubscriptionPlansResponse
		err   error
	}
	lintSubscriptionPlansResults := make(chan lintSubscriptionPlansResult, 1)
	go func() {
		reply, err := s.subscriptionPlanValidationClient.LintSubscriptionPlans(ctx, lintSubscriptionPlansRequest)
		lintSubscriptionPlansResults <- lintSubscriptionPlansResult{reply, err}
	}()
	var lintSubscriptionPlansReply *subscriptionsv1.LintSubscriptionPlansResponse
	select {
	case result := <-lintSubscriptionPlansResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.subscriptions.v1.SubscriptionPlanValidationService/LintSubscriptionPlans")
		}
		lintSubscriptionPlansReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &ordersv1.NormalizeOrderNoteResponse{
		Outcome: &ordersv1.NormalizeOrderNoteResponse_Outcome{
			ReferenceId: lintSubscriptionPlansReply.GetOutcome().GetReferenceId(),
		},
		OrderNote: &ordersv1.OrderNote{
			OrderNoteId: req.GetOrderNoteId(),
			Attributes:  req.GetOrderNote().GetAttributes(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ExplainOrderNoteDecision explains an order note.
func (s *OrderNoteValidationServer) ExplainOrderNoteDecision(ctx context.Context, req *ordersv1.ExplainOrderNoteDecisionRequest) (*ordersv1.ExplainOrderNoteDecisionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ExplainOrderNoteDecision", "error", err)
		return nil, err
	}
	if err := s.store.SaveOrderNote(ctx, req.GetContext().GetTenantId(), req.GetOrderNote()); err != nil {
		return nil, grpcx.Wrap(err, "save order note")
	}
	s.log.InfoContext(ctx, "explain order note decision", "tenant", req.GetContext().GetTenantId())
	for _, measurement := range req.GetOrderNote().GetMeasurements() {
		if _, err := s.orderAmendmentCalculationClient.RecalculateOrderAmendments(ctx, &ordersv1.RecalculateOrderAmendmentsRequest{
			Context: req.GetContext(),
			OrderAmendment: &ordersv1.OrderAmendment{
				Audit: &commonv1.AuditInfo{UpdatedAt: measurement.GetObservedAt()},
				Counterparty: &ordersv1.BackorderRef{
					DisplayName: req.GetOrderNote().GetSubject().GetDisplayName(),
				},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.orders.v1.OrderAmendmentCalculationService/RecalculateOrderAmendments")
		}
	}
	var envelope ordersv1.OrderRef
	if err := req.GetOrderNote().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	checkBackorderEligibilityRequest := &ordersv1.CheckBackorderEligibilityRequest{
		Context: req.GetContext(),
		Details: &ordersv1.CheckBackorderEligibilityRequest_Details{
			Contact: &commonv1.ContactInfo{
				Name:             req.GetDetails().GetContact().GetName(),
				PreferredChannel: req.GetDetails().GetContact().GetPreferredChannel(),
			},
		},
		BackorderId: envelope.GetDisplayName(),
	}
	checkBackorderEligibilityReply, err := s.checkBackorderEligibilityFn(ctx, checkBackorderEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.BackorderValidationService/CheckBackorderEligibility")
	}
	orderNoteCalculation, ok := s.clients.Get("acme.orders.v1.OrderNoteCalculationService").(ordersv1.OrderNoteCalculationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.orders.v1.OrderNoteCalculationService")
	}
	var holder struct{ OrderId string }
	if record := req.GetOrderNote().GetSubject(); record != nil {
		if err := mapper.CopyFields(&holder, record, "OrderId"); err != nil {
			s.log.WarnContext(ctx, "copy order_id", "error", err)
		}
	}
	orderNoteCache := cache.New[string, *ordersv1.OrderNote](1)
	orderNoteCache.Put(req.GetContext().GetTraceId(), req.GetOrderNote())
	cachedOrderNote, _ := orderNoteCache.Get(req.GetContext().GetTraceId())
	packed, err := anypb.New(req.GetOrderNote())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	var envelope2 ordersv1.OrderRef
	if err := req.GetOrderNote().GetDetail().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	calculateOrderNoteRequest := &ordersv1.CalculateOrderNoteRequest{
		Context: req.GetContext(),
		OrderNote: &ordersv1.OrderNote{
			Subject: &ordersv1.OrderRef{
				OrderId:     holder.OrderId,
				DisplayName: checkBackorderEligibilityReply.GetBackorder().GetAssignee().GetDisplayName(),
				Number:      cachedOrderNote.GetSubject().GetNumber(),
			},
			Detail: packed,
		},
		OrderNoteId: envelope2.GetDisplayName(),
	}
	calculateOrderNoteReply, err := orderNoteCalculation.CalculateOrderNote(ctx, calculateOrderNoteRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderNoteCalculationService/CalculateOrderNote")
	}
	estimateOrderRequest := buildEstimateOrderRequestForExplainOrderNoteDecision(req)
	estimateOrderReply, err := s.orderCalculationClient.EstimateOrder(ctx, estimateOrderRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderCalculationService/EstimateOrder")
	}
	return &ordersv1.ExplainOrderNoteDecisionResponse{
		Result: &commonv1.OperationResult{
			Message:       checkBackorderEligibilityReply.GetResult().GetMessage(),
			CorrelationId: calculateOrderNoteReply.GetOrderNote().GetCorrelationId(),
			Status:        "ok",
		},
		Outcome: &ordersv1.ExplainOrderNoteDecisionResponse_Outcome{
			Status: checkBackorderEligibilityReply.GetOutcome().GetStatus(),
		},
		OrderNote: &ordersv1.OrderNote{
			Subject:     &ordersv1.OrderRef{OrderId: estimateOrderReply.GetOrder().GetOrderId()},
			OrderNoteId: req.GetOrderNoteId(),
			Source:      req.GetOrderNote().GetSource(),
		},
	}, nil
}

// ScoreOrderNote scores an order note.
func (s *OrderNoteValidationServer) ScoreOrderNote(ctx context.Context, req *ordersv1.ScoreOrderNoteRequest) (*ordersv1.ScoreOrderNoteResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ScoreOrderNote", "error", err)
		return nil, err
	}
	if err := s.store.SaveOrderNote(ctx, req.GetContext().GetTenantId(), req.GetOrderNote()); err != nil {
		return nil, grpcx.Wrap(err, "save order note")
	}
	s.log.InfoContext(ctx, "score order note", "tenant", req.GetContext().GetTenantId())
	reassignTenantRequest := &identityv1.ReassignTenantRequest{
		Context:  req.GetContext(),
		TenantId: req.GetContext().GetTenantId(),
		Details: &identityv1.ReassignTenantRequest_Details{
			Assignee: &identityv1.UserRef{
				DisplayName: req.GetOrderNote().GetSubject().GetDisplayName(),
				UserId:      req.GetContext().GetActor().GetUserId(),
				Email:       req.GetContext().GetActor().GetEmail(),
			},
		},
		Reason: req.GetReason(),
	}
	reassignTenantReply := new(identityv1.ReassignTenantResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", identityTenantAdminServiceName, "ReassignTenant"), reassignTenantRequest, reassignTenantReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantAdminService/ReassignTenant")
	}
	jobs := make(chan *ordersv1.OrderNoteMeasurement)
	failures := make(chan error, 4)
	workCtx, stopWork := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for measurement := range jobs {
				if _, err := s.concessionStandQueryClient.FindConcessionStands(workCtx, &concessionsv1.FindConcessionStandsRequest{
					Context: req.GetContext(),
					Filter: &concessionsv1.FindConcessionStandsRequest_Filter{
						Region:       req.GetDetails().GetDestination().GetRegion(),
						CreatedAfter: measurement.GetObservedAt(),
						OwnerId:      reassignTenantReply.GetTenant().GetParentId(),
						Labels:       reassignTenantReply.GetTenant().GetAttributes(),
					},
				}); err != nil {
					failures <- err
					stopWork()
					return
				}
			}
		}()
	}
	for _, queued := range req.GetOrderNote().GetMeasurements() {
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
		return nil, grpcx.Wrap(err, "acme.concessions.v1.ConcessionStandQueryService/FindConcessionStands")
	}
	return &ordersv1.ScoreOrderNoteResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: reassignTenantReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		OrderNote: &ordersv1.OrderNote{
			Attributes:  reassignTenantReply.GetTenant().GetAttributes(),
			OrderNoteId: req.GetOrderNoteId(),
		},
		Breakdown: &ordersv1.ScoreOrderNoteResponse_Breakdown{CurrencyCode: req.GetDetails().GetCurrencyCode()},
	}, nil
}

// LintOrderNotes lints an order note.
func (s *OrderNoteValidationServer) LintOrderNotes(ctx context.Context, req *ordersv1.LintOrderNotesRequest) (*ordersv1.LintOrderNotesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveOrderNote(ctx, req.GetContext().GetTenantId(), req.GetOrderNote()); err != nil {
		return nil, grpcx.Wrap(err, "save order note")
	}
	s.log.InfoContext(ctx, "lint order notes", "tenant", req.GetContext().GetTenantId())
	detailsCache := cache.New[string, *ordersv1.LintOrderNotesRequest_Details](1)
	detailsCache.Put(req.GetContext().GetTraceId(), req.GetDetails())
	cachedDetails, _ := detailsCache.Get(req.GetContext().GetTraceId())
	var holder struct{ Phone string }
	if record := req.GetDetails().GetContact(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Phone"); err != nil {
			s.log.WarnContext(ctx, "copy phone", "error", err)
		}
	}
	packed, err := anypb.New(req.GetOrderNote())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	var envelope ordersv1.OrderRef
	if err := req.GetOrderNote().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	sendApiCredentialTestNoticeRequest := &identityv1.SendApiCredentialTestNoticeRequest{
		Context: req.GetContext(),
		Details: &identityv1.SendApiCredentialTestNoticeRequest_Details{
			Recipient: &commonv1.ContactInfo{
				Name:             req.GetDetails().GetContact().GetName(),
				PreferredChannel: req.GetDetails().GetContact().GetPreferredChannel(),
				Email:            cachedDetails.GetContact().GetEmail(),
				Phone:            holder.Phone,
			},
			Payload: packed,
		},
		ApiCredentialId: envelope.GetDisplayName(),
	}
	type sendApiCredentialTestNoticeResult struct {
		reply *identityv1.SendApiCredentialTestNoticeResponse
		err   error
	}
	sendApiCredentialTestNoticeResults := make(chan sendApiCredentialTestNoticeResult, 1)
	go func() {
		reply, err := s.apiCredentialNotificationClient.SendApiCredentialTestNotice(ctx, sendApiCredentialTestNoticeRequest)
		sendApiCredentialTestNoticeResults <- sendApiCredentialTestNoticeResult{reply, err}
	}()
	var sendApiCredentialTestNoticeReply *identityv1.SendApiCredentialTestNoticeResponse
	select {
	case result := <-sendApiCredentialTestNoticeResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.identity.v1.ApiCredentialNotificationService/SendApiCredentialTestNotice")
		}
		sendApiCredentialTestNoticeReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	packed2, err := anypb.New(req.GetOrderNote())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	checkFraudSignalEligibilityRequest := &fraudv1.CheckFraudSignalEligibilityRequest{}
	checkFraudSignalEligibilityRequest.Context = req.GetContext()
	checkFraudSignalEligibilityRequest.Details = &fraudv1.CheckFraudSignalEligibilityRequest_Details{
		Contact: &commonv1.ContactInfo{
			PreferredChannel: sendApiCredentialTestNoticeReply.GetApiCredential().GetContact().GetPreferredChannel(),
			Phone:            sendApiCredentialTestNoticeReply.GetApiCredential().GetContact().GetPhone(),
		},
	}
	checkFraudSignalEligibilityRequest.FraudSignal = &fraudv1.FraudSignal{
		Subject: &fraudv1.FraudRuleRef{
			DisplayName: req.GetOrderNote().GetSubject().GetDisplayName(),
		},
		Detail: packed2,
	}
	checkFraudSignalEligibilityReply, err := s.fraudSignalValidationClient.CheckFraudSignalEligibility(ctx, checkFraudSignalEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.FraudSignalValidationService/CheckFraudSignalEligibility")
	}
	compareOrderAmendmentOptionsRequest := buildCompareOrderAmendmentOptionsRequestForLintOrderNotes(req, sendApiCredentialTestNoticeReply)
	compareOrderAmendmentOptionsReply, err := s.orderAmendmentCalculationClient.CompareOrderAmendmentOptions(ctx, compareOrderAmendmentOptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderAmendmentCalculationService/CompareOrderAmendmentOptions")
	}
	return &ordersv1.LintOrderNotesResponse{
		Result: &commonv1.OperationResult{
			Status:       sendApiCredentialTestNoticeReply.GetResult().GetStatus(),
			ReferenceIds: checkFraudSignalEligibilityReply.GetResult().GetReferenceIds(),
		},
		Outcome: &ordersv1.LintOrderNotesResponse_Outcome{
			Status: compareOrderAmendmentOptionsReply.GetResult().GetStatus(),
		},
		OrderNote: &ordersv1.OrderNote{
			OrderNoteId:   req.GetOrderNoteId(),
			CorrelationId: req.GetOrderNote().GetCorrelationId(),
		},
	}, nil
}

// GetOrderNoteRules returns an order note.
func (s *OrderNoteValidationServer) GetOrderNoteRules(ctx context.Context, req *ordersv1.GetOrderNoteRulesRequest) (*ordersv1.GetOrderNoteRulesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "order_note", req.GetOrderNoteId(), "get_order_note_rules"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get order note rules", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "order_note_id", req.GetOrderNoteId())
	recalculateOrderNotesRequest := &ordersv1.RecalculateOrderNotesRequest{
		Context:     req.GetContext(),
		OrderNoteId: tenancy.Value(ctx, "order_note_id"),
		OrderNote: &ordersv1.OrderNote{
			Measurements: []*ordersv1.OrderNoteMeasurement{&ordersv1.OrderNoteMeasurement{ObservedAt: req.GetContext().GetDeadline()}},
		},
	}
	recalculateOrderNotesReply := new(ordersv1.RecalculateOrderNotesResponse)
	if err := grpcx.Invoke(ctx, s.conn, ordersv1.OrderNoteCalculationService_RecalculateOrderNotes_FullMethodName, recalculateOrderNotesRequest, recalculateOrderNotesReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderNoteCalculationService/RecalculateOrderNotes")
	}
	return &ordersv1.GetOrderNoteRulesResponse{
		OrderNote: &ordersv1.OrderNote{
			Subject: &ordersv1.OrderRef{
				Number: recalculateOrderNotesReply.GetOrderNote().GetSubject().GetNumber(),
			},
			OrderNoteId: req.GetOrderNoteId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}
