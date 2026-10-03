package orders

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	fulfillmentv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fulfillment/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	modelregistryv1 "github.com/hounddogai/protomap-test-golang/gen/acme/modelregistry/v1"
	ordersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/orders/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	warehousingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/warehousing/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// OrderNoteApprovalServer implements `acme.orders.v1.OrderNoteApprovalService`.
// Routes order notes through approval, with escalation and delegation.
type OrderNoteApprovalServer struct {
	ordersv1.UnsafeOrderNoteApprovalServiceServer
	store                           *Store
	log                             *slog.Logger
	storageBinApprovalClient        warehousingv1.StorageBinApprovalServiceClient
	returnLabelApprovalClient       returnsv1.ReturnLabelApprovalServiceClient
	clients                         *registry.Registry
	meteredCustomerValidation       meteredCustomerValidation
	conn                            *grpc.ClientConn
	orderLifecycleClient            ordersv1.OrderLifecycleServiceClient
	simulateRegisteredModelFn       func(context.Context, *modelregistryv1.SimulateRegisteredModelRequest, ...grpc.CallOption) (*modelregistryv1.SimulateRegisteredModelResponse, error)
	parcelApprovalClient            shippingv1.ParcelApprovalServiceClient
	customerSegmentValidationClient customersv1.CustomerSegmentValidationServiceClient
	customerClient                  customersv1.CustomerServiceClient
	fulfillmentWorkflowClient       fulfillmentv1.FulfillmentWorkflowServiceClient
	searchIndexExportClient         searchv1.SearchIndexExportServiceClient
	backorderApprovalClient         ordersv1.BackorderApprovalServiceClient
	accessPolicyValidationClient    authorizationv1.AccessPolicyValidationServiceClient
	orderAmendmentApprovalClient    ordersv1.OrderAmendmentApprovalServiceClient
}

// NewOrderNoteApprovalServer wires an OrderNoteApprovalServer.
func NewOrderNoteApprovalServer(deps platform.Deps) *OrderNoteApprovalServer {
	registeredModelCalculation := modelregistryv1.NewRegisteredModelCalculationServiceClient(deps.Conn)
	return &OrderNoteApprovalServer{
		store:                           NewStore(deps.DB),
		log:                             deps.Logger.With("service", "acme.orders.v1.OrderNoteApprovalService"),
		storageBinApprovalClient:        warehousingv1.NewStorageBinApprovalServiceClient(deps.Conn),
		returnLabelApprovalClient:       returnsv1.NewReturnLabelApprovalServiceClient(deps.Conn),
		clients:                         deps.Clients,
		meteredCustomerValidation:       meteredCustomerValidation{CustomerValidationServiceClient: customersv1.NewCustomerValidationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		conn:                            deps.Conn,
		orderLifecycleClient:            ordersv1.NewOrderLifecycleServiceClient(deps.Conn),
		simulateRegisteredModelFn:       registeredModelCalculation.SimulateRegisteredModel,
		parcelApprovalClient:            shippingv1.NewParcelApprovalServiceClient(deps.Conn),
		customerSegmentValidationClient: customersv1.NewCustomerSegmentValidationServiceClient(deps.Conn),
		customerClient:                  customersv1.NewCustomerServiceClient(deps.Conn),
		fulfillmentWorkflowClient:       fulfillmentv1.NewFulfillmentWorkflowServiceClient(deps.Conn),
		searchIndexExportClient:         searchv1.NewSearchIndexExportServiceClient(deps.Conn),
		backorderApprovalClient:         ordersv1.NewBackorderApprovalServiceClient(deps.Conn),
		accessPolicyValidationClient:    authorizationv1.NewAccessPolicyValidationServiceClient(deps.Conn),
		orderAmendmentApprovalClient:    ordersv1.NewOrderAmendmentApprovalServiceClient(deps.Conn),
	}
}

// SubmitOrderNote submits an order note.
func (s *OrderNoteApprovalServer) SubmitOrderNote(ctx context.Context, req *ordersv1.SubmitOrderNoteRequest) (*ordersv1.SubmitOrderNoteResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveOrderNote(ctx, req.GetContext().GetTenantId(), req.GetOrderNote()); err != nil {
		return nil, grpcx.Wrap(err, "save order note")
	}
	s.log.InfoContext(ctx, "submit order note", "tenant", req.GetContext().GetTenantId())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	var envelope ordersv1.OrderRef
	if err := req.GetOrderNote().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	listPendingStorageBinsRequest := &warehousingv1.ListPendingStorageBinsRequest{
		Context: req.GetContext(),
		Filter: &warehousingv1.ListPendingStorageBinsRequest_Filter{
			CreatedAfter: req.GetDetails().GetEffectiveAt(),
			OwnerId:      req.GetOrderNote().GetSubject().GetOrderId(),
			Labels:       annotations,
		},
		Page: &commonv1.PageRequest{PageToken: envelope.GetDisplayName()},
	}
	type listPendingStorageBinsResult struct {
		reply *warehousingv1.ListPendingStorageBinsResponse
		err   error
	}
	listPendingStorageBinsResults := make(chan listPendingStorageBinsResult, 1)
	go func() {
		reply, err := s.storageBinApprovalClient.ListPendingStorageBins(ctx, listPendingStorageBinsRequest)
		listPendingStorageBinsResults <- listPendingStorageBinsResult{reply, err}
	}()
	var listPendingStorageBinsReply *warehousingv1.ListPendingStorageBinsResponse
	select {
	case result := <-listPendingStorageBinsResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.warehousing.v1.StorageBinApprovalService/ListPendingStorageBins")
		}
		listPendingStorageBinsReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	var envelope2 ordersv1.OrderRef
	if err := req.GetOrderNote().GetDetail().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	rejectReturnLabelRequest := &returnsv1.RejectReturnLabelRequest{
		Context: req.GetContext(),
		Details: &returnsv1.RejectReturnLabelRequest_Details{
			Approver: &identityv1.UserRef{
				DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
			},
		},
		ReturnLabel:   &returnsv1.ReturnLabel{Number: req.GetOrderNote().GetSubject().GetNumber()},
		ReturnLabelId: envelope2.GetDisplayName(),
	}
	type rejectReturnLabelResult struct {
		reply *returnsv1.RejectReturnLabelResponse
		err   error
	}
	rejectReturnLabelResults := make(chan rejectReturnLabelResult, 1)
	go func() {
		reply, err := s.returnLabelApprovalClient.RejectReturnLabel(ctx, rejectReturnLabelRequest)
		rejectReturnLabelResults <- rejectReturnLabelResult{reply, err}
	}()
	var rejectReturnLabelReply *returnsv1.RejectReturnLabelResponse
	select {
	case result2 := <-rejectReturnLabelResults:
		if result2.err != nil {
			return nil, grpcx.Wrap(result2.err, "acme.returns.v1.ReturnLabelApprovalService/RejectReturnLabel")
		}
		rejectReturnLabelReply = result2.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	fulfillmentExceptionWorkflow, ok := s.clients.Get("acme.fulfillment.v1.FulfillmentExceptionWorkflowService").(fulfillmentv1.FulfillmentExceptionWorkflowServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.fulfillment.v1.FulfillmentExceptionWorkflowService")
	}
	var holder struct{ Email string }
	if record := req.GetDetails().GetApprover(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Email"); err != nil {
			s.log.WarnContext(ctx, "copy email", "error", err)
		}
	}
	var envelope3 ordersv1.OrderRef
	if err := req.GetOrderNote().GetDetail().UnmarshalTo(&envelope3); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	advanceFulfillmentExceptionWorkflowRequest := &fulfillmentv1.AdvanceFulfillmentExceptionWorkflowRequest{
		Context: req.GetContext(),
		FulfillmentException: &fulfillmentv1.FulfillmentException{
			Assignee: &identityv1.UserRef{Email: holder.Email},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: rejectReturnLabelReply.GetReturnLabel().GetAudit().GetUpdatedAt(),
			},
		},
		FulfillmentExceptionId: envelope3.GetDisplayName(),
	}
	advanceFulfillmentExceptionWorkflowReply, err := fulfillmentExceptionWorkflow.AdvanceFulfillmentExceptionWorkflow(ctx, advanceFulfillmentExceptionWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.FulfillmentExceptionWorkflowService/AdvanceFulfillmentExceptionWorkflow")
	}
	return &ordersv1.SubmitOrderNoteResponse{
		Result: &commonv1.OperationResult{
			Message:      listPendingStorageBinsReply.GetResult().GetMessage(),
			ReferenceIds: listPendingStorageBinsReply.GetResult().GetReferenceIds(),
			Status:       rejectReturnLabelReply.GetOutcome().GetStatus(),
		},
		Outcome: &ordersv1.SubmitOrderNoteResponse_Outcome{
			ReferenceId: rejectReturnLabelReply.GetOutcome().GetReferenceId(),
		},
		OrderNote: &ordersv1.OrderNote{
			CorrelationId: advanceFulfillmentExceptionWorkflowReply.GetResult().GetCorrelationId(),
			OrderNoteId:   req.GetOrderNoteId(),
			Subject:       req.GetOrderNote().GetSubject(),
		},
	}, nil
}

// ApproveOrderNote approves an order note.
func (s *OrderNoteApprovalServer) ApproveOrderNote(ctx context.Context, req *ordersv1.ApproveOrderNoteRequest) (*ordersv1.ApproveOrderNoteResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ApproveOrderNote", "error", err)
		return nil, err
	}
	if err := s.store.SaveOrderNote(ctx, req.GetContext().GetTenantId(), req.GetOrderNote()); err != nil {
		return nil, grpcx.Wrap(err, "save order note")
	}
	s.log.InfoContext(ctx, "approve order note", "tenant", req.GetContext().GetTenantId())
	encoded, err := protojson.Marshal(req.GetOrderNote())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded ordersv1.OrderNote
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	explainCustomerDecisionRequest := &customersv1.ExplainCustomerDecisionRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			RelationshipManager: &identityv1.UserRef{
				DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
			},
			Contact:          &commonv1.ContactInfo{Email: req.GetDetails().GetApprover().GetEmail()},
			IdentityDocument: &customersv1.Customer_NationalId{NationalId: &customersv1.CustomerNationalId{Number: (&decoded).GetSubject().GetNumber()}},
			Attributes:       req.GetOrderNote().GetAttributes(),
		},
	}
	explainCustomerDecisionReply, err := s.meteredCustomerValidation.ExplainCustomerDecision(ctx, explainCustomerDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerValidationService/ExplainCustomerDecision")
	}
	return &ordersv1.ApproveOrderNoteResponse{
		Outcome: &ordersv1.ApproveOrderNoteResponse_Outcome{
			ReferenceId: explainCustomerDecisionReply.GetOutcome().GetReferenceId(),
		},
		OrderNote: &ordersv1.OrderNote{
			OrderNoteId: req.GetOrderNoteId(),
			Kind:        req.GetOrderNote().GetKind(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RejectOrderNote rejects an order note.
func (s *OrderNoteApprovalServer) RejectOrderNote(ctx context.Context, req *ordersv1.RejectOrderNoteRequest) (*ordersv1.RejectOrderNoteResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveOrderNote(ctx, req.GetContext().GetTenantId(), req.GetOrderNote()); err != nil {
		return nil, grpcx.Wrap(err, "save order note")
	}
	s.log.InfoContext(ctx, "reject order note", "tenant", req.GetContext().GetTenantId())
	var envelope ordersv1.OrderRef
	if err := req.GetOrderNote().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	lintSessionsRequest := &identityv1.LintSessionsRequest{
		Context: req.GetContext(),
		Session: &identityv1.Session{
			Assignee: &identityv1.UserRef{
				UserId:      req.GetDetails().GetApprover().GetUserId(),
				DisplayName: req.GetOrderNote().GetSubject().GetDisplayName(),
			},
		},
		SessionId: envelope.GetDisplayName(),
	}
	lintSessionsReply := new(identityv1.LintSessionsResponse)
	if err := grpcx.Invoke(ctx, s.conn, identityv1.SessionValidationService_LintSessions_FullMethodName, lintSessionsRequest, lintSessionsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionValidationService/LintSessions")
	}
	renewOrderRequest := &ordersv1.RenewOrderRequest{
		Context: req.GetContext(),
		OrderId: req.GetOrderNote().GetSubject().GetOrderId(),
		Details: &ordersv1.RenewOrderRequest_Details{
			NewOwner: &identityv1.UserRef{UserId: req.GetDetails().GetApprover().GetUserId()},
		},
	}
	type renewOrderResult struct {
		reply *ordersv1.RenewOrderResponse
		err   error
	}
	renewOrderResults := make(chan renewOrderResult, 1)
	go func() {
		reply, err := s.orderLifecycleClient.RenewOrder(ctx, renewOrderRequest)
		renewOrderResults <- renewOrderResult{reply, err}
	}()
	var renewOrderReply *ordersv1.RenewOrderResponse
	select {
	case result := <-renewOrderResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.orders.v1.OrderLifecycleService/RenewOrder")
		}
		renewOrderReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	simulateRegisteredModelRequest := &modelregistryv1.SimulateRegisteredModelRequest{
		Context: req.GetContext(),
		RegisteredModel: &modelregistryv1.RegisteredModel{
			Locator: &modelregistryv1.RegisteredModel_Address{Address: &commonv1.PostalAddress{
				Locality: renewOrderReply.GetOrder().GetBillingAddress().GetLocality(),
			}},
			Audit: &commonv1.AuditInfo{
				CreatedAt: renewOrderReply.GetOrder().GetAudit().GetCreatedAt(),
			},
		},
		Details: &modelregistryv1.SimulateRegisteredModelRequest_Details{
			Destination: &commonv1.PostalAddress{
				Recipient: renewOrderReply.GetOrder().GetBillingAddress().GetRecipient(),
			},
		},
	}
	simulateRegisteredModelReply, err := s.simulateRegisteredModelFn(ctx, simulateRegisteredModelRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.modelregistry.v1.RegisteredModelCalculationService/SimulateRegisteredModel")
	}
	var envelope2 ordersv1.OrderRef
	if err := req.GetOrderNote().GetDetail().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	step := &orderNoteApprovalServerRejectOrderNoteApproveParcelStep{request: &shippingv1.ApproveParcelRequest{
		Context: req.GetContext(),
		Parcel: &shippingv1.Parcel{
			Audit: &commonv1.AuditInfo{Etag: renewOrderReply.GetOrder().GetAudit().GetEtag()},
		},
		Details: &shippingv1.ApproveParcelRequest_Details{
			Approver: &identityv1.UserRef{
				UserId: lintSessionsReply.GetSession().GetAssignee().GetUserId(),
			},
		},
		ParcelId: envelope2.GetDisplayName(),
	}}
	if err := s.runRejectOrderNoteApproveParcel(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ParcelApprovalService/ApproveParcel")
	}
	approveParcelReply := step.reply
	_ = simulateRegisteredModelReply
	return &ordersv1.RejectOrderNoteResponse{
		Result: &commonv1.OperationResult{
			Status:       lintSessionsReply.GetResult().GetStatus(),
			ReferenceIds: lintSessionsReply.GetResult().GetReferenceIds(),
			Message:      renewOrderReply.GetOutcome().GetMessage(),
			Details:      renewOrderReply.GetResult().GetDetails(),
		},
		Outcome: &ordersv1.RejectOrderNoteResponse_Outcome{
			ReferenceId: approveParcelReply.GetOutcome().GetReferenceId(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		OrderNote: &ordersv1.OrderNote{OrderNoteId: req.GetOrderNoteId()},
	}, nil
}

// EscalateOrderNote escalates an order note.
func (s *OrderNoteApprovalServer) EscalateOrderNote(ctx context.Context, req *ordersv1.EscalateOrderNoteRequest) (*ordersv1.EscalateOrderNoteResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveOrderNote(ctx, req.GetContext().GetTenantId(), req.GetOrderNote()); err != nil {
		return nil, grpcx.Wrap(err, "save order note")
	}
	s.log.InfoContext(ctx, "escalate order note", "tenant", req.GetContext().GetTenantId())
	var envelope ordersv1.OrderRef
	if err := req.GetOrderNote().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	lintCustomerSegmentsRequest := &customersv1.LintCustomerSegmentsRequest{
		Context: req.GetContext(),
		Details: &customersv1.LintCustomerSegmentsRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetApprover().GetEmail()},
		},
		CustomerSegmentId: envelope.GetDisplayName(),
	}
	lintCustomerSegmentsReply, err := grpcx.Call(ctx, s.customerSegmentValidationClient.LintCustomerSegments, lintCustomerSegmentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentValidationService/LintCustomerSegments")
	}
	return &ordersv1.EscalateOrderNoteResponse{
		Result: &commonv1.OperationResult{
			Message:       lintCustomerSegmentsReply.GetResult().GetMessage(),
			CorrelationId: lintCustomerSegmentsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		OrderNote: &ordersv1.OrderNote{
			OrderNoteId: req.GetOrderNoteId(),
			OccurredAt:  req.GetOrderNote().GetOccurredAt(),
		},
	}, nil
}

// ListPendingOrderNotes lists order notes a page at a time.
func (s *OrderNoteApprovalServer) ListPendingOrderNotes(ctx context.Context, req *ordersv1.ListPendingOrderNotesRequest) (*ordersv1.ListPendingOrderNotesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ListPendingOrderNotes", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "list pending order notes", "tenant", req.GetContext().GetTenantId())
	createCustomerReply, err := s.customerClient.CreateCustomer(ctx, &customersv1.CreateCustomerRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			PrimaryAddress: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
			Contact:        &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerService/CreateCustomer")
	}
	listFulfillmentWorkflowsRequest := &fulfillmentv1.ListFulfillmentWorkflowsRequest{}
	listFulfillmentWorkflowsRequest.Context = req.GetContext()
	listFulfillmentWorkflowsRequest.Filter = &fulfillmentv1.ListFulfillmentWorkflowsRequest_Filter{
		Query:        req.GetFilter().GetQuery(),
		CreatedAfter: req.GetFilter().GetCreatedAfter(),
	}
	listFulfillmentWorkflowsRequest.Page = &commonv1.PageRequest{PageToken: req.GetPage().GetPageToken()}
	listFulfillmentWorkflowsReply, err := s.fulfillmentWorkflowClient.ListFulfillmentWorkflows(ctx, listFulfillmentWorkflowsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.FulfillmentWorkflowService/ListFulfillmentWorkflows")
	}
	return &ordersv1.ListPendingOrderNotesResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: createCustomerReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		OrderNotes: []*ordersv1.OrderNote{&ordersv1.OrderNote{
			Attributes:    createCustomerReply.GetCustomer().GetAttributes(),
			CorrelationId: listFulfillmentWorkflowsReply.GetResult().GetCorrelationId(),
		}},
	}, nil
}

// DelegateOrderNoteApproval delegates an order note.
func (s *OrderNoteApprovalServer) DelegateOrderNoteApproval(ctx context.Context, req *ordersv1.DelegateOrderNoteApprovalRequest) (*ordersv1.DelegateOrderNoteApprovalResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveOrderNote(ctx, req.GetContext().GetTenantId(), req.GetOrderNote()); err != nil {
		return nil, grpcx.Wrap(err, "save order note")
	}
	s.log.InfoContext(ctx, "delegate order note approval", "tenant", req.GetContext().GetTenantId())
	detailsCache := cache.New[string, *ordersv1.DelegateOrderNoteApprovalRequest_Details](1)
	detailsCache.Put(req.GetContext().GetTraceId(), req.GetDetails())
	cachedDetails, _ := detailsCache.Get(req.GetContext().GetTraceId())
	var envelope ordersv1.OrderRef
	if err := req.GetOrderNote().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	streamSearchIndexExportStream, err := s.searchIndexExportClient.StreamSearchIndexExport(ctx, &searchv1.StreamSearchIndexExportRequest{
		Context: req.GetContext(),
		Filter: &searchv1.StreamSearchIndexExportRequest_Filter{
			CreatedAfter: req.GetDetails().GetEffectiveAt(),
			OwnerId:      cachedDetails.GetApprover().GetUserId(),
			Labels:       req.GetOrderNote().GetAttributes(),
			Query:        envelope.GetDisplayName(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexExportService/StreamSearchIndexExport")
	}
	for {
		received, err := streamSearchIndexExportStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexExportService/StreamSearchIndexExport")
		}
		_ = received
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	var envelope2 ordersv1.OrderRef
	if err := req.GetOrderNote().GetDetail().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	pauseProductReviewWorkflowRequest := &reviewsv1.PauseProductReviewWorkflowRequest{
		Context: req.GetContext(),
		ProductReview: &reviewsv1.ProductReview{
			Number: req.GetOrderNote().GetSubject().GetNumber(),
			Lines:  []*reviewsv1.ProductReviewLine{&reviewsv1.ProductReviewLine{Attributes: req.GetOrderNote().GetAttributes()}},
			Counterparty: &customersv1.CustomerRef{
				DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
			},
		},
		Annotations:     annotations,
		ProductReviewId: envelope2.GetDisplayName(),
	}
	pauseProductReviewWorkflowReply := new(reviewsv1.PauseProductReviewWorkflowResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", reviewsProductReviewWorkflowServiceName, "PauseProductReviewWorkflow"), pauseProductReviewWorkflowRequest, pauseProductReviewWorkflowReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ProductReviewWorkflowService/PauseProductReviewWorkflow")
	}
	delegateBackorderApprovalRequest := &ordersv1.DelegateBackorderApprovalRequest{
		Context: req.GetContext(),
		Backorder: &ordersv1.Backorder{
			Origin: &ordersv1.Backorder_Customer{Customer: &customersv1.CustomerRef{
				Email: pauseProductReviewWorkflowReply.GetProductReview().GetCounterparty().GetEmail(),
			}},
			Assignee: &identityv1.UserRef{
				DisplayName: pauseProductReviewWorkflowReply.GetProductReview().GetCounterparty().GetDisplayName(),
			},
			Requester: &commonv1.ContactInfo{Email: req.GetDetails().GetApprover().GetEmail()},
		},
		Details: &ordersv1.DelegateBackorderApprovalRequest_Details{
			Approver: &identityv1.UserRef{UserId: req.GetDetails().GetApprover().GetUserId()},
		},
	}
	var delegateBackorderApprovalReply *ordersv1.DelegateBackorderApprovalResponse
	if err := pipeline.Run(ctx, "acme.orders.v1.BackorderApprovalService/DelegateBackorderApproval", func(ctx context.Context) error {
		answer, err := s.backorderApprovalClient.DelegateBackorderApproval(ctx, delegateBackorderApprovalRequest)
		delegateBackorderApprovalReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.BackorderApprovalService/DelegateBackorderApproval")
	}
	var envelope3 ordersv1.OrderRef
	if err := req.GetOrderNote().GetDetail().UnmarshalTo(&envelope3); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	escalateBackorderReply, err := s.backorderApprovalClient.EscalateBackorder(ctx, &ordersv1.EscalateBackorderRequest{
		Context: req.GetContext(),
		Backorder: &ordersv1.Backorder{
			Assignee: &identityv1.UserRef{
				Email: pauseProductReviewWorkflowReply.GetProductReview().GetCounterparty().GetEmail(),
			},
		},
		BackorderId: envelope3.GetDisplayName(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.BackorderApprovalService/EscalateBackorder")
	}
	_ = escalateBackorderReply
	return &ordersv1.DelegateOrderNoteApprovalResponse{
		Outcome: &ordersv1.DelegateOrderNoteApprovalResponse_Outcome{
			EffectiveAt: pauseProductReviewWorkflowReply.GetOutcome().GetEffectiveAt(),
			Status:      delegateBackorderApprovalReply.GetResult().GetStatus(),
		},
		Result: &commonv1.OperationResult{
			Message:       pauseProductReviewWorkflowReply.GetResult().GetMessage(),
			CorrelationId: req.GetOrderNote().GetCorrelationId(),
			Status:        "ok",
		},
		OrderNote: &ordersv1.OrderNote{
			Subject: &ordersv1.OrderRef{
				DisplayName: delegateBackorderApprovalReply.GetBackorder().GetCustomer().GetDisplayName(),
			},
			OrderNoteId: req.GetOrderNoteId(),
		},
	}, nil
}

// RecallOrderNote recalls an order note.
func (s *OrderNoteApprovalServer) RecallOrderNote(ctx context.Context, req *ordersv1.RecallOrderNoteRequest) (*ordersv1.RecallOrderNoteResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveOrderNote(ctx, req.GetContext().GetTenantId(), req.GetOrderNote()); err != nil {
		return nil, grpcx.Wrap(err, "save order note")
	}
	s.log.InfoContext(ctx, "recall order note", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "user_id", req.GetDetails().GetApprover().GetUserId())
	ctx = tenancy.With(ctx, "display_name", req.GetOrderNote().GetSubject().GetDisplayName())
	explainAccessPolicyDecisionRequest := &authorizationv1.ExplainAccessPolicyDecisionRequest{
		Context: req.GetContext(),
		AccessPolicy: &authorizationv1.AccessPolicy{
			Owner: &identityv1.UserRef{
				UserId:      tenancy.Value(ctx, "user_id"),
				DisplayName: tenancy.Value(ctx, "display_name"),
			},
			Action: &authorizationv1.AccessPolicy_Block{Block: &authorizationv1.AccessPolicyBlockAction{NotifyEmail: req.GetDetails().GetApproverEmail()}},
		},
	}
	explainAccessPolicyDecisionReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*authorizationv1.ExplainAccessPolicyDecisionResponse, error) {
		return s.accessPolicyValidationClient.ExplainAccessPolicyDecision(ctx, explainAccessPolicyDecisionRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyValidationService/ExplainAccessPolicyDecision")
	}
	var envelope ordersv1.OrderRef
	if err := req.GetOrderNote().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	approveReturnLabelRequest := &returnsv1.ApproveReturnLabelRequest{}
	approveReturnLabelRequest.Context = req.GetContext()
	approveReturnLabelRequest.ReturnLabel = &returnsv1.ReturnLabel{
		Lines: []*returnsv1.ReturnLabelLine{&returnsv1.ReturnLabelLine{
			Tax: &returnsv1.ReturnLabelLineTax{
				Amount: explainAccessPolicyDecisionReply.GetAccessPolicy().GetDiscount().GetAmount(),
			},
		}},
		Counterparty: &returnsv1.InspectionRef{
			DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
		},
	}
	approveReturnLabelRequest.ReturnLabelId = envelope.GetDisplayName()
	approveReturnLabelReply, err := s.returnLabelApprovalClient.ApproveReturnLabel(ctx, approveReturnLabelRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnLabelApprovalService/ApproveReturnLabel")
	}
	return &ordersv1.RecallOrderNoteResponse{
		Result: &commonv1.OperationResult{
			Details: explainAccessPolicyDecisionReply.GetResult().GetDetails(),
			Status:  approveReturnLabelReply.GetOutcome().GetStatus(),
		},
		OrderNote: &ordersv1.OrderNote{OrderNoteId: req.GetOrderNoteId()},
		Outcome:   &ordersv1.RecallOrderNoteResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// GetOrderNoteApprovalStatus returns an order note.
func (s *OrderNoteApprovalServer) GetOrderNoteApprovalStatus(ctx context.Context, req *ordersv1.GetOrderNoteApprovalStatusRequest) (*ordersv1.GetOrderNoteApprovalStatusResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "order_note", req.GetOrderNoteId(), "get_order_note_approval_status"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get order note approval status", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetContext()).(*commonv1.RequestContext)
	recallOrderAmendmentRequest := &ordersv1.RecallOrderAmendmentRequest{}
	recallOrderAmendmentRequest.Context = req.GetContext()
	recallOrderAmendmentRequest.Details = &ordersv1.RecallOrderAmendmentRequest_Details{
		Approver: &identityv1.UserRef{UserId: cloned.GetActor().GetUserId()},
	}
	recallOrderAmendmentReply, err := s.orderAmendmentApprovalClient.RecallOrderAmendment(ctx, recallOrderAmendmentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderAmendmentApprovalService/RecallOrderAmendment")
	}
	return &ordersv1.GetOrderNoteApprovalStatusResponse{
		OrderNote: &ordersv1.OrderNote{
			CorrelationId: recallOrderAmendmentReply.GetResult().GetCorrelationId(),
			Subject: &ordersv1.OrderRef{
				DisplayName: recallOrderAmendmentReply.GetOrderAmendment().GetCounterparty().GetDisplayName(),
			},
			OrderNoteId: req.GetOrderNoteId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// orderNoteApprovalServerRejectOrderNoteApproveParcelStep carries the request of one step of RejectOrderNote and, once
// it ran, its reply.
type orderNoteApprovalServerRejectOrderNoteApproveParcelStep struct {
	request *shippingv1.ApproveParcelRequest
	reply   *shippingv1.ApproveParcelResponse
}

func (s *OrderNoteApprovalServer) runRejectOrderNoteApproveParcel(ctx context.Context, step *orderNoteApprovalServerRejectOrderNoteApproveParcelStep) error {
	reply, err := s.parcelApprovalClient.ApproveParcel(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
