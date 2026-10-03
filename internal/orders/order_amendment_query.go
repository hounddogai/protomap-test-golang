package orders

import (
	"context"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	ordersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/orders/v1"
	toursv1 "github.com/hounddogai/protomap-test-golang/gen/acme/tours/v1"
	warehousingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/warehousing/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/anypb"
)

// OrderAmendmentQueryServer implements `acme.orders.v1.OrderAmendmentQueryService`.
// Answers read-only questions about order amendments, for screens and reports.
type OrderAmendmentQueryServer struct {
	ordersv1.UnimplementedOrderAmendmentQueryServiceServer
	store                              *Store
	log                                *slog.Logger
	orderNoteCalculationClient         ordersv1.OrderNoteCalculationServiceClient
	roleAdminClient                    identityv1.RoleAdminServiceClient
	orderAmendmentValidationClient     ordersv1.OrderAmendmentValidationServiceClient
	orderNoteCalculationClientFallback ordersv1.OrderNoteCalculationServiceClient
	apiCredentialNotificationClient    identityv1.ApiCredentialNotificationServiceClient
	pickListValidationClient           warehousingv1.PickListValidationServiceClient
	backorderCalculationClient         ordersv1.BackorderCalculationServiceClient
	backorderCalculationClientFallback ordersv1.BackorderCalculationServiceClient
	tourQueryPort                      tourQueryPort
	compareApiCredentialVersionsFn     func(context.Context, *identityv1.CompareApiCredentialVersionsRequest, ...grpc.CallOption) (*identityv1.CompareApiCredentialVersionsResponse, error)
}

// NewOrderAmendmentQueryServer wires an OrderAmendmentQueryServer.
func NewOrderAmendmentQueryServer(deps platform.Deps) *OrderAmendmentQueryServer {
	apiCredentialHistory := identityv1.NewApiCredentialHistoryServiceClient(deps.Conn)
	return &OrderAmendmentQueryServer{
		store:                              NewStore(deps.DB),
		log:                                deps.Logger.With("service", "acme.orders.v1.OrderAmendmentQueryService"),
		orderNoteCalculationClient:         ordersv1.NewOrderNoteCalculationServiceClient(deps.Conn),
		roleAdminClient:                    identityv1.NewRoleAdminServiceClient(deps.Conn),
		orderAmendmentValidationClient:     ordersv1.NewOrderAmendmentValidationServiceClient(deps.Conn),
		orderNoteCalculationClientFallback: ordersv1.NewOrderNoteCalculationServiceClient(deps.Conn),
		apiCredentialNotificationClient:    identityv1.NewApiCredentialNotificationServiceClient(deps.Conn),
		pickListValidationClient:           warehousingv1.NewPickListValidationServiceClient(deps.Conn),
		backorderCalculationClient:         ordersv1.NewBackorderCalculationServiceClient(deps.Conn),
		backorderCalculationClientFallback: ordersv1.NewBackorderCalculationServiceClient(deps.Conn),
		tourQueryPort:                      newTourQueryPort(deps),
		compareApiCredentialVersionsFn:     apiCredentialHistory.CompareApiCredentialVersions,
	}
}

// LookupOrderAmendment returns an order amendment.
func (s *OrderAmendmentQueryServer) LookupOrderAmendment(ctx context.Context, req *ordersv1.LookupOrderAmendmentRequest) (*ordersv1.LookupOrderAmendmentResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "order_amendment", req.GetOrderAmendmentId(), "lookup_order_amendment"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "lookup order amendment", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "email", req.GetContext().GetActor().GetEmail())
	packed, err := anypb.New(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	explainOrderNoteCalculationRequest := &ordersv1.ExplainOrderNoteCalculationRequest{}
	explainOrderNoteCalculationRequest.Context = req.GetContext()
	explainOrderNoteCalculationRequest.Details = &ordersv1.ExplainOrderNoteCalculationRequest_Details{
		Contact: &commonv1.ContactInfo{Email: tenancy.Value(ctx, "email")},
		Locale:  req.GetContext().GetLocale(),
	}
	explainOrderNoteCalculationRequest.OrderNote = &ordersv1.OrderNote{
		CorrelationId: req.GetOrderAmendmentId(),
		Detail:        packed,
	}
	explainOrderNoteCalculationReply, err := s.orderNoteCalculationClient.ExplainOrderNoteCalculation(ctx, explainOrderNoteCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderNoteCalculationService/ExplainOrderNoteCalculation")
	}
	return &ordersv1.LookupOrderAmendmentResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: explainOrderNoteCalculationReply.GetResult().GetReferenceIds(),
			Status:       explainOrderNoteCalculationReply.GetResult().GetStatus(),
		},
		OrderAmendment: &ordersv1.OrderAmendment{OrderAmendmentId: req.GetOrderAmendmentId()},
	}, nil
}

// BrowseOrderAmendments lists order amendments a page at a time.
func (s *OrderAmendmentQueryServer) BrowseOrderAmendments(ctx context.Context, req *ordersv1.BrowseOrderAmendmentsRequest) (*ordersv1.BrowseOrderAmendmentsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "browse order amendments", "tenant", req.GetContext().GetTenantId())
	compareOrderNoteOptionsRequest := buildCompareOrderNoteOptionsRequestForBrowseOrderAmendments(req)
	compareOrderNoteOptionsReply, err := s.orderNoteCalculationClient.CompareOrderNoteOptions(ctx, compareOrderNoteOptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderNoteCalculationService/CompareOrderNoteOptions")
	}
	return &ordersv1.BrowseOrderAmendmentsResponse{
		Result: &commonv1.OperationResult{
			Status:  compareOrderNoteOptionsReply.GetResult().GetStatus(),
			Message: compareOrderNoteOptionsReply.GetResult().GetMessage(),
		},
	}, nil
}

// FindOrderAmendments lists order amendments a page at a time.
func (s *OrderAmendmentQueryServer) FindOrderAmendments(ctx context.Context, req *ordersv1.FindOrderAmendmentsRequest) (*ordersv1.FindOrderAmendmentsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "find order amendments", "tenant", req.GetContext().GetTenantId())
	encoded, err := protojson.Marshal(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded commonv1.RequestContext
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	snapshot, err := protojson.Marshal(req.GetPage())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	repairRoleRequest := &identityv1.RepairRoleRequest{
		Context: req.GetContext(),
		Role: &identityv1.Role{
			Owner: &identityv1.UserRef{
				Email:  req.GetContext().GetActor().GetEmail(),
				UserId: (&decoded).GetActor().GetUserId(),
			},
		},
		Details: &identityv1.RepairRoleRequest_Details{Payload: snapshot},
	}
	type repairRoleResult struct {
		reply *identityv1.RepairRoleResponse
		err   error
	}
	repairRoleResults := make(chan repairRoleResult, 1)
	go func() {
		reply, err := s.roleAdminClient.RepairRole(ctx, repairRoleRequest)
		repairRoleResults <- repairRoleResult{reply, err}
	}()
	var repairRoleReply *identityv1.RepairRoleResponse
	select {
	case result := <-repairRoleResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.identity.v1.RoleAdminService/RepairRole")
		}
		repairRoleReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	normalizeOrderAmendmentReply, err := s.orderAmendmentValidationClient.NormalizeOrderAmendment(ctx, &ordersv1.NormalizeOrderAmendmentRequest{
		Context: req.GetContext(),
		OrderAmendment: &ordersv1.OrderAmendment{
			Lines: []*ordersv1.OrderAmendmentLine{&ordersv1.OrderAmendmentLine{Amount: repairRoleReply.GetRole().GetDiscount().GetAmount()}},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: repairRoleReply.GetRole().GetAudit().GetUpdatedBy(),
			},
			Counterparty: &ordersv1.BackorderRef{
				DisplayName: repairRoleReply.GetRole().GetOwner().GetDisplayName(),
			},
		},
		Details: &ordersv1.NormalizeOrderAmendmentRequest_Details{
			Contact: &commonv1.ContactInfo{Email: repairRoleReply.GetRole().GetOwner().GetEmail()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderAmendmentValidationService/NormalizeOrderAmendment")
	}
	return &ordersv1.FindOrderAmendmentsResponse{
		OrderAmendments: []*ordersv1.OrderAmendment{&ordersv1.OrderAmendment{
			Audit: &commonv1.AuditInfo{
				CreatedBy: repairRoleReply.GetRole().GetAudit().GetCreatedBy(),
			},
			BillingAddress: &commonv1.PostalAddress{
				PostalCode: normalizeOrderAmendmentReply.GetOrderAmendment().GetBillingAddress().GetPostalCode(),
			},
			OrderAmendmentId: normalizeOrderAmendmentReply.GetOrderAmendment().GetOrderAmendmentId(),
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CountOrderAmendments counts order amendments by status.
func (s *OrderAmendmentQueryServer) CountOrderAmendments(ctx context.Context, req *ordersv1.CountOrderAmendmentsRequest) (*ordersv1.CountOrderAmendmentsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CountOrderAmendments", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "count order amendments", "tenant", req.GetContext().GetTenantId())
	routes := map[string]func(context.Context, *ordersv1.EstimateOrderNoteRequest, ...grpc.CallOption) (*ordersv1.EstimateOrderNoteResponse, error){
		"primary":  s.orderNoteCalculationClient.EstimateOrderNote,
		"fallback": s.orderNoteCalculationClientFallback.EstimateOrderNote,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	packed, err := anypb.New(req.GetPage())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	estimateOrderNoteRequest := &ordersv1.EstimateOrderNoteRequest{
		Context: req.GetContext(),
		Details: &ordersv1.EstimateOrderNoteRequest_Details{
			Destination: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
		OrderNote: &ordersv1.OrderNote{
			OccurredAt: req.GetFilter().GetCreatedAfter(),
			Attributes: req.GetFilter().GetLabels(),
			Detail:     packed,
		},
	}
	estimateOrderNoteReply, err := route(ctx, estimateOrderNoteRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderNoteCalculationService/EstimateOrderNote")
	}
	return &ordersv1.CountOrderAmendmentsResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: estimateOrderNoteReply.GetOrderNote().GetCorrelationId(),
			Message:       estimateOrderNoteReply.GetResult().GetMessage(),
			Status:        "ok",
		},
	}, nil
}

// ResolveOrderAmendments returns several order amendments at once.
func (s *OrderAmendmentQueryServer) ResolveOrderAmendments(ctx context.Context, req *ordersv1.ResolveOrderAmendmentsRequest) (*ordersv1.ResolveOrderAmendmentsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ResolveOrderAmendments", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "resolve order amendments", "tenant", req.GetContext().GetTenantId())
	baggage := make(map[string]string, len(req.GetContext().GetBaggage()))
	for key, value := range req.GetContext().GetBaggage() {
		baggage[key] = value
	}
	unsubscribeApiCredentialUpdatesRequest := &identityv1.UnsubscribeApiCredentialUpdatesRequest{
		Context: req.GetContext(),
		Details: &identityv1.UnsubscribeApiCredentialUpdatesRequest_Details{
			Recipient: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Variables: baggage,
		},
	}
	unsubscribeApiCredentialUpdatesReply, err := grpcx.Call(ctx, s.apiCredentialNotificationClient.UnsubscribeApiCredentialUpdates, unsubscribeApiCredentialUpdatesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.ApiCredentialNotificationService/UnsubscribeApiCredentialUpdates")
	}
	packed, err := anypb.New(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	calculateOrderNoteRequest := &ordersv1.CalculateOrderNoteRequest{
		Context: req.GetContext(),
		OrderNote: &ordersv1.OrderNote{
			Subject: &ordersv1.OrderRef{
				DisplayName: unsubscribeApiCredentialUpdatesReply.GetApiCredential().GetHolder().GetDisplayName(),
			},
			CorrelationId: unsubscribeApiCredentialUpdatesReply.GetResult().GetCorrelationId(),
			OccurredAt:    unsubscribeApiCredentialUpdatesReply.GetApiCredential().GetAudit().GetCreatedAt(),
			Detail:        packed,
		},
		Details: &ordersv1.CalculateOrderNoteRequest_Details{
			CurrencyCode: unsubscribeApiCredentialUpdatesReply.GetApiCredential().GetCurrencyCode(),
		},
	}
	calculateOrderNoteReply, err := grpcx.Call(ctx, s.orderNoteCalculationClient.CalculateOrderNote, calculateOrderNoteRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderNoteCalculationService/CalculateOrderNote")
	}
	return &ordersv1.ResolveOrderAmendmentsResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: unsubscribeApiCredentialUpdatesReply.GetResult().GetCorrelationId(),
			Details:       calculateOrderNoteReply.GetResult().GetDetails(),
			Status:        calculateOrderNoteReply.GetResult().GetStatus(),
		},
		OrderAmendments: []*ordersv1.OrderAmendment{&ordersv1.OrderAmendment{
			Counterparty: &ordersv1.BackorderRef{
				Priority: unsubscribeApiCredentialUpdatesReply.GetApiCredential().GetHolder().GetPriority(),
			},
		}},
	}, nil
}

// ExportOrderAmendments streams every matching order amendment.
func (s *OrderAmendmentQueryServer) ExportOrderAmendments(req *ordersv1.ExportOrderAmendmentsRequest, stream grpc.ServerStreamingServer[ordersv1.ExportOrderAmendmentsResponse]) error {
	ctx := stream.Context()
	if err := validateContext(req.GetContext()); err != nil {
		return err
	}
	s.log.InfoContext(ctx, "export order amendments", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "region", req.GetFilter().GetRegion())
	explainPickListDecisionReply, err := s.pickListValidationClient.ExplainPickListDecision(ctx, &warehousingv1.ExplainPickListDecisionRequest{
		Context: req.GetContext(),
		PickList: &warehousingv1.PickList{
			BillingAddress: &commonv1.PostalAddress{Region: tenancy.Value(ctx, "region")},
		},
	})
	if err != nil {
		return grpcx.Wrap(err, "acme.warehousing.v1.PickListValidationService/ExplainPickListDecision")
	}
	packed, err := anypb.New(req.GetFilter())
	if err != nil {
		return grpcx.Wrap(err, "pack payload")
	}
	recalculateOrderNotesRequest := &ordersv1.RecalculateOrderNotesRequest{}
	recalculateOrderNotesRequest.Context = req.GetContext()
	recalculateOrderNotesRequest.OrderNote = &ordersv1.OrderNote{
		Subject: &ordersv1.OrderRef{
			DisplayName: explainPickListDecisionReply.GetPickList().GetCounterparty().GetDisplayName(),
		},
		Detail: packed,
	}
	recalculateOrderNotesReply, err := s.orderNoteCalculationClient.RecalculateOrderNotes(ctx, recalculateOrderNotesRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.orders.v1.OrderNoteCalculationService/RecalculateOrderNotes")
	}
	if err := stream.Send(&ordersv1.ExportOrderAmendmentsResponse{
		Result: &commonv1.OperationResult{
			Status:       explainPickListDecisionReply.GetResult().GetStatus(),
			ReferenceIds: recalculateOrderNotesReply.GetResult().GetReferenceIds(),
		},
		OrderAmendment: &ordersv1.OrderAmendment{
			Lines: []*ordersv1.OrderAmendmentLine{&ordersv1.OrderAmendmentLine{
				Attributes: recalculateOrderNotesReply.GetOrderNote().GetAttributes(),
			}},
			BillingAddress: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
	}); err != nil {
		return err
	}
	return nil
}

// SummarizeOrderAmendments summarizes an order amendment.
func (s *OrderAmendmentQueryServer) SummarizeOrderAmendments(ctx context.Context, req *ordersv1.SummarizeOrderAmendmentsRequest) (*ordersv1.SummarizeOrderAmendmentsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SummarizeOrderAmendments", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "order_amendment", req.GetOrderAmendmentId(), "summarize_order_amendments"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "summarize order amendments", "tenant", req.GetContext().GetTenantId())
	routes := map[string]func(context.Context, *ordersv1.CalculateBackorderRequest, ...grpc.CallOption) (*ordersv1.CalculateBackorderResponse, error){
		"primary":  s.backorderCalculationClient.CalculateBackorder,
		"fallback": s.backorderCalculationClientFallback.CalculateBackorder,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	calculateBackorderRequest := &ordersv1.CalculateBackorderRequest{
		Context: req.GetContext(),
		Reason:  req.GetReason(),
	}
	calculateBackorderReply, err := route(ctx, calculateBackorderRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.BackorderCalculationService/CalculateBackorder")
	}
	resolveToursRequest := &toursv1.ResolveToursRequest{Context: req.GetContext()}
	resolveToursReply, err := s.tourQueryPort.ResolveTours(ctx, resolveToursRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.tours.v1.TourQueryService/ResolveTours")
	}
	return &ordersv1.SummarizeOrderAmendmentsResponse{
		Result: &commonv1.OperationResult{
			Status:        calculateBackorderReply.GetResult().GetStatus(),
			CorrelationId: resolveToursReply.GetResult().GetCorrelationId(),
			Details:       resolveToursReply.GetResult().GetDetails(),
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// SuggestOrderAmendments lists order amendments a page at a time.
func (s *OrderAmendmentQueryServer) SuggestOrderAmendments(ctx context.Context, req *ordersv1.SuggestOrderAmendmentsRequest) (*ordersv1.SuggestOrderAmendmentsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SuggestOrderAmendments", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "suggest order amendments", "tenant", req.GetContext().GetTenantId())
	return &ordersv1.SuggestOrderAmendmentsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// PeekOrderAmendment returns an order amendment.
func (s *OrderAmendmentQueryServer) PeekOrderAmendment(ctx context.Context, req *ordersv1.PeekOrderAmendmentRequest) (*ordersv1.PeekOrderAmendmentResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "order_amendment", req.GetOrderAmendmentId(), "peek_order_amendment"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "peek order amendment", "tenant", req.GetContext().GetTenantId())
	snapshot, err := protojson.Marshal(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	compareApiCredentialVersionsRequest := &identityv1.CompareApiCredentialVersionsRequest{
		Context: req.GetContext(),
		ApiCredential: &identityv1.ApiCredential{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Holder:  &identityv1.SessionRef{SessionId: req.GetOrderAmendmentId()},
			Audit:   &commonv1.AuditInfo{CreatedAt: req.GetContext().GetDeadline()},
			Funding: &identityv1.ApiCredential_ExternalAccountId{ExternalAccountId: req.GetContext().GetActor().GetUserId()},
		},
		Details: &identityv1.CompareApiCredentialVersionsRequest_Details{Snapshot: snapshot},
	}
	compareApiCredentialVersionsReply, err := s.compareApiCredentialVersionsFn(ctx, compareApiCredentialVersionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.ApiCredentialHistoryService/CompareApiCredentialVersions")
	}
	routes := map[string]func(context.Context, *ordersv1.ExplainOrderNoteCalculationRequest, ...grpc.CallOption) (*ordersv1.ExplainOrderNoteCalculationResponse, error){
		"primary":  s.orderNoteCalculationClient.ExplainOrderNoteCalculation,
		"fallback": s.orderNoteCalculationClientFallback.ExplainOrderNoteCalculation,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	packed, err := anypb.New(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	explainOrderNoteCalculationRequest := &ordersv1.ExplainOrderNoteCalculationRequest{
		Context: req.GetContext(),
		Details: &ordersv1.ExplainOrderNoteCalculationRequest_Details{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: compareApiCredentialVersionsReply.GetApiCredential().GetContact().GetPreferredChannel(),
			},
		},
		OrderNote: &ordersv1.OrderNote{Detail: packed},
	}
	explainOrderNoteCalculationReply, err := route(ctx, explainOrderNoteCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderNoteCalculationService/ExplainOrderNoteCalculation")
	}
	_ = explainOrderNoteCalculationReply
	return &ordersv1.PeekOrderAmendmentResponse{
		Result: &commonv1.OperationResult{
			Message: compareApiCredentialVersionsReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		OrderAmendment: &ordersv1.OrderAmendment{OrderAmendmentId: req.GetOrderAmendmentId()},
	}, nil
}
