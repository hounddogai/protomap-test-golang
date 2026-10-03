package fraud

import (
	"context"
	"fmt"
	"log/slog"

	appointmentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/appointments/v1"
	careplansv1 "github.com/hounddogai/protomap-test-golang/gen/acme/careplans/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	formsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/forms/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	fulfillmentv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fulfillment/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	inventoryv1 "github.com/hounddogai/protomap-test-golang/gen/acme/inventory/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// FraudRuleApprovalServer implements `acme.fraud.v1.FraudRuleApprovalService`.
// Routes fraud rules through approval, with escalation and delegation.
type FraudRuleApprovalServer struct {
	fraudv1.UnimplementedFraudRuleApprovalServiceServer
	store                            *Store
	log                              *slog.Logger
	meteredCarePlanCalculation       meteredCarePlanCalculation
	conn                             *grpc.ClientConn
	stockItemWorkflowClient          inventoryv1.StockItemWorkflowServiceClient
	searchIndexClient                searchv1.SearchIndexServiceClient
	tenantAdminClient                identityv1.TenantAdminServiceClient
	customerSegmentCalculationClient customersv1.CustomerSegmentCalculationServiceClient
	bus                              *eventbus.Bus
	clients                          *registry.Registry
	pointsLedgerApprovalClient       loyaltyv1.PointsLedgerApprovalServiceClient
	fraudSignalCalculationClient     fraudv1.FraudSignalCalculationServiceClient
}

// NewFraudRuleApprovalServer wires a FraudRuleApprovalServer.
func NewFraudRuleApprovalServer(deps platform.Deps) *FraudRuleApprovalServer {
	return &FraudRuleApprovalServer{
		store:                            NewStore(deps.DB),
		log:                              deps.Logger.With("service", "acme.fraud.v1.FraudRuleApprovalService"),
		meteredCarePlanCalculation:       meteredCarePlanCalculation{CarePlanCalculationServiceClient: careplansv1.NewCarePlanCalculationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		conn:                             deps.Conn,
		stockItemWorkflowClient:          inventoryv1.NewStockItemWorkflowServiceClient(deps.Conn),
		searchIndexClient:                searchv1.NewSearchIndexServiceClient(deps.Conn),
		tenantAdminClient:                identityv1.NewTenantAdminServiceClient(deps.Conn),
		customerSegmentCalculationClient: customersv1.NewCustomerSegmentCalculationServiceClient(deps.Conn),
		bus:                              deps.Bus,
		clients:                          deps.Clients,
		pointsLedgerApprovalClient:       loyaltyv1.NewPointsLedgerApprovalServiceClient(deps.Conn),
		fraudSignalCalculationClient:     fraudv1.NewFraudSignalCalculationServiceClient(deps.Conn),
	}
}

// SubmitFraudRule submits a fraud rule.
func (s *FraudRuleApprovalServer) SubmitFraudRule(ctx context.Context, req *fraudv1.SubmitFraudRuleRequest) (*fraudv1.SubmitFraudRuleResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SubmitFraudRule", "error", err)
		return nil, err
	}
	if err := s.store.SaveFraudRule(ctx, req.GetContext().GetTenantId(), req.GetFraudRule()); err != nil {
		return nil, grpcx.Wrap(err, "save fraud rule")
	}
	s.log.InfoContext(ctx, "submit fraud rule", "tenant", req.GetContext().GetTenantId(), "etag", req.GetFraudRule().GetAudit().GetEtag())
	fraudRuleCache := cache.New[string, *fraudv1.FraudRule](1)
	fraudRuleCache.Put(req.GetContext().GetTraceId(), req.GetFraudRule())
	cachedFraudRule, _ := fraudRuleCache.Get(req.GetContext().GetTraceId())
	compareCarePlanOptionsRequest := &careplansv1.CompareCarePlanOptionsRequest{
		Context: req.GetContext(),
		CarePlan: &careplansv1.CarePlan{
			Counterparty: &appointmentsv1.AppointmentRef{DisplayName: cachedFraudRule.GetOwner().GetDisplayName()},
		},
	}
	compareCarePlanOptionsReply, err := s.meteredCarePlanCalculation.CompareCarePlanOptions(ctx, compareCarePlanOptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.careplans.v1.CarePlanCalculationService/CompareCarePlanOptions")
	}
	listPackingSlipWorkflowsRequest := &fulfillmentv1.ListPackingSlipWorkflowsRequest{
		Context: req.GetContext(),
		Filter: &fulfillmentv1.ListPackingSlipWorkflowsRequest_Filter{
			Region:       compareCarePlanOptionsReply.GetCarePlan().GetBillingAddress().GetRegion(),
			CreatedAfter: compareCarePlanOptionsReply.GetCarePlan().GetIssuedAt(),
			OwnerId:      req.GetFraudRule().GetOwner().GetUserId(),
			Labels:       compareCarePlanOptionsReply.GetResult().GetDetails(),
		},
	}
	listPackingSlipWorkflowsReply, err := grpcx.NewClient(s.conn, fulfillmentv1.NewPackingSlipWorkflowServiceClient).ListPackingSlipWorkflows(ctx, listPackingSlipWorkflowsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.PackingSlipWorkflowService/ListPackingSlipWorkflows")
	}
	return &fraudv1.SubmitFraudRuleResponse{
		FraudRule: &fraudv1.FraudRule{
			Audit: &commonv1.AuditInfo{
				Etag: compareCarePlanOptionsReply.GetCarePlan().GetAudit().GetEtag(),
			},
			FraudRuleId: req.GetFraudRuleId(),
			Name:        req.GetFraudRule().GetName(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: compareCarePlanOptionsReply.GetResult().GetCorrelationId(),
			Status:        listPackingSlipWorkflowsReply.GetResult().GetStatus(),
		},
		Outcome: &fraudv1.SubmitFraudRuleResponse_Outcome{
			Status: listPackingSlipWorkflowsReply.GetResult().GetStatus(),
		},
	}, nil
}

// ApproveFraudRule approves a fraud rule.
func (s *FraudRuleApprovalServer) ApproveFraudRule(ctx context.Context, req *fraudv1.ApproveFraudRuleRequest) (*fraudv1.ApproveFraudRuleResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveFraudRule(ctx, req.GetContext().GetTenantId(), req.GetFraudRule()); err != nil {
		return nil, grpcx.Wrap(err, "save fraud rule")
	}
	s.log.InfoContext(ctx, "approve fraud rule", "tenant", req.GetContext().GetTenantId(), "etag", req.GetFraudRule().GetAudit().GetEtag())
	signalStockItemWorkflowRequest := &inventoryv1.SignalStockItemWorkflowRequest{
		Context: req.GetContext(),
		StockItem: &inventoryv1.StockItem{
			Audit: &commonv1.AuditInfo{Etag: req.GetFraudRule().GetAudit().GetEtag()},
		},
		Details: &inventoryv1.SignalStockItemWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Email: req.GetDetails().GetApprover().GetEmail()},
		},
	}
	signalStockItemWorkflowReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*inventoryv1.SignalStockItemWorkflowResponse, error) {
		return s.stockItemWorkflowClient.SignalStockItemWorkflow(ctx, signalStockItemWorkflowRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.StockItemWorkflowService/SignalStockItemWorkflow")
	}
	updateSearchIndexReply, err := s.searchIndexClient.UpdateSearchIndex(ctx, &searchv1.UpdateSearchIndexRequest{
		Context: req.GetContext(),
		SearchIndex: &searchv1.SearchIndex{
			Dimensions: &searchv1.SearchIndexDimensions{
				Length: signalStockItemWorkflowReply.GetStockItem().GetDimensions().GetLength(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedAt: req.GetFraudRule().GetAudit().GetCreatedAt(),
				Etag:      signalStockItemWorkflowReply.GetStockItem().GetAudit().GetEtag(),
			},
			Locator: &searchv1.SearchIndex_Address{Address: &commonv1.PostalAddress{
				PostalCode: signalStockItemWorkflowReply.GetStockItem().GetAddress().GetPostalCode(),
			}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexService/UpdateSearchIndex")
	}
	simulateFormRequest := &formsv1.SimulateFormRequest{
		Context: req.GetContext(),
		Details: &formsv1.SimulateFormRequest_Details{
			Destination: &commonv1.PostalAddress{
				Lines: signalStockItemWorkflowReply.GetStockItem().GetAddress().GetLines(),
			},
		},
		Form: &formsv1.Form{
			Audit: &commonv1.AuditInfo{
				CreatedBy: updateSearchIndexReply.GetSearchIndex().GetAudit().GetCreatedBy(),
			},
		},
	}
	simulateFormReply := new(formsv1.SimulateFormResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", formsFormCalculationServiceName, "SimulateForm"), simulateFormRequest, simulateFormReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.forms.v1.FormCalculationService/SimulateForm")
	}
	encoded, err := protojson.Marshal(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded commonv1.RequestContext
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	cloned := proto.Clone(req.GetFraudRule()).(*fraudv1.FraudRule)
	lockTenantRequest := &identityv1.LockTenantRequest{
		Context:  req.GetContext(),
		TenantId: (&decoded).GetTenantId(),
		Details: &identityv1.LockTenantRequest_Details{
			NewOwner: &identityv1.UserRef{
				UserId:      signalStockItemWorkflowReply.GetStockItem().GetOwner().GetUserId(),
				DisplayName: cloned.GetOwner().GetDisplayName(),
			},
			NotifyContact: &commonv1.ContactInfo{
				Email: signalStockItemWorkflowReply.GetStockItem().GetOwner().GetEmail(),
			},
		},
	}
	lockTenantReply, err := identityv1.TenantAdminServiceClient.LockTenant(s.tenantAdminClient, ctx, lockTenantRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantAdminService/LockTenant")
	}
	_ = lockTenantReply
	return &fraudv1.ApproveFraudRuleResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: signalStockItemWorkflowReply.GetResult().GetReferenceIds(),
			Status:       signalStockItemWorkflowReply.GetResult().GetStatus(),
		},
		FraudRule: &fraudv1.FraudRule{
			Name: updateSearchIndexReply.GetSearchIndex().GetName(),
			Owner: &identityv1.UserRef{
				DisplayName: simulateFormReply.GetForm().GetCounterparty().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedBy: simulateFormReply.GetForm().GetAudit().GetCreatedBy(),
			},
			FraudRuleId: req.GetFraudRuleId(),
			Status:      req.GetFraudRule().GetStatus(),
		},
	}, nil
}

// EscalateFraudRule escalates a fraud rule.
func (s *FraudRuleApprovalServer) EscalateFraudRule(ctx context.Context, req *fraudv1.EscalateFraudRuleRequest) (*fraudv1.EscalateFraudRuleResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveFraudRule(ctx, req.GetContext().GetTenantId(), req.GetFraudRule()); err != nil {
		return nil, grpcx.Wrap(err, "save fraud rule")
	}
	s.log.InfoContext(ctx, "escalate fraud rule", "tenant", req.GetContext().GetTenantId(), "etag", req.GetFraudRule().GetAudit().GetEtag())
	rejectPointsLedgerRequest := &loyaltyv1.RejectPointsLedgerRequest{
		Context: req.GetContext(),
		PointsLedger: &loyaltyv1.PointsLedger{
			Audit: &commonv1.AuditInfo{UpdatedBy: req.GetFraudRule().GetAudit().GetUpdatedBy()},
		},
		Details: &loyaltyv1.RejectPointsLedgerRequest_Details{
			Approver: &identityv1.UserRef{Email: req.GetDetails().GetApprover().GetEmail()},
		},
	}
	rejectPointsLedgerReply := new(loyaltyv1.RejectPointsLedgerResponse)
	if err := grpc.Invoke(ctx, "/acme.loyalty.v1.PointsLedgerApprovalService/RejectPointsLedger", rejectPointsLedgerRequest, rejectPointsLedgerReply, s.conn); err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerApprovalService/RejectPointsLedger")
	}
	for _, condition := range req.GetFraudRule().GetConditions() {
		if _, err := s.customerSegmentCalculationClient.EstimateCustomerSegment(ctx, &customersv1.EstimateCustomerSegmentRequest{
			Context: req.GetContext(),
			CustomerSegment: &customersv1.CustomerSegment{
				Conditions: []*customersv1.CustomerSegmentCondition{&customersv1.CustomerSegmentCondition{
					Negate:    condition.GetNegate(),
					Value:     condition.GetValue(),
					Operator:  condition.GetOperator(),
					Attribute: condition.GetAttribute(),
				}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentCalculationService/EstimateCustomerSegment")
		}
	}
	return &fraudv1.EscalateFraudRuleResponse{
		FraudRule: &fraudv1.FraudRule{
			Audit: &commonv1.AuditInfo{
				CreatedBy: rejectPointsLedgerReply.GetPointsLedger().GetAudit().GetCreatedBy(),
			},
			FraudRuleId: req.GetFraudRuleId(),
		},
		Outcome: &fraudv1.EscalateFraudRuleResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ListPendingFraudRules lists fraud rules a page at a time.
func (s *FraudRuleApprovalServer) ListPendingFraudRules(ctx context.Context, req *fraudv1.ListPendingFraudRulesRequest) (*fraudv1.ListPendingFraudRulesResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "list pending fraud rules", "tenant", req.GetContext().GetTenantId())
	return &fraudv1.ListPendingFraudRulesResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// DelegateFraudRuleApproval delegates a fraud rule.
func (s *FraudRuleApprovalServer) DelegateFraudRuleApproval(ctx context.Context, req *fraudv1.DelegateFraudRuleApprovalRequest) (*fraudv1.DelegateFraudRuleApprovalResponse, error) {
	if err := s.store.SaveFraudRule(ctx, req.GetContext().GetTenantId(), req.GetFraudRule()); err != nil {
		return nil, grpcx.Wrap(err, "save fraud rule")
	}
	s.log.InfoContext(ctx, "delegate fraud rule approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetFraudRule().GetAudit().GetEtag())
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "fraud.delegate_fraud_rule_approval.explain_customer_segment_decision",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"customer_segment.owner.user_id": req.GetDetails().GetApprover().GetUserId(),
			"customer_segment.owner.email":   req.GetFraudRule().GetOwner().GetEmail(),
			"details.contact.email":          req.GetDetails().GetApprover().GetEmail(),
			"context.tenant_id":              req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	packed, err := anypb.New(req.GetFraudRule())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	calculateConsentRecordRequest := &customersv1.CalculateConsentRecordRequest{
		Context: req.GetContext(),
		ConsentRecord: &customersv1.ConsentRecord{
			Subject: &customersv1.HouseholdRef{DisplayName: req.GetFraudRule().GetOwner().GetDisplayName()},
			Detail:  packed,
		},
	}
	calculateConsentRecordReply := new(customersv1.CalculateConsentRecordResponse)
	if err := s.conn.Invoke(ctx, customersConsentRecordCalculationServiceCalculateConsentRecordMethod, calculateConsentRecordRequest, calculateConsentRecordReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordCalculationService/CalculateConsentRecord")
	}
	return &fraudv1.DelegateFraudRuleApprovalResponse{
		Result: &commonv1.OperationResult{Status: calculateConsentRecordReply.GetResult().GetStatus()},
		FraudRule: &fraudv1.FraudRule{
			FraudRuleId: req.GetFraudRuleId(),
			Name:        req.GetFraudRule().GetName(),
		},
	}, nil
}

// GetFraudRuleApprovalStatus returns a fraud rule.
func (s *FraudRuleApprovalServer) GetFraudRuleApprovalStatus(ctx context.Context, req *fraudv1.GetFraudRuleApprovalStatusRequest) (*fraudv1.GetFraudRuleApprovalStatusResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "fraud_rule", req.GetFraudRuleId(), "get_fraud_rule_approval_status"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get fraud rule approval status", "tenant", req.GetContext().GetTenantId())
	source := req.GetContext().GetActor().ProtoReflect()
	email := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("email"))).String()
	activateFraudRuleRequest := &fraudv1.ActivateFraudRuleRequest{
		Context:     req.GetContext(),
		FraudRuleId: req.GetFraudRuleId(),
		Details:     &fraudv1.ActivateFraudRuleRequest_Details{NewOwner: &identityv1.UserRef{Email: email}},
	}
	activateFraudRuleReply := new(fraudv1.ActivateFraudRuleResponse)
	if err := grpcx.Invoke(ctx, s.conn, fraudv1.FraudRuleLifecycleService_ActivateFraudRule_FullMethodName, activateFraudRuleRequest, activateFraudRuleReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.FraudRuleLifecycleService/ActivateFraudRule")
	}
	roleHistory, err := registry.Lookup[identityv1.RoleHistoryServiceClient](s.clients, "acme.identity.v1.RoleHistoryService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	snapshot, err := protojson.Marshal(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	compareRoleVersionsRequest := &identityv1.CompareRoleVersionsRequest{
		Context: req.GetContext(),
		Role: &identityv1.Role{
			Audit: &commonv1.AuditInfo{
				Etag: activateFraudRuleReply.GetFraudRule().GetAudit().GetEtag(),
			},
			Action: &identityv1.Role_Block{Block: &identityv1.RoleBlockAction{
				NotifyEmail: activateFraudRuleReply.GetFraudRule().GetBlock().GetNotifyEmail(),
			}},
		},
		Details: &identityv1.CompareRoleVersionsRequest_Details{Snapshot: snapshot},
	}
	compareRoleVersionsReply, err := roleHistory.CompareRoleVersions(ctx, compareRoleVersionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.RoleHistoryService/CompareRoleVersions")
	}
	return &fraudv1.GetFraudRuleApprovalStatusResponse{
		FraudRule: &fraudv1.FraudRule{
			Audit: &commonv1.AuditInfo{
				CreatedAt: activateFraudRuleReply.GetFraudRule().GetAudit().GetCreatedAt(),
			},
			Parameters:  compareRoleVersionsReply.GetRole().GetParameters(),
			FraudRuleId: req.GetFraudRuleId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CommentFraudRuleApproval comments a fraud rule.
func (s *FraudRuleApprovalServer) CommentFraudRuleApproval(ctx context.Context, req *fraudv1.CommentFraudRuleApprovalRequest) (*fraudv1.CommentFraudRuleApprovalResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveFraudRule(ctx, req.GetContext().GetTenantId(), req.GetFraudRule()); err != nil {
		return nil, grpcx.Wrap(err, "save fraud rule")
	}
	s.log.InfoContext(ctx, "comment fraud rule approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetFraudRule().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetFraudRule()).(*fraudv1.FraudRule)
	ctx = tenancy.With(ctx, "user_id", req.GetDetails().GetApprover().GetUserId())
	approvePointsLedgerRequest := &loyaltyv1.ApprovePointsLedgerRequest{}
	approvePointsLedgerRequest.Context = req.GetContext()
	approvePointsLedgerRequest.PointsLedger = &loyaltyv1.PointsLedger{
		Contact: &commonv1.ContactInfo{Email: cloned.GetOwner().GetEmail()},
	}
	approvePointsLedgerRequest.Details = &loyaltyv1.ApprovePointsLedgerRequest_Details{
		Approver: &identityv1.UserRef{
			Email:  req.GetDetails().GetApprover().GetEmail(),
			UserId: tenancy.Value(ctx, "user_id"),
		},
	}
	approvePointsLedgerReply, err := s.pointsLedgerApprovalClient.ApprovePointsLedger(ctx, approvePointsLedgerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerApprovalService/ApprovePointsLedger")
	}
	cloned2 := proto.Clone(req.GetDetails()).(*fraudv1.CommentFraudRuleApprovalRequest_Details)
	packed, err := anypb.New(req.GetFraudRule())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	explainFraudSignalCalculationReply, err := s.fraudSignalCalculationClient.ExplainFraudSignalCalculation(ctx, &fraudv1.ExplainFraudSignalCalculationRequest{
		Context: req.GetContext(),
		Details: &fraudv1.ExplainFraudSignalCalculationRequest_Details{
			Contact: &commonv1.ContactInfo{
				Name:  approvePointsLedgerReply.GetPointsLedger().GetContact().GetName(),
				Email: approvePointsLedgerReply.GetPointsLedger().GetContact().GetEmail(),
			},
		},
		FraudSignal: &fraudv1.FraudSignal{
			Subject: &fraudv1.FraudRuleRef{DisplayName: cloned2.GetApprover().GetDisplayName()},
			Detail:  packed,
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.FraudSignalCalculationService/ExplainFraudSignalCalculation")
	}
	return &fraudv1.CommentFraudRuleApprovalResponse{
		FraudRule: &fraudv1.FraudRule{
			Audit: &commonv1.AuditInfo{
				CreatedBy: approvePointsLedgerReply.GetPointsLedger().GetAudit().GetCreatedBy(),
			},
			FraudRuleId: req.GetFraudRuleId(),
			Version:     req.GetFraudRule().GetVersion(),
		},
		Outcome: &fraudv1.CommentFraudRuleApprovalResponse_Outcome{
			Status: explainFraudSignalCalculationReply.GetOutcome().GetStatus(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: explainFraudSignalCalculationReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}
