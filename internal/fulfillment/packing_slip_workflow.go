package fulfillment

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	auditv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audit/v1"
	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	fulfillmentv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fulfillment/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	pipelinesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/pipelines/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	workplacesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/workplaces/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// PackingSlipWorkflowServer implements `acme.fulfillment.v1.PackingSlipWorkflowService`.
// Runs the long-lived workflow that takes a packing slip from start to finish.
type PackingSlipWorkflowServer struct {
	fulfillmentv1.UnimplementedPackingSlipWorkflowServiceServer
	store                              *Store
	log                                *slog.Logger
	sellerPayoutScheduleWorkflowClient sellersv1.SellerPayoutScheduleWorkflowServiceClient
	userValidationClient               identityv1.UserValidationServiceClient
	packingSlipQueryClient             fulfillmentv1.PackingSlipQueryServiceClient
	accessPolicyValidationClient       authorizationv1.AccessPolicyValidationServiceClient
	conn                               *grpc.ClientConn
	pipelineOnce                       sync.Once
	pipelineCached                     pipelinesv1.PipelineServiceClient
	preferenceValidationClient         customersv1.PreferenceValidationServiceClient
	clients                            *registry.Registry
	consentRecordCalculationClient     customersv1.ConsentRecordCalculationServiceClient
	buildingQueryClient                workplacesv1.BuildingQueryServiceClient
	auditEventValidationClient         auditv1.AuditEventValidationServiceClient
	packingSlipApprovalClient          fulfillmentv1.PackingSlipApprovalServiceClient
	fulfillmentWorkflowClient          fulfillmentv1.FulfillmentWorkflowServiceClient
	customerQueryClient                customersv1.CustomerQueryServiceClient
	usageRecordApprovalClient          subscriptionsv1.UsageRecordApprovalServiceClient
	countSessionsFn                    func(context.Context, *identityv1.CountSessionsRequest, ...grpc.CallOption) (*identityv1.CountSessionsResponse, error)
	meteredCustomerCalculation         meteredCustomerCalculation
}

// NewPackingSlipWorkflowServer wires a PackingSlipWorkflowServer.
func NewPackingSlipWorkflowServer(deps platform.Deps) *PackingSlipWorkflowServer {
	sessionQuery := identityv1.NewSessionQueryServiceClient(deps.Conn)
	return &PackingSlipWorkflowServer{
		store:                              NewStore(deps.DB),
		log:                                deps.Logger.With("service", "acme.fulfillment.v1.PackingSlipWorkflowService"),
		sellerPayoutScheduleWorkflowClient: sellersv1.NewSellerPayoutScheduleWorkflowServiceClient(deps.Conn),
		userValidationClient:               identityv1.NewUserValidationServiceClient(deps.Conn),
		packingSlipQueryClient:             fulfillmentv1.NewPackingSlipQueryServiceClient(deps.Conn),
		accessPolicyValidationClient:       authorizationv1.NewAccessPolicyValidationServiceClient(deps.Conn),
		conn:                               deps.Conn,
		preferenceValidationClient:         customersv1.NewPreferenceValidationServiceClient(deps.Conn),
		clients:                            deps.Clients,
		consentRecordCalculationClient:     customersv1.NewConsentRecordCalculationServiceClient(deps.Conn),
		buildingQueryClient:                workplacesv1.NewBuildingQueryServiceClient(deps.Conn),
		auditEventValidationClient:         auditv1.NewAuditEventValidationServiceClient(deps.Conn),
		packingSlipApprovalClient:          fulfillmentv1.NewPackingSlipApprovalServiceClient(deps.Conn),
		fulfillmentWorkflowClient:          fulfillmentv1.NewFulfillmentWorkflowServiceClient(deps.Conn),
		customerQueryClient:                customersv1.NewCustomerQueryServiceClient(deps.Conn),
		usageRecordApprovalClient:          subscriptionsv1.NewUsageRecordApprovalServiceClient(deps.Conn),
		countSessionsFn:                    sessionQuery.CountSessions,
		meteredCustomerCalculation:         meteredCustomerCalculation{CustomerCalculationServiceClient: customersv1.NewCustomerCalculationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
	}
}

// StartPackingSlipWorkflow starts a packing slip.
func (s *PackingSlipWorkflowServer) StartPackingSlipWorkflow(ctx context.Context, req *fulfillmentv1.StartPackingSlipWorkflowRequest) (*fulfillmentv1.StartPackingSlipWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePackingSlip(ctx, req.GetContext().GetTenantId(), req.GetPackingSlip()); err != nil {
		return nil, grpcx.Wrap(err, "save packing slip")
	}
	s.log.InfoContext(ctx, "start packing slip workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPackingSlip().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "etag", req.GetPackingSlip().GetAudit().GetEtag())
	pauseSellerPayoutScheduleWorkflowReply, err := s.sellerPayoutScheduleWorkflowClient.PauseSellerPayoutScheduleWorkflow(ctx, &sellersv1.PauseSellerPayoutScheduleWorkflowRequest{
		Context: req.GetContext(),
		Details: &sellersv1.PauseSellerPayoutScheduleWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Email: req.GetDetails().GetNotify().GetEmail()},
		},
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
			Audit: &commonv1.AuditInfo{
				Etag:      tenancy.Value(ctx, "etag"),
				CreatedBy: req.GetPackingSlip().GetAudit().GetCreatedBy(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerPayoutScheduleWorkflowService/PauseSellerPayoutScheduleWorkflow")
	}
	validateUserReply, err := s.userValidationClient.ValidateUser(ctx, &identityv1.ValidateUserRequest{
		Context: req.GetContext(),
		User: &identityv1.User{
			RelationshipManager: &identityv1.UserRef{
				UserId: pauseSellerPayoutScheduleWorkflowReply.GetSellerPayoutSchedule().GetOrganizer().GetUserId(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedAt: req.GetPackingSlip().GetAudit().GetCreatedAt(),
				UpdatedAt: req.GetPackingSlip().GetAudit().GetUpdatedAt(),
				CreatedBy: req.GetPackingSlip().GetAudit().GetCreatedBy(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserValidationService/ValidateUser")
	}
	return &fulfillmentv1.StartPackingSlipWorkflowResponse{
		Outcome: &fulfillmentv1.StartPackingSlipWorkflowResponse_Outcome{
			EffectiveAt: pauseSellerPayoutScheduleWorkflowReply.GetOutcome().GetEffectiveAt(),
			Status:      validateUserReply.GetResult().GetStatus(),
		},
		PackingSlip: &fulfillmentv1.PackingSlip{
			BillingAddress: &commonv1.PostalAddress{
				Region: validateUserReply.GetUser().GetPrimaryAddress().GetRegion(),
			},
			PackingSlipId: req.GetPackingSlipId(),
			Counterparty:  req.GetPackingSlip().GetCounterparty(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// AdvancePackingSlipWorkflow advances a packing slip.
func (s *PackingSlipWorkflowServer) AdvancePackingSlipWorkflow(ctx context.Context, req *fulfillmentv1.AdvancePackingSlipWorkflowRequest) (*fulfillmentv1.AdvancePackingSlipWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected AdvancePackingSlipWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SavePackingSlip(ctx, req.GetContext().GetTenantId(), req.GetPackingSlip()); err != nil {
		return nil, grpcx.Wrap(err, "save packing slip")
	}
	s.log.InfoContext(ctx, "advance packing slip workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPackingSlip().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetPackingSlip()).(*fulfillmentv1.PackingSlip)
	summarizePackingSlipsRequest := &fulfillmentv1.SummarizePackingSlipsRequest{
		Context:       req.GetContext(),
		PackingSlipId: cloned.GetPackingSlipId(),
	}
	summarizePackingSlipsReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*fulfillmentv1.SummarizePackingSlipsResponse, error) {
		return s.packingSlipQueryClient.SummarizePackingSlips(ctx, summarizePackingSlipsRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.PackingSlipQueryService/SummarizePackingSlips")
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*authorizationv1.LintAccessPoliciesResponse, len(req.GetPackingSlip().GetLines()))
	for i, line := range req.GetPackingSlip().GetLines() {
		group.Go(func() error {
			reply, err := s.accessPolicyValidationClient.LintAccessPolicies(groupCtx, &authorizationv1.LintAccessPoliciesRequest{
				Context: req.GetContext(),
				AccessPolicy: &authorizationv1.AccessPolicy{
					Action:      &authorizationv1.AccessPolicy_Discount{Discount: &authorizationv1.AccessPolicyDiscountAction{Amount: line.GetAmount()}},
					Description: line.GetDescription(),
				},
				Details: &authorizationv1.LintAccessPoliciesRequest_Details{
					Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetNotify().GetEmail()},
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
		return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyValidationService/LintAccessPolicies")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	return &fulfillmentv1.AdvancePackingSlipWorkflowResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: summarizePackingSlipsReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		PackingSlip: &fulfillmentv1.PackingSlip{
			PackingSlipId: req.GetPackingSlipId(),
			Number:        req.GetPackingSlip().GetNumber(),
		},
	}, nil
}

// CancelPackingSlipWorkflow cancels a packing slip.
func (s *PackingSlipWorkflowServer) CancelPackingSlipWorkflow(ctx context.Context, req *fulfillmentv1.CancelPackingSlipWorkflowRequest) (*fulfillmentv1.CancelPackingSlipWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SavePackingSlip(ctx, req.GetContext().GetTenantId(), req.GetPackingSlip()); err != nil {
		return nil, grpcx.Wrap(err, "save packing slip")
	}
	s.log.InfoContext(ctx, "cancel packing slip workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPackingSlip().GetAudit().GetEtag())
	packed, err := anypb.New(req.GetPackingSlip())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	explainConsentRecordDecisionRequest := &customersv1.ExplainConsentRecordDecisionRequest{
		Context: req.GetContext(),
		ConsentRecord: &customersv1.ConsentRecord{
			Subject: &customersv1.HouseholdRef{
				DisplayName: req.GetPackingSlip().GetCounterparty().GetDisplayName(),
			},
			Detail: packed,
		},
	}
	explainConsentRecordDecisionReply := new(customersv1.ExplainConsentRecordDecisionResponse)
	if err := s.conn.Invoke(ctx, customersConsentRecordValidationServiceExplainConsentRecordDecisionMethod, explainConsentRecordDecisionRequest, explainConsentRecordDecisionReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordValidationService/ExplainConsentRecordDecision")
	}
	listPipelinesRequest := &pipelinesv1.ListPipelinesRequest{
		Context: req.GetContext(),
		Filter: &pipelinesv1.ListPipelinesRequest_Filter{
			Region:       req.GetPackingSlip().GetBillingAddress().GetRegion(),
			CreatedAfter: explainConsentRecordDecisionReply.GetOutcome().GetEffectiveAt(),
			OwnerId:      explainConsentRecordDecisionReply.GetConsentRecord().GetSubject().GetHouseholdId(),
		},
	}
	listPipelinesReply, err := s.pipelineLazy().ListPipelines(ctx, listPipelinesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pipelines.v1.PipelineService/ListPipelines")
	}
	previewPreferenceRequest := newPreviewPreferenceRequest(withPreviewPreferenceContext(req.GetContext()), withPreviewPreferencePreference(&customersv1.Preference{
		Audit: &commonv1.AuditInfo{
			UpdatedBy: req.GetPackingSlip().GetAudit().GetUpdatedBy(),
			UpdatedAt: req.GetPackingSlip().GetAudit().GetUpdatedAt(),
			Etag:      req.GetPackingSlip().GetAudit().GetEtag(),
		},
	}), withPreviewPreferenceDetails(&customersv1.PreviewPreferenceRequest_Details{
		Contact: &commonv1.ContactInfo{Name: req.GetDetails().GetNotify().GetName()},
	}))
	previewPreferenceReply, err := s.preferenceValidationClient.PreviewPreference(ctx, previewPreferenceRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceValidationService/PreviewPreference")
	}
	return &fulfillmentv1.CancelPackingSlipWorkflowResponse{
		PackingSlip: &fulfillmentv1.PackingSlip{
			Counterparty: &fulfillmentv1.FulfillmentRef{
				DisplayName: explainConsentRecordDecisionReply.GetConsentRecord().GetSubject().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedAt: previewPreferenceReply.GetPreference().GetAudit().GetCreatedAt(),
			},
			PackingSlipId: req.GetPackingSlipId(),
			IssuedAt:      req.GetPackingSlip().GetIssuedAt(),
		},
		Outcome: &fulfillmentv1.CancelPackingSlipWorkflowResponse_Outcome{Message: listPipelinesReply.GetResult().GetMessage()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RetryPackingSlipWorkflow retrys a packing slip.
func (s *PackingSlipWorkflowServer) RetryPackingSlipWorkflow(ctx context.Context, req *fulfillmentv1.RetryPackingSlipWorkflowRequest) (*fulfillmentv1.RetryPackingSlipWorkflowResponse, error) {
	if err := s.store.SavePackingSlip(ctx, req.GetContext().GetTenantId(), req.GetPackingSlip()); err != nil {
		return nil, grpcx.Wrap(err, "save packing slip")
	}
	s.log.InfoContext(ctx, "retry packing slip workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPackingSlip().GetAudit().GetEtag())
	roleNotification, err := registry.Lookup[identityv1.RoleNotificationServiceClient](s.clients, "acme.identity.v1.RoleNotificationService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	ctx = tenancy.With(ctx, "phone", req.GetDetails().GetNotify().GetPhone())
	packed, err := anypb.New(req.GetPackingSlip())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	unsubscribeRoleUpdatesRequest := &identityv1.UnsubscribeRoleUpdatesRequest{
		Context: req.GetContext(),
		Details: &identityv1.UnsubscribeRoleUpdatesRequest_Details{
			Recipient: &commonv1.ContactInfo{
				Name:             req.GetDetails().GetNotify().GetName(),
				PreferredChannel: req.GetDetails().GetNotify().GetPreferredChannel(),
				Phone:            tenancy.Value(ctx, "phone"),
			},
			Payload: packed,
		},
		Reason: req.GetReason(),
	}
	unsubscribeRoleUpdatesReply, err := roleNotification.UnsubscribeRoleUpdates(ctx, unsubscribeRoleUpdatesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.RoleNotificationService/UnsubscribeRoleUpdates")
	}
	packed2, err := anypb.New(req.GetPackingSlip())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	calculateConsentRecordRequest := &customersv1.CalculateConsentRecordRequest{
		Context: req.GetContext(),
		Details: &customersv1.CalculateConsentRecordRequest_Details{
			Destination: &commonv1.PostalAddress{
				Locality: req.GetPackingSlip().GetBillingAddress().GetLocality(),
				Lines:    req.GetPackingSlip().GetBillingAddress().GetLines(),
				Region:   req.GetPackingSlip().GetBillingAddress().GetRegion(),
			},
		},
		ConsentRecord: &customersv1.ConsentRecord{
			Subject: &customersv1.HouseholdRef{
				DisplayName: unsubscribeRoleUpdatesReply.GetRole().GetOwner().GetDisplayName(),
			},
			Detail: packed2,
		},
	}
	type calculateConsentRecordResult struct {
		reply *customersv1.CalculateConsentRecordResponse
		err   error
	}
	calculateConsentRecordResults := make(chan calculateConsentRecordResult, 1)
	go func() {
		reply, err := s.consentRecordCalculationClient.CalculateConsentRecord(ctx, calculateConsentRecordRequest)
		calculateConsentRecordResults <- calculateConsentRecordResult{reply, err}
	}()
	var calculateConsentRecordReply *customersv1.CalculateConsentRecordResponse
	select {
	case result := <-calculateConsentRecordResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.customers.v1.ConsentRecordCalculationService/CalculateConsentRecord")
		}
		calculateConsentRecordReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	exportBuildingsStream, err := s.buildingQueryClient.ExportBuildings(ctx, &workplacesv1.ExportBuildingsRequest{
		Context: req.GetContext(),
		Filter: &workplacesv1.ExportBuildingsRequest_Filter{
			Region:       req.GetPackingSlip().GetBillingAddress().GetRegion(),
			CreatedAfter: req.GetPackingSlip().GetAudit().GetCreatedAt(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.workplaces.v1.BuildingQueryService/ExportBuildings")
	}
	for {
		received, err := exportBuildingsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.workplaces.v1.BuildingQueryService/ExportBuildings")
		}
		_ = received
	}
	return &fulfillmentv1.RetryPackingSlipWorkflowResponse{
		PackingSlip: &fulfillmentv1.PackingSlip{
			Audit: &commonv1.AuditInfo{
				Etag: unsubscribeRoleUpdatesReply.GetRole().GetAudit().GetEtag(),
			},
			PackingSlipId: req.GetPackingSlipId(),
			IssuedAt:      req.GetPackingSlip().GetIssuedAt(),
		},
		Outcome: &fulfillmentv1.RetryPackingSlipWorkflowResponse_Outcome{
			Message: unsubscribeRoleUpdatesReply.GetResult().GetMessage(),
			Status:  calculateConsentRecordReply.GetResult().GetStatus(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// PausePackingSlipWorkflow pauses a packing slip.
func (s *PackingSlipWorkflowServer) PausePackingSlipWorkflow(ctx context.Context, req *fulfillmentv1.PausePackingSlipWorkflowRequest) (*fulfillmentv1.PausePackingSlipWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SavePackingSlip(ctx, req.GetContext().GetTenantId(), req.GetPackingSlip()); err != nil {
		return nil, grpcx.Wrap(err, "save packing slip")
	}
	s.log.InfoContext(ctx, "pause packing slip workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPackingSlip().GetAudit().GetEtag())
	explainAuditEventDecisionRequest := &auditv1.ExplainAuditEventDecisionRequest{}
	explainAuditEventDecisionRequest.Context = req.GetContext()
	explainAuditEventDecisionRequest.Details = &auditv1.ExplainAuditEventDecisionRequest_Details{
		Contact: &commonv1.ContactInfo{
			PreferredChannel: req.GetDetails().GetNotify().GetPreferredChannel(),
			Phone:            req.GetDetails().GetNotify().GetPhone(),
		},
	}
	explainAuditEventDecisionRequest.AuditEvent = &auditv1.AuditEvent{
		Subject: &auditv1.AuditTrailRef{
			DisplayName: req.GetPackingSlip().GetCounterparty().GetDisplayName(),
		},
	}
	explainAuditEventDecisionReply, err := s.auditEventValidationClient.ExplainAuditEventDecision(ctx, explainAuditEventDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.audit.v1.AuditEventValidationService/ExplainAuditEventDecision")
	}
	ctx = tenancy.With(ctx, "updated_by", req.GetPackingSlip().GetAudit().GetUpdatedBy())
	step := &packingSlipWorkflowServerPausePackingSlipWorkflowApprovePackingSlipStep{request: &fulfillmentv1.ApprovePackingSlipRequest{
		Context: req.GetContext(),
		PackingSlip: &fulfillmentv1.PackingSlip{
			Audit: &commonv1.AuditInfo{UpdatedBy: tenancy.Value(ctx, "updated_by")},
			Counterparty: &fulfillmentv1.FulfillmentRef{
				DisplayName: req.GetPackingSlip().GetCounterparty().GetDisplayName(),
				Number:      explainAuditEventDecisionReply.GetAuditEvent().GetSubject().GetNumber(),
			},
		},
	}}
	if err := s.runPausePackingSlipWorkflowApprovePackingSlip(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.PackingSlipApprovalService/ApprovePackingSlip")
	}
	approvePackingSlipReply := step.reply
	return &fulfillmentv1.PausePackingSlipWorkflowResponse{
		Outcome: &fulfillmentv1.PausePackingSlipWorkflowResponse_Outcome{
			NextSteps: explainAuditEventDecisionReply.GetOutcome().GetNextSteps(),
			Message:   approvePackingSlipReply.GetResult().GetMessage(),
		},
		PackingSlip: &fulfillmentv1.PackingSlip{
			Audit: &commonv1.AuditInfo{
				Etag: approvePackingSlipReply.GetPackingSlip().GetAudit().GetEtag(),
			},
			PackingSlipId: req.GetPackingSlipId(),
			DueAt:         req.GetPackingSlip().GetDueAt(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ResumePackingSlipWorkflow resumes a packing slip.
func (s *PackingSlipWorkflowServer) ResumePackingSlipWorkflow(ctx context.Context, req *fulfillmentv1.ResumePackingSlipWorkflowRequest) (*fulfillmentv1.ResumePackingSlipWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePackingSlip(ctx, req.GetContext().GetTenantId(), req.GetPackingSlip()); err != nil {
		return nil, grpcx.Wrap(err, "save packing slip")
	}
	s.log.InfoContext(ctx, "resume packing slip workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPackingSlip().GetAudit().GetEtag())
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*fulfillmentv1.PauseFulfillmentWorkflowResponse, len(req.GetPackingSlip().GetAttachments()))
	for i, attachment := range req.GetPackingSlip().GetAttachments() {
		group.Go(func() error {
			reply, err := s.fulfillmentWorkflowClient.PauseFulfillmentWorkflow(groupCtx, &fulfillmentv1.PauseFulfillmentWorkflowRequest{
				Context: req.GetContext(),
				Fulfillment: &fulfillmentv1.Fulfillment{
					Attachments: []*fulfillmentv1.FulfillmentAttachment{&fulfillmentv1.FulfillmentAttachment{
						ContentType: attachment.GetContentType(),
						Uri:         attachment.GetUri(),
					}},
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
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.FulfillmentWorkflowService/PauseFulfillmentWorkflow")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	summarizeCustomersRequest := &customersv1.SummarizeCustomersRequest{
		Context:     req.GetContext(),
		Reason:      req.GetReason(),
		Annotations: annotations,
	}
	summarizeCustomersReply, err := grpcx.Call(ctx, s.customerQueryClient.SummarizeCustomers, summarizeCustomersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerQueryService/SummarizeCustomers")
	}
	return &fulfillmentv1.ResumePackingSlipWorkflowResponse{
		Outcome: &fulfillmentv1.ResumePackingSlipWorkflowResponse_Outcome{Message: summarizeCustomersReply.GetResult().GetMessage()},
		Result: &commonv1.OperationResult{
			Message: summarizeCustomersReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		PackingSlip: &fulfillmentv1.PackingSlip{
			PackingSlipId: req.GetPackingSlipId(),
			Metadata:      req.GetPackingSlip().GetMetadata(),
		},
	}, nil
}

// InspectPackingSlipWorkflow returns a packing slip.
func (s *PackingSlipWorkflowServer) InspectPackingSlipWorkflow(ctx context.Context, req *fulfillmentv1.InspectPackingSlipWorkflowRequest) (*fulfillmentv1.InspectPackingSlipWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected InspectPackingSlipWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "packing_slip", req.GetPackingSlipId(), "inspect_packing_slip_workflow"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "inspect packing slip workflow", "tenant", req.GetContext().GetTenantId())
	baggage := make(map[string]string, len(req.GetContext().GetBaggage()))
	for key, value := range req.GetContext().GetBaggage() {
		baggage[key] = value
	}
	startFulfillmentWorkflowRequest := &fulfillmentv1.StartFulfillmentWorkflowRequest{
		Context: req.GetContext(),
		Details: &fulfillmentv1.StartFulfillmentWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
		Fulfillment: &fulfillmentv1.Fulfillment{
			Audit: &commonv1.AuditInfo{CreatedAt: req.GetContext().GetDeadline()},
		},
		Annotations: baggage,
	}
	type startFulfillmentWorkflowResult struct {
		reply *fulfillmentv1.StartFulfillmentWorkflowResponse
		err   error
	}
	startFulfillmentWorkflowResults := make(chan startFulfillmentWorkflowResult, 1)
	go func() {
		reply, err := s.fulfillmentWorkflowClient.StartFulfillmentWorkflow(ctx, startFulfillmentWorkflowRequest)
		startFulfillmentWorkflowResults <- startFulfillmentWorkflowResult{reply, err}
	}()
	var startFulfillmentWorkflowReply *fulfillmentv1.StartFulfillmentWorkflowResponse
	select {
	case result := <-startFulfillmentWorkflowResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.fulfillment.v1.FulfillmentWorkflowService/StartFulfillmentWorkflow")
		}
		startFulfillmentWorkflowReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	validateFulfillmentExceptionRequest := &fulfillmentv1.ValidateFulfillmentExceptionRequest{
		Context: req.GetContext(),
		FulfillmentException: &fulfillmentv1.FulfillmentException{
			Assignee: &identityv1.UserRef{
				DisplayName: startFulfillmentWorkflowReply.GetFulfillment().GetCounterparty().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				Etag: startFulfillmentWorkflowReply.GetFulfillment().GetAudit().GetEtag(),
			},
			Comments: []*fulfillmentv1.FulfillmentExceptionComment{&fulfillmentv1.FulfillmentExceptionComment{
				CreatedAt: startFulfillmentWorkflowReply.GetFulfillment().GetAudit().GetCreatedAt(),
			}},
		},
		Details: &fulfillmentv1.ValidateFulfillmentExceptionRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email: startFulfillmentWorkflowReply.GetFulfillment().GetCounterparty().GetEmail(),
			},
		},
	}
	validateFulfillmentExceptionReply, err := grpcx.NewClient(s.conn, fulfillmentv1.NewFulfillmentExceptionValidationServiceClient).ValidateFulfillmentException(ctx, validateFulfillmentExceptionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.FulfillmentExceptionValidationService/ValidateFulfillmentException")
	}
	return &fulfillmentv1.InspectPackingSlipWorkflowResponse{
		PackingSlip: &fulfillmentv1.PackingSlip{
			BillingAddress: &commonv1.PostalAddress{
				Lines: startFulfillmentWorkflowReply.GetFulfillment().GetBillingAddress().GetLines(),
			},
			PackingSlipId: validateFulfillmentExceptionReply.GetFulfillmentException().GetCustomer().GetPackingSlipId(),
			Audit: &commonv1.AuditInfo{
				Etag: validateFulfillmentExceptionReply.GetFulfillmentException().GetAudit().GetEtag(),
			},
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ListPackingSlipWorkflows lists packing slips a page at a time.
func (s *PackingSlipWorkflowServer) ListPackingSlipWorkflows(ctx context.Context, req *fulfillmentv1.ListPackingSlipWorkflowsRequest) (*fulfillmentv1.ListPackingSlipWorkflowsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "list packing slip workflows", "tenant", req.GetContext().GetTenantId())
	submitUsageRecordRequest := buildSubmitUsageRecordRequestForListPackingSlipWorkflows(req)
	submitUsageRecordReply, err := s.usageRecordApprovalClient.SubmitUsageRecord(ctx, submitUsageRecordRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.UsageRecordApprovalService/SubmitUsageRecord")
	}
	return &fulfillmentv1.ListPackingSlipWorkflowsResponse{
		Result: &commonv1.OperationResult{
			Message:       submitUsageRecordReply.GetOutcome().GetMessage(),
			CorrelationId: submitUsageRecordReply.GetUsageRecord().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// SignalPackingSlipWorkflow signals a packing slip.
func (s *PackingSlipWorkflowServer) SignalPackingSlipWorkflow(ctx context.Context, req *fulfillmentv1.SignalPackingSlipWorkflowRequest) (*fulfillmentv1.SignalPackingSlipWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SignalPackingSlipWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SavePackingSlip(ctx, req.GetContext().GetTenantId(), req.GetPackingSlip()); err != nil {
		return nil, grpcx.Wrap(err, "save packing slip")
	}
	s.log.InfoContext(ctx, "signal packing slip workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPackingSlip().GetAudit().GetEtag())
	for attachment := range iterx.Each(req.GetPackingSlip().GetAttachments(), func(candidate *fulfillmentv1.PackingSlipAttachment) bool { return candidate != nil }) {
		if _, err := s.packingSlipApprovalClient.ApprovePackingSlip(ctx, &fulfillmentv1.ApprovePackingSlipRequest{
			Context: req.GetContext(),
			PackingSlip: &fulfillmentv1.PackingSlip{
				Attachments: []*fulfillmentv1.PackingSlipAttachment{&fulfillmentv1.PackingSlipAttachment{
					FileName:    attachment.GetFileName(),
					ContentType: attachment.GetContentType(),
					SizeBytes:   attachment.GetSizeBytes(),
				}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.fulfillment.v1.PackingSlipApprovalService/ApprovePackingSlip")
		}
	}
	encoded, err := protojson.Marshal(req.GetPackingSlip())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded fulfillmentv1.PackingSlip
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	countSessionsRequest := &identityv1.CountSessionsRequest{
		Context: req.GetContext(),
		Filter: &identityv1.CountSessionsRequest_Filter{
			Region:       (&decoded).GetBillingAddress().GetRegion(),
			CreatedAfter: req.GetPackingSlip().GetAudit().GetCreatedAt(),
		},
	}
	countSessionsReply, err := s.countSessionsFn(ctx, countSessionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionQueryService/CountSessions")
	}
	for line := range iterx.Each(req.GetPackingSlip().GetLines(), func(candidate2 *fulfillmentv1.PackingSlipLine) bool { return candidate2 != nil }) {
		if _, err := s.packingSlipApprovalClient.RecallPackingSlip(ctx, &fulfillmentv1.RecallPackingSlipRequest{
			Context: req.GetContext(),
			PackingSlip: &fulfillmentv1.PackingSlip{
				Lines: []*fulfillmentv1.PackingSlipLine{&fulfillmentv1.PackingSlipLine{
					Tax:       &fulfillmentv1.PackingSlipLineTax{Amount: line.GetTax().GetAmount()},
					Quantity:  line.GetQuantity(),
					UnitPrice: line.GetUnitPrice(),
					Amount:    line.GetAmount(),
				}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.fulfillment.v1.PackingSlipApprovalService/RecallPackingSlip")
		}
	}
	ctx = tenancy.With(ctx, "updated_by", req.GetPackingSlip().GetAudit().GetUpdatedBy())
	simulateCustomerRequest := &customersv1.SimulateCustomerRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			Audit: &commonv1.AuditInfo{
				CreatedBy: req.GetPackingSlip().GetAudit().GetCreatedBy(),
				UpdatedBy: tenancy.Value(ctx, "updated_by"),
			},
		},
	}
	simulateCustomerReply, err := s.meteredCustomerCalculation.SimulateCustomer(ctx, simulateCustomerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerCalculationService/SimulateCustomer")
	}
	return &fulfillmentv1.SignalPackingSlipWorkflowResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: countSessionsReply.GetResult().GetCorrelationId(),
			ReferenceIds:  simulateCustomerReply.GetResult().GetReferenceIds(),
			Status:        "ok",
		},
		PackingSlip: &fulfillmentv1.PackingSlip{
			Number:        simulateCustomerReply.GetCustomer().GetPassport().GetNumber(),
			PackingSlipId: req.GetPackingSlipId(),
			DueAt:         req.GetPackingSlip().GetDueAt(),
		},
	}, nil
}

// pipelineLazy dials acme.pipelines.v1.PipelineService the first time a handler needs it.
func (s *PackingSlipWorkflowServer) pipelineLazy() pipelinesv1.PipelineServiceClient {
	s.pipelineOnce.Do(func() { s.pipelineCached = pipelinesv1.NewPipelineServiceClient(s.conn) })
	return s.pipelineCached
}

// packingSlipWorkflowServerPausePackingSlipWorkflowApprovePackingSlipStep carries the request of one step of
// PausePackingSlipWorkflow and, once it ran, its reply.
type packingSlipWorkflowServerPausePackingSlipWorkflowApprovePackingSlipStep struct {
	request *fulfillmentv1.ApprovePackingSlipRequest
	reply   *fulfillmentv1.ApprovePackingSlipResponse
}

func (s *PackingSlipWorkflowServer) runPausePackingSlipWorkflowApprovePackingSlip(ctx context.Context, step *packingSlipWorkflowServerPausePackingSlipWorkflowApprovePackingSlipStep) error {
	reply, err := s.packingSlipApprovalClient.ApprovePackingSlip(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
