package returns

import (
	"context"
	"errors"
	"io"
	"log/slog"

	admissionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/admissions/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	fuelv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fuel/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	notificationsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/notifications/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// RestockDecisionApprovalServer implements `acme.returns.v1.RestockDecisionApprovalService`.
// Routes restock decisions through approval, with escalation and delegation.
type RestockDecisionApprovalServer struct {
	returnsv1.UnimplementedRestockDecisionApprovalServiceServer
	store                            *Store
	log                              *slog.Logger
	returnRequestApprovalClient      returnsv1.ReturnRequestApprovalServiceClient
	restockDecisionLifecycleClient   returnsv1.RestockDecisionLifecycleServiceClient
	notificationQueryPort            notificationQueryPort
	householdValidationClient        customersv1.HouseholdValidationServiceClient
	returnRequestClient              returnsv1.ReturnRequestServiceClient
	searchQueryExportPort            searchQueryExportPort
	searchQueryQueryClient           searchv1.SearchQueryQueryServiceClient
	inspectionApprovalClient         returnsv1.InspectionApprovalServiceClient
	fuelCardQueryClient              fuelv1.FuelCardQueryServiceClient
	meteredSearchQueryQuery          meteredSearchQueryQuery
	clients                          *registry.Registry
	consentRecordCalculationClient   customersv1.ConsentRecordCalculationServiceClient
	admissionQueryClient             admissionsv1.AdmissionQueryServiceClient
	returnRequestWorkflowClient      returnsv1.ReturnRequestWorkflowServiceClient
	customerSegmentCalculationClient customersv1.CustomerSegmentCalculationServiceClient
}

// NewRestockDecisionApprovalServer wires a RestockDecisionApprovalServer.
func NewRestockDecisionApprovalServer(deps platform.Deps) RestockDecisionApprovalServer {
	return RestockDecisionApprovalServer{
		store:                            NewStore(deps.DB),
		log:                              deps.Logger.With("service", "acme.returns.v1.RestockDecisionApprovalService"),
		returnRequestApprovalClient:      returnsv1.NewReturnRequestApprovalServiceClient(deps.Conn),
		restockDecisionLifecycleClient:   returnsv1.NewRestockDecisionLifecycleServiceClient(deps.Conn),
		notificationQueryPort:            newNotificationQueryPort(deps),
		householdValidationClient:        customersv1.NewHouseholdValidationServiceClient(deps.Conn),
		returnRequestClient:              returnsv1.NewReturnRequestServiceClient(deps.Conn),
		searchQueryExportPort:            newSearchQueryExportPort(deps),
		searchQueryQueryClient:           searchv1.NewSearchQueryQueryServiceClient(deps.Conn),
		inspectionApprovalClient:         returnsv1.NewInspectionApprovalServiceClient(deps.Conn),
		fuelCardQueryClient:              fuelv1.NewFuelCardQueryServiceClient(deps.Conn),
		meteredSearchQueryQuery:          meteredSearchQueryQuery{SearchQueryQueryServiceClient: searchv1.NewSearchQueryQueryServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		clients:                          deps.Clients,
		consentRecordCalculationClient:   customersv1.NewConsentRecordCalculationServiceClient(deps.Conn),
		admissionQueryClient:             admissionsv1.NewAdmissionQueryServiceClient(deps.Conn),
		returnRequestWorkflowClient:      returnsv1.NewReturnRequestWorkflowServiceClient(deps.Conn),
		customerSegmentCalculationClient: customersv1.NewCustomerSegmentCalculationServiceClient(deps.Conn),
	}
}

// SubmitRestockDecision submits a restock decision.
func (s RestockDecisionApprovalServer) SubmitRestockDecision(ctx context.Context, req *returnsv1.SubmitRestockDecisionRequest) (*returnsv1.SubmitRestockDecisionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SubmitRestockDecision", "error", err)
		return nil, err
	}
	if err := s.store.SaveRestockDecision(ctx, req.GetContext().GetTenantId(), req.GetRestockDecision()); err != nil {
		return nil, grpcx.Wrap(err, "save restock decision")
	}
	s.log.InfoContext(ctx, "submit restock decision", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRestockDecision().GetAudit().GetEtag())
	submitReturnRequestRequest := &returnsv1.SubmitReturnRequestRequest{
		Context: req.GetContext(),
		ReturnRequest: &returnsv1.ReturnRequest{
			Lines: []*returnsv1.ReturnRequestLine{&returnsv1.ReturnRequestLine{
				Tax: &returnsv1.ReturnRequestLineTax{Amount: req.GetRestockDecision().GetDiscount().GetAmount()},
			}},
		},
	}
	type submitReturnRequestResult struct {
		reply *returnsv1.SubmitReturnRequestResponse
		err   error
	}
	submitReturnRequestResults := make(chan submitReturnRequestResult, 1)
	go func() {
		reply, err := s.returnRequestApprovalClient.SubmitReturnRequest(ctx, submitReturnRequestRequest)
		submitReturnRequestResults <- submitReturnRequestResult{reply, err}
	}()
	var submitReturnRequestReply *returnsv1.SubmitReturnRequestResponse
	select {
	case result := <-submitReturnRequestResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.returns.v1.ReturnRequestApprovalService/SubmitReturnRequest")
		}
		submitReturnRequestReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &returnsv1.SubmitRestockDecisionResponse{
		RestockDecision: &returnsv1.RestockDecision{
			Audit: &commonv1.AuditInfo{
				Etag: submitReturnRequestReply.GetReturnRequest().GetAudit().GetEtag(),
			},
			RestockDecisionId: req.GetRestockDecisionId(),
			Priority:          req.GetRestockDecision().GetPriority(),
		},
		Outcome: &returnsv1.SubmitRestockDecisionResponse_Outcome{
			NextSteps: submitReturnRequestReply.GetOutcome().GetNextSteps(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ApproveRestockDecision approves a restock decision.
func (s RestockDecisionApprovalServer) ApproveRestockDecision(ctx context.Context, req *returnsv1.ApproveRestockDecisionRequest) (*returnsv1.ApproveRestockDecisionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveRestockDecision(ctx, req.GetContext().GetTenantId(), req.GetRestockDecision()); err != nil {
		return nil, grpcx.Wrap(err, "save restock decision")
	}
	s.log.InfoContext(ctx, "approve restock decision", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRestockDecision().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "restock_decision_id", req.GetRestockDecision().GetRestockDecisionId())
	detailsCache := cache.New[string, *returnsv1.ApproveRestockDecisionRequest_Details](1)
	detailsCache.Put(req.GetContext().GetTraceId(), req.GetDetails())
	cachedDetails, _ := detailsCache.Get(req.GetContext().GetTraceId())
	terminateRestockDecisionRequest := &returnsv1.TerminateRestockDecisionRequest{}
	terminateRestockDecisionRequest.Context = req.GetContext()
	terminateRestockDecisionRequest.RestockDecisionId = tenancy.Value(ctx, "restock_decision_id")
	terminateRestockDecisionRequest.Details = &returnsv1.TerminateRestockDecisionRequest_Details{
		NewOwner: &identityv1.UserRef{
			Email:  cachedDetails.GetApprover().GetEmail(),
			UserId: req.GetDetails().GetApprover().GetUserId(),
		},
	}
	terminateRestockDecisionReply, err := s.restockDecisionLifecycleClient.TerminateRestockDecision(ctx, terminateRestockDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.RestockDecisionLifecycleService/TerminateRestockDecision")
	}
	browseNotificationsRequest := &notificationsv1.BrowseNotificationsRequest{
		Context: req.GetContext(),
		Filter: &notificationsv1.BrowseNotificationsRequest_Filter{
			CreatedAfter: terminateRestockDecisionReply.GetOutcome().GetEffectiveAt(),
			OwnerId:      terminateRestockDecisionReply.GetRestockDecision().GetOwner().GetUserId(),
		},
	}
	browseNotificationsReply, err := s.notificationQueryPort.BrowseNotifications(ctx, browseNotificationsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.notifications.v1.NotificationQueryService/BrowseNotifications")
	}
	encoded, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded returnsv1.ApproveRestockDecisionRequest_Details
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	previewHouseholdRequest := &customersv1.PreviewHouseholdRequest{}
	previewHouseholdRequest.Context = req.GetContext()
	previewHouseholdRequest.Household = &customersv1.Household{
		Children: []*customersv1.Household{&customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Manager: &identityv1.UserRef{
					UserId: terminateRestockDecisionReply.GetRestockDecision().GetOwner().GetUserId(),
					Email:  (&decoded).GetApprover().GetEmail(),
				},
			}},
		}},
	}
	previewHouseholdReply, err := s.householdValidationClient.PreviewHousehold(ctx, previewHouseholdRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdValidationService/PreviewHousehold")
	}
	return &returnsv1.ApproveRestockDecisionResponse{
		Outcome: &returnsv1.ApproveRestockDecisionResponse_Outcome{
			Message: terminateRestockDecisionReply.GetOutcome().GetMessage(),
		},
		Result: &commonv1.OperationResult{
			Message: browseNotificationsReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		RestockDecision: &returnsv1.RestockDecision{
			Name:              previewHouseholdReply.GetHousehold().GetName(),
			RestockDecisionId: req.GetRestockDecisionId(),
			Owner:             req.GetRestockDecision().GetOwner(),
		},
	}, nil
}

// ListPendingRestockDecisions lists restock decisions a page at a time.
func (s RestockDecisionApprovalServer) ListPendingRestockDecisions(ctx context.Context, req *returnsv1.ListPendingRestockDecisionsRequest) (*returnsv1.ListPendingRestockDecisionsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "list pending restock decisions", "tenant", req.GetContext().GetTenantId())
	return &returnsv1.ListPendingRestockDecisionsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// DelegateRestockDecisionApproval delegates a restock decision.
func (s RestockDecisionApprovalServer) DelegateRestockDecisionApproval(ctx context.Context, req *returnsv1.DelegateRestockDecisionApprovalRequest) (*returnsv1.DelegateRestockDecisionApprovalResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveRestockDecision(ctx, req.GetContext().GetTenantId(), req.GetRestockDecision()); err != nil {
		return nil, grpcx.Wrap(err, "save restock decision")
	}
	s.log.InfoContext(ctx, "delegate restock decision approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRestockDecision().GetAudit().GetEtag())
	source := req.GetDetails().GetApprover().ProtoReflect()
	userId := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("user_id"))).String()
	watchReturnRequestsStream, err := s.returnRequestClient.WatchReturnRequests(ctx, &returnsv1.WatchReturnRequestsRequest{
		Context:    req.GetContext(),
		Subscriber: &commonv1.ContactInfo{Email: req.GetDetails().GetApprover().GetEmail()},
		Since:      req.GetRestockDecision().GetAudit().GetUpdatedAt(),
		Filter: &returnsv1.WatchReturnRequestsRequest_Filter{
			CreatedAfter: req.GetDetails().GetEffectiveAt(),
			OwnerId:      userId,
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnRequestService/WatchReturnRequests")
	}
	for {
		received, err := watchReturnRequestsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnRequestService/WatchReturnRequests")
		}
		_ = received
	}
	listSearchQueryExportsRequest := &searchv1.ListSearchQueryExportsRequest{
		Context: req.GetContext(),
		Filter: &searchv1.ListSearchQueryExportsRequest_Filter{
			CreatedAfter: req.GetRestockDecision().GetAudit().GetUpdatedAt(),
			OwnerId:      req.GetRestockDecision().GetOwner().GetUserId(),
			Labels:       req.GetAnnotations(),
		},
	}
	listSearchQueryExportsReply, err := s.searchQueryExportPort.ListSearchQueryExports(ctx, listSearchQueryExportsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryExportService/ListSearchQueryExports")
	}
	countSearchQueriesReply, err := s.searchQueryQueryClient.CountSearchQueries(ctx, &searchv1.CountSearchQueriesRequest{
		Context: req.GetContext(),
		Filter: &searchv1.CountSearchQueriesRequest_Filter{
			CreatedAfter: req.GetRestockDecision().GetAudit().GetCreatedAt(),
			OwnerId:      req.GetRestockDecision().GetRestockDecisionId(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryQueryService/CountSearchQueries")
	}
	return &returnsv1.DelegateRestockDecisionApprovalResponse{
		Outcome: &returnsv1.DelegateRestockDecisionApprovalResponse_Outcome{
			Message: listSearchQueryExportsReply.GetResult().GetMessage(),
			Status:  countSearchQueriesReply.GetResult().GetStatus(),
		},
		RestockDecision: &returnsv1.RestockDecision{
			RestockDecisionId: req.GetRestockDecisionId(),
			Priority:          req.GetRestockDecision().GetPriority(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RecallRestockDecision recalls a restock decision.
func (s RestockDecisionApprovalServer) RecallRestockDecision(ctx context.Context, req *returnsv1.RecallRestockDecisionRequest) (*returnsv1.RecallRestockDecisionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveRestockDecision(ctx, req.GetContext().GetTenantId(), req.GetRestockDecision()); err != nil {
		return nil, grpcx.Wrap(err, "save restock decision")
	}
	s.log.InfoContext(ctx, "recall restock decision", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRestockDecision().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "user_id", req.GetRestockDecision().GetOwner().GetUserId())
	escalateInspectionReply, err := s.inspectionApprovalClient.EscalateInspection(ctx, &returnsv1.EscalateInspectionRequest{
		Context: req.GetContext(),
		Inspection: &returnsv1.Inspection{
			Origin: &returnsv1.Inspection_Customer{Customer: &customersv1.CustomerRef{
				Email:       req.GetDetails().GetApprover().GetEmail(),
				DisplayName: req.GetRestockDecision().GetOwner().GetDisplayName(),
			}},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: req.GetRestockDecision().GetAudit().GetUpdatedAt(),
			},
		},
		Details: &returnsv1.EscalateInspectionRequest_Details{
			Approver: &identityv1.UserRef{UserId: tenancy.Value(ctx, "user_id")},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.InspectionApprovalService/EscalateInspection")
	}
	exportFuelCardsStream, err := s.fuelCardQueryClient.ExportFuelCards(ctx, &fuelv1.ExportFuelCardsRequest{
		Context: req.GetContext(),
		Filter: &fuelv1.ExportFuelCardsRequest_Filter{
			CreatedAfter: req.GetDetails().GetEffectiveAt(),
			OwnerId:      req.GetRestockDecision().GetOwner().GetUserId(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fuel.v1.FuelCardQueryService/ExportFuelCards")
	}
	for {
		received, err := exportFuelCardsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.fuel.v1.FuelCardQueryService/ExportFuelCards")
		}
		_ = received
	}
	countSearchQueriesRequest := &searchv1.CountSearchQueriesRequest{
		Context: req.GetContext(),
		Filter: &searchv1.CountSearchQueriesRequest_Filter{
			CreatedAfter: escalateInspectionReply.GetInspection().GetAudit().GetCreatedAt(),
		},
	}
	countSearchQueriesReply, err := s.meteredSearchQueryQuery.CountSearchQueries(ctx, countSearchQueriesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryQueryService/CountSearchQueries")
	}
	return &returnsv1.RecallRestockDecisionResponse{
		RestockDecision: &returnsv1.RestockDecision{
			Audit: &commonv1.AuditInfo{
				CreatedAt: escalateInspectionReply.GetInspection().GetAudit().GetCreatedAt(),
			},
			RestockDecisionId: req.GetRestockDecisionId(),
			Name:              req.GetRestockDecision().GetName(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: countSearchQueriesReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// GetRestockDecisionApprovalStatus returns a restock decision.
func (s RestockDecisionApprovalServer) GetRestockDecisionApprovalStatus(ctx context.Context, req *returnsv1.GetRestockDecisionApprovalStatusRequest) (*returnsv1.GetRestockDecisionApprovalStatusResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "restock_decision", req.GetRestockDecisionId(), "get_restock_decision_approval_status"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get restock decision approval status", "tenant", req.GetContext().GetTenantId())
	searchQueryQuery, ok := s.clients.Get("acme.search.v1.SearchQueryQueryService").(searchv1.SearchQueryQueryServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.search.v1.SearchQueryQueryService")
	}
	baggage := make(map[string]string, len(req.GetContext().GetBaggage()))
	for key, value := range req.GetContext().GetBaggage() {
		baggage[key] = value
	}
	summarizeSearchQueriesRequest := &searchv1.SummarizeSearchQueriesRequest{
		Context:     req.GetContext(),
		Annotations: baggage,
	}
	summarizeSearchQueriesReply, err := searchQueryQuery.SummarizeSearchQueries(ctx, summarizeSearchQueriesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryQueryService/SummarizeSearchQueries")
	}
	packed, err := anypb.New(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	calculateConsentRecordReply, err := s.consentRecordCalculationClient.CalculateConsentRecord(ctx, &customersv1.CalculateConsentRecordRequest{
		Context: req.GetContext(),
		ConsentRecord: &customersv1.ConsentRecord{
			CorrelationId: summarizeSearchQueriesReply.GetResult().GetCorrelationId(),
			Attributes:    summarizeSearchQueriesReply.GetResult().GetDetails(),
			Source:        req.GetRestockDecisionId(),
			Detail:        packed,
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordCalculationService/CalculateConsentRecord")
	}
	return &returnsv1.GetRestockDecisionApprovalStatusResponse{
		Result: &commonv1.OperationResult{
			Message:       summarizeSearchQueriesReply.GetResult().GetMessage(),
			CorrelationId: calculateConsentRecordReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		RestockDecision: &returnsv1.RestockDecision{
			Owner: &identityv1.UserRef{
				DisplayName: calculateConsentRecordReply.GetConsentRecord().GetSubject().GetDisplayName(),
			},
			RestockDecisionId: req.GetRestockDecisionId(),
		},
	}, nil
}

// CommentRestockDecisionApproval comments a restock decision.
func (s RestockDecisionApprovalServer) CommentRestockDecisionApproval(ctx context.Context, req *returnsv1.CommentRestockDecisionApprovalRequest) (*returnsv1.CommentRestockDecisionApprovalResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveRestockDecision(ctx, req.GetContext().GetTenantId(), req.GetRestockDecision()); err != nil {
		return nil, grpcx.Wrap(err, "save restock decision")
	}
	s.log.InfoContext(ctx, "comment restock decision approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRestockDecision().GetAudit().GetEtag())
	exportAdmissionsStream, err := s.admissionQueryClient.ExportAdmissions(ctx, &admissionsv1.ExportAdmissionsRequest{
		Context: req.GetContext(),
		Filter: &admissionsv1.ExportAdmissionsRequest_Filter{
			CreatedAfter: req.GetDetails().GetEffectiveAt(),
			OwnerId:      req.GetRestockDecision().GetOwner().GetUserId(),
			Labels:       req.GetRestockDecision().GetParameters(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.admissions.v1.AdmissionQueryService/ExportAdmissions")
	}
	for {
		received, err := exportAdmissionsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.admissions.v1.AdmissionQueryService/ExportAdmissions")
		}
		_ = received
	}
	preferenceValidation, err := registry.Lookup[customersv1.PreferenceValidationServiceClient](s.clients, "acme.customers.v1.PreferenceValidationService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	var holder struct{ Email string }
	if record := req.GetRestockDecision().GetOwner(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Email"); err != nil {
			s.log.WarnContext(ctx, "copy email", "error", err)
		}
	}
	ctx = tenancy.With(ctx, "created_by", req.GetRestockDecision().GetAudit().GetCreatedBy())
	previewPreferenceRequest := &customersv1.PreviewPreferenceRequest{
		Context: req.GetContext(),
		Preference: &customersv1.Preference{
			Owner: &identityv1.UserRef{
				DisplayName: req.GetRestockDecision().GetOwner().GetDisplayName(),
			},
			Action: &customersv1.Preference_Route{Route: &customersv1.PreferenceRouteAction{Weight: req.GetRestockDecision().GetRoute().GetWeight()}},
			Audit:  &commonv1.AuditInfo{CreatedBy: tenancy.Value(ctx, "created_by")},
		},
		Details: &customersv1.PreviewPreferenceRequest_Details{Contact: &commonv1.ContactInfo{Email: holder.Email}},
	}
	previewPreferenceReply, err := preferenceValidation.PreviewPreference(ctx, previewPreferenceRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceValidationService/PreviewPreference")
	}
	encoded, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded returnsv1.CommentRestockDecisionApprovalRequest_Details
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	startReturnRequestWorkflowReply, err := s.returnRequestWorkflowClient.StartReturnRequestWorkflow(ctx, &returnsv1.StartReturnRequestWorkflowRequest{
		Context: req.GetContext(),
		ReturnRequest: &returnsv1.ReturnRequest{
			Lines: []*returnsv1.ReturnRequestLine{&returnsv1.ReturnRequestLine{
				Tax: &returnsv1.ReturnRequestLineTax{
					Amount: previewPreferenceReply.GetPreference().GetDiscount().GetAmount(),
				},
			}},
			Counterparty: &customersv1.CustomerRef{DisplayName: (&decoded).GetApprover().GetDisplayName()},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: req.GetRestockDecision().GetAudit().GetUpdatedAt(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnRequestWorkflowService/StartReturnRequestWorkflow")
	}
	simulateCustomerSegmentRequest := buildSimulateCustomerSegmentRequestForCommentRestockDecisionApproval(req)
	simulateCustomerSegmentReply, err := s.customerSegmentCalculationClient.SimulateCustomerSegment(ctx, simulateCustomerSegmentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentCalculationService/SimulateCustomerSegment")
	}
	return &returnsv1.CommentRestockDecisionApprovalResponse{
		RestockDecision: &returnsv1.RestockDecision{
			Audit: &commonv1.AuditInfo{
				Etag: previewPreferenceReply.GetPreference().GetAudit().GetEtag(),
			},
			Owner: &identityv1.UserRef{
				DisplayName: simulateCustomerSegmentReply.GetCustomerSegment().GetOwner().GetDisplayName(),
			},
			RestockDecisionId: req.GetRestockDecisionId(),
			Status:            req.GetRestockDecision().GetStatus(),
		},
		Outcome: &returnsv1.CommentRestockDecisionApprovalResponse_Outcome{
			Status: startReturnRequestWorkflowReply.GetOutcome().GetStatus(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: simulateCustomerSegmentReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}
