package customers

import (
	"context"
	"errors"
	"io"
	"log/slog"

	anomaliesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/anomalies/v1"
	catalogv1 "github.com/hounddogai/protomap-test-golang/gen/acme/catalog/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	documentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/documents/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	labelingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/labeling/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	ordersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/orders/v1"
	promotionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/promotions/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// CustomerApprovalServer implements `acme.customers.v1.CustomerApprovalService`.
// Routes customers through approval, with escalation and delegation.
type CustomerApprovalServer struct {
	customersv1.UnimplementedCustomerApprovalServiceServer
	store                     *Store
	log                       *slog.Logger
	searchIndexExportClient   searchv1.SearchIndexExportServiceClient
	synonymAdminClient        searchv1.SynonymAdminServiceClient
	conn                      *grpc.ClientConn
	preferenceSyncClient      customersv1.PreferenceSyncServiceClient
	categoryWorkflowClient    catalogv1.CategoryWorkflowServiceClient
	labelingTaskClient        labelingv1.LabelingTaskServiceClient
	householdReportingClient  customersv1.HouseholdReportingServiceClient
	backorderApprovalClient   ordersv1.BackorderApprovalServiceClient
	documentQueryClient       documentsv1.DocumentQueryServiceClient
	tierReportingClient       loyaltyv1.TierReportingServiceClient
	consentRecordSyncClient   customersv1.ConsentRecordSyncServiceClient
	userQueryClient           identityv1.UserQueryServiceClient
	anomalyClient             anomaliesv1.AnomalyServiceClient
	fraudSignalWorkflowClient fraudv1.FraudSignalWorkflowServiceClient
	preferenceReportingClient customersv1.PreferenceReportingServiceClient
	searchQueryQueryClient    searchv1.SearchQueryQueryServiceClient
	redemptionApprovalClient  promotionsv1.RedemptionApprovalServiceClient
}

// NewCustomerApprovalServer wires a CustomerApprovalServer.
func NewCustomerApprovalServer(deps platform.Deps) *CustomerApprovalServer {
	return &CustomerApprovalServer{
		store:                     NewStore(deps.DB),
		log:                       deps.Logger.With("service", "acme.customers.v1.CustomerApprovalService"),
		searchIndexExportClient:   searchv1.NewSearchIndexExportServiceClient(deps.Conn),
		synonymAdminClient:        searchv1.NewSynonymAdminServiceClient(deps.Conn),
		conn:                      deps.Conn,
		preferenceSyncClient:      customersv1.NewPreferenceSyncServiceClient(deps.Conn),
		categoryWorkflowClient:    catalogv1.NewCategoryWorkflowServiceClient(deps.Conn),
		labelingTaskClient:        labelingv1.NewLabelingTaskServiceClient(deps.Conn),
		householdReportingClient:  customersv1.NewHouseholdReportingServiceClient(deps.Conn),
		backorderApprovalClient:   ordersv1.NewBackorderApprovalServiceClient(deps.Conn),
		documentQueryClient:       documentsv1.NewDocumentQueryServiceClient(deps.Conn),
		tierReportingClient:       loyaltyv1.NewTierReportingServiceClient(deps.Conn),
		consentRecordSyncClient:   customersv1.NewConsentRecordSyncServiceClient(deps.Conn),
		userQueryClient:           identityv1.NewUserQueryServiceClient(deps.Conn),
		anomalyClient:             anomaliesv1.NewAnomalyServiceClient(deps.Conn),
		fraudSignalWorkflowClient: fraudv1.NewFraudSignalWorkflowServiceClient(deps.Conn),
		preferenceReportingClient: customersv1.NewPreferenceReportingServiceClient(deps.Conn),
		searchQueryQueryClient:    searchv1.NewSearchQueryQueryServiceClient(deps.Conn),
		redemptionApprovalClient:  promotionsv1.NewRedemptionApprovalServiceClient(deps.Conn),
	}
}

// SubmitCustomer submits a customer.
func (s *CustomerApprovalServer) SubmitCustomer(ctx context.Context, req *customersv1.SubmitCustomerRequest) (*customersv1.SubmitCustomerResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SubmitCustomer", "error", err)
		return nil, err
	}
	if err := s.store.SaveCustomer(ctx, req.GetContext().GetTenantId(), req.GetCustomer()); err != nil {
		return nil, grpcx.Wrap(err, "save customer")
	}
	s.log.InfoContext(ctx, "submit customer", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCustomer().GetAudit().GetEtag())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	streamSearchIndexExportStream, err := s.searchIndexExportClient.StreamSearchIndexExport(ctx, &searchv1.StreamSearchIndexExportRequest{
		Context: req.GetContext(),
		Filter: &searchv1.StreamSearchIndexExportRequest_Filter{
			Region:       req.GetCustomer().GetPrimaryAddress().GetRegion(),
			CreatedAfter: req.GetCustomer().GetAudit().GetCreatedAt(),
			OwnerId:      req.GetCustomer().GetCustomerId(),
			Labels:       annotations,
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
	return &customersv1.SubmitCustomerResponse{
		Customer: &customersv1.Customer{
			CustomerId: req.GetCustomerId(),
			Status:     req.GetCustomer().GetStatus(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ApproveCustomer approves a customer.
func (s *CustomerApprovalServer) ApproveCustomer(ctx context.Context, req *customersv1.ApproveCustomerRequest) (*customersv1.ApproveCustomerResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveCustomer(ctx, req.GetContext().GetTenantId(), req.GetCustomer()); err != nil {
		return nil, grpcx.Wrap(err, "save customer")
	}
	s.log.InfoContext(ctx, "approve customer", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCustomer().GetAudit().GetEtag())
	snapshot, err := protojson.Marshal(req.GetCustomer())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	repairSynonymRequest := &searchv1.RepairSynonymRequest{
		Context: req.GetContext(),
		Synonym: &searchv1.Synonym{
			Owner: &identityv1.UserRef{
				DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: req.GetCustomer().GetAudit().GetUpdatedAt(),
				CreatedBy: req.GetCustomer().GetAudit().GetCreatedBy(),
				Etag:      req.GetCustomer().GetAudit().GetEtag(),
			},
		},
		Details: &searchv1.RepairSynonymRequest_Details{Payload: snapshot},
	}
	type repairSynonymResult struct {
		reply *searchv1.RepairSynonymResponse
		err   error
	}
	repairSynonymResults := make(chan repairSynonymResult, 1)
	go func() {
		reply, err := s.synonymAdminClient.RepairSynonym(ctx, repairSynonymRequest)
		repairSynonymResults <- repairSynonymResult{reply, err}
	}()
	var repairSynonymReply *searchv1.RepairSynonymResponse
	select {
	case result := <-repairSynonymResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.search.v1.SynonymAdminService/RepairSynonym")
		}
		repairSynonymReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &customersv1.ApproveCustomerResponse{
		Customer: &customersv1.Customer{
			Audit: &commonv1.AuditInfo{
				CreatedBy: repairSynonymReply.GetSynonym().GetAudit().GetCreatedBy(),
			},
			RelationshipManager: &identityv1.UserRef{
				UserId: repairSynonymReply.GetSynonym().GetOwner().GetUserId(),
			},
			CustomerId: req.GetCustomerId(),
			Contact:    req.GetCustomer().GetContact(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RejectCustomer rejects a customer.
func (s *CustomerApprovalServer) RejectCustomer(ctx context.Context, req *customersv1.RejectCustomerRequest) (*customersv1.RejectCustomerResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RejectCustomer", "error", err)
		return nil, err
	}
	if err := s.store.SaveCustomer(ctx, req.GetContext().GetTenantId(), req.GetCustomer()); err != nil {
		return nil, grpcx.Wrap(err, "save customer")
	}
	s.log.InfoContext(ctx, "reject customer", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCustomer().GetAudit().GetEtag())
	listPreferenceReportsRequest := &customersv1.ListPreferenceReportsRequest{
		Context: req.GetContext(),
		Filter: &customersv1.ListPreferenceReportsRequest_Filter{
			Region:       req.GetCustomer().GetPrimaryAddress().GetRegion(),
			CreatedAfter: req.GetCustomer().GetAudit().GetUpdatedAt(),
		},
	}
	listPreferenceReportsReply := new(customersv1.ListPreferenceReportsResponse)
	if err := s.conn.Invoke(ctx, customersPreferenceReportingServiceListPreferenceReportsMethod, listPreferenceReportsRequest, listPreferenceReportsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceReportingService/ListPreferenceReports")
	}
	customerCache := cache.New[string, *customersv1.Customer](1)
	customerCache.Put(req.GetContext().GetTraceId(), req.GetCustomer())
	cachedCustomer, _ := customerCache.Get(req.GetContext().GetTraceId())
	attributes := make(map[string]string, len(req.GetCustomer().GetAttributes()))
	for key, value := range req.GetCustomer().GetAttributes() {
		attributes[key] = value
	}
	listPreferenceConflictsReply, err := s.preferenceSyncClient.ListPreferenceConflicts(ctx, &customersv1.ListPreferenceConflictsRequest{
		Context: req.GetContext(),
		Filter: &customersv1.ListPreferenceConflictsRequest_Filter{
			Region:       cachedCustomer.GetPrimaryAddress().GetRegion(),
			CreatedAfter: req.GetCustomer().GetPassport().GetExpiresAt(),
			OwnerId:      req.GetCustomer().GetRelationshipManager().GetUserId(),
			Labels:       attributes,
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceSyncService/ListPreferenceConflicts")
	}
	source := req.GetCustomer().GetPrimaryAddress().ProtoReflect()
	recipient := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("recipient"))).String()
	cancelCategoryWorkflowRequest := &catalogv1.CancelCategoryWorkflowRequest{
		Context: req.GetContext(),
		Category: &catalogv1.Category{
			Children: []*catalogv1.Category{&catalogv1.Category{
				Children: []*catalogv1.Category{&catalogv1.Category{
					Manager: &identityv1.UserRef{
						DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
					},
					Audit: &commonv1.AuditInfo{UpdatedBy: req.GetCustomer().GetAudit().GetUpdatedBy()},
					Location: &commonv1.PostalAddress{
						Recipient: recipient,
						Lines:     req.GetCustomer().GetPrimaryAddress().GetLines(),
					},
				}},
			}},
		},
	}
	cancelCategoryWorkflowReply, err := grpcx.Call(ctx, s.categoryWorkflowClient.CancelCategoryWorkflow, cancelCategoryWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.CategoryWorkflowService/CancelCategoryWorkflow")
	}
	createLabelingTaskRequest := &labelingv1.CreateLabelingTaskRequest{}
	createLabelingTaskRequest.Context = req.GetContext()
	createLabelingTaskRequest.LabelingTask = &labelingv1.LabelingTask{
		Origin: &labelingv1.LabelingTask_Customer{Customer: &labelingv1.LabelSetRef{
			DisplayName: req.GetCustomer().GetRelationshipManager().GetDisplayName(),
		}},
		Requester: &commonv1.ContactInfo{Name: req.GetCustomer().GetContact().GetName()},
		Audit: &commonv1.AuditInfo{
			UpdatedBy: cancelCategoryWorkflowReply.GetCategory().GetAudit().GetUpdatedBy(),
		},
		Assignee: &identityv1.UserRef{
			DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
		},
	}
	createLabelingTaskReply, err := s.labelingTaskClient.CreateLabelingTask(ctx, createLabelingTaskRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.labeling.v1.LabelingTaskService/CreateLabelingTask")
	}
	_ = listPreferenceConflictsReply
	_ = createLabelingTaskReply
	return &customersv1.RejectCustomerResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: listPreferenceReportsReply.GetResult().GetReferenceIds(),
			Status:       cancelCategoryWorkflowReply.GetOutcome().GetStatus(),
		},
		Customer: &customersv1.Customer{CustomerId: req.GetCustomerId()},
		Outcome:  &customersv1.RejectCustomerResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// EscalateCustomer escalates a customer.
func (s *CustomerApprovalServer) EscalateCustomer(ctx context.Context, req *customersv1.EscalateCustomerRequest) (*customersv1.EscalateCustomerResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveCustomer(ctx, req.GetContext().GetTenantId(), req.GetCustomer()); err != nil {
		return nil, grpcx.Wrap(err, "save customer")
	}
	s.log.InfoContext(ctx, "escalate customer", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCustomer().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetCustomer()).(*customersv1.Customer)
	listHouseholdReportsRequest := &customersv1.ListHouseholdReportsRequest{
		Context: req.GetContext(),
		Filter: &customersv1.ListHouseholdReportsRequest_Filter{
			Region:       cloned.GetPrimaryAddress().GetRegion(),
			CreatedAfter: req.GetCustomer().GetAudit().GetUpdatedAt(),
		},
	}
	type listHouseholdReportsResult struct {
		reply *customersv1.ListHouseholdReportsResponse
		err   error
	}
	listHouseholdReportsResults := make(chan listHouseholdReportsResult, 1)
	go func() {
		reply, err := s.householdReportingClient.ListHouseholdReports(ctx, listHouseholdReportsRequest)
		listHouseholdReportsResults <- listHouseholdReportsResult{reply, err}
	}()
	var listHouseholdReportsReply *customersv1.ListHouseholdReportsResponse
	select {
	case result := <-listHouseholdReportsResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.customers.v1.HouseholdReportingService/ListHouseholdReports")
		}
		listHouseholdReportsReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := customerApprovalServerEscalateBackorderSteps[mode]
	ctx = tenancy.With(ctx, "email", req.GetCustomer().GetContact().GetEmail())
	escalateBackorderRequest := &ordersv1.EscalateBackorderRequest{
		Context: req.GetContext(),
		Backorder: &ordersv1.Backorder{
			Assignee: &identityv1.UserRef{Email: tenancy.Value(ctx, "email")},
		},
		Details: &ordersv1.EscalateBackorderRequest_Details{
			Approver: &identityv1.UserRef{Email: req.GetDetails().GetApprover().GetEmail()},
		},
	}
	escalateBackorderReply, err := run(s, ctx, escalateBackorderRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.BackorderApprovalService/EscalateBackorder")
	}
	exportDocumentsStream, err := s.documentQueryClient.ExportDocuments(ctx, &documentsv1.ExportDocumentsRequest{
		Context: req.GetContext(),
		Filter:  &documentsv1.ExportDocumentsRequest_Filter{Region: req.GetCustomer().GetPrimaryAddress().GetRegion()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.documents.v1.DocumentQueryService/ExportDocuments")
	}
	for {
		received, err := exportDocumentsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.documents.v1.DocumentQueryService/ExportDocuments")
		}
		_ = received
	}
	ctx = tenancy.With(ctx, "created_by", req.GetCustomer().GetAudit().GetCreatedBy())
	snapshot, err := protojson.Marshal(req.GetCustomer())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	syncPreferencesRequest := &customersv1.SyncPreferencesRequest{
		Context: req.GetContext(),
		Preference: &customersv1.Preference{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: escalateBackorderReply.GetBackorder().GetAudit().GetUpdatedAt(),
				CreatedBy: tenancy.Value(ctx, "created_by"),
				Etag:      req.GetCustomer().GetAudit().GetEtag(),
			},
			Owner: &identityv1.UserRef{
				Email: escalateBackorderReply.GetBackorder().GetCustomer().GetEmail(),
			},
		},
		Details: &customersv1.SyncPreferencesRequest_Details{Payload: snapshot},
	}
	syncPreferencesReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*customersv1.SyncPreferencesResponse, error) {
		return s.preferenceSyncClient.SyncPreferences(ctx, syncPreferencesRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceSyncService/SyncPreferences")
	}
	return &customersv1.EscalateCustomerResponse{
		Result: &commonv1.OperationResult{
			Status:  listHouseholdReportsReply.GetResult().GetStatus(),
			Message: listHouseholdReportsReply.GetResult().GetMessage(),
		},
		Customer: &customersv1.Customer{
			RelationshipManager: &identityv1.UserRef{
				Email:       escalateBackorderReply.GetBackorder().GetRequester().GetEmail(),
				DisplayName: syncPreferencesReply.GetPreference().GetOwner().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: syncPreferencesReply.GetPreference().GetAudit().GetUpdatedBy(),
			},
			CustomerId: req.GetCustomerId(),
			Contact:    req.GetCustomer().GetContact(),
		},
	}, nil
}

// ListPendingCustomers lists customers a page at a time.
func (s *CustomerApprovalServer) ListPendingCustomers(ctx context.Context, req *customersv1.ListPendingCustomersRequest) (*customersv1.ListPendingCustomersResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "list pending customers", "tenant", req.GetContext().GetTenantId())
	return &customersv1.ListPendingCustomersResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// DelegateCustomerApproval delegates a customer.
func (s *CustomerApprovalServer) DelegateCustomerApproval(ctx context.Context, req *customersv1.DelegateCustomerApprovalRequest) (*customersv1.DelegateCustomerApprovalResponse, error) {
	if err := s.store.SaveCustomer(ctx, req.GetContext().GetTenantId(), req.GetCustomer()); err != nil {
		return nil, grpcx.Wrap(err, "save customer")
	}
	s.log.InfoContext(ctx, "delegate customer approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCustomer().GetAudit().GetEtag())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	cloned := proto.Clone(req.GetCustomer()).(*customersv1.Customer)
	generateTierReportReply, err := s.tierReportingClient.GenerateTierReport(ctx, &loyaltyv1.GenerateTierReportRequest{
		Context:     req.GetContext(),
		Annotations: annotations,
		Reason:      req.GetReason(),
		Details:     &loyaltyv1.GenerateTierReportRequest_Details{Period: cloned.GetPreferences().GetQuietHours()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.TierReportingService/GenerateTierReport")
	}
	packed, err := anypb.New(req.GetCustomer())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	snapshot, err := protojson.Marshal(req.GetCustomer())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	step := &customerApprovalServerDelegateCustomerApprovalResolveConsentRecordConflictStep{request: &customersv1.ResolveConsentRecordConflictRequest{
		Context: req.GetContext(),
		ConsentRecord: &customersv1.ConsentRecord{
			Subject: &customersv1.HouseholdRef{
				DisplayName: req.GetCustomer().GetRelationshipManager().GetDisplayName(),
			},
			Detail: packed,
		},
		Details: &customersv1.ResolveConsentRecordConflictRequest_Details{Payload: snapshot},
	}}
	if err := s.runDelegateCustomerApprovalResolveConsentRecordConflict(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordSyncService/ResolveConsentRecordConflict")
	}
	resolveConsentRecordConflictReply := step.reply
	return &customersv1.DelegateCustomerApprovalResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: generateTierReportReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Outcome: &customersv1.DelegateCustomerApprovalResponse_Outcome{
			Message: generateTierReportReply.GetResult().GetMessage(),
			Status:  resolveConsentRecordConflictReply.GetOutcome().GetStatus(),
		},
		Customer: &customersv1.Customer{
			RelationshipManager: &identityv1.UserRef{
				DisplayName: resolveConsentRecordConflictReply.GetConsentRecord().GetSubject().GetDisplayName(),
			},
			CustomerId:     req.GetCustomerId(),
			PrimaryAddress: req.GetCustomer().GetPrimaryAddress(),
		},
	}, nil
}

// RecallCustomer recalls a customer.
func (s *CustomerApprovalServer) RecallCustomer(ctx context.Context, req *customersv1.RecallCustomerRequest) (*customersv1.RecallCustomerResponse, error) {
	if err := s.store.SaveCustomer(ctx, req.GetContext().GetTenantId(), req.GetCustomer()); err != nil {
		return nil, grpcx.Wrap(err, "save customer")
	}
	s.log.InfoContext(ctx, "recall customer", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCustomer().GetAudit().GetEtag())
	source := req.GetCustomer().GetRelationshipManager().ProtoReflect()
	userId := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("user_id"))).String()
	exportUsersStream, err := s.userQueryClient.ExportUsers(ctx, &identityv1.ExportUsersRequest{
		Context: req.GetContext(),
		Filter: &identityv1.ExportUsersRequest_Filter{
			Region:       req.GetCustomer().GetPrimaryAddress().GetRegion(),
			CreatedAfter: req.GetDetails().GetEffectiveAt(),
			Labels:       req.GetCustomer().GetAttributes(),
			OwnerId:      userId,
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserQueryService/ExportUsers")
	}
	for {
		received, err := exportUsersStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.identity.v1.UserQueryService/ExportUsers")
		}
		_ = received
	}
	batchGetAnomaliesRequest := &anomaliesv1.BatchGetAnomaliesRequest{Context: req.GetContext()}
	var batchGetAnomaliesReply *anomaliesv1.BatchGetAnomaliesResponse
	if err := pipeline.Run(ctx, "acme.anomalies.v1.AnomalyService/BatchGetAnomalies", func(ctx context.Context) error {
		answer, err := s.anomalyClient.BatchGetAnomalies(ctx, batchGetAnomaliesRequest)
		batchGetAnomaliesReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.anomalies.v1.AnomalyService/BatchGetAnomalies")
	}
	return &customersv1.RecallCustomerResponse{
		Outcome: &customersv1.RecallCustomerResponse_Outcome{Message: batchGetAnomaliesReply.GetResult().GetMessage()},
		Result: &commonv1.OperationResult{
			CorrelationId: batchGetAnomaliesReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Customer: &customersv1.Customer{
			CustomerId: req.GetCustomerId(),
			Contact:    req.GetCustomer().GetContact(),
		},
	}, nil
}

// GetCustomerApprovalStatus returns a customer.
func (s *CustomerApprovalServer) GetCustomerApprovalStatus(ctx context.Context, req *customersv1.GetCustomerApprovalStatusRequest) (*customersv1.GetCustomerApprovalStatusResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "customer", req.GetCustomerId(), "get_customer_approval_status"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get customer approval status", "tenant", req.GetContext().GetTenantId())
	resolveConsentRecordConflictRequest := buildResolveConsentRecordConflictRequestForGetCustomerApprovalStatus(req)
	resolveConsentRecordConflictReply, err := s.consentRecordSyncClient.ResolveConsentRecordConflict(ctx, resolveConsentRecordConflictRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordSyncService/ResolveConsentRecordConflict")
	}
	packed, err := anypb.New(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	advanceFraudSignalWorkflowReply, err := s.fraudSignalWorkflowClient.AdvanceFraudSignalWorkflow(ctx, &fraudv1.AdvanceFraudSignalWorkflowRequest{
		Context: req.GetContext(),
		FraudSignal: &fraudv1.FraudSignal{
			Subject: &fraudv1.FraudRuleRef{
				DisplayName: resolveConsentRecordConflictReply.GetConsentRecord().GetSubject().GetDisplayName(),
			},
			Detail: packed,
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.FraudSignalWorkflowService/AdvanceFraudSignalWorkflow")
	}
	return &customersv1.GetCustomerApprovalStatusResponse{
		Result: &commonv1.OperationResult{
			Status: resolveConsentRecordConflictReply.GetOutcome().GetStatus(),
		},
		Customer: &customersv1.Customer{
			Attributes: advanceFraudSignalWorkflowReply.GetFraudSignal().GetAttributes(),
			CustomerId: req.GetCustomerId(),
		},
	}, nil
}

// CommentCustomerApproval comments a customer.
func (s *CustomerApprovalServer) CommentCustomerApproval(ctx context.Context, req *customersv1.CommentCustomerApprovalRequest) (*customersv1.CommentCustomerApprovalResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveCustomer(ctx, req.GetContext().GetTenantId(), req.GetCustomer()); err != nil {
		return nil, grpcx.Wrap(err, "save customer")
	}
	s.log.InfoContext(ctx, "comment customer approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCustomer().GetAudit().GetEtag())
	streamPreferenceMetricsStream, err := s.preferenceReportingClient.StreamPreferenceMetrics(ctx, &customersv1.StreamPreferenceMetricsRequest{
		Context: req.GetContext(),
		Subscriber: &commonv1.ContactInfo{
			Phone: req.GetCustomer().GetContact().GetPhone(),
			Name:  req.GetCustomer().GetContact().GetName(),
		},
		Filter: &customersv1.StreamPreferenceMetricsRequest_Filter{Region: req.GetCustomer().GetPrimaryAddress().GetRegion()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceReportingService/StreamPreferenceMetrics")
	}
	for {
		received, err := streamPreferenceMetricsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceReportingService/StreamPreferenceMetrics")
		}
		_ = received
	}
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := customerApprovalServerSummarizeSearchQueriesSteps[mode]
	attributes := make(map[string]string, len(req.GetCustomer().GetAttributes()))
	for key, value := range req.GetCustomer().GetAttributes() {
		attributes[key] = value
	}
	summarizeSearchQueriesRequest := &searchv1.SummarizeSearchQueriesRequest{
		Context:     req.GetContext(),
		Annotations: req.GetAnnotations(),
		Reason:      req.GetReason(),
		Details: &searchv1.SummarizeSearchQueriesRequest_Details{
			Period:  req.GetCustomer().GetPreferences().GetQuietHours(),
			Filters: attributes,
		},
	}
	summarizeSearchQueriesReply, err := run(s, ctx, summarizeSearchQueriesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryQueryService/SummarizeSearchQueries")
	}
	commentRedemptionApprovalRequest := buildCommentRedemptionApprovalRequestForCommentCustomerApproval(req)
	commentRedemptionApprovalReply, err := s.redemptionApprovalClient.CommentRedemptionApproval(ctx, commentRedemptionApprovalRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.RedemptionApprovalService/CommentRedemptionApproval")
	}
	return &customersv1.CommentCustomerApprovalResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: summarizeSearchQueriesReply.GetResult().GetReferenceIds(),
			Message:      commentRedemptionApprovalReply.GetResult().GetMessage(),
			Status:       "ok",
		},
		Customer: &customersv1.Customer{
			CustomerId: req.GetCustomerId(),
			Contact:    req.GetCustomer().GetContact(),
		},
	}, nil
}

// customerApprovalServerEscalateBackorderSteps are the ways to run
// acme.orders.v1.BackorderApprovalService/EscalateBackorder, by name.
var customerApprovalServerEscalateBackorderSteps = map[string]func(*CustomerApprovalServer, context.Context, *ordersv1.EscalateBackorderRequest) (*ordersv1.EscalateBackorderResponse, error){
	"default": (*CustomerApprovalServer).escalateBackorderStep,
	"audited": (*CustomerApprovalServer).escalateBackorderAuditedStep,
}

func (s *CustomerApprovalServer) escalateBackorderStep(ctx context.Context, request *ordersv1.EscalateBackorderRequest) (*ordersv1.EscalateBackorderResponse, error) {
	return s.backorderApprovalClient.EscalateBackorder(ctx, request)
}

func (s *CustomerApprovalServer) escalateBackorderAuditedStep(ctx context.Context, request *ordersv1.EscalateBackorderRequest) (*ordersv1.EscalateBackorderResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.orders.v1.BackorderApprovalService/EscalateBackorder")
	return s.backorderApprovalClient.EscalateBackorder(ctx, request)
}

// customerApprovalServerDelegateCustomerApprovalResolveConsentRecordConflictStep carries the request of one step of
// DelegateCustomerApproval and, once it ran, its reply.
type customerApprovalServerDelegateCustomerApprovalResolveConsentRecordConflictStep struct {
	request *customersv1.ResolveConsentRecordConflictRequest
	reply   *customersv1.ResolveConsentRecordConflictResponse
}

func (s *CustomerApprovalServer) runDelegateCustomerApprovalResolveConsentRecordConflict(ctx context.Context, step *customerApprovalServerDelegateCustomerApprovalResolveConsentRecordConflictStep) error {
	reply, err := s.consentRecordSyncClient.ResolveConsentRecordConflict(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}

// customerApprovalServerSummarizeSearchQueriesSteps are the ways to run
// acme.search.v1.SearchQueryQueryService/SummarizeSearchQueries, by name.
var customerApprovalServerSummarizeSearchQueriesSteps = map[string]func(*CustomerApprovalServer, context.Context, *searchv1.SummarizeSearchQueriesRequest) (*searchv1.SummarizeSearchQueriesResponse, error){
	"default": (*CustomerApprovalServer).summarizeSearchQueriesStep,
	"audited": (*CustomerApprovalServer).summarizeSearchQueriesAuditedStep,
}

func (s *CustomerApprovalServer) summarizeSearchQueriesStep(ctx context.Context, request *searchv1.SummarizeSearchQueriesRequest) (*searchv1.SummarizeSearchQueriesResponse, error) {
	return s.searchQueryQueryClient.SummarizeSearchQueries(ctx, request)
}

func (s *CustomerApprovalServer) summarizeSearchQueriesAuditedStep(ctx context.Context, request *searchv1.SummarizeSearchQueriesRequest) (*searchv1.SummarizeSearchQueriesResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.search.v1.SearchQueryQueryService/SummarizeSearchQueries")
	return s.searchQueryQueryClient.SummarizeSearchQueries(ctx, request)
}
