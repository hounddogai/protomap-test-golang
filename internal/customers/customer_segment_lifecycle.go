package customers

import (
	"context"
	"log/slog"
	"sync"

	boxofficev1 "github.com/hounddogai/protomap-test-golang/gen/acme/boxoffice/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	lineagev1 "github.com/hounddogai/protomap-test-golang/gen/acme/lineage/v1"
	modelregistryv1 "github.com/hounddogai/protomap-test-golang/gen/acme/modelregistry/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// CustomerSegmentLifecycleServer implements `acme.customers.v1.CustomerSegmentLifecycleService`.
// Moves a customer segment between the states of its lifecycle.
type CustomerSegmentLifecycleServer struct {
	customersv1.UnimplementedCustomerSegmentLifecycleServiceServer
	store                          *Store
	log                            *slog.Logger
	conn                           *grpc.ClientConn
	watchlistApprovalClient        fraudv1.WatchlistApprovalServiceClient
	clients                        *registry.Registry
	customerSegmentSyncClient      customersv1.CustomerSegmentSyncServiceClient
	customerSegmentImportClient    customersv1.CustomerSegmentImportServiceClient
	watchlistLifecycleClient       fraudv1.WatchlistLifecycleServiceClient
	householdReportingClient       customersv1.HouseholdReportingServiceClient
	boxOfficeSaleCalculationClient boxofficev1.BoxOfficeSaleCalculationServiceClient
	synonymValidationOnce          sync.Once
	synonymValidationCached        searchv1.SynonymValidationServiceClient
	lineageNodeQueryClient         lineagev1.LineageNodeQueryServiceClient
	registeredModelCalculationPort registeredModelCalculationPort
}

// NewCustomerSegmentLifecycleServer wires a CustomerSegmentLifecycleServer.
func NewCustomerSegmentLifecycleServer(deps platform.Deps) *CustomerSegmentLifecycleServer {
	return &CustomerSegmentLifecycleServer{
		store:                          NewStore(deps.DB),
		log:                            deps.Logger.With("service", "acme.customers.v1.CustomerSegmentLifecycleService"),
		conn:                           deps.Conn,
		watchlistApprovalClient:        fraudv1.NewWatchlistApprovalServiceClient(deps.Conn),
		clients:                        deps.Clients,
		customerSegmentSyncClient:      customersv1.NewCustomerSegmentSyncServiceClient(deps.Conn),
		customerSegmentImportClient:    customersv1.NewCustomerSegmentImportServiceClient(deps.Conn),
		watchlistLifecycleClient:       fraudv1.NewWatchlistLifecycleServiceClient(deps.Conn),
		householdReportingClient:       customersv1.NewHouseholdReportingServiceClient(deps.Conn),
		boxOfficeSaleCalculationClient: boxofficev1.NewBoxOfficeSaleCalculationServiceClient(deps.Conn),
		lineageNodeQueryClient:         lineagev1.NewLineageNodeQueryServiceClient(deps.Conn),
		registeredModelCalculationPort: newRegisteredModelCalculationPort(deps),
	}
}

// ActivateCustomerSegment activates a customer segment.
func (s *CustomerSegmentLifecycleServer) ActivateCustomerSegment(ctx context.Context, req *customersv1.ActivateCustomerSegmentRequest) (*customersv1.ActivateCustomerSegmentResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "customer_segment", req.GetCustomerSegmentId(), "activate_customer_segment"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "activate customer segment", "tenant", req.GetContext().GetTenantId())
	listApiCredentialVersionsRequest := &identityv1.ListApiCredentialVersionsRequest{
		Context: req.GetContext(),
		Filter:  &identityv1.ListApiCredentialVersionsRequest_Filter{CreatedAfter: req.GetDetails().GetEffectiveAt()},
	}
	listApiCredentialVersionsReply := new(identityv1.ListApiCredentialVersionsResponse)
	if err := grpcx.Invoke(ctx, s.conn, identityv1.ApiCredentialHistoryService_ListApiCredentialVersions_FullMethodName, listApiCredentialVersionsRequest, listApiCredentialVersionsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.ApiCredentialHistoryService/ListApiCredentialVersions")
	}
	return &customersv1.ActivateCustomerSegmentResponse{
		Result: &commonv1.OperationResult{
			Status:       listApiCredentialVersionsReply.GetResult().GetStatus(),
			ReferenceIds: listApiCredentialVersionsReply.GetResult().GetReferenceIds(),
		},
		CustomerSegment: &customersv1.CustomerSegment{CustomerSegmentId: req.GetCustomerSegmentId()},
		Outcome:         &customersv1.ActivateCustomerSegmentResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// SuspendCustomerSegment suspends a customer segment.
func (s *CustomerSegmentLifecycleServer) SuspendCustomerSegment(ctx context.Context, req *customersv1.SuspendCustomerSegmentRequest) (*customersv1.SuspendCustomerSegmentResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SuspendCustomerSegment", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "customer_segment", req.GetCustomerSegmentId(), "suspend_customer_segment"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "suspend customer segment", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetDetails()).(*customersv1.SuspendCustomerSegmentRequest_Details)
	commentWatchlistApprovalRequest := &fraudv1.CommentWatchlistApprovalRequest{
		Context: req.GetContext(),
		Watchlist: &fraudv1.Watchlist{
			Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
				Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
					Manager: &identityv1.UserRef{
						UserId: cloned.GetNewOwner().GetUserId(),
						Email:  req.GetDetails().GetNotifyContact().GetEmail(),
					},
				}},
			}},
		},
	}
	send := s.commentWatchlistApprovalSender(commentWatchlistApprovalRequest)
	commentWatchlistApprovalReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.WatchlistApprovalService/CommentWatchlistApproval")
	}
	return &customersv1.SuspendCustomerSegmentResponse{
		CustomerSegment: &customersv1.CustomerSegment{
			Owner: &identityv1.UserRef{
				DisplayName: commentWatchlistApprovalReply.GetWatchlist().GetManager().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				Etag: commentWatchlistApprovalReply.GetWatchlist().GetAudit().GetEtag(),
			},
			CustomerSegmentId: req.GetCustomerSegmentId(),
		},
		Outcome: &customersv1.SuspendCustomerSegmentResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ReinstateCustomerSegment reinstates a customer segment.
func (s *CustomerSegmentLifecycleServer) ReinstateCustomerSegment(ctx context.Context, req *customersv1.ReinstateCustomerSegmentRequest) (*customersv1.ReinstateCustomerSegmentResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "customer_segment", req.GetCustomerSegmentId(), "reinstate_customer_segment"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "reinstate customer segment", "tenant", req.GetContext().GetTenantId())
	customerSegmentSync, ok := s.clients.Get("acme.customers.v1.CustomerSegmentSyncService").(customersv1.CustomerSegmentSyncServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.customers.v1.CustomerSegmentSyncService")
	}
	encoded, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded customersv1.ReinstateCustomerSegmentRequest_Details
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	snapshot, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	pushCustomerSegmentRequest := &customersv1.PushCustomerSegmentRequest{
		Context: req.GetContext(),
		CustomerSegment: &customersv1.CustomerSegment{
			Owner: &identityv1.UserRef{
				UserId:      req.GetDetails().GetNewOwner().GetUserId(),
				DisplayName: (&decoded).GetNewOwner().GetDisplayName(),
				Email:       req.GetDetails().GetNewOwner().GetEmail(),
			},
		},
		Details: &customersv1.PushCustomerSegmentRequest_Details{Payload: snapshot},
	}
	pushCustomerSegmentReply, err := customerSegmentSync.PushCustomerSegment(ctx, pushCustomerSegmentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentSyncService/PushCustomerSegment")
	}
	getCustomerSegmentSyncStateRequest := newGetCustomerSegmentSyncStateRequest(withGetCustomerSegmentSyncStateContext(req.GetContext()), withGetCustomerSegmentSyncStateCustomerSegmentId(req.GetCustomerSegmentId()))
	getCustomerSegmentSyncStateReply, err := s.customerSegmentSyncClient.GetCustomerSegmentSyncState(ctx, getCustomerSegmentSyncStateRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentSyncService/GetCustomerSegmentSyncState")
	}
	roleHistory, ok := s.clients.Get("acme.identity.v1.RoleHistoryService").(identityv1.RoleHistoryServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.identity.v1.RoleHistoryService")
	}
	snapshot2, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	annotateRoleVersionRequest := &identityv1.AnnotateRoleVersionRequest{
		Context: req.GetContext(),
		Role: &identityv1.Role{
			Owner: &identityv1.UserRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
			Action: &identityv1.Role_Block{Block: &identityv1.RoleBlockAction{
				NotifyEmail: getCustomerSegmentSyncStateReply.GetCustomerSegment().GetBlock().GetNotifyEmail(),
			}},
			Audit: &commonv1.AuditInfo{
				CreatedAt: pushCustomerSegmentReply.GetCustomerSegment().GetAudit().GetCreatedAt(),
			},
		},
		Details: &identityv1.AnnotateRoleVersionRequest_Details{Snapshot: snapshot2},
	}
	annotateRoleVersionReply, err := roleHistory.AnnotateRoleVersion(ctx, annotateRoleVersionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.RoleHistoryService/AnnotateRoleVersion")
	}
	retryCustomerSegmentImportReply, err := s.customerSegmentImportClient.RetryCustomerSegmentImport(ctx, &customersv1.RetryCustomerSegmentImportRequest{
		Context: req.GetContext(),
		CustomerSegment: &customersv1.CustomerSegment{
			Action: &customersv1.CustomerSegment_Discount{Discount: &customersv1.CustomerSegmentDiscountAction{
				Percent: getCustomerSegmentSyncStateReply.GetCustomerSegment().GetDiscount().GetPercent(),
			}},
			Owner: &identityv1.UserRef{
				Email: getCustomerSegmentSyncStateReply.GetCustomerSegment().GetOwner().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{
				Etag:      getCustomerSegmentSyncStateReply.GetCustomerSegment().GetAudit().GetEtag(),
				UpdatedBy: pushCustomerSegmentReply.GetCustomerSegment().GetAudit().GetUpdatedBy(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentImportService/RetryCustomerSegmentImport")
	}
	return &customersv1.ReinstateCustomerSegmentResponse{
		Result: &commonv1.OperationResult{
			Status:  pushCustomerSegmentReply.GetOutcome().GetStatus(),
			Message: annotateRoleVersionReply.GetResult().GetMessage(),
		},
		CustomerSegment: &customersv1.CustomerSegment{
			CustomerSegmentId: pushCustomerSegmentReply.GetCustomerSegment().GetCustomerSegmentId(),
			Description:       getCustomerSegmentSyncStateReply.GetCustomerSegment().GetDescription(),
			Name:              getCustomerSegmentSyncStateReply.GetCustomerSegment().GetName(),
			Owner:             retryCustomerSegmentImportReply.GetCustomerSegment().GetOwner(),
		},
		Outcome: &customersv1.ReinstateCustomerSegmentResponse_Outcome{
			Status:      retryCustomerSegmentImportReply.GetOutcome().GetStatus(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
	}, nil
}

// TerminateCustomerSegment terminates a customer segment.
func (s *CustomerSegmentLifecycleServer) TerminateCustomerSegment(ctx context.Context, req *customersv1.TerminateCustomerSegmentRequest) (*customersv1.TerminateCustomerSegmentResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "customer_segment", req.GetCustomerSegmentId(), "terminate_customer_segment"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "terminate customer segment", "tenant", req.GetContext().GetTenantId())
	resumeFraudSignalWorkflowRequest := &fraudv1.ResumeFraudSignalWorkflowRequest{
		Context: req.GetContext(),
		Details: &fraudv1.ResumeFraudSignalWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				Phone:            req.GetDetails().GetNotifyContact().GetPhone(),
				PreferredChannel: req.GetDetails().GetNotifyContact().GetPreferredChannel(),
				Email:            req.GetDetails().GetNewOwner().GetEmail(),
				Name:             req.GetDetails().GetNotifyContact().GetName(),
			},
		},
	}
	resumeFraudSignalWorkflowReply := new(fraudv1.ResumeFraudSignalWorkflowResponse)
	if err := grpcx.Invoke(ctx, s.conn, fraudv1.FraudSignalWorkflowService_ResumeFraudSignalWorkflow_FullMethodName, resumeFraudSignalWorkflowRequest, resumeFraudSignalWorkflowReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.FraudSignalWorkflowService/ResumeFraudSignalWorkflow")
	}
	getCustomerSegmentImportStatusRequest := &customersv1.GetCustomerSegmentImportStatusRequest{
		Context:           req.GetContext(),
		CustomerSegmentId: req.GetCustomerSegmentId(),
	}
	getCustomerSegmentImportStatusReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*customersv1.GetCustomerSegmentImportStatusResponse, error) {
		return s.customerSegmentImportClient.GetCustomerSegmentImportStatus(ctx, getCustomerSegmentImportStatusRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentImportService/GetCustomerSegmentImportStatus")
	}
	scheduleWatchlistExpiryRequest := &fraudv1.ScheduleWatchlistExpiryRequest{
		Context: req.GetContext(),
		Watchlist: &fraudv1.Watchlist{
			Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
				Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
					Manager: &identityv1.UserRef{
						DisplayName: getCustomerSegmentImportStatusReply.GetCustomerSegment().GetOwner().GetDisplayName(),
					},
					Audit: &commonv1.AuditInfo{
						UpdatedAt: getCustomerSegmentImportStatusReply.GetCustomerSegment().GetAudit().GetUpdatedAt(),
					},
				}},
			}},
		},
	}
	scheduleWatchlistExpiryReply, err := grpcx.Call(ctx, s.watchlistLifecycleClient.ScheduleWatchlistExpiry, scheduleWatchlistExpiryRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.WatchlistLifecycleService/ScheduleWatchlistExpiry")
	}
	return &customersv1.TerminateCustomerSegmentResponse{
		Outcome: &customersv1.TerminateCustomerSegmentResponse_Outcome{
			Message:     resumeFraudSignalWorkflowReply.GetOutcome().GetMessage(),
			EffectiveAt: resumeFraudSignalWorkflowReply.GetOutcome().GetEffectiveAt(),
			NextSteps:   scheduleWatchlistExpiryReply.GetOutcome().GetNextSteps(),
			Status:      scheduleWatchlistExpiryReply.GetResult().GetStatus(),
		},
		CustomerSegment: &customersv1.CustomerSegment{
			Owner:             getCustomerSegmentImportStatusReply.GetCustomerSegment().GetOwner(),
			CustomerSegmentId: req.GetCustomerSegmentId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RenewCustomerSegment renews a customer segment.
func (s *CustomerSegmentLifecycleServer) RenewCustomerSegment(ctx context.Context, req *customersv1.RenewCustomerSegmentRequest) (*customersv1.RenewCustomerSegmentResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "customer_segment", req.GetCustomerSegmentId(), "renew_customer_segment"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "renew customer segment", "tenant", req.GetContext().GetTenantId())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	listHouseholdReportsRequest := &customersv1.ListHouseholdReportsRequest{
		Context: req.GetContext(),
		Filter: &customersv1.ListHouseholdReportsRequest_Filter{
			CreatedAfter: req.GetDetails().GetEffectiveAt(),
			OwnerId:      req.GetDetails().GetNewOwner().GetUserId(),
			Labels:       annotations,
		},
	}
	send := s.listHouseholdReportsSender(listHouseholdReportsRequest)
	listHouseholdReportsReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdReportingService/ListHouseholdReports")
	}
	return &customersv1.RenewCustomerSegmentResponse{
		Outcome: &customersv1.RenewCustomerSegmentResponse_Outcome{
			Message:     listHouseholdReportsReply.GetResult().GetMessage(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		CustomerSegment: &customersv1.CustomerSegment{CustomerSegmentId: req.GetCustomerSegmentId()},
		Result:          &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ExpireCustomerSegment expires a customer segment.
func (s *CustomerSegmentLifecycleServer) ExpireCustomerSegment(ctx context.Context, req *customersv1.ExpireCustomerSegmentRequest) (*customersv1.ExpireCustomerSegmentResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ExpireCustomerSegment", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "customer_segment", req.GetCustomerSegmentId(), "expire_customer_segment"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "expire customer segment", "tenant", req.GetContext().GetTenantId())
	delegateWatchlistApprovalReply, err := s.watchlistApprovalClient.DelegateWatchlistApproval(ctx, &fraudv1.DelegateWatchlistApprovalRequest{
		Context: req.GetContext(),
		Watchlist: &fraudv1.Watchlist{
			Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
				Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
					Manager: &identityv1.UserRef{
						DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
						Email:       req.GetDetails().GetNotifyContact().GetEmail(),
					},
				}},
			}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.WatchlistApprovalService/DelegateWatchlistApproval")
	}
	ctx = tenancy.With(ctx, "display_name", req.GetDetails().GetNewOwner().GetDisplayName())
	recalculateBoxOfficeSalesRequest := &boxofficev1.RecalculateBoxOfficeSalesRequest{
		Context: req.GetContext(),
		BoxOfficeSale: &boxofficev1.BoxOfficeSale{
			Counterparty: &boxofficev1.CashDrawerRef{DisplayName: tenancy.Value(ctx, "display_name")},
			Audit: &commonv1.AuditInfo{
				CreatedBy: delegateWatchlistApprovalReply.GetWatchlist().GetAudit().GetCreatedBy(),
			},
		},
	}
	recalculateBoxOfficeSalesReply, err := grpcx.Call(ctx, s.boxOfficeSaleCalculationClient.RecalculateBoxOfficeSales, recalculateBoxOfficeSalesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.boxoffice.v1.BoxOfficeSaleCalculationService/RecalculateBoxOfficeSales")
	}
	previewSynonymRequest := &searchv1.PreviewSynonymRequest{
		Context: req.GetContext(),
		Synonym: &searchv1.Synonym{
			Owner: &identityv1.UserRef{
				UserId:      delegateWatchlistApprovalReply.GetWatchlist().GetManager().GetUserId(),
				Email:       req.GetDetails().GetNewOwner().GetEmail(),
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
		},
	}
	previewSynonymReply, err := s.synonymValidationLazy().PreviewSynonym(ctx, previewSynonymRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymValidationService/PreviewSynonym")
	}
	return &customersv1.ExpireCustomerSegmentResponse{
		Result: &commonv1.OperationResult{
			Status:  delegateWatchlistApprovalReply.GetResult().GetStatus(),
			Message: previewSynonymReply.GetResult().GetMessage(),
		},
		CustomerSegment: &customersv1.CustomerSegment{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: recalculateBoxOfficeSalesReply.GetBoxOfficeSale().GetAudit().GetUpdatedBy(),
			},
			Owner: &identityv1.UserRef{
				UserId: previewSynonymReply.GetSynonym().GetOwner().GetUserId(),
			},
			CustomerSegmentId: req.GetCustomerSegmentId(),
		},
		Outcome: &customersv1.ExpireCustomerSegmentResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// GetCustomerSegmentLifecycle returns a customer segment.
func (s *CustomerSegmentLifecycleServer) GetCustomerSegmentLifecycle(ctx context.Context, req *customersv1.GetCustomerSegmentLifecycleRequest) (*customersv1.GetCustomerSegmentLifecycleResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "customer_segment", req.GetCustomerSegmentId(), "get_customer_segment_lifecycle"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get customer segment lifecycle", "tenant", req.GetContext().GetTenantId())
	return &customersv1.GetCustomerSegmentLifecycleResponse{
		CustomerSegment: &customersv1.CustomerSegment{CustomerSegmentId: req.GetCustomerSegmentId()},
		Result:          &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ScheduleCustomerSegmentExpiry schedules a customer segment.
func (s *CustomerSegmentLifecycleServer) ScheduleCustomerSegmentExpiry(ctx context.Context, req *customersv1.ScheduleCustomerSegmentExpiryRequest) (*customersv1.ScheduleCustomerSegmentExpiryResponse, error) {
	if err := s.store.SaveCustomerSegment(ctx, req.GetContext().GetTenantId(), req.GetCustomerSegment()); err != nil {
		return nil, grpcx.Wrap(err, "save customer segment")
	}
	s.log.InfoContext(ctx, "schedule customer segment expiry", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCustomerSegment().GetAudit().GetEtag())
	resolveLineageNodesRequest := buildResolveLineageNodesRequestForScheduleCustomerSegmentExpiry(req)
	resolveLineageNodesReply, err := s.lineageNodeQueryClient.ResolveLineageNodes(ctx, resolveLineageNodesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.lineage.v1.LineageNodeQueryService/ResolveLineageNodes")
	}
	ctx = tenancy.With(ctx, "email", req.GetCustomerSegment().GetOwner().GetEmail())
	estimateRegisteredModelRequest := &modelregistryv1.EstimateRegisteredModelRequest{
		Context: req.GetContext(),
		RegisteredModel: &modelregistryv1.RegisteredModel{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: req.GetCustomerSegment().GetAudit().GetUpdatedBy(),
				Etag:      req.GetCustomerSegment().GetAudit().GetEtag(),
				UpdatedAt: req.GetCustomerSegment().GetAudit().GetUpdatedAt(),
			},
			Owner: &identityv1.UserRef{Email: tenancy.Value(ctx, "email")},
		},
	}
	estimateRegisteredModelReply, err := s.registeredModelCalculationPort.EstimateRegisteredModel(ctx, estimateRegisteredModelRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.modelregistry.v1.RegisteredModelCalculationService/EstimateRegisteredModel")
	}
	return &customersv1.ScheduleCustomerSegmentExpiryResponse{
		Result: &commonv1.OperationResult{
			Message: resolveLineageNodesReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Outcome: &customersv1.ScheduleCustomerSegmentExpiryResponse_Outcome{Message: resolveLineageNodesReply.GetResult().GetMessage()},
		CustomerSegment: &customersv1.CustomerSegment{
			Name:              estimateRegisteredModelReply.GetRegisteredModel().GetName(),
			Description:       estimateRegisteredModelReply.GetRegisteredModel().GetComponents()["primary"].GetDescription(),
			CustomerSegmentId: req.GetCustomerSegmentId(),
			Version:           req.GetCustomerSegment().GetVersion(),
		},
	}, nil
}

// commentWatchlistApprovalSender returns a function that sends one request to
// acme.fraud.v1.WatchlistApprovalService/CommentWatchlistApproval.
func (s *CustomerSegmentLifecycleServer) commentWatchlistApprovalSender(request *fraudv1.CommentWatchlistApprovalRequest) func(context.Context) (*fraudv1.CommentWatchlistApprovalResponse, error) {
	return func(ctx context.Context) (*fraudv1.CommentWatchlistApprovalResponse, error) {
		return s.watchlistApprovalClient.CommentWatchlistApproval(ctx, request)
	}
}

// listHouseholdReportsSender returns a function that sends one request to
// acme.customers.v1.HouseholdReportingService/ListHouseholdReports.
func (s *CustomerSegmentLifecycleServer) listHouseholdReportsSender(request *customersv1.ListHouseholdReportsRequest) func(context.Context) (*customersv1.ListHouseholdReportsResponse, error) {
	return func(ctx context.Context) (*customersv1.ListHouseholdReportsResponse, error) {
		return s.householdReportingClient.ListHouseholdReports(ctx, request)
	}
}

// synonymValidationLazy dials acme.search.v1.SynonymValidationService the first time a handler needs it.
func (s *CustomerSegmentLifecycleServer) synonymValidationLazy() searchv1.SynonymValidationServiceClient {
	s.synonymValidationOnce.Do(func() { s.synonymValidationCached = searchv1.NewSynonymValidationServiceClient(s.conn) })
	return s.synonymValidationCached
}
