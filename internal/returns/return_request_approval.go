package returns

import (
	"context"
	"errors"
	"io"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// ReturnRequestApprovalServer implements `acme.returns.v1.ReturnRequestApprovalService`.
// Routes return requests through approval, with escalation and delegation.
type ReturnRequestApprovalServer struct {
	returnsv1.UnimplementedReturnRequestApprovalServiceServer
	store                          *Store
	log                            *slog.Logger
	customerQueryClient            customersv1.CustomerQueryServiceClient
	meteredTenantAdmin             meteredTenantAdmin
	synonymSyncClient              searchv1.SynonymSyncServiceClient
	placementWorkflowClient        merchandisingv1.PlacementWorkflowServiceClient
	userClient                     identityv1.UserServiceClient
	tenantAdminClient              identityv1.TenantAdminServiceClient
	conn                           *grpc.ClientConn
	roleNotificationClient         identityv1.RoleNotificationServiceClient
	searchQueryAdminClient         searchv1.SearchQueryAdminServiceClient
	ledgerAccountClient            ledgerv1.LedgerAccountServiceClient
	returnRequestPort              returnRequestPort
	sessionHistoryClient           identityv1.SessionHistoryServiceClient
	returnRequestCalculationClient returnsv1.ReturnRequestCalculationServiceClient
	inspectTenantFn                func(context.Context, *identityv1.InspectTenantRequest, ...grpc.CallOption) (*identityv1.InspectTenantResponse, error)
	returnLabelValidationClient    returnsv1.ReturnLabelValidationServiceClient
}

// NewReturnRequestApprovalServer wires a ReturnRequestApprovalServer.
func NewReturnRequestApprovalServer(deps platform.Deps) *ReturnRequestApprovalServer {
	tenantAdmin := identityv1.NewTenantAdminServiceClient(deps.Conn)
	return &ReturnRequestApprovalServer{
		store:                          NewStore(deps.DB),
		log:                            deps.Logger.With("service", "acme.returns.v1.ReturnRequestApprovalService"),
		customerQueryClient:            customersv1.NewCustomerQueryServiceClient(deps.Conn),
		meteredTenantAdmin:             meteredTenantAdmin{TenantAdminServiceClient: identityv1.NewTenantAdminServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		synonymSyncClient:              searchv1.NewSynonymSyncServiceClient(deps.Conn),
		placementWorkflowClient:        merchandisingv1.NewPlacementWorkflowServiceClient(deps.Conn),
		userClient:                     identityv1.NewUserServiceClient(deps.Conn),
		tenantAdminClient:              identityv1.NewTenantAdminServiceClient(deps.Conn),
		conn:                           deps.Conn,
		roleNotificationClient:         identityv1.NewRoleNotificationServiceClient(deps.Conn),
		searchQueryAdminClient:         searchv1.NewSearchQueryAdminServiceClient(deps.Conn),
		ledgerAccountClient:            ledgerv1.NewLedgerAccountServiceClient(deps.Conn),
		returnRequestPort:              newReturnRequestPort(deps),
		sessionHistoryClient:           identityv1.NewSessionHistoryServiceClient(deps.Conn),
		returnRequestCalculationClient: returnsv1.NewReturnRequestCalculationServiceClient(deps.Conn),
		inspectTenantFn:                tenantAdmin.InspectTenant,
		returnLabelValidationClient:    returnsv1.NewReturnLabelValidationServiceClient(deps.Conn),
	}
}

// SubmitReturnRequest submits a return request.
func (s *ReturnRequestApprovalServer) SubmitReturnRequest(ctx context.Context, req *returnsv1.SubmitReturnRequestRequest) (*returnsv1.SubmitReturnRequestResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveReturnRequest(ctx, req.GetContext().GetTenantId(), req.GetReturnRequest()); err != nil {
		return nil, grpcx.Wrap(err, "save return request")
	}
	s.log.InfoContext(ctx, "submit return request", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReturnRequest().GetAudit().GetEtag())
	summarizeCustomersRequest := &customersv1.SummarizeCustomersRequest{
		Context:    req.GetContext(),
		CustomerId: req.GetReturnRequest().GetCounterparty().GetCustomerId(),
		Reason:     req.GetReason(),
	}
	summarizeCustomersReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*customersv1.SummarizeCustomersResponse, error) {
		return s.customerQueryClient.SummarizeCustomers(ctx, summarizeCustomersRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerQueryService/SummarizeCustomers")
	}
	return &returnsv1.SubmitReturnRequestResponse{
		Result: &commonv1.OperationResult{
			Status:       summarizeCustomersReply.GetResult().GetStatus(),
			ReferenceIds: summarizeCustomersReply.GetResult().GetReferenceIds(),
		},
		ReturnRequest: &returnsv1.ReturnRequest{
			ReturnRequestId: req.GetReturnRequestId(),
			Notes:           req.GetReturnRequest().GetNotes(),
		},
	}, nil
}

// ApproveReturnRequest approves a return request.
func (s *ReturnRequestApprovalServer) ApproveReturnRequest(ctx context.Context, req *returnsv1.ApproveReturnRequestRequest) (*returnsv1.ApproveReturnRequestResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveReturnRequest(ctx, req.GetContext().GetTenantId(), req.GetReturnRequest()); err != nil {
		return nil, grpcx.Wrap(err, "save return request")
	}
	s.log.InfoContext(ctx, "approve return request", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReturnRequest().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "display_name", req.GetReturnRequest().GetCounterparty().GetDisplayName())
	cloned := proto.Clone(req.GetReturnRequest()).(*returnsv1.ReturnRequest)
	reassignTenantRequest := &identityv1.ReassignTenantRequest{
		Context:  req.GetContext(),
		TenantId: req.GetContext().GetTenantId(),
		Details: &identityv1.ReassignTenantRequest_Details{
			Assignee: &identityv1.UserRef{
				DisplayName: tenancy.Value(ctx, "display_name"),
				UserId:      req.GetDetails().GetApprover().GetUserId(),
				Email:       cloned.GetCounterparty().GetEmail(),
			},
		},
		Reason: req.GetReason(),
	}
	reassignTenantReply, err := s.meteredTenantAdmin.ReassignTenant(ctx, reassignTenantRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantAdminService/ReassignTenant")
	}
	snapshot, err := protojson.Marshal(req.GetReturnRequest())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	pushSynonymRequest := &searchv1.PushSynonymRequest{
		Context: req.GetContext(),
		Synonym: &searchv1.Synonym{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: reassignTenantReply.GetTenant().GetAudit().GetUpdatedAt(),
				CreatedBy: req.GetReturnRequest().GetAudit().GetCreatedBy(),
			},
			Owner: &identityv1.UserRef{Email: req.GetReturnRequest().GetCounterparty().GetEmail()},
		},
		Details: &searchv1.PushSynonymRequest_Details{Payload: snapshot},
	}
	pushSynonymReply, err := searchv1.SynonymSyncServiceClient.PushSynonym(s.synonymSyncClient, ctx, pushSynonymRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymSyncService/PushSynonym")
	}
	return &returnsv1.ApproveReturnRequestResponse{
		Result: &commonv1.OperationResult{
			Message:      reassignTenantReply.GetResult().GetMessage(),
			ReferenceIds: pushSynonymReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		ReturnRequest: &returnsv1.ReturnRequest{
			BillingAddress: &commonv1.PostalAddress{
				Recipient: reassignTenantReply.GetTenant().GetLocation().GetRecipient(),
			},
			ReturnRequestId: req.GetReturnRequestId(),
			Total:           req.GetReturnRequest().GetTotal(),
		},
	}, nil
}

// RejectReturnRequest rejects a return request.
func (s *ReturnRequestApprovalServer) RejectReturnRequest(ctx context.Context, req *returnsv1.RejectReturnRequestRequest) (*returnsv1.RejectReturnRequestResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RejectReturnRequest", "error", err)
		return nil, err
	}
	if err := s.store.SaveReturnRequest(ctx, req.GetContext().GetTenantId(), req.GetReturnRequest()); err != nil {
		return nil, grpcx.Wrap(err, "save return request")
	}
	s.log.InfoContext(ctx, "reject return request", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReturnRequest().GetAudit().GetEtag())
	listPlacementWorkflowsRequest := &merchandisingv1.ListPlacementWorkflowsRequest{
		Context: req.GetContext(),
		Filter: &merchandisingv1.ListPlacementWorkflowsRequest_Filter{
			Region: req.GetReturnRequest().GetBillingAddress().GetRegion(),
		},
	}
	listPlacementWorkflowsReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*merchandisingv1.ListPlacementWorkflowsResponse, error) {
		return s.placementWorkflowClient.ListPlacementWorkflows(ctx, listPlacementWorkflowsRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.PlacementWorkflowService/ListPlacementWorkflows")
	}
	return &returnsv1.RejectReturnRequestResponse{
		Result: &commonv1.OperationResult{
			Message: listPlacementWorkflowsReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		ReturnRequest: &returnsv1.ReturnRequest{
			ReturnRequestId: req.GetReturnRequestId(),
			Metadata:        req.GetReturnRequest().GetMetadata(),
		},
	}, nil
}

// EscalateReturnRequest escalates a return request.
func (s *ReturnRequestApprovalServer) EscalateReturnRequest(ctx context.Context, req *returnsv1.EscalateReturnRequestRequest) (*returnsv1.EscalateReturnRequestResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveReturnRequest(ctx, req.GetContext().GetTenantId(), req.GetReturnRequest()); err != nil {
		return nil, grpcx.Wrap(err, "save return request")
	}
	s.log.InfoContext(ctx, "escalate return request", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReturnRequest().GetAudit().GetEtag())
	watchUsersStream, err := s.userClient.WatchUsers(ctx, &identityv1.WatchUsersRequest{
		Context:    req.GetContext(),
		Subscriber: &commonv1.ContactInfo{Email: req.GetReturnRequest().GetCounterparty().GetEmail()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserService/WatchUsers")
	}
	for {
		received, err := watchUsersStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.identity.v1.UserService/WatchUsers")
		}
		_ = received
	}
	var holder struct{ Email string }
	if record := req.GetDetails().GetApprover(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Email"); err != nil {
			s.log.WarnContext(ctx, "copy email", "error", err)
		}
	}
	step := &returnRequestApprovalServerEscalateReturnRequestReassignTenantStep{request: &identityv1.ReassignTenantRequest{
		Context:  req.GetContext(),
		TenantId: req.GetContext().GetTenantId(),
		Details: &identityv1.ReassignTenantRequest_Details{
			Assignee: &identityv1.UserRef{
				UserId: req.GetDetails().GetApprover().GetUserId(),
				Email:  holder.Email,
			},
		},
	}}
	if err := s.runEscalateReturnRequestReassignTenant(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantAdminService/ReassignTenant")
	}
	reassignTenantReply := step.reply
	return &returnsv1.EscalateReturnRequestResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: reassignTenantReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		ReturnRequest: &returnsv1.ReturnRequest{
			ReturnRequestId: req.GetReturnRequestId(),
			Total:           req.GetReturnRequest().GetTotal(),
		},
	}, nil
}

// ListPendingReturnRequests lists return requests a page at a time.
func (s *ReturnRequestApprovalServer) ListPendingReturnRequests(ctx context.Context, req *returnsv1.ListPendingReturnRequestsRequest) (*returnsv1.ListPendingReturnRequestsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "list pending return requests", "tenant", req.GetContext().GetTenantId())
	validateCustomerSegmentRequest := &customersv1.ValidateCustomerSegmentRequest{
		Context: req.GetContext(),
		Details: &customersv1.ValidateCustomerSegmentRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
	}
	validateCustomerSegmentReply := new(customersv1.ValidateCustomerSegmentResponse)
	if err := grpcx.Invoke(ctx, s.conn, customersv1.CustomerSegmentValidationService_ValidateCustomerSegment_FullMethodName, validateCustomerSegmentRequest, validateCustomerSegmentReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentValidationService/ValidateCustomerSegment")
	}
	return &returnsv1.ListPendingReturnRequestsResponse{
		ReturnRequests: []*returnsv1.ReturnRequest{&returnsv1.ReturnRequest{
			Counterparty: &customersv1.CustomerRef{
				Email: validateCustomerSegmentReply.GetCustomerSegment().GetOwner().GetEmail(),
			},
		}},
		Result: &commonv1.OperationResult{
			ReferenceIds: validateCustomerSegmentReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// DelegateReturnRequestApproval delegates a return request.
func (s *ReturnRequestApprovalServer) DelegateReturnRequestApproval(ctx context.Context, req *returnsv1.DelegateReturnRequestApprovalRequest) (*returnsv1.DelegateReturnRequestApprovalResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveReturnRequest(ctx, req.GetContext().GetTenantId(), req.GetReturnRequest()); err != nil {
		return nil, grpcx.Wrap(err, "save return request")
	}
	s.log.InfoContext(ctx, "delegate return request approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReturnRequest().GetAudit().GetEtag())
	packed, err := anypb.New(req.GetReturnRequest())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	notifyRoleWatchersRequest := &identityv1.NotifyRoleWatchersRequest{
		Context: req.GetContext(),
		Details: &identityv1.NotifyRoleWatchersRequest_Details{
			Recipient: &commonv1.ContactInfo{Email: req.GetReturnRequest().GetCounterparty().GetEmail()},
			Payload:   packed,
		},
	}
	send := s.notifyRoleWatchersSender(notifyRoleWatchersRequest)
	notifyRoleWatchersReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.RoleNotificationService/NotifyRoleWatchers")
	}
	snapshot, err := protojson.Marshal(req.GetReturnRequest())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	repairSearchQueryRequest := &searchv1.RepairSearchQueryRequest{}
	repairSearchQueryRequest.Context = req.GetContext()
	repairSearchQueryRequest.SearchQuery = &searchv1.SearchQuery{
		Subject: &searchv1.SearchIndexRef{
			DisplayName: notifyRoleWatchersReply.GetRole().GetOwner().GetDisplayName(),
		},
		CorrelationId: notifyRoleWatchersReply.GetResult().GetCorrelationId(),
	}
	repairSearchQueryRequest.Reason = notifyRoleWatchersReply.GetRole().GetBlock().GetReason()
	repairSearchQueryRequest.Details = &searchv1.RepairSearchQueryRequest_Details{Payload: snapshot}
	repairSearchQueryReply, err := s.searchQueryAdminClient.RepairSearchQuery(ctx, repairSearchQueryRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryAdminService/RepairSearchQuery")
	}
	createLedgerAccountRequest := &ledgerv1.CreateLedgerAccountRequest{
		Context: req.GetContext(),
		LedgerAccount: &ledgerv1.LedgerAccount{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: notifyRoleWatchersReply.GetRole().GetAudit().GetUpdatedAt(),
			},
			Holder: &ledgerv1.PostingBatchRef{
				DisplayName: notifyRoleWatchersReply.GetRole().GetOwner().GetDisplayName(),
			},
		},
	}
	createLedgerAccountReply, err := grpcx.Call(ctx, s.ledgerAccountClient.CreateLedgerAccount, createLedgerAccountRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountService/CreateLedgerAccount")
	}
	updateReturnRequestRequest := &returnsv1.UpdateReturnRequestRequest{
		Context: req.GetContext(),
		ReturnRequest: &returnsv1.ReturnRequest{
			Lines: []*returnsv1.ReturnRequestLine{&returnsv1.ReturnRequestLine{
				Tax: &returnsv1.ReturnRequestLineTax{
					Amount: notifyRoleWatchersReply.GetRole().GetDiscount().GetAmount(),
				},
			}},
			Audit: &commonv1.AuditInfo{
				CreatedBy: notifyRoleWatchersReply.GetRole().GetAudit().GetCreatedBy(),
			},
		},
	}
	updateReturnRequestReply, err := s.returnRequestPort.UpdateReturnRequest(ctx, updateReturnRequestRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnRequestService/UpdateReturnRequest")
	}
	_ = createLedgerAccountReply
	return &returnsv1.DelegateReturnRequestApprovalResponse{
		Outcome: &returnsv1.DelegateReturnRequestApprovalResponse_Outcome{
			Status:      notifyRoleWatchersReply.GetOutcome().GetStatus(),
			Message:     repairSearchQueryReply.GetResult().GetMessage(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: repairSearchQueryReply.GetSearchQuery().GetCorrelationId(),
			Status:        "ok",
		},
		ReturnRequest: &returnsv1.ReturnRequest{
			Counterparty: &customersv1.CustomerRef{
				DisplayName: updateReturnRequestReply.GetReturnRequest().GetCounterparty().GetDisplayName(),
			},
			ReturnRequestId: req.GetReturnRequestId(),
		},
	}, nil
}

// RecallReturnRequest recalls a return request.
func (s *ReturnRequestApprovalServer) RecallReturnRequest(ctx context.Context, req *returnsv1.RecallReturnRequestRequest) (*returnsv1.RecallReturnRequestResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveReturnRequest(ctx, req.GetContext().GetTenantId(), req.GetReturnRequest()); err != nil {
		return nil, grpcx.Wrap(err, "save return request")
	}
	s.log.InfoContext(ctx, "recall return request", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReturnRequest().GetAudit().GetEtag())
	source := req.GetReturnRequest().GetBillingAddress().ProtoReflect()
	region := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("region"))).String()
	listSessionVersionsRequest := &identityv1.ListSessionVersionsRequest{}
	listSessionVersionsRequest.Context = req.GetContext()
	listSessionVersionsRequest.Filter = &identityv1.ListSessionVersionsRequest_Filter{
		Region:       region,
		CreatedAfter: req.GetReturnRequest().GetAudit().GetUpdatedAt(),
		OwnerId:      req.GetReturnRequest().GetCounterparty().GetCustomerId(),
		Labels:       req.GetReturnRequest().GetMetadata(),
	}
	listSessionVersionsReply, err := s.sessionHistoryClient.ListSessionVersions(ctx, listSessionVersionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionHistoryService/ListSessionVersions")
	}
	explainReturnRequestCalculationReply, err := s.returnRequestCalculationClient.ExplainReturnRequestCalculation(ctx, &returnsv1.ExplainReturnRequestCalculationRequest{
		Context: req.GetContext(),
		ReturnRequest: &returnsv1.ReturnRequest{
			BillingAddress: &commonv1.PostalAddress{
				CountryCode: req.GetReturnRequest().GetBillingAddress().GetCountryCode(),
			},
			Audit: &commonv1.AuditInfo{Etag: req.GetReturnRequest().GetAudit().GetEtag()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnRequestCalculationService/ExplainReturnRequestCalculation")
	}
	return &returnsv1.RecallReturnRequestResponse{
		Outcome: &returnsv1.RecallReturnRequestResponse_Outcome{
			Status:      listSessionVersionsReply.GetResult().GetStatus(),
			ReferenceId: explainReturnRequestCalculationReply.GetOutcome().GetReferenceId(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: listSessionVersionsReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		ReturnRequest: &returnsv1.ReturnRequest{
			Audit: &commonv1.AuditInfo{
				CreatedBy: explainReturnRequestCalculationReply.GetReturnRequest().GetAudit().GetCreatedBy(),
			},
			ReturnRequestId: req.GetReturnRequestId(),
			Metadata:        req.GetReturnRequest().GetMetadata(),
		},
	}, nil
}

// GetReturnRequestApprovalStatus returns a return request.
func (s *ReturnRequestApprovalServer) GetReturnRequestApprovalStatus(ctx context.Context, req *returnsv1.GetReturnRequestApprovalStatusRequest) (*returnsv1.GetReturnRequestApprovalStatusResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "return_request", req.GetReturnRequestId(), "get_return_request_approval_status"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get return request approval status", "tenant", req.GetContext().GetTenantId())
	quoteConsentRecordRequest := &customersv1.QuoteConsentRecordRequest{
		Context:       req.GetContext(),
		ConsentRecord: &customersv1.ConsentRecord{CorrelationId: req.GetReturnRequestId()},
	}
	quoteConsentRecordReply := new(customersv1.QuoteConsentRecordResponse)
	if err := grpc.Invoke(ctx, "/acme.customers.v1.ConsentRecordCalculationService/QuoteConsentRecord", quoteConsentRecordRequest, quoteConsentRecordReply, s.conn); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordCalculationService/QuoteConsentRecord")
	}
	inspectTenantRequest := &identityv1.InspectTenantRequest{
		Context:  req.GetContext(),
		TenantId: req.GetContext().GetTenantId(),
	}
	inspectTenantReply, err := s.inspectTenantFn(ctx, inspectTenantRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantAdminService/InspectTenant")
	}
	return &returnsv1.GetReturnRequestApprovalStatusResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: quoteConsentRecordReply.GetResult().GetCorrelationId(),
			ReferenceIds:  inspectTenantReply.GetResult().GetReferenceIds(),
			Status:        "ok",
		},
		ReturnRequest: &returnsv1.ReturnRequest{
			BillingAddress: &commonv1.PostalAddress{
				CountryCode: inspectTenantReply.GetTenant().GetLocation().GetCountryCode(),
			},
			ReturnRequestId: req.GetReturnRequestId(),
		},
	}, nil
}

// CommentReturnRequestApproval comments a return request.
func (s *ReturnRequestApprovalServer) CommentReturnRequestApproval(ctx context.Context, req *returnsv1.CommentReturnRequestApprovalRequest) (*returnsv1.CommentReturnRequestApprovalResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CommentReturnRequestApproval", "error", err)
		return nil, err
	}
	if err := s.store.SaveReturnRequest(ctx, req.GetContext().GetTenantId(), req.GetReturnRequest()); err != nil {
		return nil, grpcx.Wrap(err, "save return request")
	}
	s.log.InfoContext(ctx, "comment return request approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReturnRequest().GetAudit().GetEtag())
	encoded, err := protojson.Marshal(req.GetReturnRequest())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded returnsv1.ReturnRequest
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	createCustomerRequest := &customersv1.CreateCustomerRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			RelationshipManager: &identityv1.UserRef{
				DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
				Email:       req.GetReturnRequest().GetCounterparty().GetEmail(),
			},
			PrimaryAddress: &commonv1.PostalAddress{
				Lines:    (&decoded).GetBillingAddress().GetLines(),
				Locality: req.GetReturnRequest().GetBillingAddress().GetLocality(),
			},
		},
	}
	createCustomerReply, err := createCustomerCall(ctx, createCustomerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerService/CreateCustomer")
	}
	browseCustomersRequest := newBrowseCustomersRequest(withBrowseCustomersContext(req.GetContext()), withBrowseCustomersFilter(&customersv1.BrowseCustomersRequest_Filter{
		Region:       createCustomerReply.GetCustomer().GetPrimaryAddress().GetRegion(),
		CreatedAfter: createCustomerReply.GetCustomer().GetAudit().GetCreatedAt(),
	}))
	browseCustomersReply, err := s.customerQueryClient.BrowseCustomers(ctx, browseCustomersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerQueryService/BrowseCustomers")
	}
	explainReturnLabelDecisionReply, err := s.returnLabelValidationClient.ExplainReturnLabelDecision(ctx, &returnsv1.ExplainReturnLabelDecisionRequest{
		Context: req.GetContext(),
		Details: &returnsv1.ExplainReturnLabelDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{
				Phone: createCustomerReply.GetCustomer().GetContact().GetPhone(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnLabelValidationService/ExplainReturnLabelDecision")
	}
	return &returnsv1.CommentReturnRequestApprovalResponse{
		ReturnRequest: &returnsv1.ReturnRequest{
			Audit: &commonv1.AuditInfo{
				Etag:      createCustomerReply.GetCustomer().GetAudit().GetEtag(),
				UpdatedAt: createCustomerReply.GetCustomer().GetAudit().GetUpdatedAt(),
			},
			IssuedAt:        explainReturnLabelDecisionReply.GetReturnLabel().GetIssuedAt(),
			ReturnRequestId: req.GetReturnRequestId(),
			Total:           req.GetReturnRequest().GetTotal(),
		},
		Result: &commonv1.OperationResult{
			Details: browseCustomersReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}

// returnRequestApprovalServerEscalateReturnRequestReassignTenantStep carries the request of one step of
// EscalateReturnRequest and, once it ran, its reply.
type returnRequestApprovalServerEscalateReturnRequestReassignTenantStep struct {
	request *identityv1.ReassignTenantRequest
	reply   *identityv1.ReassignTenantResponse
}

func (s *ReturnRequestApprovalServer) runEscalateReturnRequestReassignTenant(ctx context.Context, step *returnRequestApprovalServerEscalateReturnRequestReassignTenantStep) error {
	reply, err := s.tenantAdminClient.ReassignTenant(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}

// notifyRoleWatchersSender returns a function that sends one request to
// acme.identity.v1.RoleNotificationService/NotifyRoleWatchers.
func (s *ReturnRequestApprovalServer) notifyRoleWatchersSender(request *identityv1.NotifyRoleWatchersRequest) func(context.Context) (*identityv1.NotifyRoleWatchersResponse, error) {
	return func(ctx context.Context) (*identityv1.NotifyRoleWatchersResponse, error) {
		return s.roleNotificationClient.NotifyRoleWatchers(ctx, request)
	}
}
